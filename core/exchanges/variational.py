import logging
import math
import re

import httpx

logger = logging.getLogger(__name__)

# Публичный API (статистика, без авторизации)
VARIATIONAL_META_BASE = "https://omni-client-api.prod.ap-northeast-1.variational.io"
# Приватный API (ордера, позиции — требует vr-token)
VARIATIONAL_API_BASE = "https://omni.variational.io/api"

MAX_SLIPPAGE = 0.005  # 0.5%
# Variational принимает только этот интервал во всех инструментах
# (metadata/stats возвращает 14400/28800, но quotes/indicative их не принимает)
FUNDING_INTERVAL_S = 3600


def _make_chrome_session():
    """
    Создаёт HTTP-сессию с TLS-отпечатком Chrome.
    curl-cffi имитирует браузер на уровне TLS — Cloudflare не отличает от реального Chrome.
    """
    try:
        from curl_cffi.requests import AsyncSession
        return AsyncSession(impersonate="chrome110")
    except ImportError:
        raise RuntimeError("curl-cffi не установлен: pip install curl-cffi")


class VariationalExecutor:
    """
    Клиент для торговли на Variational (peer-to-peer perp DEX).

    Авторизация через vr-token — сессионный токен из браузера:
      1. Войди на omni.variational.io
      2. F12 → Application → Cookies → скопируй значение vr-token
    Помести в .env: VARIATIONAL_TOKEN=... VARIATIONAL_WALLET=0x...

    Схема маркет-ордера (двухшаговая RFQ):
      1. POST /api/quotes/indicative  { instrument, qty }  → quote_id
      2. POST /api/orders/new/market  { quote_id, side, max_slippage, is_reduce_only }
    """

    def __init__(self, vr_token: str, wallet_address: str, cf_clearance: str = ""):
        self._token = vr_token
        self._wallet = wallet_address.lower()
        self._cf_clearance = cf_clearance  # оставлен для совместимости
        self._assets: dict = {}  # symbol → {funding_interval_s, ...}

    def _headers(self) -> dict:
        return {
            "vr-connected-address": self._wallet,
            "origin": "https://omni.variational.io",
            "referer": "https://omni.variational.io/",
            "content-type": "application/json",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        }

    def _cookies(self) -> dict:
        cookies = {"vr-token": self._token}
        if self._cf_clearance:
            cookies["cf_clearance"] = self._cf_clearance
        return cookies

    async def _ensure_assets(self):
        """Загружает список доступных тикеров."""
        if self._assets:
            return
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{VARIATIONAL_META_BASE}/metadata/stats",
                headers=self._headers(),
                cookies=self._cookies(),
            )
            data = resp.json()
        for item in data.get("listings", []):
            ticker = (item.get("ticker") or "").upper()
            if ticker:
                self._assets[ticker] = {}
        logger.info(f"Variational: загружено {len(self._assets)} активов")

    async def _get_mark_price(self, symbol: str) -> float:
        """Возвращает текущую mark price из публичной статистики."""
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{VARIATIONAL_META_BASE}/metadata/stats",
                headers=self._headers(),
                cookies=self._cookies(),
            )
            data = resp.json()
        for item in data.get("listings", []):
            if (item.get("ticker") or "").upper() == symbol.upper():
                price = float(item.get("mark_price") or 0)
                if price:
                    return price
        raise ValueError(f"Variational: mark price для {symbol} не найдена")

    async def _get_indicative_quote(self, symbol: str, qty: float) -> dict:
        """
        Шаг 1: запрашивает indicative quote.
        Возвращает dict с quote_id, mark_price, ask, bid и др.
        """
        payload = {
            "instrument": {
                "funding_interval_s": FUNDING_INTERVAL_S,
                "instrument_type": "perpetual_future",
                "settlement_asset": "USDC",
                "underlying": symbol.upper(),
            },
            "qty": f"{qty:.8f}",
        }
        async with _make_chrome_session() as client:
            resp = await client.post(
                f"{VARIATIONAL_API_BASE}/quotes/indicative",
                json=payload,
                headers=self._headers(),
                cookies=self._cookies(),
                timeout=15,
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Variational: ошибка получения quote для {symbol}: "
                f"HTTP {resp.status_code} — {resp.text[:200]}"
            )
        try:
            return resp.json()
        except Exception:
            raise RuntimeError(
                f"Variational: неожиданный ответ от quotes/indicative: {resp.text[:200]}"
            )

    async def _submit_market_order(
        self, quote_id: str, side: str, is_reduce_only: bool
    ) -> dict:
        """
        Шаг 2: отправляет маркет-ордер по quote_id.
        side: "buy" / "sell"
        """
        async with _make_chrome_session() as client:
            resp = await client.post(
                f"{VARIATIONAL_API_BASE}/orders/new/market",
                json={
                    "quote_id": quote_id,
                    "side": side,
                    "max_slippage": MAX_SLIPPAGE,
                    "is_reduce_only": is_reduce_only,
                },
                headers=self._headers(),
                cookies=self._cookies(),
                timeout=15,
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Variational: ошибка отправки ордера: "
                f"HTTP {resp.status_code} — {resp.text[:200]}"
            )
        try:
            return resp.json()
        except Exception:
            raise RuntimeError(
                f"Variational: неожиданный ответ от orders/new/market: {resp.text[:200]}"
            )

    def _snap_qty(self, qty: float, quote_response: dict) -> float:
        """Округляет qty вниз до минимального тика инструмента из ответа quote."""
        try:
            tick = float(
                (quote_response.get("qty_limits") or {}).get("min_qty_tick") or 0.1
            )
        except (TypeError, ValueError):
            tick = 0.1
        if tick <= 0:
            tick = 0.1
        return math.floor(qty / tick) * tick

    def _fix_qty_from_error(self, error_text: str, approx_qty: float) -> float | None:
        """
        Парсит ошибку 422 и возвращает исправленный qty.

        Variational возвращает два формата:
          1) "qty × leg ratio (4152.7) must be a multiple of min-qty-tick (1)"
             → snapped = floor(product / tick) * tick
             → corrected_qty = snapped * approx_qty / product
          2) "qty must be multiple of min-qty-tick (0.10)"
             → corrected_qty = floor(approx_qty / tick) * tick
        """
        # Формат 1: leg ratio
        m = re.search(
            r"leg ratio \(([0-9.]+)\).*min-qty-tick \(([0-9.]+)\)", error_text
        )
        if m:
            product = float(m.group(1))   # qty * leg_ratio
            tick = float(m.group(2))
            if product > 0 and tick > 0 and approx_qty > 0:
                snapped = math.floor(product / tick) * tick
                corrected = snapped * approx_qty / product if snapped > 0 else None
                logger.info(
                    f"Variational tick-fix (leg ratio): product={product}, "
                    f"tick={tick}, snapped={snapped}, corrected_qty={corrected}"
                )
                return corrected

        # Формат 2: простой тик
        m = re.search(r"min-qty-tick \(([0-9.]+)\)", error_text)
        if m:
            tick = float(m.group(1))
            if tick > 0 and approx_qty > 0:
                corrected = math.floor(approx_qty / tick) * tick
                logger.info(
                    f"Variational tick-fix (simple): tick={tick}, "
                    f"approx_qty={approx_qty}, corrected_qty={corrected}"
                )
                return corrected if corrected > 0 else None

        return None

    async def market_open(self, symbol: str, is_long: bool, size_usd: float) -> dict:
        """Открывает маркет-позицию через двухшаговую RFQ."""
        await self._ensure_assets()

        # Получаем mark_price для расчёта примерного qty
        mark_price = await self._get_mark_price(symbol)
        if mark_price == 0:
            raise RuntimeError(f"Variational: нулевая цена для {symbol}")

        approx_qty = size_usd / mark_price
        side = "buy" if is_long else "sell"

        # Шаг 1a: получаем quote с примерным qty, чтобы узнать min_qty_tick.
        # Если Variational вернёт 422 с tick-ошибкой — парсим нужный тик из текста
        # и повторяем с исправленным qty (нужно для токенов с tick=1, например STRK, LINEA).
        try:
            quote = await self._get_indicative_quote(symbol, approx_qty)
            qty = self._snap_qty(approx_qty, quote)
        except RuntimeError as e:
            corrected = self._fix_qty_from_error(str(e), approx_qty)
            if corrected is None or corrected <= 0:
                raise
            logger.info(
                f"Variational: пересчитываем qty {symbol}: "
                f"{approx_qty:.6f} → {corrected:.6f}"
            )
            quote = await self._get_indicative_quote(symbol, corrected)
            qty = corrected

        if qty <= 0:
            raise RuntimeError(
                f"Variational: qty после округления = 0 для {symbol} "
                f"(size_usd={size_usd}, price={mark_price})"
            )

        # Шаг 1b: если qty изменился после округления — запрашиваем quote заново
        if abs(qty - approx_qty) > 1e-9:
            try:
                quote = await self._get_indicative_quote(symbol, qty)
            except RuntimeError as e:
                corrected = self._fix_qty_from_error(str(e), qty)
                if corrected is None or corrected <= 0:
                    raise
                qty = corrected
                quote = await self._get_indicative_quote(symbol, qty)

        mark_price = float(quote.get("mark_price") or mark_price)
        logger.info(
            f"Variational: {'лонг' if is_long else 'шорт'} {symbol}, "
            f"qty={qty}, price≈{mark_price}"
        )

        quote_id = quote.get("quote_id") or ""
        if not quote_id:
            raise RuntimeError(
                f"Variational: quote_id не получен для {symbol}, ответ: {quote}"
            )

        # Шаг 2: отправляем ордер
        # Если Variational вернёт 422 с tick-ошибкой (leg ratio) — исправляем qty и повторяем
        try:
            result = await self._submit_market_order(quote_id, side, is_reduce_only=False)
        except RuntimeError as e:
            corrected = self._fix_qty_from_error(str(e), qty)
            if corrected is None or corrected <= 0:
                raise
            logger.info(
                f"Variational: submit tick-fix {symbol}: qty {qty:.6f} → {corrected:.6f}"
            )
            qty = corrected
            quote = await self._get_indicative_quote(symbol, qty)
            quote_id = quote.get("quote_id") or ""
            if not quote_id:
                raise RuntimeError(
                    f"Variational: quote_id не получен после tick-fix для {symbol}"
                )
            result = await self._submit_market_order(quote_id, side, is_reduce_only=False)
        order_id = (
            result.get("rfq_id")
            or result.get("order_id")
            or result.get("id")
            or ""
        )

        logger.info(f"Variational: ордер принят {symbol}, id={order_id}")
        return {
            "order_id": order_id,
            "size": qty,
            "size_usd": size_usd,
            "price": mark_price,
        }

    async def market_close(self, symbol: str, original_size: float, was_long: bool) -> dict:
        """Закрывает позицию через reduce_only маркет-ордер."""
        await self._ensure_assets()

        mark_price = await self._get_mark_price(symbol)
        close_side = "sell" if was_long else "buy"

        logger.info(
            f"Variational: закрытие {symbol}, size={original_size:.6f}, side={close_side}"
        )

        # Шаг 1: получаем quote_id
        quote = await self._get_indicative_quote(symbol, original_size)
        quote_id = quote.get("quote_id") or ""
        if not quote_id:
            raise RuntimeError(
                f"Variational: quote_id не получен для закрытия {symbol}, ответ: {quote}"
            )
        mark_price = float(quote.get("mark_price") or mark_price)

        # Шаг 2: отправляем reduce_only ордер
        try:
            await self._submit_market_order(quote_id, close_side, is_reduce_only=True)
            logger.info(f"Variational: позиция {symbol} закрыта")
            return {"symbol": symbol, "price": mark_price}
        except RuntimeError as e:
            err_str = str(e).lower()
            safe_errors = (
                "no position", "nothing to close", "reduce only",
                "no open position", "position not found",
            )
            if any(s in err_str for s in safe_errors):
                logger.warning(f"Variational закрытие {symbol}: позиция уже закрыта ({e})")
                return {"symbol": symbol, "price": mark_price}
            raise

    async def get_positions(self) -> list | None:
        """
        Возвращает открытые позиции.
        None при ошибке, [] если позиций нет.
        Каждый элемент: {symbol, qty} (qty > 0 = long, < 0 = short).
        """
        try:
            async with _make_chrome_session() as client:
                resp = await client.get(
                    f"{VARIATIONAL_API_BASE}/positions",
                    headers=self._headers(),
                    cookies=self._cookies(),
                    timeout=10,
                )
            if resp.status_code != 200:
                logger.warning(
                    f"Variational positions: HTTP {resp.status_code}: {resp.text[:200]}"
                )
                return None

            data = resp.json()
            items = data if isinstance(data, list) else data.get("positions", [])

            positions = []
            for item in items:
                pos_info = item.get("position_info") or item
                instrument = pos_info.get("instrument") or {}
                symbol = (
                    instrument.get("underlying")
                    or pos_info.get("symbol")
                    or ""
                ).upper()
                if not symbol:
                    continue
                qty_raw = pos_info.get("qty") or pos_info.get("quantity") or 0
                qty = float(qty_raw)
                if qty == 0:
                    continue
                side = (pos_info.get("side") or item.get("side") or "").lower()
                if side == "sell":
                    qty = -abs(qty)
                positions.append({"symbol": symbol, "qty": qty})

            logger.debug(f"Variational positions: {positions}")
            return positions

        except Exception as e:
            logger.warning(f"Variational get_positions ошибка: {e}")
            return None
