import logging
import math
import re

import httpx

logger = logging.getLogger(__name__)


async def _siwe_login(wallet: str, private_key: str) -> str:
    """
    Авторизация на Variational через подпись кошелька (SIWE — Sign-In with Ethereum).
    Возвращает новый vr-token.
    Вызывается автоматически при HTTP 401 (протухший токен).
    """
    try:
        from eth_account import Account
        from eth_account.messages import encode_defunct
    except ImportError:
        raise RuntimeError(
            "eth-account не установлен: pip install eth-account"
        )

    async with _make_chrome_session() as client:
        # Шаг 1: получаем SIWE-сообщение для подписи
        resp = await client.post(
            f"{VARIATIONAL_API_BASE}/auth/generate_signing_data",
            json={"address": wallet},
            headers={
                "vr-connected-address": wallet,
                "content-type": "application/json",
                "origin": "https://omni.variational.io",
                "referer": "https://omni.variational.io/",
            },
            timeout=15,
        )
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Variational auth: generate_signing_data HTTP {resp.status_code}: {resp.text[:200]}"
        )

    siwe_message = resp.text.strip().strip('"')
    if not siwe_message:
        raise RuntimeError("Variational auth: пустое SIWE-сообщение")

    # Шаг 2: подписываем приватным ключом
    msg = encode_defunct(text=siwe_message)
    signed = Account.sign_message(msg, private_key=private_key)
    signature = signed.signature.hex()
    if signature.startswith("0x"):
        signature = signature[2:]

    # Шаг 3: логинимся и получаем новый токен
    async with _make_chrome_session() as client:
        resp = await client.post(
            f"{VARIATIONAL_API_BASE}/auth/login",
            json={"address": wallet, "signed_message": signature},
            headers={
                "vr-connected-address": wallet,
                "content-type": "application/json",
                "origin": "https://omni.variational.io",
                "referer": "https://omni.variational.io/",
            },
            timeout=15,
        )
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Variational auth: login HTTP {resp.status_code}: {resp.text[:200]}"
        )

    data = resp.json()
    new_token = data.get("token") or ""
    if not new_token:
        raise RuntimeError(f"Variational auth: токен не получен, ответ: {data}")

    logger.info("Variational: токен успешно обновлён через SIWE")
    return new_token

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

    def __init__(
        self,
        vr_token: str,
        wallet_address: str,
        cf_clearance: str = "",
        private_key: str = "",
    ):
        self._token = vr_token
        self._wallet = wallet_address.lower()
        self._cf_clearance = cf_clearance  # оставлен для совместимости
        self._private_key = private_key   # приватный ключ для авто-обновления токена
        self._assets: dict = {}  # symbol → {funding_interval_s, ...}
        self._qty_ticks: dict[str, float] = {}  # symbol → min_qty_tick (кеш из ошибок 422)

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

    async def _refresh_token(self) -> bool:
        """
        Пытается обновить vr-token через SIWE (Sign-In with Ethereum).
        Возвращает True если успешно. Нужен VARIATIONAL_PRIVATE_KEY в .env.
        """
        if not self._private_key:
            logger.warning(
                "Variational: VARIATIONAL_PRIVATE_KEY не задан — "
                "авто-обновление токена невозможно. "
                "Обнови VARIATIONAL_TOKEN в .env вручную."
            )
            return False
        try:
            new_token = await _siwe_login(self._wallet, self._private_key)
            self._token = new_token
            return True
        except Exception as e:
            logger.error(f"Variational: не удалось обновить токен: {e}")
            return False

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

    def _format_qty(self, qty: float) -> str:
        """Форматирует qty для API: целое число если qty целый, иначе до 8 знаков."""
        if qty == int(qty):
            return str(int(qty))
        return f"{qty:.8f}".rstrip("0").rstrip(".")

    def _snap_to_tick(self, symbol: str, qty: float) -> float:
        """Округляет qty вниз к кешированному min_qty_tick. Если тик неизвестен — не трогает."""
        tick = self._qty_ticks.get(symbol.upper())
        if tick and tick > 0:
            snapped = math.floor(qty / tick) * tick
            if snapped != qty:
                logger.info(
                    f"Variational: pre-snap {symbol}: {qty:.6f} → {snapped:.6f} (tick={tick})"
                )
            return snapped
        return qty

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
            "qty": self._format_qty(qty),
        }
        async with _make_chrome_session() as client:
            resp = await client.post(
                f"{VARIATIONAL_API_BASE}/quotes/indicative",
                json=payload,
                headers=self._headers(),
                cookies=self._cookies(),
                timeout=15,
            )
        if resp.status_code == 401:
            logger.warning(f"Variational: токен протух (401), пробуем обновить...")
            if await self._refresh_token():
                async with _make_chrome_session() as client:
                    resp = await client.post(
                        f"{VARIATIONAL_API_BASE}/quotes/indicative",
                        json=payload,
                        headers=self._headers(),
                        cookies=self._cookies(),
                        timeout=15,
                    )
            else:
                raise RuntimeError(
                    f"Variational: ошибка получения quote для {symbol}: "
                    f"HTTP 401 — токен истёк, обнови VARIATIONAL_TOKEN в .env"
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
        if resp.status_code == 401:
            logger.warning("Variational: токен протух при отправке ордера (401), обновляем...")
            if await self._refresh_token():
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
            else:
                raise RuntimeError(
                    "Variational: ошибка отправки ордера: HTTP 401 — токен истёк, "
                    "обнови VARIATIONAL_TOKEN в .env"
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

    def _parse_tick_from_error(self, error_text: str) -> float | None:
        """Извлекает min_qty_tick / min-qty-tick из текста ошибки 422."""
        m = re.search(r"min[-_]qty[-_]tick \(([0-9.]+)\)", error_text)
        if m:
            return float(m.group(1))
        return None

    def _fix_qty_for_tick(self, qty: float, tick: float) -> float:
        """Округляет qty вниз к ближайшему кратному tick."""
        return math.floor(qty / tick) * tick

    async def _quote_and_submit(
        self, symbol: str, qty: float, side: str, is_reduce_only: bool,
    ) -> tuple[dict, float]:
        """
        Запрашивает quote и отправляет ордер. Retry loop при 422 (tick-ошибка):
        парсит min-qty-tick из ошибки, кеширует, округляет qty и повторяет.
        Возвращает (result, final_qty).
        """
        MAX_RETRIES = 5

        for attempt in range(MAX_RETRIES):
            # Перед каждой попыткой: snap к кешированному тику
            qty = self._snap_to_tick(symbol, qty)
            if qty <= 0:
                raise RuntimeError(f"Variational: qty=0 после округления для {symbol}")

            quote = await self._get_indicative_quote(symbol, qty)
            quote_id = quote.get("quote_id") or ""
            if not quote_id:
                raise RuntimeError(
                    f"Variational: quote_id не получен для {symbol}, ответ: {quote}"
                )

            try:
                result = await self._submit_market_order(quote_id, side, is_reduce_only)
                return result, qty
            except RuntimeError as e:
                err = str(e)
                tick = self._parse_tick_from_error(err)
                if tick is None or tick <= 0:
                    raise  # не tick-ошибка — пробрасываем
                # Кешируем тик и пересчитываем qty
                self._qty_ticks[symbol.upper()] = tick
                new_qty = self._fix_qty_for_tick(qty, tick)
                logger.info(
                    f"Variational: tick-fix {symbol} (attempt {attempt+1}): "
                    f"tick={tick}, qty {qty} → {new_qty}"
                )
                if new_qty <= 0 or new_qty == qty:
                    raise  # не удалось исправить
                qty = new_qty

        raise RuntimeError(
            f"Variational: не удалось подобрать qty для {symbol} за {MAX_RETRIES} попыток"
        )

    async def market_open(self, symbol: str, is_long: bool, size_usd: float) -> dict:
        """Открывает маркет-позицию через двухшаговую RFQ."""
        await self._ensure_assets()

        mark_price = await self._get_mark_price(symbol)
        if mark_price == 0:
            raise RuntimeError(f"Variational: нулевая цена для {symbol}")

        qty = size_usd / mark_price
        side = "buy" if is_long else "sell"

        # Snap к кешированному тику (если уже известен)
        qty = self._snap_to_tick(symbol, qty)

        logger.info(
            f"Variational: {'лонг' if is_long else 'шорт'} {symbol}, "
            f"qty={qty}, price≈{mark_price}"
        )

        result, final_qty = await self._quote_and_submit(
            symbol, qty, side, is_reduce_only=False,
        )

        order_id = (
            result.get("rfq_id")
            or result.get("order_id")
            or result.get("id")
            or ""
        )

        logger.info(f"Variational: ордер принят {symbol}, id={order_id}")
        return {
            "order_id": order_id,
            "size": final_qty,
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

        try:
            result, _ = await self._quote_and_submit(
                symbol, original_size, close_side, is_reduce_only=True,
            )
            mark_price = float(result.get("mark_price", mark_price))
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
            if resp.status_code == 401:
                logger.warning("Variational: токен протух при чтении позиций (401), обновляем...")
                if await self._refresh_token():
                    async with _make_chrome_session() as client:
                        resp = await client.get(
                            f"{VARIATIONAL_API_BASE}/positions",
                            headers=self._headers(),
                            cookies=self._cookies(),
                            timeout=10,
                        )
                else:
                    logger.warning("Variational: не удалось обновить токен, позиции недоступны")
                    return None
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
