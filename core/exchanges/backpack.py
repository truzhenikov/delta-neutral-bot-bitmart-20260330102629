import base64
import logging
import time

import httpx

logger = logging.getLogger(__name__)


class BackpackExecutor:
    """Клиент для торговли на Backpack Exchange (Ed25519 аутентификация)."""

    BASE_URL = "https://api.backpack.exchange"

    def __init__(self, api_key: str, api_secret: str):
        """
        api_key:    Base64-encoded Ed25519 public key (из настроек Backpack)
        api_secret: Base64-encoded Ed25519 private key seed (32 байта)
        """
        self.api_key = api_key
        self._markets: dict = {}  # symbol → {step_size}
        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
            raw = base64.b64decode(api_secret)
            if len(raw) == 64:
                raw = raw[:32]  # если отдали полный ключ — берём только seed
            self._private_key = Ed25519PrivateKey.from_private_bytes(raw)
        except ImportError:
            raise RuntimeError("Установи: pip install cryptography")

    def _sign(self, instruction: str, params: dict) -> dict:
        """Формирует заголовки по протоколу Backpack (сортировка + Ed25519 подпись)."""
        timestamp = int(time.time() * 1000)
        window = 5000
        # Булевы значения сериализуем как lowercase (как в JSON), остальные — через str()
        def _val(v):
            if isinstance(v, bool):
                return "true" if v else "false"
            return v
        sorted_body = "&".join(f"{k}={_val(v)}" for k, v in sorted(params.items()))
        if sorted_body:
            message = f"instruction={instruction}&{sorted_body}&timestamp={timestamp}&window={window}"
        else:
            message = f"instruction={instruction}&timestamp={timestamp}&window={window}"
        signature = self._private_key.sign(message.encode("utf-8"))
        return {
            "X-API-Key": self.api_key,
            "X-Signature": base64.b64encode(signature).decode(),
            "X-Timestamp": str(timestamp),
            "X-Window": str(window),
            "Content-Type": "application/json; charset=utf-8",
        }

    def _bp_symbol(self, symbol: str) -> str:
        """BTC → BTC_USDC_PERP"""
        return f"{symbol.upper()}_USDC_PERP"

    async def _ensure_markets(self):
        """Загружает step_size для всех символов."""
        if self._markets:
            return
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{self.BASE_URL}/api/v1/markets")
            data = resp.json()
        for m in data:
            if m.get("marketType") != "PERP":
                continue
            sym = m.get("baseSymbol", "").upper()
            step = float(m["filters"]["quantity"]["stepSize"])
            self._markets[sym] = {"step_size": step}

    def _round_qty(self, symbol: str, qty: float) -> float:
        """Округляет количество до ближайшего кратного stepSize."""
        step = self._markets.get(symbol.upper(), {}).get("step_size", 0.000001)
        import math
        rounded = math.floor(qty / step) * step
        # Убираем лишние нули через форматирование
        decimals = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
        return round(rounded, decimals)

    async def get_mark_price(self, symbol: str) -> float:
        """Получает текущую mark price."""
        bp_symbol = self._bp_symbol(symbol)
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{self.BASE_URL}/api/v1/markPrices",
                params={"marketType": "PERP"},
            )
            data = resp.json()
        for item in data:
            if item.get("symbol") == bp_symbol:
                return float(item.get("markPrice") or item.get("price") or 0)
        raise ValueError(f"Цена {symbol} не найдена на Backpack")

    async def market_open(self, symbol: str, is_long: bool, size_usd: float) -> dict:
        """Открывает рыночный ордер. is_long=True — лонг, False — шорт."""
        await self._ensure_markets()
        bp_symbol = self._bp_symbol(symbol)
        price = await self.get_mark_price(symbol)
        quantity = self._round_qty(symbol, size_usd / price)

        side = "Bid" if is_long else "Ask"
        params = {
            "orderType": "Market",
            "quantity": str(quantity),
            "side": side,
            "symbol": bp_symbol,
        }
        headers = self._sign("orderExecute", params)

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{self.BASE_URL}/api/v1/order",
                json=params,
                headers=headers,
            )
            result = resp.json()

        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Backpack ошибка открытия: {result}")

        executed_qty = float(result.get("executedQuantity") or quantity)
        executed_price = float(result.get("avgPrice") or price)
        logger.info(f"Backpack: открыт {'лонг' if is_long else 'шорт'} {symbol}, "
                    f"qty={executed_qty}, price={executed_price}")
        return {
            "order_id": result.get("id"),
            "size": executed_qty,
            "size_usd": size_usd,
            "price": executed_price,
        }

    async def market_close(self, symbol: str, close_qty: float | None = None) -> dict:
        """
        Закрывает позицию через reduceOnly ордер.
        close_qty — если передан, закрывает только это количество (в base tokens).
        Если None — закрывает всю позицию (читает с биржи).
        """
        bp_symbol = self._bp_symbol(symbol)

        if close_qty is not None:
            # Частичное закрытие: нужно знать сторону — смотрим позицию
            positions = await self.get_positions()
            pos = next((p for p in positions if p.get("symbol") == bp_symbol), None)
            if not pos:
                logger.info(f"Backpack: позиция {symbol} не найдена — считаем закрытой")
                return {"symbol": symbol, "closed_qty": 0}
            qty_full = float(pos.get("netQuantity") or pos.get("quantity") or 0)
            side = "Ask" if qty_full > 0 else "Bid"
            qty = close_qty
        else:
            positions = await self.get_positions()
            pos = next((p for p in positions if p.get("symbol") == bp_symbol), None)
            if not pos:
                # Позиции нет на бирже — значит уже закрыта, это ок
                logger.info(f"Backpack: позиция {symbol} не найдена — считаем уже закрытой")
                return {"symbol": symbol, "closed_qty": 0}

            qty = float(pos.get("netQuantity") or pos.get("quantity") or 0)
            if qty == 0:
                # Позиция есть, но с нулевым размером — тоже считаем закрытой
                logger.info(f"Backpack: позиция {symbol} имеет нулевой размер — считаем закрытой")
                return {"symbol": symbol, "closed_qty": 0}
            side = "Ask" if qty > 0 else "Bid"

        params = {
            "orderType": "Market",
            "quantity": f"{abs(qty):g}",  # убираем лишние нули без float-мусора
            "reduceOnly": True,            # boolean, не строка — Backpack ожидает true
            "side": side,
            "symbol": bp_symbol,
        }
        headers = self._sign("orderExecute", params)

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{self.BASE_URL}/api/v1/order",
                json=params,
                headers=headers,
            )
            result = resp.json()

        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Backpack ошибка закрытия: {result}")

        exit_price = float(result.get("avgPrice") or result.get("price") or 0)
        fees_paid = float(result.get("fee") or 0)
        logger.info(f"Backpack: закрыта позиция {symbol}, qty={abs(qty)}, price={exit_price}")
        return {"symbol": symbol, "closed_qty": abs(qty), "price": exit_price, "fee": fees_paid}

    async def get_usdc_balance(self) -> float:
        """
        Возвращает доступный баланс для фьючерсной ноги на Backpack.

        Важно: используем collateral endpoint и netEquityAvailable,
        т.к. /api/v1/capital может показывать 0 available при lend-режиме,
        хотя маржа для фьючерсов доступна.
        """
        params = {}
        headers = self._sign("collateralQuery", params)
        get_headers = {k: v for k, v in headers.items() if k != "Content-Type"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{self.BASE_URL}/api/v1/capital/collateral",
                headers=get_headers,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Backpack collateral error: {resp.text}")
            data = resp.json()

        if isinstance(data, dict):
            # Предпочитаем futures-доступную маржу
            nea = data.get("netEquityAvailable")
            if nea is not None:
                return float(nea or 0)

            # Фолбэк на старый формат, если API изменится обратно
            if "USDC" in data and isinstance(data["USDC"], dict):
                return float(data["USDC"].get("available", 0) or 0)

        return 0.0

    async def get_positions(self) -> list:
        """Возвращает список открытых перп-позиций."""
        params = {}
        headers = self._sign("positionQuery", params)
        get_headers = {k: v for k, v in headers.items() if k != "Content-Type"}
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{self.BASE_URL}/api/v1/position",
                headers=get_headers,
            )
            if resp.status_code != 200:
                logger.error(f"Backpack positions error: {resp.text}")
                return []
            return resp.json()
