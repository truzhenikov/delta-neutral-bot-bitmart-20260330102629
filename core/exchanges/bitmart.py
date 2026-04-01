import hashlib
import hmac
import json
import logging
import math
import time

import httpx

logger = logging.getLogger(__name__)


class BitMartExecutor:
    """Клиент для торговли BitMart Futures."""

    BASE_URL = "https://api-cloud-v2.bitmart.com"

    def __init__(self, api_key: str, api_secret: str, api_memo: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_memo = api_memo
        self._markets: dict[str, dict] = {}

    def _headers_from_body_str(self, body_str: str) -> dict:
        """BitMart signature must match the exact raw JSON body bytes."""
        timestamp = str(int(time.time() * 1000))
        sign_payload = f"{timestamp}#{self.api_memo}#{body_str}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-BM-KEY": self.api_key,
            "X-BM-SIGN": signature,
            "X-BM-TIMESTAMP": timestamp,
        }

    def _headers(self, body: dict | None = None) -> dict:
        body_str = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
        return self._headers_from_body_str(body_str)

    async def _keyed_get(self, path: str, params: dict | None = None) -> dict:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{self.BASE_URL}{path}",
                params=params,
                headers={"X-BM-KEY": self.api_key},
            )
        data = resp.json()
        if resp.status_code != 200 or data.get("code") != 1000:
            raise RuntimeError(f"BitMart API error {path}: {data}")
        return data

    async def _signed_post(self, path: str, body: dict) -> dict:
        # Важно: подписываем и отправляем один и тот же сериализованный JSON,
        # иначе BitMart может вернуть "Header X-BM-SIGN is wrong".
        body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        headers = self._headers_from_body_str(body_str)
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self.BASE_URL}{path}",
                content=body_str,
                headers=headers,
            )
        data = resp.json()
        if resp.status_code != 200 or data.get("code") != 1000:
            raise RuntimeError(f"BitMart API error {path}: {data}")
        return data

    def _bm_symbol(self, symbol: str) -> str:
        return f"{symbol.upper()}USDT"

    async def _ensure_markets(self):
        if self._markets:
            return
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{self.BASE_URL}/contract/public/details")
        data = resp.json()
        symbols = ((data or {}).get("data") or {}).get("symbols") or []
        for item in symbols:
            base = (item.get("base_currency") or "").upper()
            quote = (item.get("quote_currency") or "").upper()
            if not base or quote != "USDT":
                continue
            self._markets[base] = {
                "symbol": item.get("symbol") or self._bm_symbol(base),
                "price": float(item.get("last_price") or item.get("index_price") or 0),
                "contract_size": float(item.get("contract_size") or 1),
                "min_volume": int(float(item.get("min_volume") or 1)),
                "max_volume": int(float(item.get("market_max_volume") or item.get("max_volume") or 0)),
                "taker_fee_rate": 0.0006,
            }

    async def _get_market(self, symbol: str) -> dict:
        await self._ensure_markets()
        market = self._markets.get(symbol.upper())
        if not market:
            raise ValueError(f"Рынок {symbol} не найден на BitMart")
        return market

    def _contracts_from_usd(self, market: dict, size_usd: float) -> int:
        price = market["price"]
        contract_size = market["contract_size"] or 1
        if price <= 0:
            raise RuntimeError("BitMart: не удалось определить цену для расчёта размера")
        contracts = math.floor(size_usd / (price * contract_size))
        contracts = max(contracts, market["min_volume"])
        max_volume = market.get("max_volume") or 0
        if max_volume:
            contracts = min(contracts, max_volume)
        return int(contracts)

    async def _get_order_detail(self, symbol: str, order_id: str | int) -> dict:
        data = await self._keyed_get(
            "/contract/private/order",
            params={"symbol": self._bm_symbol(symbol), "order_id": str(order_id)},
        )
        return data.get("data") or {}

    async def get_mark_price(self, symbol: str) -> float:
        market = await self._get_market(symbol)
        return float(market["price"])

    async def market_open(self, symbol: str, is_long: bool, size_usd: float) -> dict:
        market = await self._get_market(symbol)
        contracts = self._contracts_from_usd(market, size_usd)
        side = 1 if is_long else 4
        body = {
            "symbol": market["symbol"],
            "client_order_id": f"bm_{int(time.time() * 1000)}",
            "side": side,
            "type": "market",
            "size": contracts,
        }
        submit = await self._signed_post("/contract/private/submit-order", body)
        order_id = ((submit.get("data") or {}).get("order_id")) or ""
        detail = await self._get_order_detail(symbol, order_id) if order_id else {}

        price = float(detail.get("deal_avg_price") or market["price"] or 0)
        deal_size = int(float(detail.get("deal_size") or contracts))
        base_size = deal_size * market["contract_size"]

        logger.info(
            f"BitMart: открыт {'лонг' if is_long else 'шорт'} {symbol}, "
            f"contracts={deal_size}, price={price}"
        )
        return {
            "order_id": str(order_id),
            "contracts": deal_size,
            "size": base_size,
            "size_usd": base_size * price if price else size_usd,
            "price": price,
            "fee": round(base_size * price * market["taker_fee_rate"], 6) if price else 0.0,
        }

    async def market_close(self, symbol: str, close_qty: float | None = None, was_long: bool | None = None) -> dict:
        market = await self._get_market(symbol)
        positions = await self.get_positions()
        pos = next((p for p in positions if p.get("symbol") == market["symbol"]), None)
        if not pos:
            logger.info(f"BitMart: позиция {symbol} не найдена — считаем уже закрытой")
            return {"symbol": symbol, "closed_qty": 0, "price": market["price"], "fee": 0.0}

        current_amount = abs(int(float(pos.get("current_amount") or 0)))
        if current_amount == 0:
            logger.info(f"BitMart: позиция {symbol} нулевая — считаем уже закрытой")
            return {"symbol": symbol, "closed_qty": 0, "price": market["price"], "fee": 0.0}

        if close_qty is not None:
            contracts = max(1, math.floor(close_qty / market["contract_size"]))
            contracts = min(contracts, current_amount)
        else:
            contracts = current_amount

        if was_long is None:
            was_long = int(pos.get("position_type") or 0) == 1
        side = 3 if was_long else 2
        body = {
            "symbol": market["symbol"],
            "client_order_id": f"bm_close_{int(time.time() * 1000)}",
            "side": side,
            "type": "market",
            "size": int(contracts),
        }
        submit = await self._signed_post("/contract/private/submit-order", body)
        order_id = ((submit.get("data") or {}).get("order_id")) or ""
        detail = await self._get_order_detail(symbol, order_id) if order_id else {}

        price = float(detail.get("deal_avg_price") or market["price"] or 0)
        deal_size = int(float(detail.get("deal_size") or contracts))
        base_size = deal_size * market["contract_size"]
        fee = round(base_size * price * market["taker_fee_rate"], 6) if price else 0.0

        logger.info(f"BitMart: позиция {symbol} закрыта, contracts={deal_size}, price={price}")
        return {
            "symbol": symbol,
            "closed_qty": base_size,
            "price": price,
            "fee": fee,
        }

    async def get_usdt_balance(self) -> float:
        data = await self._keyed_get("/contract/private/assets-detail")
        for asset in data.get("data") or []:
            if (asset.get("currency") or "").upper() == "USDT":
                return float(asset.get("available_balance") or 0)
        return 0.0

    async def get_positions(self) -> list[dict]:
        data = await self._keyed_get("/contract/private/position")
        positions = []
        for pos in data.get("data") or []:
            current_amount = float(pos.get("current_amount") or 0)
            if current_amount == 0:
                continue
            positions.append(pos)
        return positions

    async def get_cumulative_funding_payment(self, symbol: str, since_ts: float | None = None) -> float:
        """
        Возвращает фактически накопленный funding payment по символу.
        BitMart отдаёт его через transaction-history с flow_type=3 (Funding Fee).

        since_ts: unix timestamp (sec). Если задан, считаем только записи после открытия позиции.
        """
        data = await self._keyed_get(
            "/contract/private/transaction-history",
            params={
                "symbol": self._bm_symbol(symbol),
                "flow_type": 3,
                "account": "futures",
                "page_size": 1000,
            },
        )
        total = 0.0
        since_ms = int(since_ts * 1000) if since_ts else None
        for item in data.get("data") or []:
            try:
                if since_ms is not None:
                    ts_ms = int(item.get("time") or 0)
                    if ts_ms and ts_ms < since_ms:
                        continue
                total += float(item.get("amount") or 0)
            except (TypeError, ValueError):
                continue
        return total
