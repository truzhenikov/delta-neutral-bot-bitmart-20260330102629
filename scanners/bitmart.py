import logging

import httpx

from .base import BaseScanner, FundingRate

logger = logging.getLogger(__name__)


class BitMartScanner(BaseScanner):
    """BitMart Futures — публичный API, без авторизации."""

    BASE_URL = "https://api-cloud-v2.bitmart.com"

    async def get_funding_rates(self) -> list[FundingRate]:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{self.BASE_URL}/contract/public/details")
                payload = resp.json()
        except Exception as e:
            logger.error(f"BitMart: ошибка запроса: {e}")
            return []

        symbols = ((payload or {}).get("data") or {}).get("symbols") or []
        rates: list[FundingRate] = []

        for item in symbols:
            try:
                symbol = (item.get("base_currency") or "").upper()
                quote = (item.get("quote_currency") or "").upper()
                status = str(item.get("status") or "").lower()
                if not symbol or quote != "USDT" or status == "delisted":
                    continue

                funding_rate = float(item.get("funding_rate") or 0)
                interval_hours = int(item.get("funding_interval_hours") or 8)
                mark_price = float(item.get("last_price") or item.get("index_price") or 0)
                apr = funding_rate * (24 / interval_hours) * 365 * 100

                rates.append(FundingRate(
                    exchange="BitMart",
                    symbol=symbol,
                    rate=funding_rate,
                    interval_hours=interval_hours,
                    apr=apr,
                    open_interest_usd=float(item.get("open_interest_value") or 0),
                    volume_usd=float(item.get("turnover_24h") or 0),
                    mark_price=mark_price,
                ))
            except Exception as e:
                logger.debug(f"BitMart: ошибка парсинга {item.get('symbol')}: {e}")

        logger.info(f"BitMart: получено {len(rates)} рынков")
        return rates
