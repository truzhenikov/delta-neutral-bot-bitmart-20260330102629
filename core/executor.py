import asyncio
import logging
import time

import eth_account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from config import (
    HYPERLIQUID_PRIVATE_KEY, WALLET_ADDRESS, POSITION_SIZE_USD,
    BACKPACK_API_KEY, BACKPACK_API_SECRET,
    BITMART_API_KEY, BITMART_API_SECRET, BITMART_API_MEMO,
    VARIATIONAL_TOKEN, VARIATIONAL_WALLET, VARIATIONAL_CF_CLEARANCE, VARIATIONAL_PRIVATE_KEY,
    EXTENDED_API_KEY, EXTENDED_PUBLIC_KEY, EXTENDED_PRIVATE_KEY, EXTENDED_VAULT_ID,
)
from db.database import save_position, save_pair, scale_pair_db_generic, close_position, close_pair as db_close_pair

logger = logging.getLogger(__name__)

HL_BASE_URL = constants.MAINNET_API_URL


# ─── Hyperliquid (одиночная нога, Фаза 1) ────────────────────────────────────

def _get_hl_exchange():
    account = eth_account.Account.from_key(HYPERLIQUID_PRIVATE_KEY)
    return Exchange(account, HL_BASE_URL, account_address=WALLET_ADDRESS)


def _get_hl_info():
    return Info(HL_BASE_URL, skip_ws=True)


async def open_position(symbol: str, direction: str, entry_apr: float) -> dict:
    """Открывает одиночную перп-позицию на Hyperliquid."""
    info = _get_hl_info()
    exchange = _get_hl_exchange()

    meta = info.meta()
    asset_meta = next((a for a in meta["universe"] if a["name"] == symbol), None)
    if not asset_meta:
        raise ValueError(f"Монета {symbol} не найдена на Hyperliquid")

    sz_decimals = asset_meta["szDecimals"]
    all_mids = info.all_mids()
    price = float(all_mids.get(symbol, 0))
    if price == 0:
        raise ValueError(f"Не удалось получить цену {symbol}")

    size = round(POSITION_SIZE_USD / price, sz_decimals)
    is_buy = (direction == "LONG")

    result = exchange.market_open(symbol, is_buy, size, None, 0.01)
    if result.get("status") != "ok":
        raise RuntimeError(f"Ошибка открытия позиции: {result}")

    await save_position(
        symbol=symbol, exchange="Hyperliquid", direction=direction,
        size=size, entry_price=price, position_size_usd=POSITION_SIZE_USD,
        entry_apr=entry_apr,
    )
    return {"symbol": symbol, "size": size, "price": price, "direction": direction}


async def close_full_position(position_id: int, symbol: str) -> dict:
    """Закрывает одиночную перп-позицию на Hyperliquid."""
    exchange = _get_hl_exchange()
    result = exchange.market_close(symbol)
    if result.get("status") != "ok":
        raise RuntimeError(f"Hyperliquid не закрыл позицию {symbol}: {result}")
    await close_position(position_id)
    return {"position_id": position_id, "symbol": symbol}


# ─── BitMart + Backpack (дельта-нейтральная пара, Фаза 2) ─────────────────────

def _get_backpack():
    from core.exchanges.backpack import BackpackExecutor
    if not BACKPACK_API_KEY or not BACKPACK_API_SECRET:
        raise RuntimeError("Backpack API ключи не заданы в .env")
    return BackpackExecutor(BACKPACK_API_KEY, BACKPACK_API_SECRET)


def _get_bitmart():
    from core.exchanges.bitmart import BitMartExecutor
    if not BITMART_API_KEY or not BITMART_API_SECRET or not BITMART_API_MEMO:
        raise RuntimeError("BitMart: BITMART_API_KEY, BITMART_API_SECRET и BITMART_API_MEMO должны быть заданы в .env")
    return BitMartExecutor(BITMART_API_KEY, BITMART_API_SECRET, BITMART_API_MEMO)


async def open_pair(
    symbol: str,
    lighter_dir: str,   # "LONG" или "SHORT" на BitMart
    backpack_dir: str,  # "LONG" или "SHORT"
    size_usd: float,
    entry_apr: float,
) -> dict:
    """
    Открывает дельта-нейтральную пару BitMart + Backpack одновременно.
    Возвращает pair_id по которому можно закрыть обе ноги.
    """
    bitmart = _get_bitmart()
    backpack = _get_backpack()

    bitmart_is_long = (lighter_dir == "LONG")
    backpack_is_long = (backpack_dir == "LONG")

    logger.info(
        f"Открываем пару {symbol}: "
        f"BitMart {'лонг' if bitmart_is_long else 'шорт'}, "
        f"Backpack {'лонг' if backpack_is_long else 'шорт'}, "
        f"${size_usd}"
    )

    # ── Проверка балансов перед открытием ──────────────────────────────────────
    try:
        bm_balance = await bitmart.get_usdt_balance()
        logger.info(f"BitMart баланс: ${bm_balance:.2f} USDT (нужно: ${size_usd:.2f})")
        if bm_balance < size_usd * 0.05:
            raise RuntimeError(
                f"BitMart: недостаточно баланса. "
                f"Есть: ${bm_balance:.2f}, нужно как минимум: ${size_usd * 0.05:.2f}."
            )
    except RuntimeError:
        raise
    except Exception as e:
        logger.warning(f"Не удалось проверить баланс BitMart: {e} — продолжаем")

    try:
        bp_balance = await backpack.get_usdc_balance()
        logger.info(f"Backpack баланс: ${bp_balance:.2f} USDC (нужно: ${size_usd:.2f})")
        if bp_balance < size_usd * 0.1:
            # Меньше 10% от размера позиции — скорее всего недостаточно даже с плечом
            raise RuntimeError(
                f"Backpack: недостаточно баланса. "
                f"Есть: ${bp_balance:.2f}, нужно как минимум: ${size_usd * 0.1:.2f} (10% от позиции). "
                f"Пополни счёт или уменьши POSITION_SIZE_USD."
            )
        if bp_balance < size_usd:
            logger.warning(
                f"Backpack баланс ${bp_balance:.2f} < ${size_usd:.2f}. "
                f"Позиция откроется только если хватит маржи с учётом плеча."
            )
    except RuntimeError:
        raise  # пробрасываем если это наша ошибка с балансом
    except Exception as e:
        logger.warning(f"Не удалось проверить баланс Backpack: {e} — продолжаем")

    # Открываем обе ноги параллельно, перехватываем ошибки каждой
    lt_result, bp_result = await asyncio.gather(
        bitmart.market_open(symbol, bitmart_is_long, size_usd),
        backpack.market_open(symbol, backpack_is_long, size_usd),
        return_exceptions=True,
    )

    lt_ok = not isinstance(lt_result, Exception)
    bp_ok = not isinstance(bp_result, Exception)

    # Если одна нога упала — закрываем вторую и сообщаем об ошибке
    if lt_ok and not bp_ok:
        logger.error(f"Backpack не открылся: {bp_result} — закрываем BitMart автоматически")
        try:
            await bitmart.market_close(symbol, lt_result["size"], bitmart_is_long)
            logger.info("BitMart автоматически закрыт после ошибки Backpack")
        except Exception as e:
            logger.error(f"Не удалось закрыть BitMart после ошибки Backpack: {e}")
            try:
                from bot.telegram import send_message
                await send_message(
                    f"🚨 *КРИТИЧНО! НЕЗАХЕДЖИРОВАННАЯ ПОЗИЦИЯ!*\n\n"
                    f"*{symbol}* — BitMart открылся, Backpack упал.\n"
                    f"Автозакрытие BitMart тоже провалилось!\n\n"
                    f"❌ Backpack: `{bp_result}`\n"
                    f"❌ Автозакрытие: `{e}`\n\n"
                    f"⚠️ *Немедленно закрой {symbol} на BitMart вручную!*"
                )
            except Exception:
                pass
        raise RuntimeError(f"Backpack ошибка: {bp_result}\nBitMart закрыт автоматически.")

    if not lt_ok and bp_ok:
        logger.error(f"BitMart не открылся: {lt_result} — закрываем Backpack автоматически")
        try:
            await backpack.market_close(symbol)
            logger.info("Backpack автоматически закрыт после ошибки BitMart")
        except Exception as e:
            logger.error(f"Не удалось закрыть Backpack после ошибки BitMart: {e}")
            try:
                from bot.telegram import send_message
                await send_message(
                    f"🚨 *КРИТИЧНО! НЕЗАХЕДЖИРОВАННАЯ ПОЗИЦИЯ!*\n\n"
                    f"*{symbol}* — Backpack открылся, BitMart упал.\n"
                    f"Автозакрытие Backpack тоже провалилось!\n\n"
                    f"❌ BitMart: `{lt_result}`\n"
                    f"❌ Автозакрытие: `{e}`\n\n"
                    f"⚠️ *Немедленно закрой {symbol} на Backpack вручную!*"
                )
            except Exception:
                pass
        raise RuntimeError(f"BitMart ошибка: {lt_result}\nBackpack закрыт автоматически.")

    if not lt_ok and not bp_ok:
        raise RuntimeError(f"Обе ноги не открылись.\nBitMart: {lt_result}\nBackpack: {bp_result}")

    # Обе ноги открыты успешно — сохраняем атомарно в одной транзакции
    pair_id = f"{int(time.time())}_{symbol}_LT_BP"
    await save_pair(pair_id, [
        {
            "symbol": symbol, "exchange": "BitMart", "direction": lighter_dir,
            "size": lt_result["size"], "entry_price": lt_result["price"],
            "position_size_usd": size_usd, "entry_apr": entry_apr,
        },
        {
            "symbol": symbol, "exchange": "Backpack", "direction": backpack_dir,
            "size": bp_result["size"], "entry_price": bp_result["price"],
            "position_size_usd": size_usd, "entry_apr": entry_apr,
        },
    ])

    logger.info(f"✅ Пара открыта: {pair_id}")
    return {
        "pair_id": pair_id,
        "symbol": symbol,
        "bitmart": lt_result,
        "backpack": bp_result,
    }


async def close_pair(pair_id: str, symbol: str, legs: list[dict]) -> dict:
    """
    Закрывает обе ноги дельта-нейтральной пары BitMart + Backpack.
    legs: список позиций из БД (обе ноги).
    """
    bitmart = _get_bitmart()
    backpack = _get_backpack()

    lighter_leg = next((l for l in legs if l["exchange"] == "BitMart"), None)
    backpack_leg = next((l for l in legs if l["exchange"] == "Backpack"), None)

    tasks = []
    if lighter_leg:
        was_long = (lighter_leg["direction"] == "LONG")
        tasks.append(bitmart.market_close(symbol, lighter_leg["size"], was_long))
    if backpack_leg:
        tasks.append(backpack.market_close(symbol))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [str(r) for r in results if isinstance(r, Exception)]

    if errors:
        # Одна или обе ноги не закрылись — НЕ помечаем пару закрытой в БД
        raise RuntimeError(
            f"Не удалось закрыть ногу(и) пары {pair_id}:\n" + "\n".join(errors) +
            "\n⚠️ Проверь позиции на биржах вручную!"
        )

    # Собираем реальные данные P&L по каждой ноге
    leg_pnl: dict = {}
    result_idx = 0

    if lighter_leg:
        lt_res = results[result_idx]
        result_idx += 1
        if not isinstance(lt_res, Exception):
            exit_price = lt_res.get("price") or lighter_leg["entry_price"]
            was_long = (lighter_leg["direction"] == "LONG")
            pnl_price = (exit_price - lighter_leg["entry_price"]) * lighter_leg["size"]
            if not was_long:
                pnl_price = -pnl_price
            leg_pnl[lighter_leg["id"]] = {
                "exit_price": exit_price,
                "pnl_price_usd": round(pnl_price, 6),
                "fees_usd": round(lt_res.get("fee") or 0.0, 6),
            }

    if backpack_leg:
        bp_res = results[result_idx]
        if not isinstance(bp_res, Exception):
            # Пробуем взять реальную цену из ответа; если 0 — используем entry_price
            exit_price = bp_res.get("price") or backpack_leg["entry_price"]
            # Реальная комиссия из ответа, иначе считаем по тарифу: 0.04% × 2 сделки
            fees = bp_res.get("fee") or (backpack_leg["position_size_usd"] * 0.0004 * 2)
            was_long = (backpack_leg["direction"] == "LONG")
            pnl_price = (exit_price - backpack_leg["entry_price"]) * backpack_leg["size"]
            if not was_long:
                pnl_price = -pnl_price
            leg_pnl[backpack_leg["id"]] = {
                "exit_price": exit_price,
                "pnl_price_usd": round(pnl_price, 6),
                "fees_usd": round(fees, 6),
            }

    await db_close_pair(pair_id, leg_pnl if leg_pnl else None)
    logger.info(f"✅ Пара закрыта: {pair_id}")
    return {"pair_id": pair_id, "symbol": symbol}


async def scale_in_pair(pair_id: str, symbol: str, legs: list, add_size_usd: float) -> dict:
    """
    Увеличивает существующую дельта-нейтральную пару на add_size_usd (на каждую биржу).
    Обновляет средневзвешенную цену входа и размер в БД.
    """
    lighter_leg = next((l for l in legs if l["exchange"] == "BitMart"), None)
    backpack_leg = next((l for l in legs if l["exchange"] == "Backpack"), None)
    if not lighter_leg or not backpack_leg:
        raise RuntimeError("Не найдены обе ноги пары")

    bitmart = _get_bitmart()
    backpack = _get_backpack()
    lighter_is_long = (lighter_leg["direction"] == "LONG")
    backpack_is_long = (backpack_leg["direction"] == "LONG")

    lt_result, bp_result = await asyncio.gather(
        bitmart.market_open(symbol, lighter_is_long, add_size_usd),
        backpack.market_open(symbol, backpack_is_long, add_size_usd),
        return_exceptions=True,
    )

    lt_ok = not isinstance(lt_result, Exception)
    bp_ok = not isinstance(bp_result, Exception)

    # Откат при частичном сбое
    if lt_ok and not bp_ok:
        logger.error(f"Scale in Backpack не открылся: {bp_result} — откатываем BitMart")
        try:
            await bitmart.market_close(symbol, lt_result["size"], lighter_is_long)
        except Exception as e:
            logger.error(f"Не удалось откатить BitMart при scale_in: {e}")
        raise RuntimeError(f"Scale in отменён. Backpack: {bp_result}")

    if bp_ok and not lt_ok:
        logger.error(f"Scale in BitMart не открылся: {lt_result} — откатываем Backpack")
        try:
            # Передаём точный размер scale_in, чтобы не закрыть основную позицию
            await backpack.market_close(symbol, close_qty=bp_result["size"])
        except Exception as e:
            logger.error(f"Не удалось откатить Backpack при scale_in: {e}")
        raise RuntimeError(f"Scale in отменён. BitMart: {lt_result}")

    if not lt_ok and not bp_ok:
        raise RuntimeError(f"Scale in: обе ноги не открылись.\nBitMart: {lt_result}\nBackpack: {bp_result}")

    await scale_pair_db_generic(
        legs=legs,
        results_by_exchange={
            "BitMart": {"size": lt_result["size"], "price": lt_result["price"]},
            "Backpack": {"size": bp_result["size"], "price": bp_result["price"]},
        },
        add_size_usd=add_size_usd,
    )

    logger.info(f"✅ Scale in выполнен: {pair_id} +${add_size_usd}")
    return {"pair_id": pair_id, "symbol": symbol, "bitmart": lt_result, "backpack": bp_result, "added_usd": add_size_usd}


# ─── Variational + Extended (дельта-нейтральная пара) ─────────────────────────

def _get_variational():
    from core.exchanges.variational import VariationalExecutor
    if not VARIATIONAL_TOKEN or not VARIATIONAL_WALLET:
        raise RuntimeError("Variational: VARIATIONAL_TOKEN и VARIATIONAL_WALLET не заданы в .env")
    return VariationalExecutor(
        VARIATIONAL_TOKEN, VARIATIONAL_WALLET,
        VARIATIONAL_CF_CLEARANCE, VARIATIONAL_PRIVATE_KEY,
    )


def _get_extended():
    from core.exchanges.extended import ExtendedExecutor
    if not EXTENDED_API_KEY or not EXTENDED_PRIVATE_KEY:
        raise RuntimeError("Extended: EXTENDED_API_KEY и EXTENDED_PRIVATE_KEY не заданы в .env")
    return ExtendedExecutor(EXTENDED_API_KEY, EXTENDED_PUBLIC_KEY, EXTENDED_PRIVATE_KEY, EXTENDED_VAULT_ID)


async def open_pair_vr_ext(
    symbol: str,
    vr_dir: str,    # "LONG" или "SHORT"
    ext_dir: str,   # "LONG" или "SHORT"
    size_usd: float,
    entry_apr: float,
) -> dict:
    """Открывает дельта-нейтральную пару Variational + Extended одновременно."""
    vr = _get_variational()
    ext = _get_extended()

    vr_is_long = (vr_dir == "LONG")
    ext_is_long = (ext_dir == "LONG")

    logger.info(
        f"Открываем пару VR+EXT {symbol}: "
        f"Variational {'лонг' if vr_is_long else 'шорт'}, "
        f"Extended {'лонг' if ext_is_long else 'шорт'}, ${size_usd}"
    )

    vr_result, ext_result = await asyncio.gather(
        vr.market_open(symbol, vr_is_long, size_usd),
        ext.market_open(symbol, ext_is_long, size_usd),
        return_exceptions=True,
    )

    vr_ok = not isinstance(vr_result, Exception)
    ext_ok = not isinstance(ext_result, Exception)

    if vr_ok and not ext_ok:
        logger.error(f"Extended не открылся: {ext_result} — закрываем Variational")
        try:
            await vr.market_close(symbol, vr_result["size"], vr_is_long)
        except Exception as e:
            logger.error(f"Не удалось откатить Variational: {e}")
            try:
                from bot.telegram import send_message
                await send_message(
                    f"🚨 *КРИТИЧНО! НЕЗАХЕДЖИРОВАННАЯ ПОЗИЦИЯ!*\n\n"
                    f"*{symbol}* — Variational открылся, Extended упал.\n"
                    f"Автооткат Variational тоже провалился!\n\n"
                    f"❌ Extended: `{ext_result}`\n❌ Откат: `{e}`\n\n"
                    f"⚠️ *Немедленно закрой {symbol} на Variational вручную!*"
                )
            except Exception:
                pass
        raise RuntimeError(f"Extended ошибка: {ext_result}\nVariational закрыт автоматически.")

    if not vr_ok and ext_ok:
        logger.error(f"Variational не открылся: {vr_result} — закрываем Extended")
        try:
            await ext.market_close(symbol, ext_result["size"], ext_is_long)
        except Exception as e:
            logger.error(f"Не удалось откатить Extended: {e}")
            try:
                from bot.telegram import send_message
                await send_message(
                    f"🚨 *КРИТИЧНО! НЕЗАХЕДЖИРОВАННАЯ ПОЗИЦИЯ!*\n\n"
                    f"*{symbol}* — Extended открылся, Variational упал.\n"
                    f"Автооткат Extended тоже провалился!\n\n"
                    f"❌ Variational: `{vr_result}`\n❌ Откат: `{e}`\n\n"
                    f"⚠️ *Немедленно закрой {symbol} на Extended вручную!*"
                )
            except Exception:
                pass
        raise RuntimeError(f"Variational ошибка: {vr_result}\nExtended закрыт автоматически.")

    if not vr_ok and not ext_ok:
        raise RuntimeError(f"Обе ноги не открылись.\nVariational: {vr_result}\nExtended: {ext_result}")

    pair_id = f"{int(time.time())}_{symbol}_VR_EXT"
    await save_pair(pair_id, [
        {
            "symbol": symbol, "exchange": "Variational", "direction": vr_dir,
            "size": vr_result["size"], "entry_price": vr_result["price"],
            "position_size_usd": size_usd, "entry_apr": entry_apr,
        },
        {
            "symbol": symbol, "exchange": "Extended", "direction": ext_dir,
            "size": ext_result["size"], "entry_price": ext_result["price"],
            "position_size_usd": size_usd, "entry_apr": entry_apr,
        },
    ])

    logger.info(f"✅ Пара VR+EXT открыта: {pair_id}")
    return {"pair_id": pair_id, "symbol": symbol, "variational": vr_result, "extended": ext_result}


async def close_pair_vr_ext(pair_id: str, symbol: str, legs: list[dict]) -> dict:
    """Закрывает обе ноги дельта-нейтральной пары Variational + Extended."""
    vr = _get_variational()
    ext = _get_extended()

    vr_leg = next((l for l in legs if l["exchange"] == "Variational"), None)
    ext_leg = next((l for l in legs if l["exchange"] == "Extended"), None)

    tasks = []
    if vr_leg:
        tasks.append(vr.market_close(symbol, vr_leg["size"], vr_leg["direction"] == "LONG"))
    if ext_leg:
        tasks.append(ext.market_close(symbol, ext_leg["size"], ext_leg["direction"] == "LONG"))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [str(r) for r in results if isinstance(r, Exception)]

    if errors:
        raise RuntimeError(
            f"Не удалось закрыть ногу(и) пары {pair_id}:\n" + "\n".join(errors) +
            "\n⚠️ Проверь позиции на биржах вручную!"
        )

    leg_pnl: dict = {}
    result_idx = 0
    for leg, res in zip([vr_leg, ext_leg], results):
        if leg and not isinstance(res, Exception):
            exit_price = res.get("price") or leg["entry_price"]
            was_long = (leg["direction"] == "LONG")
            pnl = (exit_price - leg["entry_price"]) * leg["size"]
            if not was_long:
                pnl = -pnl
            fees = leg["position_size_usd"] * 0.0008  # ~0.08% за открытие+закрытие
            leg_pnl[leg["id"]] = {
                "exit_price": exit_price,
                "pnl_price_usd": round(pnl, 6),
                "fees_usd": round(fees, 6),
            }

    await db_close_pair(pair_id, leg_pnl if leg_pnl else None)
    logger.info(f"✅ Пара VR+EXT закрыта: {pair_id}")
    return {"pair_id": pair_id, "symbol": symbol}


async def scale_in_pair_vr_ext(pair_id: str, symbol: str, legs: list, add_size_usd: float) -> dict:
    """Увеличивает существующую VR+EXT пару на add_size_usd на каждой бирже."""
    vr_leg = next((l for l in legs if l["exchange"] == "Variational"), None)
    ext_leg = next((l for l in legs if l["exchange"] == "Extended"), None)
    if not vr_leg or not ext_leg:
        raise RuntimeError("Не найдены обе ноги VR+EXT пары")

    vr = _get_variational()
    ext = _get_extended()
    vr_is_long = (vr_leg["direction"] == "LONG")
    ext_is_long = (ext_leg["direction"] == "LONG")

    vr_result, ext_result = await asyncio.gather(
        vr.market_open(symbol, vr_is_long, add_size_usd),
        ext.market_open(symbol, ext_is_long, add_size_usd),
        return_exceptions=True,
    )

    vr_ok = not isinstance(vr_result, Exception)
    ext_ok = not isinstance(ext_result, Exception)

    if vr_ok and not ext_ok:
        logger.error(f"Scale in Extended не открылся: {ext_result} — откатываем Variational")
        try:
            await vr.market_close(symbol, vr_result["size"], vr_is_long)
        except Exception as e:
            logger.error(f"Не удалось откатить Variational при scale_in: {e}")
        raise RuntimeError(f"Scale in отменён. Extended: {ext_result}")

    if ext_ok and not vr_ok:
        logger.error(f"Scale in Variational не открылся: {vr_result} — откатываем Extended")
        try:
            await ext.market_close(symbol, ext_result["size"], ext_is_long)
        except Exception as e:
            logger.error(f"Не удалось откатить Extended при scale_in: {e}")
        raise RuntimeError(f"Scale in отменён. Variational: {vr_result}")

    if not vr_ok and not ext_ok:
        raise RuntimeError(f"Scale in: обе ноги не открылись.\nVariational: {vr_result}\nExtended: {ext_result}")

    await scale_pair_db_generic(
        legs=legs,
        results_by_exchange={
            "Variational": {"size": vr_result["size"], "price": vr_result["price"]},
            "Extended": {"size": ext_result["size"], "price": ext_result["price"]},
        },
        add_size_usd=add_size_usd,
    )

    logger.info(f"✅ Scale in VR+EXT выполнен: {pair_id} +${add_size_usd}")
    return {"pair_id": pair_id, "symbol": symbol, "variational": vr_result, "extended": ext_result, "added_usd": add_size_usd}
