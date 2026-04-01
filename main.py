import asyncio
import base64
import json
import logging
import os
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, ContextTypes, filters
from telegram.constants import ParseMode

from config import (
    SCAN_INTERVAL_SECONDS, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, POSITION_SIZE_USD,
    AUTHOR_CHANNEL, AUTHOR_CHANNEL_NAME, DONATION_WALLET_EVM, DONATION_WALLET_SOL,
    VARIATIONAL_TOKEN, VARIATIONAL_PRIVATE_KEY,
)
from scanners.hyperliquid import HyperliquidScanner
from scanners.bybit import BybitScanner
from scanners.bitmart import BitMartScanner
from scanners.backpack import BackpackScanner
from scanners.extended import ExtendedScanner
from scanners.variational import VariationalScanner
from core.analyzer import find_best_opportunities
from core.executor import (
    open_position, close_full_position,
    open_pair, close_pair, scale_in_pair,
    open_pair_vr_ext, close_pair_vr_ext, scale_in_pair_vr_ext,
)
from bot.telegram import send_opportunity, send_message, send_message_get_id, pin_message, unpin_message
from db.database import (
    init_db, get_open_positions, save_funding_snapshot, get_funding_stats,
    get_open_pairs, get_positions_by_pair, get_closed_pairs, count_closed_pairs,
    save_setting, load_setting,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_sent_signals: dict[str, tuple[float, float]] = {}
SIGNAL_COOLDOWN_HOURS = 4
APR_GROWTH_THRESHOLD = 0.5
TOP_OPPORTUNITIES_TO_SHOW = 10

# Верификация позиций: не спамить одно и то же расхождение чаще раза в 5 минут
_verify_alerts_sent: dict[str, float] = {}
VERIFY_ALERT_COOLDOWN_SECONDS = 300

# Ликвидационные алерты: не спамить чаще раза в 30 минут
_liq_alerts_sent: dict[str, float] = {}
LIQ_ALERT_COOLDOWN_SECONDS = 1800

# Фандинг в минусе: запоминаем время начала (для таймера ожидания)
_negative_funding_since: dict[str, float] = {}
# Ошибки factual funding API: не спамить одно и то же слишком часто
_funding_data_alerts_sent: dict[str, float] = {}

# Размеры позиций по типу пары
_position_sizes: dict = {
    "LT_BP": POSITION_SIZE_USD,   # BitMart × Backpack
    "VR_EXT": POSITION_SIZE_USD,  # Variational × Extended
}

# Включены ли сигналы для каждой связки
_signals_enabled: dict = {
    "LT_BP": True,
    "VR_EXT": True,
}

# Ожидаем ввод размера позиции от пользователя: None или "LT_BP"/"VR_EXT"
_waiting_for_size: str | None = None
# Ожидаем ввод суммы для scale_in: None или (pair_id, symbol)
_waiting_for_scale_in: tuple | None = None

# Защита от двойного нажатия: пары в процессе открытия
_opening_pairs: set = set()

# Текст кнопок постоянной клавиатуры
BTN_POSITIONS = "📊 Мои позиции"
BTN_SCAN = "🔍 Сканировать сейчас"
BTN_HISTORY = "📋 История"
BTN_SETTINGS = "⚙️ Настройки"
BTN_SUPPORT = "💙 Поддержать автора"

# Путь к .env для обновления токена
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

# Когда последний раз отправляли предупреждение о токене (timestamp)
_vr_token_last_warned: float = 0
# message_id запиненного предупреждения (None если не запинено)
_vr_token_pinned_msg_id: int | None = None


def _parse_jwt_exp(token: str) -> float | None:
    """Возвращает timestamp истечения JWT или None если не удалось распарсить."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload))
        return float(data.get("exp", 0)) or None
    except Exception:
        return None


def _is_jwt(text: str) -> bool:
    """Проверяет что строка похожа на JWT (три части через точку)."""
    parts = text.strip().split(".")
    return len(parts) == 3 and all(len(p) > 10 for p in parts)


def _update_env_token(new_token: str):
    """Обновляет VARIATIONAL_TOKEN в .env файле."""
    try:
        with open(_ENV_PATH, "r") as f:
            lines = f.readlines()
        updated = False
        for i, line in enumerate(lines):
            if line.startswith("VARIATIONAL_TOKEN="):
                lines[i] = f"VARIATIONAL_TOKEN={new_token}\n"
                updated = True
                break
        if not updated:
            lines.append(f"VARIATIONAL_TOKEN={new_token}\n")
        with open(_ENV_PATH, "w") as f:
            f.writelines(lines)
    except Exception as e:
        logger.error(f"Не удалось обновить .env: {e}")


async def _check_variational_token():
    """
    Проверка срока жизни Variational токена. Запускается каждый час.

    С приватником: при < 2 днях молча обновляет через SIWE.
    Без приватника: эскалирующие предупреждения + пин первого сообщения:
      - 2 дня ... 1 день  → каждые 8 часов (3 раза в сутки)
      - 1 день ... 6 часов → каждые 3 часа
      - < 6 часов          → каждый час
    """
    global _vr_token_last_warned, _vr_token_pinned_msg_id

    import config as _cfg
    token = _cfg.VARIATIONAL_TOKEN
    exp = _parse_jwt_exp(token)
    if not exp:
        return

    days_left = (exp - time.time()) / 86400

    # Токен живой — ничего делать не надо
    if days_left >= 2:
        return

    # Если есть приватник — обновляем автоматически
    if VARIATIONAL_PRIVATE_KEY:
        try:
            from config import VARIATIONAL_WALLET
            from core.exchanges.variational import _siwe_login
            new_token = await _siwe_login(VARIATIONAL_WALLET, VARIATIONAL_PRIVATE_KEY)
            _update_env_token(new_token)
            _cfg.VARIATIONAL_TOKEN = new_token
            logger.info(f"Variational: токен обновлён через SIWE (оставалось {days_left:.1f} дн.)")
        except Exception as e:
            logger.error(f"Variational: не удалось авто-обновить токен: {e}")
            await send_message(
                f"⚠️ *Variational: не удалось обновить токен автоматически*\n"
                f"Ошибка: `{e}`\n\n"
                f"Зайди на omni.variational.io → F12 → Application → Cookies → скопируй `vr-token` и отправь мне.",
            )
        return

    # Без приватника — определяем интервал предупреждений
    hours_left = days_left * 24
    if hours_left <= 6:
        warn_interval_h = 1       # каждый час
    elif hours_left <= 24:
        warn_interval_h = 3       # каждые 3 часа
    else:
        warn_interval_h = 8       # каждые 8 часов (3 раза в сутки)

    since_last = (time.time() - _vr_token_last_warned) / 3600
    if since_last < warn_interval_h:
        return  # ещё не время

    # Формируем текст предупреждения
    if hours_left <= 1:
        time_str = f"{hours_left * 60:.0f} минут"
    elif hours_left < 24:
        time_str = f"{hours_left:.0f} часов"
    else:
        time_str = f"{days_left:.0f} дня" if days_left < 2 else f"{days_left:.0f} дней"

    text = (
        f"⚠️ *Variational токен истекает через {time_str}*\n\n"
        f"Чтобы обновить:\n"
        f"1. Зайди на omni.variational.io\n"
        f"2. F12 → Application → Cookies\n"
        f"3. Скопируй значение `vr-token`\n"
        f"4. Отправь его прямо сюда в чат\n\n"
        f"Бот подхватит автоматически."
    )

    pinned_in_db = await load_setting("vr_token_pinned_msg_id", "")
    is_first_warning = not pinned_in_db

    msg_id = await send_message_get_id(text)
    _vr_token_last_warned = time.time()

    # Пинаем только первое предупреждение, сохраняем id в базу
    if is_first_warning and msg_id:
        _vr_token_pinned_msg_id = msg_id
        await save_setting("vr_token_pinned_msg_id", str(msg_id))
        await pin_message(msg_id)


ALL_SCANNERS = [
    HyperliquidScanner(),
    BybitScanner(),
    BitMartScanner(),
    BackpackScanner(),
    ExtendedScanner(),
    VariationalScanner(),
]


def persistent_keyboard():
    """Постоянная клавиатура — висит внизу чата всегда."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_POSITIONS), KeyboardButton(BTN_SCAN)],
            [KeyboardButton(BTN_HISTORY), KeyboardButton(BTN_SETTINGS)],
            [KeyboardButton(BTN_SUPPORT)],
        ],
        resize_keyboard=True,
    )


async def fetch_all_rates() -> dict:
    """Запускает все сканеры параллельно. Возвращает {exchange: [FundingRate]}."""
    results = await asyncio.gather(
        *[s.get_funding_rates() for s in ALL_SCANNERS],
        return_exceptions=True,
    )
    exchange_rates = {}
    for scanner, result in zip(ALL_SCANNERS, results):
        name = scanner.__class__.__name__.replace("Scanner", "")
        if isinstance(result, Exception):
            logger.error(f"Сканер {name}: {result}")
        elif result:
            exchange_rates[result[0].exchange] = result
    return exchange_rates


def should_send_signal(key: str, current_apr: float) -> bool:
    if key not in _sent_signals:
        return True
    prev_apr, prev_time = _sent_signals[key]
    hours_passed = (time.time() - prev_time) / 3600
    if hours_passed >= SIGNAL_COOLDOWN_HOURS:
        return True
    # Повторный сигнал только если APR значительно вырос — и только для положительных значений.
    # Для отрицательных эта проверка некорректна: -9 > -15 было бы всегда true.
    if current_apr > 0 and prev_apr > 0 and current_apr > prev_apr * (1 + APR_GROWTH_THRESHOLD):
        return True
    return False


async def _verify_positions():
    """
    Сверяет открытые пары в БД с реальными позициями на биржах.
    Если позиция есть в БД, но её нет на бирже — шлёт алерт.
    Работает тихо при ошибках API, не даёт ложных тревог.
    """
    pairs = await get_open_pairs()
    if not pairs:
        return

    # ── Backpack: получаем реальные позиции ────────────────────────────────────
    bp_map: dict | None = None  # None = не удалось получить; {} = нет позиций
    try:
        from core.executor import _get_backpack
        backpack = _get_backpack()
        bp_real = await backpack.get_positions()
        bp_map = {}
        for pos in bp_real:
            sym = pos.get("symbol", "").replace("_USDC_PERP", "").upper()
            qty = float(pos.get("netQuantity") or pos.get("quantity") or 0)
            if qty != 0:
                bp_map[sym] = qty  # + = лонг, - = шорт
    except Exception as e:
        logger.warning(f"Верификация: Backpack positions недоступны: {e}")

    # ── BitMart: получаем реальные позиции ─────────────────────────────────────
    lt_map: dict | None = None
    try:
        from core.executor import _get_bitmart
        bitmart = _get_bitmart()
        lt_real = await bitmart.get_positions()
        if lt_real is not None:
            lt_map = {}
            for pos in lt_real:
                sym = pos.get("symbol", "").replace("USDT", "").upper()
                qty = float(pos.get("current_amount") or 0)
                if int(pos.get("position_type") or 0) == 2:
                    qty = -qty
                if qty != 0:
                    lt_map[sym] = qty
    except Exception as e:
        logger.warning(f"Верификация: BitMart positions недоступны: {e}")

    # ── Сверяем с БД ───────────────────────────────────────────────────────────
    alerts = []
    for pair in pairs:
        legs = pair["legs"]
        if not legs:
            continue
        symbol = legs[0]["symbol"]
        bp_leg = next((l for l in legs if l["exchange"] == "Backpack"), None)
        lt_leg = next((l for l in legs if l["exchange"] == "BitMart"), None)

        # Проверяем Backpack
        if bp_map is not None and bp_leg:
            bp_qty = bp_map.get(symbol, 0)
            expected_long = (bp_leg["direction"] == "LONG")
            if bp_qty == 0:
                alerts.append(f"*{symbol}* Backpack: позиция исчезла (в БД: {bp_leg['direction']})")
            elif (bp_qty > 0) != expected_long:
                real_dir = "LONG" if bp_qty > 0 else "SHORT"
                alerts.append(f"*{symbol}* Backpack: направление не совпадает (БД: {bp_leg['direction']}, биржа: {real_dir})")

        # Проверяем BitMart
        if lt_map is not None and lt_leg:
            lt_qty = lt_map.get(symbol, 0)
            expected_long = (lt_leg["direction"] == "LONG")
            if lt_qty == 0:
                alerts.append(f"*{symbol}* BitMart: позиция исчезла (в БД: {lt_leg['direction']})")
            elif (lt_qty > 0) != expected_long:
                real_dir = "LONG" if lt_qty > 0 else "SHORT"
                alerts.append(f"*{symbol}* BitMart: направление не совпадает (БД: {lt_leg['direction']}, биржа: {real_dir})")

    if not alerts:
        return

    # Антиспам: не шлём одно и то же расхождение слишком часто
    alert_key = "|".join(sorted(alerts))
    last_sent = _verify_alerts_sent.get(alert_key, 0)
    if time.time() - last_sent < VERIFY_ALERT_COOLDOWN_SECONDS:
        return

    _verify_alerts_sent[alert_key] = time.time()
    await send_message(
        "🚨 *РАСХОЖДЕНИЕ ПОЗИЦИЙ!*\n\n" +
        "\n".join(f"⚠️ {a}" for a in alerts) +
        "\n\n_Проверь позиции на биржах и в боте (кнопка 📊 Мои позиции)._"
    )


async def scan_and_notify():
    """Сканируем все биржи, сохраняем историю, ищем пары BitMart × Backpack."""
    logger.info("Запуск сканирования всех бирж...")

    exchange_rates = await fetch_all_rates()
    if not exchange_rates:
        logger.error("Нет данных ни от одной биржи")
        return

    # Сохраняем историю для анализа
    await save_funding_snapshot(exchange_rates)

    # Проверяем одиночные позиции на Hyperliquid — не упал ли фандинг (если есть)
    hl_rates = exchange_rates.get("Hyperliquid", [])
    if hl_rates:
        open_positions = await get_open_positions()
        if open_positions:
            rates_map = {r.symbol: r for r in hl_rates}
            for pos in open_positions:
                # Только HL-позиции; BitMart/Backpack мониторит _monitor_open_pairs
                if pos.get("exchange") != "Hyperliquid":
                    continue
                current = rates_map.get(pos["symbol"])
                if current and abs(current.apr) < 20:
                    alert_key = f"hl_low:{pos['symbol']}"
                    if should_send_signal(alert_key, abs(current.apr)):
                        _sent_signals[alert_key] = (abs(current.apr), time.time())
                        await send_message(
                            f"⚠️ *Фандинг упал!*\n\n"
                            f"*{pos['symbol']}* — текущий APR: `{abs(current.apr):.1f}%`\n"
                            f"Рекомендую закрыть позицию.\n\n"
                            f"Нажми кнопку *{BTN_POSITIONS}* чтобы закрыть."
                        )

    # Ищем кросс-биржевые пары BitMart + Backpack (работает независимо от HL)
    lighter_rates = exchange_rates.get("BitMart", [])
    backpack_rates = exchange_rates.get("Backpack", [])
    if lighter_rates and backpack_rates:
        await _verify_positions()
        await _monitor_open_pairs(lighter_rates, backpack_rates)
        await _scan_pair_opportunities(lighter_rates, backpack_rates)
    else:
        logger.warning("BitMart или Backpack не ответили — пропускаем скан пар")

    # Ищем кросс-биржевые пары Variational + Extended
    vr_rates = exchange_rates.get("Variational", [])
    ext_rates = exchange_rates.get("Extended", [])
    if vr_rates and ext_rates:
        await _monitor_open_pairs_vr_ext(vr_rates, ext_rates)
        await _scan_pair_opportunities_vr_ext(vr_rates, ext_rates)
    else:
        logger.warning(f"Variational или Extended не ответили — пропускаем скан VR+EXT пар")


async def _monitor_open_pairs(lighter_rates: list, backpack_rates: list):
    """Мониторинг открытых пар: ликвидация и фандинг с автозакрытием при рисках."""
    from db.database import get_open_pairs
    pairs = await get_open_pairs()
    if not pairs:
        return

    lt_map = {r.symbol: r for r in lighter_rates}
    bp_map = {r.symbol: r for r in backpack_rates}

    # Получаем реальные позиции Backpack для проверки ликвидации
    bp_positions_real: dict = {}
    bitmart_funding_real: dict[str, float] = {}
    try:
        from core.executor import _get_backpack
        backpack = _get_backpack()
        bp_real_list = await backpack.get_positions()
        for pos in bp_real_list:
            sym = pos.get("symbol", "").replace("_USDC_PERP", "").upper()
            bp_positions_real[sym] = pos
    except Exception as e:
        logger.warning(f"_monitor_open_pairs: Backpack positions недоступны: {e}")

    bitmart = None
    try:
        from core.executor import _get_bitmart
        bitmart = _get_bitmart()
    except Exception as e:
        logger.warning(f"_monitor_open_pairs: BitMart funding API недоступен: {e}")

    for pair in pairs:
        legs = pair["legs"]
        if len(legs) < 2:
            continue

        pair_id = pair["pair_id"]
        symbol = legs[0]["symbol"]

        lt_rate = lt_map.get(symbol)
        bp_rate = bp_map.get(symbol)
        if not lt_rate or not bp_rate:
            continue

        lt_leg = next((l for l in legs if l["exchange"] == "BitMart"), None)
        bp_leg = next((l for l in legs if l["exchange"] == "Backpack"), None)
        if not lt_leg or not bp_leg:
            continue

        lt_dir = lt_leg["direction"]
        bp_dir = bp_leg["direction"]

        # Нетто APR: SHORT получает фандинг, LONG платит
        lt_income = lt_rate.apr if lt_dir == "SHORT" else -lt_rate.apr
        bp_income = bp_rate.apr if bp_dir == "SHORT" else -bp_rate.apr
        net_apr = lt_income + bp_income

        # ── Проверка накопленного фандинга (только фактические данные) ───────────
        bp_pos_data = bp_positions_real.get(symbol)
        bp_funding = None
        if bp_pos_data is not None:
            raw = bp_pos_data.get("cumulativeFundingPayment") or bp_pos_data.get("fundingPayment")
            if raw is not None:
                bp_funding = float(raw)

        lt_funding = None
        if bitmart and lt_leg:
            try:
                opened_at = float(lt_leg.get("opened_at") or 0)
                lt_funding = await bitmart.get_cumulative_funding_payment(symbol, since_ts=opened_at if opened_at > 0 else None)
            except Exception as e:
                logger.warning(f"_monitor_open_pairs: BitMart funding недоступен для {symbol}: {e}")

        total_funding_usd = None if (bp_funding is None or lt_funding is None) else (bp_funding + lt_funding)
        net_funding_pct = _net_funding_pct(total_funding_usd, legs)

        if net_funding_pct is not None and net_funding_pct >= FUNDING_TAKE_PROFIT_PCT:
            logger.info(
                f"Автозакрытие {symbol}: накопленный funding {net_funding_pct:.3f}% >= {FUNDING_TAKE_PROFIT_PCT:.2f}%"
            )
            _negative_funding_since.pop(pair_id, None)
            await _auto_close_pair(
                pair_id, symbol, legs,
                reason=(
                    f"накопленный чистый funding достиг `{net_funding_pct:.3f}%` "
                    f"(порог `{FUNDING_TAKE_PROFIT_PCT:.2f}%`)\n"
                    f"Примерно: `${total_funding_usd:.4f}`"
                ),
            )
            continue

        if bp_funding is None or lt_funding is None:
            alert_key = f"funding_data_missing:{pair_id}"
            last_sent = _funding_data_alerts_sent.get(alert_key, 0)
            if time.time() - last_sent >= LIQ_ALERT_COOLDOWN_SECONDS:
                _funding_data_alerts_sent[alert_key] = time.time()
                logger.info(
                    f"{symbol}: factual funding threshold пропущен — "
                    f"Backpack={'ok' if bp_funding is not None else 'нет'}; "
                    f"BitMart={'ok' if lt_funding is not None else 'нет'}"
                )

        # ── Проверка фандинга ─────────────────────────────────────────────────────
        if net_apr < NEG_APR_HARD_CLOSE:
            # Сильный минус — закрываем немедленно без ожидания
            logger.warning(f"Автозакрытие {symbol}: нетто APR={net_apr:.1f}% < {NEG_APR_HARD_CLOSE}%")
            _negative_funding_since.pop(pair_id, None)
            await _auto_close_pair(
                pair_id, symbol, legs,
                reason=f"нетто APR упал до `{net_apr:.1f}%` (порог {NEG_APR_HARD_CLOSE}%)\n"
                       f"BitMart `{lt_rate.apr:+.1f}%` | Backpack `{bp_rate.apr:+.1f}%`",
            )
            continue

        elif net_apr < 0:
            # Лёгкий минус — запускаем таймер, через 2 часа закрываем
            if pair_id not in _negative_funding_since:
                _negative_funding_since[pair_id] = time.time()
                alert_key = f"alert:{pair_id}:negative"
                if should_send_signal(alert_key, net_apr):
                    _sent_signals[alert_key] = (net_apr, time.time())
                    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                    ]])
                    wait_h = int(NEG_APR_WAIT_HOURS)
                    await send_message(
                        f"⚠️ *Фандинг ушёл в минус — {symbol}*\n\n"
                        f"BitMart `{lt_rate.apr:+.1f}%` | Backpack `{bp_rate.apr:+.1f}%`\n"
                        f"Нетто: `{net_apr:+.1f}%` APR\n\n"
                        f"Жду `{wait_h}ч` — если не восстановится, закрою автоматически.",
                        reply_markup=keyboard,
                    )
            else:
                hours_waited = (time.time() - _negative_funding_since[pair_id]) / 3600
                if hours_waited >= NEG_APR_WAIT_HOURS:
                    logger.warning(f"Автозакрытие {symbol}: фандинг в минусе {hours_waited:.1f}ч")
                    _negative_funding_since.pop(pair_id, None)
                    await _auto_close_pair(
                        pair_id, symbol, legs,
                        reason=f"нетто APR `{net_apr:+.1f}%` не восстановился за `{hours_waited:.0f}ч`",
                    )
                    continue
        else:
            # Фандинг снова плюс — сбрасываем таймер
            _negative_funding_since.pop(pair_id, None)

        # ── Проверка ликвидации: Backpack (реальная liquidationPrice) ─────────────
        if bp_pos_data:
            try:
                liq_price = float(bp_pos_data.get("liquidationPrice") or 0)
                mark_price = float(bp_pos_data.get("markPrice") or 0)
                leverage = bp_pos_data.get("leverage", "?")

                if liq_price > 0 and mark_price > 0:
                    distance_pct = abs(mark_price - liq_price) / mark_price * 100

                    if distance_pct < LIQ_AUTO_CLOSE_PCT:
                        # Опасно близко — автозакрытие
                        logger.warning(f"Автозакрытие {symbol}: до ликвидации Backpack {distance_pct:.1f}%")
                        await _auto_close_pair(
                            pair_id, symbol, legs,
                            reason=f"до ликвидации Backpack осталось `{distance_pct:.1f}%` (порог {LIQ_AUTO_CLOSE_PCT}%)\n"
                                   f"Цена: `${mark_price:.4f}` → Ликвидация: `${liq_price:.4f}` (плечо {leverage}x)",
                        )
                        continue

                    elif distance_pct < LIQ_WARN_PCT:
                        # Предупреждение — закроем автоматически при {LIQ_AUTO_CLOSE_PCT}%
                        alert_key = f"liq:{pair_id}:bp:warn"
                        last_sent = _liq_alerts_sent.get(alert_key, 0)
                        if time.time() - last_sent >= LIQ_ALERT_COOLDOWN_SECONDS:
                            _liq_alerts_sent[alert_key] = time.time()
                            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                            keyboard = InlineKeyboardMarkup([[
                                InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                            ]])
                            await send_message(
                                f"⚠️ *РИСК ЛИКВИДАЦИИ — {symbol}*\n\n"
                                f"Backpack ({bp_dir}): до ликвидации `{distance_pct:.1f}%`\n"
                                f"  Сейчас: `${mark_price:.4f}` → Ликвидация: `${liq_price:.4f}`\n"
                                f"  Плечо: `{leverage}x`\n\n"
                                f"⚠️ Закрою автоматически при `{LIQ_AUTO_CLOSE_PCT}%`",
                                reply_markup=keyboard,
                            )
            except (ValueError, TypeError) as e:
                logger.debug(f"Ошибка ликвидационной цены {symbol}: {e}")

        # ── Проверка ликвидации: BitMart (отклонение от входа) ───────────────────
        lt_entry = lt_leg.get("entry_price", 0) if lt_leg else 0
        current_price = bp_rate.mark_price if bp_rate else 0

        if lt_entry and current_price:
            if lt_dir == "LONG":
                lt_loss_pct = (lt_entry - current_price) / lt_entry * 100
            else:
                lt_loss_pct = (current_price - lt_entry) / lt_entry * 100

            direction_str = "упала" if lt_dir == "LONG" else "выросла"

            if lt_loss_pct >= LT_AUTO_CLOSE_PCT:
                # Сильное отклонение — автозакрытие
                logger.warning(f"Автозакрытие {symbol}: BitMart цена {direction_str} на {lt_loss_pct:.1f}%")
                await _auto_close_pair(
                    pair_id, symbol, legs,
                    reason=f"BitMart ({lt_dir}): цена {direction_str} на `{lt_loss_pct:.1f}%` от входа (порог {LT_AUTO_CLOSE_PCT}%)\n"
                           f"Вход: `${lt_entry:.4f}` → Сейчас: `${current_price:.4f}`",
                )
                continue

            elif lt_loss_pct > LT_WARN_MOVE_PCT:
                # Предупреждение
                alert_key = f"liq:{pair_id}:lt:warn"
                last_sent = _liq_alerts_sent.get(alert_key, 0)
                if time.time() - last_sent >= LIQ_ALERT_COOLDOWN_SECONDS:
                    _liq_alerts_sent[alert_key] = time.time()
                    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                    ]])
                    await send_message(
                        f"⚠️ *РИСК ЛИКВИДАЦИИ — {symbol}*\n\n"
                        f"BitMart ({lt_dir}): цена {direction_str} на `{lt_loss_pct:.1f}%` от входа\n"
                        f"  Вход: `${lt_entry:.4f}` → Сейчас: `${current_price:.4f}`\n\n"
                        f"⚠️ _Точную цену ликвидации BitMart тут не используем, следим по отклонению от входа._\n"
                        f"Закрою автоматически при `{LT_AUTO_CLOSE_PCT}%`",
                        reply_markup=keyboard,
                    )


MIN_PAIR_APR = 50.0       # минимальный нетто APR для уведомления о паре
MIN_VOLUME_USD = 50_000   # минимальный дневной объём на каждой бирже ($)

LIQ_WARN_PCT = 20.0        # % расстояния до ликвидации (Backpack) → предупреждение
LIQ_AUTO_CLOSE_PCT = 15.0  # % расстояния до ликвидации (Backpack) → АВТОЗАКРЫТИЕ
LT_WARN_MOVE_PCT = 10.0    # % отклонения цены против BitMart ноги → предупреждение
LT_AUTO_CLOSE_PCT = 15.0   # % отклонения цены против BitMart ноги → АВТОЗАКРЫТИЕ
FUNDING_TAKE_PROFIT_PCT = 0.15  # накопленный чистый funding % → АВТОЗАКРЫТИЕ

NEG_APR_HARD_CLOSE = -50.0  # APR пары ниже этого → АВТОЗАКРЫТИЕ немедленно
NEG_APR_WAIT_HOURS = 4.0    # часов ожидания при мягком минусе перед автозакрытием

BITMART_FEE_SITE_RATE = 0.0006   # 0.06% на сайте
BITMART_FEE_DISCOUNT = 0.70      # скидка 70% от биржевого тарифа
BACKPACK_FEE_SITE_RATE = 0.0004  # 0.04% на сайте


def _net_funding_pct(total_funding_usd: float | None, legs: list[dict]) -> float | None:
    """Переводит накопленный funding в обычный % от размера одной ноги."""
    if total_funding_usd is None or not legs:
        return None
    base_usd = min(float(l.get("position_size_usd") or 0) for l in legs)
    if base_usd <= 0:
        return None
    return (total_funding_usd / base_usd) * 100


def _estimated_leg_funding_usd(leg: dict | None, rate_obj, opened_ago_h: float) -> float | None:
    """Оценка накопленного funding в USD по текущей ставке."""
    if not leg or not rate_obj:
        return None
    sign = 1 if leg["direction"] == "SHORT" else -1
    return sign * rate_obj.rate * opened_ago_h * leg["position_size_usd"]


async def _try_reopen_if_still_top(symbol: str):
    """После автозакрытия: если пара всё ещё в топе возможностей, открываем заново."""
    if not _signals_enabled.get("LT_BP", True):
        return

    try:
        lt_rates, bp_rates = await asyncio.gather(
            BitMartScanner().get_funding_rates(),
            BackpackScanner().get_funding_rates(),
        )
    except Exception as e:
        logger.warning(f"Автопереоткрытие {symbol}: не удалось получить ставки: {e}")
        return

    lt_map = {r.symbol: r for r in lt_rates}
    bp_map = {r.symbol: r for r in bp_rates}

    opps = []
    for sym, lt in lt_map.items():
        bp = bp_map.get(sym)
        if not bp:
            continue
        if lt.apr == 0 and bp.apr == 0:
            continue
        if abs(lt.apr) > 2000 or abs(bp.apr) > 2000:
            continue
        if lt.volume_usd < MIN_VOLUME_USD or bp.volume_usd < MIN_VOLUME_USD:
            continue

        if lt.apr * bp.apr < 0:
            net_apr = abs(lt.apr) + abs(bp.apr)
            lt_dir = "SHORT" if lt.apr > 0 else "LONG"
            bp_dir = "SHORT" if bp.apr > 0 else "LONG"
        else:
            net_apr = abs(abs(lt.apr) - abs(bp.apr))
            if abs(lt.apr) >= abs(bp.apr):
                lt_dir = "SHORT" if lt.apr > 0 else "LONG"
                bp_dir = "LONG" if bp.apr > 0 else "SHORT"
            else:
                lt_dir = "LONG" if lt.apr > 0 else "SHORT"
                bp_dir = "SHORT" if bp.apr > 0 else "LONG"

        if net_apr < MIN_PAIR_APR:
            continue

        opps.append({
            "symbol": sym,
            "net_apr": net_apr,
            "lt_dir": lt_dir,
            "bp_dir": bp_dir,
        })

    if not opps:
        return

    opps.sort(key=lambda x: x["net_apr"], reverse=True)
    top = opps[:TOP_OPPORTUNITIES_TO_SHOW]
    current = next((o for o in top if o["symbol"] == symbol), None)
    if not current:
        return

    size_usd = float(_position_sizes.get("LT_BP", POSITION_SIZE_USD))
    try:
        result = await open_pair(
            symbol=symbol,
            lighter_dir=current["lt_dir"],
            backpack_dir=current["bp_dir"],
            size_usd=size_usd,
            entry_apr=current["net_apr"],
        )
        await send_message(
            f"♻️ *Пара переоткрыта — {symbol}*\n\n"
            f"После автозакрытия пара осталась в топ-возможностях.\n"
            f"Направления: BitMart `{current['lt_dir']}` | Backpack `{current['bp_dir']}`\n"
            f"Нетто APR: `~{current['net_apr']:.1f}%`\n"
            f"Размер: `${size_usd:.0f}` на ногу\n\n"
            f"Pair ID: `{result.get('pair_id', 'n/a')}`"
        )
    except Exception as e:
        logger.warning(f"Автопереоткрытие {symbol} не удалось: {e}")
        await send_message(
            f"⚠️ *Автопереоткрытие не удалось — {symbol}*\n\n"
            f"Ошибка: `{e}`"
        )


async def _auto_close_pair(pair_id: str, symbol: str, legs: list, reason: str):
    """Автоматически закрывает пару и уведомляет в Telegram."""
    try:
        if not legs:
            legs = await get_positions_by_pair(pair_id)
        await close_pair(pair_id=pair_id, symbol=symbol, legs=legs)
        await send_message(
            f"🤖 *АВТОЗАКРЫТИЕ — {symbol}*\n\n"
            f"Причина: {reason}\n\n"
            f"✅ Пара закрыта автоматически."
        )
        logger.info(f"Автозакрытие {pair_id} ({symbol}): успешно")

        # По запросу: если после закрытия пара всё ещё в топе возможностей — открыть заново
        await _try_reopen_if_still_top(symbol)
    except Exception as e:
        logger.error(f"Автозакрытие {pair_id} ({symbol}) провалилось: {e}")
        await send_message(
            f"🚨 *АВТОЗАКРЫТИЕ ПРОВАЛИЛОСЬ — {symbol}!*\n\n"
            f"Причина закрытия: {reason}\n\n"
            f"❌ Ошибка: `{e}`\n\n"
            f"⚠️ *Закрой пару вручную немедленно!*"
        )


async def _scan_pair_opportunities(lighter_rates: list, backpack_rates: list):
    """Ищет возможности BitMart + Backpack и отправляет уведомления."""
    if not _signals_enabled["LT_BP"]:
        return
    lt_map = {r.symbol: r for r in lighter_rates}
    bp_map = {r.symbol: r for r in backpack_rates}

    opps = []
    for symbol, lt in lt_map.items():
        bp = bp_map.get(symbol)
        if not bp:
            continue
        if lt.apr == 0 and bp.apr == 0:
            continue
        if abs(lt.apr) > 2000 or abs(bp.apr) > 2000:
            continue
        if lt.volume_usd < MIN_VOLUME_USD or bp.volume_usd < MIN_VOLUME_USD:
            continue

        if lt.apr * bp.apr < 0:
            net_apr = abs(lt.apr) + abs(bp.apr)
            lt_dir = "SHORT" if lt.apr > 0 else "LONG"
            bp_dir = "SHORT" if bp.apr > 0 else "LONG"
        else:
            net_apr = abs(abs(lt.apr) - abs(bp.apr))
            if abs(lt.apr) >= abs(bp.apr):
                lt_dir = "SHORT" if lt.apr > 0 else "LONG"
                bp_dir = "LONG" if bp.apr > 0 else "SHORT"
            else:
                lt_dir = "LONG" if lt.apr > 0 else "SHORT"
                bp_dir = "SHORT" if bp.apr > 0 else "LONG"

        if net_apr < MIN_PAIR_APR:
            continue

        opps.append({
            "symbol": symbol, "net_apr": net_apr,
            "lt_apr": lt.apr, "lt_dir": lt_dir,
            "bp_apr": bp.apr, "bp_dir": bp_dir,
        })

    if not opps:
        return

    opps.sort(key=lambda x: x["net_apr"], reverse=True)
    logger.info(f"BitMart×Backpack: найдено {len(opps)} возможностей")

    for opp in opps[:TOP_OPPORTUNITIES_TO_SHOW]:
        signal_key = f"LT_BP:{opp['symbol']}:{opp['lt_dir']}:{opp['bp_dir']}"
        if not should_send_signal(signal_key, opp["net_apr"]):
            continue

        lt_label = "шорт ↓" if opp["lt_dir"] == "SHORT" else "лонг ↑"
        bp_label = "шорт ↓" if opp["bp_dir"] == "SHORT" else "лонг ↑"

        text = (
            f"🔀 *{opp['symbol']}* — BitMart × Backpack\n\n"
            f"  BitMart ({lt_label}): `{opp['lt_apr']:+.1f}%`\n"
            f"  Backpack ({bp_label}): `{opp['bp_apr']:+.1f}%`\n"
            f"  📈 Нетто: `~{opp['net_apr']:.1f}% APR`\n\n"
            f"  💸 BitMart: 0.06% | Backpack: 0.04%"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "✅ Открыть пару",
                callback_data=f"open_pair:BitMart:Backpack:{opp['symbol']}:{opp['lt_dir']}:{opp['bp_dir']}"
            ),
            InlineKeyboardButton("❌ Пропустить", callback_data="skip"),
        ]])
        await send_message(text, reply_markup=keyboard)
        _sent_signals[signal_key] = (opp["net_apr"], time.time())


async def _monitor_open_pairs_vr_ext(vr_rates: list, ext_rates: list):
    """Мониторинг открытых VR+EXT пар: фандинг, цена, автозакрытие и алерты."""
    pairs = await get_open_pairs()
    vr_ext_pairs = [
        p for p in pairs
        if len(p.get("legs", [])) == 2
        and {l["exchange"] for l in p["legs"]} == {"Variational", "Extended"}
    ]
    if not vr_ext_pairs:
        return

    vr_map = {r.symbol: r for r in vr_rates}
    ext_map = {r.symbol: r for r in ext_rates}

    for group in vr_ext_pairs:
        legs = group["legs"]
        pair_id = group["pair_id"]
        vr_leg = next(l for l in legs if l["exchange"] == "Variational")
        ext_leg = next(l for l in legs if l["exchange"] == "Extended")
        symbol = vr_leg["symbol"]
        opened_at = min(l["opened_at"] for l in legs)
        opened_ago_h = (time.time() - opened_at) / 3600

        vr_rate = vr_map.get(symbol)
        ext_rate = ext_map.get(symbol)
        if not vr_rate or not ext_rate:
            continue

        vr_dir = vr_leg["direction"]
        ext_dir = ext_leg["direction"]
        vr_income = vr_rate.apr if vr_dir == "SHORT" else -vr_rate.apr
        ext_income = ext_rate.apr if ext_dir == "SHORT" else -ext_rate.apr
        net_apr = vr_income + ext_income

        # ── Проверка APR ─────────────────────────────────────────────────────
        if net_apr < NEG_APR_HARD_CLOSE:
            logger.warning(f"Автозакрытие VR+EXT {symbol}: нетто APR={net_apr:.1f}%")
            _negative_funding_since.pop(pair_id, None)
            await _auto_close_pair_vr_ext(
                pair_id, symbol, legs,
                reason=f"нетто APR упал до `{net_apr:.1f}%` (порог {NEG_APR_HARD_CLOSE}%)\n"
                       f"Variational `{vr_rate.apr:+.1f}%` | Extended `{ext_rate.apr:+.1f}%`",
            )
            continue

        elif net_apr < 0:
            if pair_id not in _negative_funding_since:
                _negative_funding_since[pair_id] = time.time()
                await send_message(
                    f"⚠️ *Фандинг стал отрицательным — {symbol}* (VR+EXT)\n\n"
                    f"Нетто APR: `{net_apr:+.1f}%`\n"
                    f"Variational `{vr_rate.apr:+.1f}%` | Extended `{ext_rate.apr:+.1f}%`\n"
                    f"Открыта `{opened_ago_h:.1f}ч` назад\n\n"
                    f"_Жду {int(NEG_APR_WAIT_HOURS)}ч — если не восстановится, закрою автоматически._",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                    ]]),
                )
            else:
                hours_waited = (time.time() - _negative_funding_since[pair_id]) / 3600
                if hours_waited >= NEG_APR_WAIT_HOURS:
                    logger.warning(f"Автозакрытие VR+EXT {symbol}: фандинг в минусе {hours_waited:.1f}ч")
                    _negative_funding_since.pop(pair_id, None)
                    await _auto_close_pair_vr_ext(
                        pair_id, symbol, legs,
                        reason=f"нетто APR `{net_apr:+.1f}%` не восстановился за `{hours_waited:.0f}ч`",
                    )
                    continue
        else:
            _negative_funding_since.pop(pair_id, None)

        # ── Проверка отклонения цены от входа (Variational нога) ─────────────
        vr_entry = vr_leg.get("entry_price", 0)
        vr_cur = vr_rate.mark_price
        if vr_entry and vr_cur:
            vr_move = (vr_cur - vr_entry) / vr_entry * 100 if vr_dir == "SHORT" else (vr_entry - vr_cur) / vr_entry * 100
            direction_str = "выросла" if vr_dir == "SHORT" else "упала"
            if vr_move >= LT_AUTO_CLOSE_PCT:
                logger.warning(f"Автозакрытие VR+EXT {symbol}: Variational цена {direction_str} на {vr_move:.1f}%")
                await _auto_close_pair_vr_ext(
                    pair_id, symbol, legs,
                    reason=f"Variational ({vr_dir}): цена {direction_str} на `{vr_move:.1f}%` от входа (порог {LT_AUTO_CLOSE_PCT}%)\n"
                           f"Вход: `${vr_entry:.4f}` → Сейчас: `${vr_cur:.4f}`",
                )
                continue
            elif vr_move > LT_WARN_MOVE_PCT:
                alert_key = f"liq:{pair_id}:vr:warn"
                if time.time() - _liq_alerts_sent.get(alert_key, 0) >= LIQ_ALERT_COOLDOWN_SECONDS:
                    _liq_alerts_sent[alert_key] = time.time()
                    await send_message(
                        f"⚠️ *РИСК ЛИКВИДАЦИИ — {symbol}* (VR+EXT)\n\n"
                        f"Variational ({vr_dir}): цена {direction_str} на `{vr_move:.1f}%` от входа\n"
                        f"  Вход: `${vr_entry:.4f}` → Сейчас: `${vr_cur:.4f}`\n\n"
                        f"⚠️ Закрою автоматически при `{LT_AUTO_CLOSE_PCT}%`",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                        ]]),
                    )

        # ── Проверка отклонения цены от входа (Extended нога) ────────────────
        ext_entry = ext_leg.get("entry_price", 0)
        ext_cur = ext_rate.mark_price
        if ext_entry and ext_cur:
            ext_move = (ext_cur - ext_entry) / ext_entry * 100 if ext_dir == "SHORT" else (ext_entry - ext_cur) / ext_entry * 100
            direction_str = "выросла" if ext_dir == "SHORT" else "упала"
            if ext_move >= LT_AUTO_CLOSE_PCT:
                logger.warning(f"Автозакрытие VR+EXT {symbol}: Extended цена {direction_str} на {ext_move:.1f}%")
                await _auto_close_pair_vr_ext(
                    pair_id, symbol, legs,
                    reason=f"Extended ({ext_dir}): цена {direction_str} на `{ext_move:.1f}%` от входа (порог {LT_AUTO_CLOSE_PCT}%)\n"
                           f"Вход: `${ext_entry:.4f}` → Сейчас: `${ext_cur:.4f}`",
                )
                continue
            elif ext_move > LT_WARN_MOVE_PCT:
                alert_key = f"liq:{pair_id}:ext:warn"
                if time.time() - _liq_alerts_sent.get(alert_key, 0) >= LIQ_ALERT_COOLDOWN_SECONDS:
                    _liq_alerts_sent[alert_key] = time.time()
                    await send_message(
                        f"⚠️ *РИСК ЛИКВИДАЦИИ — {symbol}* (VR+EXT)\n\n"
                        f"Extended ({ext_dir}): цена {direction_str} на `{ext_move:.1f}%` от входа\n"
                        f"  Вход: `${ext_entry:.4f}` → Сейчас: `${ext_cur:.4f}`\n\n"
                        f"⚠️ Закрою автоматически при `{LT_AUTO_CLOSE_PCT}%`",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                        ]]),
                    )


async def _auto_close_pair_vr_ext(pair_id: str, symbol: str, legs: list, reason: str):
    """Автоматически закрывает VR+EXT пару и уведомляет."""
    try:
        await close_pair_vr_ext(pair_id=pair_id, symbol=symbol, legs=legs)
        await send_message(
            f"🔴 *АВТОЗАКРЫТИЕ — {symbol}* (VR+EXT)\n\n"
            f"Причина: {reason}\n\n✅ Пара закрыта."
        )
    except Exception as e:
        logger.error(f"Ошибка автозакрытия VR+EXT {pair_id}: {e}")
        await send_message(
            f"🆘 *АВТОЗАКРЫТИЕ ПРОВАЛИЛОСЬ — {symbol}* (VR+EXT)\n\n"
            f"Причина закрытия: {reason}\n\n"
            f"❌ Ошибка: `{e}`\n"
            f"⚠️ *Закрой пару вручную немедленно!*"
        )


async def _scan_pair_opportunities_vr_ext(vr_rates: list, ext_rates: list):
    """Ищет возможности Variational + Extended и отправляет уведомления."""
    if not _signals_enabled["VR_EXT"]:
        return
    vr_map = {r.symbol: r for r in vr_rates}
    ext_map = {r.symbol: r for r in ext_rates}

    opps = []
    for symbol, vr in vr_map.items():
        ext = ext_map.get(symbol)
        if not ext:
            continue
        # Фильтр мусорных символов: минимум 2 символа и хотя бы одна буква
        if len(symbol) < 2 or not any(c.isalpha() for c in symbol):
            continue
        if vr.apr == 0 and ext.apr == 0:
            continue
        # Хотя бы одна нога должна давать реальный фандинг
        if abs(vr.apr) < 1 and abs(ext.apr) < 1:
            continue
        if abs(vr.apr) > 2000 or abs(ext.apr) > 2000:
            continue
        # Фильтр по объёму: Extended должен иметь минимальный дневной объём
        if ext.volume_usd < MIN_VOLUME_USD:
            continue

        if vr.apr * ext.apr < 0:
            net_apr = abs(vr.apr) + abs(ext.apr)
            vr_dir = "SHORT" if vr.apr > 0 else "LONG"
            ext_dir = "SHORT" if ext.apr > 0 else "LONG"
        else:
            net_apr = abs(abs(vr.apr) - abs(ext.apr))
            if abs(vr.apr) >= abs(ext.apr):
                vr_dir = "SHORT" if vr.apr > 0 else "LONG"
                ext_dir = "LONG" if ext.apr > 0 else "SHORT"
            else:
                vr_dir = "LONG" if vr.apr > 0 else "SHORT"
                ext_dir = "SHORT" if ext.apr > 0 else "LONG"

        if net_apr < MIN_PAIR_APR:
            continue

        opps.append({
            "symbol": symbol, "net_apr": net_apr,
            "vr_apr": vr.apr, "vr_dir": vr_dir,
            "ext_apr": ext.apr, "ext_dir": ext_dir,
        })

    if not opps:
        return

    opps.sort(key=lambda x: x["net_apr"], reverse=True)
    logger.info(f"Variational×Extended: найдено {len(opps)} возможностей")

    for opp in opps:
        signal_key = f"VR_EXT:{opp['symbol']}:{opp['vr_dir']}:{opp['ext_dir']}"
        if not should_send_signal(signal_key, opp["net_apr"]):
            continue

        vr_label = "шорт ↓" if opp["vr_dir"] == "SHORT" else "лонг ↑"
        ext_label = "шорт ↓" if opp["ext_dir"] == "SHORT" else "лонг ↑"

        text = (
            f"🔀 *{opp['symbol']}* — Variational × Extended\n\n"
            f"  Variational ({vr_label}): `{opp['vr_apr']:+.1f}%`\n"
            f"  Extended ({ext_label}): `{opp['ext_apr']:+.1f}%`\n"
            f"  📈 Нетто: `~{opp['net_apr']:.1f}% APR`\n\n"
            f"  💸 Variational: 0% комиссия | Extended: 0.025%"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "✅ Открыть пару",
                callback_data=f"open_pair:Variational:Extended:{opp['symbol']}:{opp['vr_dir']}:{opp['ext_dir']}"
            ),
            InlineKeyboardButton("❌ Пропустить", callback_data="skip"),
        ]])
        await send_message(text, reply_markup=keyboard)
        _sent_signals[signal_key] = (opp["net_apr"], time.time())


async def show_settings(update: Update):
    """Показывает меню настроек с размерами позиций и переключателями сигналов."""
    lt_bp = _position_sizes["LT_BP"]
    vr_ext = _position_sizes["VR_EXT"]
    lt_bp_on = _signals_enabled["LT_BP"]
    vr_ext_on = _signals_enabled["VR_EXT"]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("── Биржи ──", callback_data="noop")],
        [
            InlineKeyboardButton(
                f"{'✅' if lt_bp_on else '❌'} BitMart × Backpack",
                callback_data="toggle_signals:LT_BP"
            ),
            InlineKeyboardButton(
                f"{'✅' if vr_ext_on else '❌'} Variational × Extended",
                callback_data="toggle_signals:VR_EXT"
            ),
        ],
        [InlineKeyboardButton("── Размер позиций ──", callback_data="noop")],
        [InlineKeyboardButton(f"── BitMart × Backpack (${lt_bp:.0f}) ──", callback_data="noop")],
        [
            InlineKeyboardButton("$15",   callback_data="setsize:LT_BP:15"),
            InlineKeyboardButton("$50",   callback_data="setsize:LT_BP:50"),
            InlineKeyboardButton("$100",  callback_data="setsize:LT_BP:100"),
            InlineKeyboardButton("$250",  callback_data="setsize:LT_BP:250"),
            InlineKeyboardButton("$500",  callback_data="setsize:LT_BP:500"),
        ],
        [InlineKeyboardButton("✏️ Ввести вручную (BM+BP)", callback_data="setsize:LT_BP:manual")],
        [InlineKeyboardButton(f"── Variational × Extended (${vr_ext:.0f}) ──", callback_data="noop")],
        [
            InlineKeyboardButton("$15",   callback_data="setsize:VR_EXT:15"),
            InlineKeyboardButton("$50",   callback_data="setsize:VR_EXT:50"),
            InlineKeyboardButton("$100",  callback_data="setsize:VR_EXT:100"),
            InlineKeyboardButton("$250",  callback_data="setsize:VR_EXT:250"),
            InlineKeyboardButton("$500",  callback_data="setsize:VR_EXT:500"),
        ],
        [InlineKeyboardButton("✏️ Ввести вручную (VR+EXT)", callback_data="setsize:VR_EXT:manual")],
    ])
    await update.message.reply_text(
        f"⚙️ *Настройки*\n\n"
        f"BitMart × Backpack: `${lt_bp:.0f}` на каждую ногу\n"
        f"Variational × Extended: `${vr_ext:.0f}` на каждую ногу\n\n"
        f"Выбери размер или включи/выключи сигналы:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard,
    )


async def show_positions(update: Update):
    """Показывает открытые позиции (одиночные и пары)."""
    position_groups = await get_open_pairs()

    if not position_groups:
        await update.message.reply_text("📭 Открытых позиций нет")
        return

    # Запрашиваем все данные параллельно — ставки + реальные позиции Backpack
    async def _fetch_lt_rates():
        try:
            return await BitMartScanner().get_funding_rates()
        except Exception:
            return []

    async def _fetch_bp_rates():
        try:
            return await BackpackScanner().get_funding_rates()
        except Exception:
            return []

    async def _fetch_hl_rates():
        try:
            return await HyperliquidScanner().get_funding_rates()
        except Exception:
            return []

    async def _fetch_bp_positions():
        try:
            from core.executor import _get_backpack
            backpack = _get_backpack()
            return await backpack.get_positions()
        except Exception as e:
            logger.warning(f"show_positions: Backpack positions недоступны: {e}")
            return []

    async def _fetch_vr_rates():
        try:
            return await VariationalScanner().get_funding_rates()
        except Exception:
            return []

    async def _fetch_ext_rates():
        try:
            return await ExtendedScanner().get_funding_rates()
        except Exception:
            return []

    lt_rates, bp_rates, hl_rates, bp_positions_raw, vr_rates, ext_rates = await asyncio.gather(
        _fetch_lt_rates(),
        _fetch_bp_rates(),
        _fetch_hl_rates(),
        _fetch_bp_positions(),
        _fetch_vr_rates(),
        _fetch_ext_rates(),
    )

    rates_map = {f"BitMart:{r.symbol}": r for r in lt_rates}
    rates_map.update({f"Backpack:{r.symbol}": r for r in bp_rates})
    rates_map.update({f"Hyperliquid:{r.symbol}": r for r in hl_rates})
    rates_map.update({f"Variational:{r.symbol}": r for r in vr_rates})
    rates_map.update({f"Extended:{r.symbol}": r for r in ext_rates})

    # symbol → полный dict позиции с Backpack API
    bp_real_map: dict = {}
    for pos in bp_positions_raw:
        sym = pos.get("symbol", "").replace("_USDC_PERP", "").upper()
        bp_real_map[sym] = pos

    for group in position_groups:
        legs = group["legs"]
        pair_id = group["pair_id"]
        symbol = legs[0]["symbol"]
        opened_ago = (time.time() - legs[0]["opened_at"]) / 3600

        if pair_id and len(legs) == 2:
            # Двуногая дельта-нейтральная пара
            leg_a, leg_b = legs[0], legs[1]
            total_usd = leg_a["position_size_usd"] + leg_b["position_size_usd"]
            pair_exchanges = {l["exchange"] for l in legs}
            is_vr_ext = pair_exchanges == {"Variational", "Extended"}

            if is_vr_ext:
                # ── VR+EXT пара ───────────────────────────────────────────────
                vr_leg = next((l for l in legs if l["exchange"] == "Variational"), None)
                ext_leg = next((l for l in legs if l["exchange"] == "Extended"), None)
                vr_rate = rates_map.get(f"Variational:{symbol}")
                ext_rate = rates_map.get(f"Extended:{symbol}")

                vr_income_apr = 0.0
                if vr_leg and vr_rate:
                    vr_income_apr = vr_rate.apr if vr_leg["direction"] == "SHORT" else -vr_rate.apr
                ext_income_apr = 0.0
                if ext_leg and ext_rate:
                    ext_income_apr = ext_rate.apr if ext_leg["direction"] == "SHORT" else -ext_rate.apr
                net_apr = vr_income_apr + ext_income_apr
                apr_status = "🟢" if net_apr >= 50 else "🟡" if net_apr >= 0 else "🔴"

                vr_apr_str = f"`{vr_rate.apr:+.1f}%`" if vr_rate else "_нет данных_"
                ext_apr_str = f"`{ext_rate.apr:+.1f}%`" if ext_rate else "_нет данных_"
                dir_a = "шорт ↓" if leg_a["direction"] == "SHORT" else "лонг ↑"
                dir_b = "шорт ↓" if leg_b["direction"] == "SHORT" else "лонг ↑"

                # ── Заработано (оценка по текущему фандингу × время) ─────────
                vr_earned = _estimated_leg_funding_usd(vr_leg, vr_rate, opened_ago)
                ext_earned = _estimated_leg_funding_usd(ext_leg, ext_rate, opened_ago)
                net_funding_pct = _net_funding_pct(
                    None if (vr_earned is None or ext_earned is None) else (vr_earned + ext_earned),
                    legs,
                )

                if vr_earned is not None and ext_earned is not None:
                    total_earned = vr_earned + ext_earned
                    earned_str = (
                        f"`${total_earned:.4f}` (~оценка, `{net_funding_pct:.3f}%`)" if net_funding_pct is not None else f"`${total_earned:.4f}` (~оценка)"
                    )
                    earned_str += (
                        f"\n  ├ Variational: `${vr_earned:.4f}` (~)\n"
                        f"  └ Extended: `${ext_earned:.4f}` (~)"
                    )
                else:
                    earned_str = "_нет данных_"

                # ── Отклонение цены от входа ──────────────────────────────────
                price_risk_lines = ""
                for leg, leg_name, rate_obj in [
                    (vr_leg, "VR", vr_rate), (ext_leg, "EXT", ext_rate)
                ]:
                    if not leg or not rate_obj or not rate_obj.mark_price:
                        continue
                    entry = leg.get("entry_price", 0)
                    if not entry:
                        continue
                    cur = rate_obj.mark_price
                    direction = leg["direction"]
                    move_pct = (cur - entry) / entry * 100 if direction == "SHORT" else (entry - cur) / entry * 100
                    if move_pct > 0:
                        m_emoji = "🔴" if move_pct >= LT_AUTO_CLOSE_PCT else "🟡" if move_pct >= LT_WARN_MOVE_PCT else "🟢"
                        price_risk_lines += f"\n{m_emoji} {leg_name}: `-{move_pct:.1f}%` против позиции"
                    else:
                        price_risk_lines += f"\n🟢 {leg_name}: `+{abs(move_pct):.1f}%` в пользу позиции"

                text = (
                    f"🔀 *{symbol}* — {leg_a['exchange']} × {leg_b['exchange']} {apr_status}\n\n"
                    f"  {leg_a['exchange']} ({dir_a}): `${leg_a['entry_price']:.4f}`\n"
                    f"  {leg_b['exchange']} ({dir_b}): `${leg_b['entry_price']:.4f}`\n"
                    f"💵 Размер: `${total_usd:.0f}` (по `${leg_a['position_size_usd']:.0f}` на каждую)\n"
                    f"⏱ Открыта: `{opened_ago:.1f}ч назад`\n"
                    f"📊 APR сейчас: Variational {vr_apr_str} | Extended {ext_apr_str}\n"
                    f"  └ Нетто: `{net_apr:+.1f}%`\n"
                    f"💰 Заработано: {earned_str}"
                    + price_risk_lines
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("➕ Добавить", callback_data=f"scale_in:{pair_id}:{symbol}"),
                    InlineKeyboardButton("❌ Закрыть пару", callback_data=f"close_pair:{pair_id}:{symbol}"),
                ]])
                await send_message(text, reply_markup=keyboard)
                continue

            # ── Backpack: берём реальный cumulativeFundingPayment если доступен ──
            bp_leg = next((l for l in legs if l["exchange"] == "Backpack"), None)
            lt_leg = next((l for l in legs if l["exchange"] == "BitMart"), None)
            bp_funding = None
            bp_data = bp_real_map.get(symbol)
            if bp_data is not None:
                raw = bp_data.get("cumulativeFundingPayment") or bp_data.get("fundingPayment")
                if raw is not None:
                    bp_funding = float(raw)

            # Расстояние до ликвидации (Backpack — реальная liquidationPrice)
            bp_liq_info = ""
            if bp_data:
                try:
                    liq_price = float(bp_data.get("liquidationPrice") or 0)
                    bp_mark = float(bp_data.get("markPrice") or 0)
                    if liq_price > 0 and bp_mark > 0:
                        dist_pct = abs(bp_mark - liq_price) / bp_mark * 100
                        liq_emoji = "🔴" if dist_pct < LIQ_AUTO_CLOSE_PCT else "🟡" if dist_pct < LIQ_WARN_PCT else "🟢"
                        bp_liq_info = f"\n{liq_emoji} До ликвидации (BP): `{dist_pct:.1f}%`"
                except (ValueError, TypeError):
                    pass

            # Отклонение цены для BitMart ноги
            lt_liq_info = ""
            if lt_leg:
                try:
                    lt_entry = lt_leg.get("entry_price", 0)
                    # Текущая цена: берём с Backpack (тот же актив)
                    cur_price = float(bp_data.get("markPrice") or 0) if bp_data else 0
                    if lt_entry and cur_price:
                        lt_dir_str = lt_leg["direction"]
                        if lt_dir_str == "LONG":
                            lt_move = (lt_entry - cur_price) / lt_entry * 100
                        else:
                            lt_move = (cur_price - lt_entry) / lt_entry * 100
                        if lt_move > 0:
                            m_emoji = "🔴" if lt_move >= LT_AUTO_CLOSE_PCT else "🟡" if lt_move >= LT_WARN_MOVE_PCT else "🟢"
                            lt_liq_info = f"\n{m_emoji} BitMart: `-{lt_move:.1f}%` против позиции"
                        else:
                            lt_liq_info = f"\n🟢 BitMart: `+{abs(lt_move):.1f}%` в пользу позиции"
                except (ValueError, TypeError):
                    pass

            # Если реальных данных нет — оцениваем с учётом направления
            if bp_funding is None and bp_leg:
                key = f"Backpack:{bp_leg['symbol']}"
                r = rates_map.get(key)
                if r:
                    bp_funding = _estimated_leg_funding_usd(bp_leg, r, opened_ago)

            # ── BitMart: оценка заработка по текущему фандингу ────────────────────
            lt_funding = None
            if lt_leg:
                key = f"BitMart:{lt_leg['symbol']}"
                r = rates_map.get(key)
                if r:
                    lt_funding = _estimated_leg_funding_usd(lt_leg, r, opened_ago)

            # ── Текущий APR каждой ноги ──────────────────────────────────────────
            lt_rate_obj = rates_map.get(f"BitMart:{symbol}") if lt_leg else None
            bp_rate_obj = rates_map.get(f"Backpack:{symbol}") if bp_leg else None

            # Доход = SHORT на положительном фандинге (или LONG на отрицательном)
            lt_income_apr = 0.0
            if lt_leg and lt_rate_obj:
                lt_income_apr = lt_rate_obj.apr if lt_leg["direction"] == "SHORT" else -lt_rate_obj.apr

            bp_income_apr = 0.0
            if bp_leg and bp_rate_obj:
                bp_income_apr = bp_rate_obj.apr if bp_leg["direction"] == "SHORT" else -bp_rate_obj.apr

            net_apr = lt_income_apr + bp_income_apr
            apr_status = "🟢" if net_apr >= 50 else "🟡" if net_apr >= 0 else "🔴"

            lt_apr_str = f"`{lt_rate_obj.apr:+.1f}%`" if lt_rate_obj else "_нет данных_"
            bp_apr_str = f"`{bp_rate_obj.apr:+.1f}%`" if bp_rate_obj else "_нет данных_"

            # ── Итоговая строка фандинга ──────────────────────────────────────────
            bp_is_real = bp_data is not None and (
                bp_data.get("cumulativeFundingPayment") is not None or
                bp_data.get("fundingPayment") is not None
            )
            net_funding_pct = _net_funding_pct(
                None if (bp_funding is None or lt_funding is None) else (bp_funding + lt_funding),
                legs,
            )
            if bp_funding is not None and lt_funding is not None:
                total_earned = bp_funding + lt_funding
                bp_label = "факт." if bp_is_real else "~"
                earned_str = (
                    f"`${total_earned:.4f}`" + (f" (`{net_funding_pct:.3f}%`)" if net_funding_pct is not None else "") + "\n"
                    f"  ├ Backpack: `${bp_funding:.4f}` ({bp_label})\n"
                    f"  └ BitMart: `${lt_funding:.4f}` (~прибл.)"
                )
            elif bp_funding is not None:
                bp_label = "факт." if bp_is_real else "~прибл."
                earned_str = f"`${bp_funding:.4f}` ({bp_label})"
            elif lt_funding is not None:
                earned_str = f"`${lt_funding:.4f}` (~прибл.)"
            else:
                earned_str = "_нет данных_"

            dir_a = "шорт ↓" if leg_a["direction"] == "SHORT" else "лонг ↑"
            dir_b = "шорт ↓" if leg_b["direction"] == "SHORT" else "лонг ↑"

            text = (
                f"🔀 *{symbol}* — {leg_a['exchange']} × {leg_b['exchange']} {apr_status}\n\n"
                f"  {leg_a['exchange']} ({dir_a}): `${leg_a['entry_price']:.4f}`\n"
                f"  {leg_b['exchange']} ({dir_b}): `${leg_b['entry_price']:.4f}`\n"
                f"💵 Размер: `${total_usd:.0f}` (по `${leg_a['position_size_usd']:.0f}` на каждую)\n"
                f"⏱ Открыта: `{opened_ago:.1f}ч назад`\n"
                f"📊 APR сейчас: BitMart {lt_apr_str} | Backpack {bp_apr_str}\n"
                f"  └ Нетто: `{net_apr:+.1f}%`\n"
                f"💰 *Заработано:* {earned_str}"
                f"{bp_liq_info}"
                f"{lt_liq_info}"
            )
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "➕ Добавить",
                    callback_data=f"scale_in:{pair_id}:{symbol}"
                ),
                InlineKeyboardButton(
                    "❌ Закрыть пару",
                    callback_data=f"close_pair:{pair_id}:{symbol}"
                ),
            ]])
        else:
            # Одиночная позиция (Hyperliquid)
            pos = legs[0]
            direction = pos.get("direction", "SHORT")
            label = "шорт" if direction == "SHORT" else "лонг"
            key = f"Hyperliquid:{pos['symbol']}"
            r = rates_map.get(key)
            current_apr = abs(r.apr) if r else 0
            earned = abs(r.rate if r else 0) * opened_ago * pos["position_size_usd"]
            status = "🟢" if current_apr >= 100 else "🟡" if current_apr >= 30 else "🔴"

            text = (
                f"📌 *{pos['symbol']}* — {pos['exchange']} {status}\n\n"
                f"📋 Тип: `{label}`\n"
                f"💵 Размер: `${pos['position_size_usd']}`\n"
                f"📈 Цена входа: `${pos['entry_price']:.4f}`\n"
                f"⏱ Открыта: `{opened_ago:.1f}ч назад`\n"
                f"📊 APR сейчас: `{current_apr:.1f}%`\n"
                f"💰 *Заработано (~прибл.):* `${earned:.4f}`"
            )
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ Закрыть позицию",
                    callback_data=f"close:{pos['id']}:{pos['symbol']}"
                )
            ]])

        await update.message.reply_text(
            text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard
        )


HISTORY_PAGE_SIZE = 5


async def _build_history_page(page: int) -> tuple:
    """Возвращает (текст, клавиатура) для страницы истории."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    total_count = await count_closed_pairs()
    if total_count == 0:
        return "📭 Закрытых позиций пока нет", None

    total_pages = (total_count + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE
    page = max(0, min(page, total_pages - 1))
    offset = page * HISTORY_PAGE_SIZE
    closed = await get_closed_pairs(limit=HISTORY_PAGE_SIZE, offset=offset)

    page_total = 0.0
    lines = [f"📋 *История позиций* (стр. {page + 1}/{total_pages}):\n"]

    for group in closed:
        legs = group["legs"]
        closed_at = group.get("closed_at") or time.time()

        if len(legs) == 2:
            symbol = legs[0]["symbol"]
            opened_at = min(l["opened_at"] for l in legs)
            duration_h = (closed_at - opened_at) / 3600
            duration_str = f"{duration_h:.1f}ч" if duration_h < 48 else f"{duration_h/24:.1f}дн"
            exch_a = legs[0]["exchange"]
            exch_b = legs[1]["exchange"]

            funding = sum(
                abs(l.get("entry_apr") or 0) / 100 / 365 / 24
                * duration_h * l["position_size_usd"]
                for l in legs
            )

            bitmart_legs = [l for l in legs if l.get("exchange") == "BitMart"]
            backpack_legs = [l for l in legs if l.get("exchange") == "Backpack"]

            bm_fee_raw = sum(float(l.get("position_size_usd") or 0) * BITMART_FEE_SITE_RATE * 2 for l in bitmart_legs)
            bm_fee_effective = bm_fee_raw * (1 - BITMART_FEE_DISCOUNT)  # скидка 70% от биржевого тарифа
            bp_fee = sum(float(l.get("position_size_usd") or 0) * BACKPACK_FEE_SITE_RATE * 2 for l in backpack_legs)

            fees_total = bm_fee_effective + bp_fee
            net = funding - fees_total
            page_total += net
            lines.append(
                f"🔀 *{symbol}* — {exch_a} × {exch_b}\n"
                f"  ⏱ Держали: `{duration_str}`\n"
                f"  📈 Фандинг (~): `+${funding:.4f}`\n"
                f"  💸 Комиссии Backpack: `-${bp_fee:.4f}`\n"
                f"  💸 Комиссии BitMart (со скидкой 70%): `-${bm_fee_effective:.4f}`\n"
                f"  ✅ Итого (~): `${net:.4f}`\n"
            )
        else:
            pos = legs[0]
            opened_at = pos["opened_at"]
            duration_h = (closed_at - opened_at) / 3600
            apr = abs(pos.get("entry_apr") or 0)
            estimated = (apr / 100 / 365 / 24) * duration_h * pos["position_size_usd"]
            page_total += estimated
            duration_str = f"{duration_h:.1f}ч" if duration_h < 48 else f"{duration_h/24:.1f}дн"
            lines.append(
                f"📌 *{pos['symbol']}* — {pos['exchange']}\n"
                f"  ⏱ Держали: `{duration_str}`\n"
                f"  💰 ~`${estimated:.4f}`\n"
            )

    lines.append(
        f"─────────────────\n"
        f"💵 *Итого на странице: `${page_total:.4f}`*\n"
        f"_Фандинг — оценка по APR_"
    )

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Назад", callback_data=f"history_page:{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶ Далее", callback_data=f"history_page:{page + 1}"))
    keyboard = InlineKeyboardMarkup([nav]) if nav else None

    return "\n".join(lines), keyboard


async def show_history(update: Update):
    """Показывает историю закрытых позиций с пагинацией."""
    text, keyboard = await _build_history_page(0)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


# --- Обработчики ---

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Первый запуск — показываем постоянную клавиатуру."""
    seen = await load_setting("welcome_seen", "0")
    if seen == "0":
        await save_setting("welcome_seen", "1")
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Подписался!", callback_data="welcome_subscribed"),
        ]])
        await update.message.reply_text(
            f"👋 Привет! Это бот для дельта-нейтральной стратегии.\n\n"
            f"Бот бесплатный — я делюсь им с сообществом. Если хочешь поддержать и следить за обновлениями, "
            f"подпишись на мой канал 👉 {AUTHOR_CHANNEL_NAME}\n\n"
            f"{AUTHOR_CHANNEL}",
            reply_markup=keyboard,
        )
    else:
        await update.message.reply_text(
            "👋 Привет! Кнопки управления внизу 👇",
            reply_markup=persistent_keyboard(),
        )


async def scan_manual(update: Update):
    """Ручное сканирование — показывает топ возможностей по всем парам, игнорируя кулдаун."""
    await update.message.reply_text("⏳ Сканирую все биржи...")

    try:
        lt_rates, bp_rates, vr_rates, ext_rates = await asyncio.gather(
            BitMartScanner().get_funding_rates(),
            BackpackScanner().get_funding_rates(),
            VariationalScanner().get_funding_rates(),
            ExtendedScanner().get_funding_rates(),
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при получении данных: {e}")
        return

    def _find_opps(map_a, map_b, vol_filter=True):
        opps = []
        for symbol, a in map_a.items():
            b = map_b.get(symbol)
            if not b:
                continue
            if len(symbol) < 2 or not any(c.isalpha() for c in symbol):
                continue
            if a.apr == 0 and b.apr == 0:
                continue
            if abs(a.apr) > 2000 or abs(b.apr) > 2000:
                continue
            if vol_filter and (a.volume_usd < MIN_VOLUME_USD or b.volume_usd < MIN_VOLUME_USD):
                continue
            if not vol_filter and b.volume_usd < MIN_VOLUME_USD:
                continue
            if a.apr * b.apr < 0:
                net_apr = abs(a.apr) + abs(b.apr)
                dir_a = "SHORT" if a.apr > 0 else "LONG"
                dir_b = "SHORT" if b.apr > 0 else "LONG"
            else:
                net_apr = abs(abs(a.apr) - abs(b.apr))
                if abs(a.apr) >= abs(b.apr):
                    dir_a = "SHORT" if a.apr > 0 else "LONG"
                    dir_b = "LONG" if b.apr > 0 else "SHORT"
                else:
                    dir_a = "LONG" if a.apr > 0 else "SHORT"
                    dir_b = "SHORT" if b.apr > 0 else "LONG"
            if net_apr < 10:
                continue
            opps.append({"symbol": symbol, "net_apr": net_apr,
                         "apr_a": a.apr, "dir_a": dir_a,
                         "apr_b": b.apr, "dir_b": dir_b})
        return sorted(opps, key=lambda x: x["net_apr"], reverse=True)

    lt_map = {r.symbol: r for r in lt_rates}
    bp_map = {r.symbol: r for r in bp_rates}
    vr_map = {r.symbol: r for r in vr_rates}
    ext_map = {r.symbol: r for r in ext_rates}

    lt_bp_opps = _find_opps(lt_map, bp_map, vol_filter=True) if _signals_enabled["LT_BP"] else []
    vr_ext_opps = _find_opps(vr_map, ext_map, vol_filter=False) if _signals_enabled["VR_EXT"] else []

    total = len(lt_bp_opps) + len(vr_ext_opps)
    if total == 0:
        await update.message.reply_text("🤷 Возможностей не найдено (или все связки выключены в настройках)")
        return

    for opp in lt_bp_opps[:TOP_OPPORTUNITIES_TO_SHOW]:
        label_a = "шорт ↓" if opp["dir_a"] == "SHORT" else "лонг ↑"
        label_b = "шорт ↓" if opp["dir_b"] == "SHORT" else "лонг ↑"
        text = (
            f"🔀 *{opp['symbol']}* — BitMart × Backpack\n\n"
            f"  BitMart ({label_a}): `{opp['apr_a']:+.1f}%`\n"
            f"  Backpack ({label_b}): `{opp['apr_b']:+.1f}%`\n"
            f"  📈 Нетто: `~{opp['net_apr']:.1f}% APR`\n\n"
            f"  💸 BitMart: 0.06% | Backpack: 0.04%"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Открыть пару",
                callback_data=f"open_pair:BitMart:Backpack:{opp['symbol']}:{opp['dir_a']}:{opp['dir_b']}"),
            InlineKeyboardButton("❌ Пропустить", callback_data="skip"),
        ]])
        _sent_signals[f"LT_BP:{opp['symbol']}:{opp['dir_a']}:{opp['dir_b']}"] = (opp["net_apr"], time.time())
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)

    for opp in vr_ext_opps[:TOP_OPPORTUNITIES_TO_SHOW]:
        label_a = "шорт ↓" if opp["dir_a"] == "SHORT" else "лонг ↑"
        label_b = "шорт ↓" if opp["dir_b"] == "SHORT" else "лонг ↑"
        text = (
            f"🔀 *{opp['symbol']}* — Variational × Extended\n\n"
            f"  Variational ({label_a}): `{opp['apr_a']:+.1f}%`\n"
            f"  Extended ({label_b}): `{opp['apr_b']:+.1f}%`\n"
            f"  📈 Нетто: `~{opp['net_apr']:.1f}% APR`\n\n"
            f"  💸 Variational: 0% комиссия | Extended: 0.025%"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Открыть пару",
                callback_data=f"open_pair:Variational:Extended:{opp['symbol']}:{opp['dir_a']}:{opp['dir_b']}"),
            InlineKeyboardButton("❌ Пропустить", callback_data="skip"),
        ]])
        _sent_signals[f"VR_EXT:{opp['symbol']}:{opp['dir_a']}:{opp['dir_b']}"] = (opp["net_apr"], time.time())
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)

    lt_str = f"{min(len(lt_bp_opps), TOP_OPPORTUNITIES_TO_SHOW)} из {len(lt_bp_opps)}" if _signals_enabled["LT_BP"] else "выкл"
    vr_str = f"{min(len(vr_ext_opps), TOP_OPPORTUNITIES_TO_SHOW)} из {len(vr_ext_opps)}" if _signals_enabled["VR_EXT"] else "выкл"
    await update.message.reply_text(f"✅ BM×BP: {lt_str} | VR×EXT: {vr_str}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий постоянной клавиатуры."""
    global _waiting_for_size, _waiting_for_scale_in
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        return  # игнорируем чужих
    text = update.message.text

    if _waiting_for_scale_in:
        pair_id, symbol = _waiting_for_scale_in
        is_vr_ext = pair_id.endswith("_VR_EXT")
        try:
            add_size = float(text.replace("$", "").replace(",", "").strip())
            if add_size < 10:
                await update.message.reply_text("❌ Минимальный размер — $10")
                return
            _waiting_for_scale_in = None
            await update.message.reply_text(f"⏳ Добавляю ${add_size:.0f} к {symbol}...")
            legs = await get_positions_by_pair(pair_id)
            if is_vr_ext:
                result = await scale_in_pair_vr_ext(
                    pair_id=pair_id, symbol=symbol, legs=legs, add_size_usd=add_size,
                )
                vr = result["variational"]
                ext = result["extended"]
                await update.message.reply_text(
                    f"✅ {symbol} (VR+EXT) увеличен на ${add_size:.0f}!\n"
                    f"Variational: {vr['size']:.4f} @ ${vr['price']:.4f}\n"
                    f"Extended: {ext['size']:.4f} @ ${ext['price']:.4f}"
                )
            else:
                result = await scale_in_pair(
                    pair_id=pair_id, symbol=symbol, legs=legs, add_size_usd=add_size,
                )
                lt = result["bitmart"]
                bp = result["backpack"]
                await update.message.reply_text(
                    f"✅ {symbol} (BM+BP) увеличен на ${add_size:.0f}!\n"
                    f"BitMart: {lt['size']:.4f} @ ${lt['price']:.4f}\n"
                    f"Backpack: {bp['size']:.4f} @ ${bp['price']:.4f}"
                )
        except ValueError:
            await update.message.reply_text("❌ Не понял. Введи число, например: `100`", parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            _waiting_for_scale_in = None
            logger.error(f"Ошибка scale_in (manual): {e}")
            await update.message.reply_text(f"❌ Ошибка: {e}")
        return

    # Пользователь прислал новый Variational токен (JWT)
    if _is_jwt(text) and not _waiting_for_size and not _waiting_for_scale_in:
        global _vr_token_last_warned, _vr_token_pinned_msg_id
        new_token = text.strip()
        exp = _parse_jwt_exp(new_token)
        if exp and exp > time.time():
            _update_env_token(new_token)
            import config as _cfg
            _cfg.VARIATIONAL_TOKEN = new_token
            _vr_token_last_warned = 0  # сбрасываем счётчик предупреждений

            # Открепляем запиненное предупреждение
            pinned_id_str = await load_setting("vr_token_pinned_msg_id", "")
            if pinned_id_str:
                await unpin_message(int(pinned_id_str))
                await save_setting("vr_token_pinned_msg_id", "")
                _vr_token_pinned_msg_id = None

            from datetime import datetime
            exp_str = datetime.fromtimestamp(exp).strftime("%d.%m.%Y %H:%M")
            await update.message.reply_text(
                f"✅ *Variational токен обновлён*\nДействителен до: {exp_str}",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await update.message.reply_text("❌ Токен уже истёк или невалидный. Попробуй снова.")
        return

    if text == BTN_POSITIONS:
        await show_positions(update)

    elif text == BTN_SCAN:
        await update.message.reply_text("🔍 Сканирую рынок...")
        await scan_manual(update)

    elif text == BTN_HISTORY:
        await show_history(update)

    elif text == BTN_SETTINGS:
        await show_settings(update)

    elif text == BTN_SUPPORT:
        await update.message.reply_text(
            f"💙 *Поддержать автора*\n\n"
            f"Если бот помогает тебе зарабатывать — буду рад донату 🙏\n\n"
            f"*EVM* (ETH, BSC, Arbitrum, Base...):\n`{DONATION_WALLET_EVM}`\n\n"
            f"*Solana:*\n`{DONATION_WALLET_SOL}`\n\n"
            f"Также подписывайся на канал: {AUTHOR_CHANNEL}",
            parse_mode=ParseMode.MARKDOWN,
        )

    elif _waiting_for_size:
        # Пользователь вводит размер позиции вручную
        pair_type = _waiting_for_size
        pair_label = "BitMart × Backpack" if pair_type == "LT_BP" else "Variational × Extended"
        try:
            new_size = float(text.replace("$", "").replace(",", "").strip())
            if new_size < 10:
                await update.message.reply_text("❌ Минимальный размер — $10")
            else:
                _position_sizes[pair_type] = new_size
                await save_setting(f"position_size_{pair_type}", str(new_size))
                _waiting_for_size = None
                await update.message.reply_text(
                    f"✅ {pair_label}: `${new_size:.0f}`",
                    parse_mode=ParseMode.MARKDOWN,
                )
        except ValueError:
            await update.message.reply_text(
                "❌ Не понял. Введи число, например: `500`",
                parse_mode=ParseMode.MARKDOWN,
            )


async def _do_open_pair(query, symbol: str, lt_dir: str, bp_dir: str):
    """Общая логика открытия пары — используется из open_pair: и open_pair_confirm:"""
    await query.edit_message_text(
        text=query.message.text + "\n\n_⏳ Открываю обе ноги..._",
        parse_mode=ParseMode.MARKDOWN,
    )
    try:
        signal_key = f"LT_BP:{symbol}:{lt_dir}:{bp_dir}"
        stored = _sent_signals.get(signal_key)
        entry_apr = stored[0] if stored else 0.0
        result = await open_pair(
            symbol=symbol,
            lighter_dir=lt_dir,
            backpack_dir=bp_dir,
            size_usd=_position_sizes["LT_BP"],
            entry_apr=entry_apr,
        )
        lt = result["bitmart"]
        bp = result["backpack"]
        await query.edit_message_text(
            text=query.message.text +
                 f"\n\n_✅ Пара открыта!_\n"
                f"_BitMart: {lt['size']:.4f} {symbol} @ ${lt['price']:.4f}_\n"
                f"_Backpack: {bp['size']:.4f} {symbol} @ ${bp['price']:.4f}_",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.error(f"Ошибка открытия пары: {e}")
        await query.edit_message_text(
            text=query.message.text + f"\n\n_❌ Ошибка: {e}_",
            parse_mode=ParseMode.MARKDOWN,
        )


async def _do_open_pair_vr_ext(query, symbol: str, vr_dir: str, ext_dir: str):
    """Открывает пару Variational + Extended."""
    await query.edit_message_text(
        text=query.message.text + "\n\n_⏳ Открываю обе ноги (VR+EXT)..._",
        parse_mode=ParseMode.MARKDOWN,
    )
    try:
        signal_key = f"VR_EXT:{symbol}:{vr_dir}:{ext_dir}"
        stored = _sent_signals.get(signal_key)
        entry_apr = stored[0] if stored else 0.0
        result = await open_pair_vr_ext(
            symbol=symbol,
            vr_dir=vr_dir,
            ext_dir=ext_dir,
            size_usd=_position_sizes["VR_EXT"],
            entry_apr=entry_apr,
        )
        vr = result["variational"]
        ext = result["extended"]
        await query.edit_message_text(
            text=query.message.text +
                 f"\n\n_✅ Пара открыта!_\n"
                 f"_Variational: {vr['size']:.4f} {symbol} @ ${vr['price']:.4f}_\n"
                 f"_Extended: {ext['size']:.4f} {symbol} @ ${ext['price']:.4f}_",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.error(f"Ошибка открытия пары VR+EXT: {e}")
        err_text = str(e).replace("_", "-").replace("*", "-").replace("`", "'")
        try:
            await query.edit_message_text(
                text=query.message.text + f"\n\n❌ Ошибка: {err_text}",
            )
        except Exception as edit_err:
            logger.error(f"Не удалось обновить сообщение об ошибке: {edit_err}")


async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка inline-кнопок (открыть/закрыть/пропустить позицию)."""
    global _waiting_for_size
    query = update.callback_query
    if str(update.effective_chat.id) != str(TELEGRAM_CHAT_ID):
        await query.answer("⛔ Нет доступа", show_alert=True)
        return
    await query.answer()

    # --- Сигналы ---
    if query.data == "skip":
        await query.edit_message_text(
            text=query.message.text + "\n\n_❌ Пропущено_",
            parse_mode=ParseMode.MARKDOWN,
        )

    elif query.data.startswith("open:"):
        parts = query.data.split(":")
        if len(parts) < 4:
            await query.answer("Некорректная команда", show_alert=True)
            return
        _, exchange, symbol, direction = parts[0], parts[1], parts[2], parts[3]
        await query.edit_message_text(
            text=query.message.text + "\n\n_⏳ Открываю позицию..._",
            parse_mode=ParseMode.MARKDOWN,
        )
        try:
            result = await open_position(symbol, direction=direction, entry_apr=0)
            label = "шорт" if direction == "SHORT" else "лонг"
            await query.edit_message_text(
                text=query.message.text +
                     f"\n\n_✅ Открыт перп {label}: {result['size']} {symbol} @ ${result['price']:.4f}_",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            logger.error(f"Ошибка открытия позиции: {e}")
            await query.edit_message_text(
                text=query.message.text + f"\n\n_❌ Ошибка: {e}_",
                parse_mode=ParseMode.MARKDOWN,
            )

    elif query.data.startswith("open_pair:"):
        # open_pair:ExchA:ExchB:SYMBOL:DIR_A:DIR_B
        parts = query.data.split(":")
        if len(parts) < 6:
            await query.answer("Некорректная команда", show_alert=True)
            return
        _, exch_a, exch_b, symbol, dir_a, dir_b = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]

        # Защита от двойного нажатия
        open_key = f"{exch_a}:{exch_b}:{symbol}:{dir_a}:{dir_b}"
        if open_key in _opening_pairs:
            await query.answer("⏳ Уже открывается, подожди...", show_alert=True)
            return
        _opening_pairs.add(open_key)

        is_vr_ext = (exch_a == "Variational" and exch_b == "Extended")

        # Для BM+BP: проверяем нет ли уже открытой пары (scale_in)
        if not is_vr_ext:
            open_pairs = await get_open_pairs()
            existing = next(
                (p for p in open_pairs
                 if p["legs"] and p["legs"][0]["symbol"] == symbol and p["pair_id"]
                 and {l["exchange"] for l in p["legs"]} == {"BitMart", "Backpack"}),
                None
            )
            if existing:
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        f"➕ Добавить ${_position_sizes['LT_BP']:.0f} к существующей",
                        callback_data=f"scale_in:{existing['pair_id']}:{symbol}",
                    ),
                    InlineKeyboardButton(
                        "🆕 Открыть отдельную",
                        callback_data=f"open_pair_confirm:{exch_a}:{exch_b}:{symbol}:{dir_a}:{dir_b}",
                    ),
                ]])
                await query.edit_message_text(
                    text=query.message.text + f"\n\n_⚠️ {symbol} уже открыт. Добавить к существующей позиции или открыть отдельную?_",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
                _opening_pairs.discard(open_key)
                return
            try:
                await _do_open_pair(query, symbol, dir_a, dir_b)
            finally:
                _opening_pairs.discard(open_key)
        else:
            try:
                await _do_open_pair_vr_ext(query, symbol, dir_a, dir_b)
            finally:
                _opening_pairs.discard(open_key)

    elif query.data.startswith("open_pair_confirm:"):
        # open_pair_confirm:ExchA:ExchB:SYMBOL:DIR_A:DIR_B
        parts = query.data.split(":")
        if len(parts) < 6:
            await query.answer("Некорректная команда", show_alert=True)
            return
        _, exch_a, exch_b, symbol, dir_a, dir_b = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
        if exch_a == "Variational" and exch_b == "Extended":
            await _do_open_pair_vr_ext(query, symbol, dir_a, dir_b)
        else:
            await _do_open_pair(query, symbol, dir_a, dir_b)

    elif query.data.startswith("scale_in:"):
        # scale_in:{pair_id}:{symbol} — показываем выбор суммы
        _, pair_id, symbol = query.data.split(":", 2)
        is_vr_ext = pair_id.endswith("_VR_EXT")
        pair_label = "VR+EXT" if is_vr_ext else "BM+BP"
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("$15",  callback_data=f"scale_in_exec:{pair_id}:{symbol}:15"),
                InlineKeyboardButton("$50",  callback_data=f"scale_in_exec:{pair_id}:{symbol}:50"),
                InlineKeyboardButton("$100", callback_data=f"scale_in_exec:{pair_id}:{symbol}:100"),
                InlineKeyboardButton("$250", callback_data=f"scale_in_exec:{pair_id}:{symbol}:250"),
                InlineKeyboardButton("$500", callback_data=f"scale_in_exec:{pair_id}:{symbol}:500"),
            ],
            [InlineKeyboardButton("✏️ Ввести вручную", callback_data=f"scale_in_exec:{pair_id}:{symbol}:manual")],
            [InlineKeyboardButton("❌ Отмена", callback_data="skip")],
        ])
        await query.edit_message_text(
            f"➕ Сколько добавить к *{symbol}* ({pair_label})?\n_Сумма на каждую ногу_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard,
        )

    elif query.data.startswith("scale_in_exec:"):
        # scale_in_exec:{pair_id}:{symbol}:{size|manual}
        global _waiting_for_scale_in
        parts = query.data.split(":")
        if len(parts) < 4:
            await query.answer("Некорректная команда", show_alert=True)
            return
        _, pair_id, symbol, size_str = parts[0], parts[1], parts[2], parts[3]
        is_vr_ext = pair_id.endswith("_VR_EXT")

        if size_str == "manual":
            _waiting_for_scale_in = (pair_id, symbol)
            await query.edit_message_text(
                f"✏️ Введи сумму для добавления к *{symbol}* ($ на каждую ногу):",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            add_size = float(size_str)
            await query.edit_message_text(f"⏳ Добавляю ${add_size:.0f} к {symbol}...")
            try:
                legs = await get_positions_by_pair(pair_id)
                if is_vr_ext:
                    result = await scale_in_pair_vr_ext(
                        pair_id=pair_id, symbol=symbol, legs=legs, add_size_usd=add_size,
                    )
                    vr = result["variational"]
                    ext = result["extended"]
                    await query.edit_message_text(
                        f"✅ {symbol} (VR+EXT) увеличен на ${add_size:.0f}!\n"
                        f"Variational: {vr['size']:.4f} @ ${vr['price']:.4f}\n"
                        f"Extended: {ext['size']:.4f} @ ${ext['price']:.4f}"
                    )
                else:
                    result = await scale_in_pair(
                        pair_id=pair_id, symbol=symbol, legs=legs, add_size_usd=add_size,
                    )
                    lt = result["bitmart"]
                    bp = result["backpack"]
                    await query.edit_message_text(
                        f"✅ {symbol} (BM+BP) увеличен на ${add_size:.0f}!\n"
                        f"BitMart: {lt['size']:.4f} @ ${lt['price']:.4f}\n"
                        f"Backpack: {bp['size']:.4f} @ ${bp['price']:.4f}"
                    )
            except Exception as e:
                logger.error(f"Ошибка scale_in: {e}")
                await query.edit_message_text(f"❌ Ошибка увеличения позиции:\n{e}")

    elif query.data.startswith("history_page:"):
        page = int(query.data.split(":")[1])
        text, keyboard = await _build_history_page(page)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)

    elif query.data.startswith("close_pair:"):
        # close_pair:{pair_id}:{symbol}
        _, pair_id, symbol = query.data.split(":", 2)
        await query.edit_message_text(f"⏳ Закрываю пару {symbol}...")
        try:
            legs = await get_positions_by_pair(pair_id)
            exchanges = {l["exchange"] for l in legs}
            if "Variational" in exchanges or "Extended" in exchanges:
                await close_pair_vr_ext(pair_id=pair_id, symbol=symbol, legs=legs)
            else:
                await close_pair(pair_id=pair_id, symbol=symbol, legs=legs)
            await query.edit_message_text(f"✅ Пара {symbol} закрыта")
            await send_message(f"✅ *Пара {symbol} закрыта вручную*")
        except Exception as e:
            logger.error(f"Ошибка закрытия пары: {e}")
            # parse_mode=None — ошибка может содержать JSON с символами ломающими Markdown
            await query.edit_message_text(f"❌ Ошибка закрытия:\n{e}")

    elif query.data == "welcome_subscribed":
        await query.edit_message_text(
            "✅ Спасибо! Добро пожаловать 🤝\n\nКнопки управления появятся внизу после нажатия /start",
        )
        await query.message.reply_text(
            "👇 Вот твои кнопки управления:",
            reply_markup=persistent_keyboard(),
        )

    # --- Настройки: размер позиции ---
    elif query.data == "noop":
        await query.answer()

    elif query.data.startswith("setsize:"):
        parts = query.data.split(":")
        pair_type = parts[1]   # LT_BP или VR_EXT
        value = parts[2]
        pair_label = "BitMart × Backpack" if pair_type == "LT_BP" else "Variational × Extended"
        if value == "manual":
            global _waiting_for_size
            _waiting_for_size = pair_type
            await query.edit_message_text(
                text=f"⚙️ *Настройки — {pair_label}*\n\n"
                     f"Текущий размер: `${_position_sizes[pair_type]:.0f}`\n\n"
                     f"✏️ Введи новый размер в долларах в чате (например: `500`)",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            _position_sizes[pair_type] = float(value)
            await save_setting(f"position_size_{pair_type}", value)
            await query.edit_message_text(
                text=f"⚙️ *Настройки*\n\n"
                     f"✅ {pair_label}: `${_position_sizes[pair_type]:.0f}`",
                parse_mode=ParseMode.MARKDOWN,
            )

    elif query.data.startswith("toggle_signals:"):
        global _signals_enabled
        pair_type = query.data.split(":")[1]   # LT_BP или VR_EXT
        _signals_enabled[pair_type] = not _signals_enabled[pair_type]
        await save_setting(f"signals_enabled_{pair_type}", "1" if _signals_enabled[pair_type] else "0")
        pair_label = "BitMart × Backpack" if pair_type == "LT_BP" else "Variational × Extended"
        state = "включены ✅" if _signals_enabled[pair_type] else "выключены ❌"
        # Обновляем клавиатуру настроек
        lt_bp_on = _signals_enabled["LT_BP"]
        vr_ext_on = _signals_enabled["VR_EXT"]
        lt_bp = _position_sizes["LT_BP"]
        vr_ext = _position_sizes["VR_EXT"]
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("── Биржи ──", callback_data="noop")],
            [
                InlineKeyboardButton(
                    f"{'✅' if lt_bp_on else '❌'} BitMart × Backpack",
                    callback_data="toggle_signals:LT_BP"
                ),
                InlineKeyboardButton(
                    f"{'✅' if vr_ext_on else '❌'} Variational × Extended",
                    callback_data="toggle_signals:VR_EXT"
                ),
            ],
            [InlineKeyboardButton("── Размер позиций ──", callback_data="noop")],
            [InlineKeyboardButton(f"── BitMart × Backpack (${lt_bp:.0f}) ──", callback_data="noop")],
            [
                InlineKeyboardButton("$15",  callback_data="setsize:LT_BP:15"),
                InlineKeyboardButton("$50",  callback_data="setsize:LT_BP:50"),
                InlineKeyboardButton("$100", callback_data="setsize:LT_BP:100"),
                InlineKeyboardButton("$250", callback_data="setsize:LT_BP:250"),
                InlineKeyboardButton("$500", callback_data="setsize:LT_BP:500"),
            ],
            [InlineKeyboardButton("✏️ Ввести вручную (BM+BP)", callback_data="setsize:LT_BP:manual")],
            [InlineKeyboardButton(f"── Variational × Extended (${vr_ext:.0f}) ──", callback_data="noop")],
            [
                InlineKeyboardButton("$15",  callback_data="setsize:VR_EXT:15"),
                InlineKeyboardButton("$50",  callback_data="setsize:VR_EXT:50"),
                InlineKeyboardButton("$100", callback_data="setsize:VR_EXT:100"),
                InlineKeyboardButton("$250", callback_data="setsize:VR_EXT:250"),
                InlineKeyboardButton("$500", callback_data="setsize:VR_EXT:500"),
            ],
            [InlineKeyboardButton("✏️ Ввести вручную (VR+EXT)", callback_data="setsize:VR_EXT:manual")],
        ])
        await query.edit_message_text(
            f"⚙️ *Настройки*\n\n"
            f"Сигналы {pair_label}: {state}\n\n"
            f"BitMart × Backpack: `${lt_bp:.0f}` на каждую ногу\n"
            f"Variational × Extended: `${vr_ext:.0f}` на каждую ногу\n\n"
            f"Выбери размер или включи/выключи сигналы:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard,
        )

    # --- Закрытие позиции ---
    elif query.data.startswith("close:"):
        _, position_id, symbol = query.data.split(":")
        await query.edit_message_text(
            text=f"⏳ Закрываю позицию {symbol}...",
            parse_mode=ParseMode.MARKDOWN,
        )
        try:
            await close_full_position(int(position_id), symbol)
            await query.edit_message_text(f"✅ Позиция {symbol} закрыта")
        except Exception as e:
            logger.error(f"Ошибка закрытия позиции: {e}")
            await query.edit_message_text(
                text=f"_❌ Ошибка при закрытии: {e}_",
                parse_mode=ParseMode.MARKDOWN,
            )


async def main():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Не заданы TELEGRAM_TOKEN или TELEGRAM_CHAT_ID в .env!")
        return

    await init_db()

    # Восстанавливаем настройки размеров позиций после рестарта
    lt_bp_size = await load_setting("position_size_LT_BP", str(POSITION_SIZE_USD))
    vr_ext_size = await load_setting("position_size_VR_EXT", str(POSITION_SIZE_USD))
    _position_sizes["LT_BP"] = float(lt_bp_size)
    _position_sizes["VR_EXT"] = float(vr_ext_size)
    logger.info(f"Размеры позиций: BM×BP=${_position_sizes['LT_BP']:.0f}, VR×EXT=${_position_sizes['VR_EXT']:.0f}")

    # Восстанавливаем настройки сигналов
    _signals_enabled["LT_BP"] = (await load_setting("signals_enabled_LT_BP", "1")) == "1"
    _signals_enabled["VR_EXT"] = (await load_setting("signals_enabled_VR_EXT", "1")) == "1"
    logger.info(f"Сигналы: BM×BP={'вкл' if _signals_enabled['LT_BP'] else 'выкл'}, VR×EXT={'вкл' if _signals_enabled['VR_EXT'] else 'выкл'}")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_button))

    await app.initialize()
    await app.start()

    logger.info("Бот запущен")
    await send_message(
        "🤖 *Бот запущен*\nНачинаю сканирование рынка...\n\nНапиши /start чтобы активировать кнопки управления"
    )

    await _check_variational_token()
    await scan_and_notify()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scan_and_notify, "interval", seconds=SCAN_INTERVAL_SECONDS)
    scheduler.add_job(_check_variational_token, "interval", hours=1)
    scheduler.start()
    logger.info(f"Планировщик запущен, интервал: {SCAN_INTERVAL_SECONDS}с")

    await app.updater.start_polling(drop_pending_updates=True)

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        scheduler.shutdown()
        try:
            await send_message(
                "⚠️ *Бот остановлен!*\n\n"
                "Позиции остаются открытыми на биржах — бот за ними больше не следит.\n"
                "Запусти снова: `python main.py`"
            )
        except Exception:
            pass  # Если Telegram недоступен — не мешаем завершению
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    asyncio.run(main())
