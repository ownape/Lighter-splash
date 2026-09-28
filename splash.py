#!/usr/bin/env python3
"""
Трекер резких движений цены («splash») на бирже Lighter → алерты в Telegram-канал.

Источник — WebSocket `market_stats/all` (wss://mainnet.zklighter.elliot.ai/stream):
после подписки приходит снапшот всех ~231 рынка, дальше пушатся только изменившиеся.
Это дешевле и быстрее поллинга REST (там 322KB на запрос).

Логика: по каждой монете держим скользящее окно WINDOW_MIN минут из (время, цена).
Если размах между MAX и MIN в окне ≥ THRESHOLD_PCT и текущая цена стоит у самого края —
это всплеск: у верхнего края (памп, 🟢), у нижнего (дамп, 🔴). Направление и отсчёт
идут от того экстремума, который был РАНЬШЕ, поэтому ⏱ — это время самого движения.

Чтобы одно движение не порождало поток сообщений, на монету ставится кулдаун.
"""
import os
import sys
import json
import time
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone, timedelta

import websockets
import requests

# ── конфиг ───────────────────────────────────────────────────────────────────
def _load_dotenv(path: str) -> None:
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    # срезаем хвостовой комментарий: "12  # порог" -> "12"
                    v = v.split("#", 1)[0]
                    os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass


_HERE = os.path.dirname(os.path.abspath(__file__))
_load_dotenv(os.path.join(_HERE, ".env"))

BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
CHANNEL       = os.getenv("CHANNEL", "")
THRESHOLD_PCT = float(os.getenv("THRESHOLD_PCT", "12"))    # порог всплеска, %
WINDOW_MIN    = float(os.getenv("WINDOW_MIN", "60"))       # окно наблюдения, мин
# По умолчанию равен порогу: повторный алерт = ещё одно полноценное движение.
# Токен пампанул 12% (алерт на 1.12) → следующий только на 1.12*1.12 = 1.2544.
RETRIGGER_PCT = float(os.getenv("RETRIGGER_PCT") or THRESHOLD_PCT)
MIN_VOL_24H   = float(os.getenv("MIN_VOL_24H", "1000"))    # отсечка мёртвых рынков, $
BLACKLIST     = {s.strip().upper() for s in os.getenv("BLACKLIST", "").split(",") if s.strip()}
TZ_OFFSET     = int(os.getenv("TZ_OFFSET", "0"))           # смещение от UTC для подписи времени
DRY_RUN       = os.getenv("DRY_RUN", "0") not in ("0", "false", "False", "")

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
TG_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
WINDOW_SEC = WINDOW_MIN * 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S", stream=sys.stdout)
log = logging.getLogger("splash")
logging.getLogger("websockets").setLevel(logging.WARNING)

history: dict[str, deque] = {}      # symbol -> deque[(ts, price)]
# symbol -> {"armed": готов ли сработать, "ref": цена последнего алерта, "dir": направление}
alert_state: dict[str, dict] = {}


# ── форматирование ────────────────────────────────────────────────────────────
def fmt_price(x: float) -> str:
    """Цены тут от 77000 (BTC) до 0.0000x — нужен адаптивный формат."""
    if x >= 1000:  return f"{x:,.2f}".replace(",", " ")
    if x >= 1:     return f"{x:.4f}".rstrip("0").rstrip(".")
    if x >= 0.001: return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def fmt_vol(v: float) -> str:
    if v >= 1e9: return f"${v/1e9:.2f}b"
    if v >= 1e6: return f"${v/1e6:.2f}m"
    if v >= 1e3: return f"${v/1e3:.2f}k"
    return f"${v:.0f}"


def build_alert(sym, pct, mx, mn, last, mark, index, dur_min, vol) -> str:
    up = pct >= 0
    ts = datetime.now(timezone(timedelta(hours=TZ_OFFSET))).strftime("%H:%M:%S")
    return (
        f"{'🟢' if up else '🔴'} ${sym}\n"
        f"Изм.: {pct:+.2f}%\n\n"
        f"MAX: {fmt_price(mx)}\n"
        f"MIN: {fmt_price(mn)}\n\n"
        f"Now last price: ${fmt_price(last)}\n"
        f"Справедл. price: ${fmt_price(mark)}\n\n"
        f"Spot price: ${fmt_price(index)}\n\n"
        f"⏱️ {dur_min:.1f} min\n"
        f"🌊 Volume 24h: {fmt_vol(vol)}\n\n"
        f"🕓 {ts} UTC+{TZ_OFFSET}\n\n"
        f"🔗 https://app.lighter.xyz/trade/{sym}"
    )


def send_alert(text: str) -> None:
    if DRY_RUN:
        log.info("[DRY_RUN] алерт:\n%s", text)
        return
    try:
        r = requests.post(TG_URL, json={"chat_id": CHANNEL, "text": text,
                                        "disable_web_page_preview": True}, timeout=15)
        if r.status_code != 200:
            log.error("Telegram %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        log.error("не смог отправить алерт: %s", e)


# ── детект ────────────────────────────────────────────────────────────────────
def check_symbol(sym: str, st: dict, now: float):
    """Оцениваем окно по одной монете. Возвращает текст алерта или None."""
    try:
        last = float(st.get("last_trade_price") or 0)
    except (TypeError, ValueError):
        return None
    if last <= 0:
        return None

    vol = float(st.get("daily_quote_token_volume") or 0)
    if vol < MIN_VOL_24H:          # мёртвые рынки не интересны
        return None

    if sym.upper() in BLACKLIST:
        return None

    dq = history.setdefault(sym, deque())
    dq.append((now, last))
    cutoff = now - WINDOW_SEC
    while dq and dq[0][0] < cutoff:
        dq.popleft()
    if len(dq) < 2:
        return None

    hi_ts, hi = max(dq, key=lambda p: p[1])[0], max(p[1] for p in dq)
    lo_ts, lo = min(dq, key=lambda p: p[1])[0], min(p[1] for p in dq)
    if lo <= 0 or hi <= 0:
        return None

    stt = alert_state.setdefault(sym, {"armed": True, "ref": 0.0, "dir": None})

    # Взводим триггер заново ТОЛЬКО когда размах окна схлопнулся ниже порога —
    # то есть старый экстремум выпал из окна и движения больше нет.
    # Раньше сброс стоял на «цена ушла от края», и это давало дубли: цена дёргалась
    # вокруг своего минимума, каждый тик чуть выше сбрасывал триггер, а возврат на
    # минимум слал тот же алерт заново.
    span_pct = (hi - lo) / lo * 100.0
    if span_pct < THRESHOLD_PCT:
        stt["armed"] = True
        return None

    # Всплеск засчитываем, только если цена СЕЙЧАС стоит у края диапазона —
    # иначе движение уже откатилось и новость протухла.
    if last >= hi:                                   # текущая = максимум → памп
        pct = span_pct
        first_ts, direction = lo_ts, "up"
    elif last <= lo:                                 # текущая = минимум → дамп
        pct = (lo - hi) / hi * 100.0
        first_ts, direction = hi_ts, "down"
    else:
        return None                                  # не у края — молчим, но НЕ взводим

    if abs(pct) < THRESHOLD_PCT:
        return None

    # Времени между алертами нет. Вместо него — правило «не повторять ОДНО И ТО ЖЕ
    # движение»: пока цена стоит у края, условие верно на каждом апдейте, и без этого
    # одно движение дало бы поток сообщений. Новый алерт по монете уйдёт сразу, если
    # движение развернулось или продлилось ещё на RETRIGGER_PCT.
    ref = stt["ref"]
    if stt["armed"]:
        pass
    elif direction != stt["dir"]:
        pass                                         # развернулось — это уже другое движение
    elif direction == "up"   and last >= ref * (1 + RETRIGGER_PCT / 100):
        pass                                         # памп продлился — новая нога
    elif direction == "down" and last <= ref * (1 - RETRIGGER_PCT / 100):
        pass
    else:
        return None                                  # то же самое движение, уже отправляли

    dur_min = max(0.0, (now - first_ts) / 60.0)
    stt.update(armed=False, ref=last, dir=direction)
    log.info("SPLASH %s %+.2f%% (max=%s min=%s, %.1f мин, vol=%s)",
             sym, pct, fmt_price(hi), fmt_price(lo), dur_min, fmt_vol(vol))
    return build_alert(
        sym, pct, hi, lo, last,
        float(st.get("mark_price") or last),
        float(st.get("index_price") or last),
        dur_min, vol,
    )


async def run():
    if not BOT_TOKEN or not CHANNEL:
        log.error("BOT_TOKEN/CHANNEL не заданы в .env"); sys.exit(1)

    log.info("старт: порог %.1f%%, окно %.0f мин, повтор при +%.1f%%, мин. объём %s, "
             "в блек-листе %d: %s%s",
             THRESHOLD_PCT, WINDOW_MIN, RETRIGGER_PCT, fmt_vol(MIN_VOL_24H),
             len(BLACKLIST), ",".join(sorted(BLACKLIST)) or "—",
             " [DRY_RUN]" if DRY_RUN else "")
    hb = time.monotonic()

    while True:  # переподключение переживает разрывы связи
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20,
                                          max_size=8 * 1024 * 1024) as ws:
                await ws.send(json.dumps({"type": "subscribe", "channel": "market_stats/all"}))
                log.info("подключён к Lighter, подписка market_stats/all")

                async for raw in ws:
                    msg = json.loads(raw)
                    stats = msg.get("market_stats")
                    if not isinstance(stats, dict):
                        continue
                    now = time.time()

                    # в канале /all приходит словарь market_id -> статистика
                    for st in stats.values():
                        if not isinstance(st, dict):
                            continue
                        sym = st.get("symbol")
                        if not sym:
                            continue
                        try:
                            text = check_symbol(sym, st, now)
                        except Exception as e:
                            log.warning("ошибка по %s: %s", sym, e)
                            continue
                        if text:
                            send_alert(text)

                    if time.monotonic() - hb >= 300:
                        tracked = sum(1 for d in history.values() if d)
                        log.info("alive · монет в окне: %d · точек: %d",
                                 tracked, sum(len(d) for d in history.values()))
                        hb = time.monotonic()

        except Exception as e:
            log.warning("соединение потеряно (%s) — переподключаюсь через 5с", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
