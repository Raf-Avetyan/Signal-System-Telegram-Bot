import requests
from config import BOT_TOKEN, CHAT_ID, SYMBOL

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def send(text, parse_mode=None):
    """Send a message via Telegram Bot API."""
    try:
        payload = {
            "chat_id": CHAT_ID,
            "text":    text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = requests.post(API_URL, data=payload)
        if not resp.ok:
            print(f"[TG ERROR] {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[TG ERROR] {e}")


def fmt_price(price):
    """Format price with comma separator."""
    return f"{price:,.2f}"


# ═══════════════════════════════════════════════════════════════
# LIQUIDITY SWEEP
# ═══════════════════════════════════════════════════════════════

def send_liquidity_sweep(side, level, price, points, strength, note=""):
    """
    🧹 Liquidity Sweep
    """
    msg = (
        f"🧹 Liquidity Sweep\n"
        f"{SYMBOL}\n"
        f"──────────\n"
        f"Points: {points}\n"
        f"Strength: {strength}\n"
        f"Side: {side}\n"
        f"Level: {level}\n"
        f"Price: {fmt_price(price)}\n"
        f"Note: {note}"
    )
    send(msg)


# ═══════════════════════════════════════════════════════════════
# VOLATILITY ZONE TOUCH
# ═══════════════════════════════════════════════════════════════

def send_volatility_touch(side, level, price, points, strength, note=""):
    """
    📊 Volatility Zone Touch
    """
    msg = (
        f"📊 Volatility Zone Touch\n"
        f"{SYMBOL}\n"
        f"──────────\n"
        f"Points: {points}\n"
        f"Strength: {strength}\n"
        f"Side: {side}\n"
        f"Level: {level}\n"
        f"Price: {fmt_price(price)}\n"
        f"Note: {note}"
    )
    send(msg)


# ═══════════════════════════════════════════════════════════════
# SCALP SYSTEM
# ═══════════════════════════════════════════════════════════════

def send_scalp_open(timeframe, side, price, emoji="⚡️"):
    """
    ⚡️/🚀 SCALP WINDOW OPEN [TF] 🟢/🔴 SIDE
    """
    side_emoji = "🟢" if side == "LONG" else "🔴"
    msg = (
        f"{emoji} SCALP WINDOW OPEN [{timeframe.upper()}] {side_emoji} {side}\n"
        f"{SYMBOL}\n"
        f"──────────\n"
        f"Momentum: Zone Entry\n"
        f"Price: {fmt_price(price)}"
    )
    send(msg)


def send_scalp_prepare(timeframe, side, points=None, strength=None, emoji="⚡️"):
    """
    ⚠️ PREPARE FOR ENTRY [TF] 🟢/🔴 SIDE
    """
    side_emoji = "🟢" if side == "LONG" else "🔴"
    lines = [
        f"⚠️ PREPARE FOR ENTRY [{timeframe.upper()}] {side_emoji} {side}",
        SYMBOL,
        "──────────",
    ]
    if points is not None:
        lines.append(f"Points: {points}")
    if strength is not None:
        lines.append(f"Strength: {strength}")
    lines.append("Signal detected inside momentum zone")
    lines.append("Awaiting confirmation on zone exit")
    send("\n".join(lines))


def send_scalp_confirmed(timeframe, side, entry, sl, tp1, tp2, tp3,
                         strength, size, emoji="⚡️"):
    """
    ⚡️/🚀 SCALP ENTRY CONFIRMED [TF] 🟢/🔴 SIDE
    """
    side_emoji = "🟢" if side == "LONG" else "🔴"
    msg = (
        f"{emoji} SCALP ENTRY CONFIRMED [{timeframe.upper()}] {side_emoji} {side}\n"
        f"{SYMBOL}\n"
        f"──────────\n"
        f"Trigger: Momentum Exit Confirmation\n"
        f"Entry: {fmt_price(entry)}\n"
        f"SL: {fmt_price(sl)}\n"
        f"\n"
        f"TP1: {fmt_price(tp1)} (30%)\n"
        f"TP2: {fmt_price(tp2)} (40%)\n"
        f"TP3: {fmt_price(tp3)} (30%)\n"
        f"──────────\n"
        f"Strength: {strength}\n"
        f"──────────\n"
        f"Size: {size}%"
    )
    send(msg)


def send_scalp_closed(timeframe, side, price, emoji="⚡️"):
    """
    ⚡️/🚀 SCALP WINDOW CLOSED [TF] 🟢/🔴 SIDE
    """
    side_emoji = "🟢" if side == "LONG" else "🔴"
    msg = (
        f"{emoji} SCALP WINDOW CLOSED [{timeframe.upper()}] {side_emoji} {side}\n"
        f"{SYMBOL}\n"
        f"──────────\n"
        f"Momentum: Zone Exit\n"
        f"Price: {fmt_price(price)}"
    )
    send(msg)


# ═══════════════════════════════════════════════════════════════
# TRADE SIGNAL
# ═══════════════════════════════════════════════════════════════

def send_trade_signal(tf, side, signal, price, indicator, points, strength, timestamp):
    """
    🎯 TRADE SIGNAL [{TF}]
    """
    msg = (
        f"🎯 TRADE SIGNAL [{tf.upper()}]\n"
        f"{SYMBOL}\n"
        f"──────────\n"
        f"Points: {points}\n"
        f"Side: {side}\n"
        f"Strength: {strength}\n"
        f"Signal: ENTRY {signal}\n"
        f"Price: {fmt_price(price)}\n"
        f"Time: {timestamp}\n"
        f"Indicator: {indicator}"
    )
    send(msg)


# ═══════════════════════════════════════════════════════════════
# CONFIRMATION AGGREGATION
# ═══════════════════════════════════════════════════════════════

def send_strong(side, total_points, confirmations, indicators_list):
    """
    ✅ STRONG
    """
    pass # Disabled as requested to match exact format


def send_extreme(side, total_points, confirmations, indicators_list):
    """
    🔥 EXTREME
    """
    pass # Disabled as requested to match exact format


# ═══════════════════════════════════════════════════════════════
# DAILY LEVELS REPORT
# ═══════════════════════════════════════════════════════════════

def send_daily_levels(date_str, daily_open, resistance, resistance_pct,
                      support, support_pct, volatility, volatility_pct,
                      critical_high, critical_low):
    """
    📊 BTCUSDT DAILY LEVELS
    """
    quote = (
        f"Level            Value\n"
        f"🟥 Resistance     {fmt_price(resistance)}  ({resistance_pct:.2f}%)\n"
        f"🟩 Support        {fmt_price(support)}  ({support_pct:.2f}%)\n"
        f"🟨 Volatility     {fmt_price(volatility)}   ({volatility_pct:.2f}%)\n"
        f"🚨 Critical High  {fmt_price(critical_high)}\n"
        f"🚨 Critical Low   {fmt_price(critical_low)}"
    )
    msg = (
        f"📊 {SYMBOL} DAILY LEVELS\n"
        f"──────────\n"
        f"• {date_str}\n"
        f"• DO: {fmt_price(daily_open)}\n"
        f"\n"
        f"<pre>{quote}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# STARTUP / DEBUG
# ═══════════════════════════════════════════════════════════════

def send_startup():
    """Send a startup notification."""
    pass