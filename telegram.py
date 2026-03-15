import requests
import os
from config import BOT_TOKEN, CHAT_ID

API_URL_MSG   = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
API_URL_PHOTO = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
API_URL_EDIT_MEDIA = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageMedia"


def send(text, parse_mode=None):
    """Send a message via Telegram Bot API."""
    try:
        payload = {
            "chat_id": CHAT_ID,
            "text":    text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = requests.post(API_URL_MSG, data=payload)
        if not resp.ok:
            print(f"[TG ERROR] {resp.status_code}: {resp.text}")
            return None
        return resp.json()
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return None


def send_photo(photo_path, caption=None):
    """Send a photo via Telegram Bot API."""
    try:
        with open(photo_path, 'rb') as f:
            files = {'photo': f}
            payload = {'chat_id': CHAT_ID}
            if caption:
                payload['caption'] = caption
                payload['parse_mode'] = 'HTML'
            
            resp = requests.post(API_URL_PHOTO, data=payload, files=files)
            if not resp.ok:
                print(f"[TG ERROR] {resp.status_code}: {resp.text}")
                return None
            return resp.json()
    except Exception as e:
        print(f"[TG ERROR] Photo: {e}")
        return None


def edit_message_media(message_id, photo_path, caption=None):
    """Edit the photo of an existing message."""
    try:
        import json
        with open(photo_path, 'rb') as f:
            media = {
                "type": "photo",
                "media": "attach://photo",
                "caption": caption if caption else "",
                "parse_mode": "HTML"
            }
            payload = {
                "chat_id": CHAT_ID,
                "message_id": message_id,
                "media": json.dumps(media)
            }
            files = {'photo': f}
            resp = requests.post(API_URL_EDIT_MEDIA, data=payload, files=files)
            if not resp.ok:
                # 1. Ignore "message is not modified" errors
                if "message is not modified" in resp.text:
                    return True
                # 2. Handle deleted messages
                if "message to edit not found" in resp.text:
                    return "DELETED"
                
                print(f"[TG ERROR] Edit Media {resp.status_code}: {resp.text}")
                return False
            return True
    except Exception as e:
        print(f"[TG ERROR] Edit Media: {e}")
        return False


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
    code_part = (
        f"Points:    {points}\n"
        f"Strength:  {strength}\n"
        f"Side:      {side}\n"
        f"Level:     {level}\n"
        f"Price:     {fmt_price(price)}\n"
        f"Note:      {note}"
    )
    
    msg = (
        f"<b>🧹 Liquidity Sweep</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# VOLATILITY ZONE TOUCH
# ═══════════════════════════════════════════════════════════════

def send_volatility_touch(side, level, price, points, strength, note=""):
    """
    📊 Volatility Zone Touch
    """
    code_part = (
        f"Points:    {points}\n"
        f"Strength:  {strength}\n"
        f"Side:      {side}\n"
        f"Level:     {level}\n"
        f"Price:     {fmt_price(price)}\n"
        f"Note:      {note}"
    )
    
    msg = (
        f"<b>📊 Volatility Zone Touch</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# SCALP SYSTEM
# ═══════════════════════════════════════════════════════════════

def send_scalp_open(timeframe, side, price, emoji="⚡️"):
    """
    ⚡️/🚀 SCALP WINDOW OPEN [TF] 🟢/🔴 SIDE
    """
    label = "SCALP" if timeframe.lower() in ["5m", "15m"] else "SIGNAL"
    side_emoji = "🟢" if side == "LONG" else "🔴"
    code_part = (
        f"Momentum: Zone Entry\n"
        f"Price:    {fmt_price(price)}"
    )
    
    msg = (
        f"<b>{emoji} {label} WINDOW OPEN</b> [{timeframe.upper()}]\n"
        f"<b>{side_emoji} {side}</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


def send_scalp_prepare(timeframe, side, points=None, strength=None, emoji="⚡️"):
    """
    ⚠️ PREPARE FOR ENTRY [TF] 🟢/🔴 SIDE
    """
    side_emoji = "🟢" if side == "LONG" else "🔴"
    msg = (
        f"⚠️ <b>PREPARE FOR ENTRY</b> [{timeframe.upper()}]\n"
        f"<b>{side_emoji} {side}</b>\n"
        f"<pre>"
    )
    
    code_lines = []
    if points is not None:
        code_lines.append(f"Points:   {points}")
    if strength is not None:
        code_lines.append(f"Strength: {strength}")
    
    code_lines.append(f"\nSignal detected inside momentum zone")
    code_lines.append(f"Awaiting confirmation on zone exit")
    
    msg += "\n".join(code_lines) + "</pre>"
    send(msg, parse_mode="HTML")


def send_scalp_confirmed(timeframe, side, entry, sl, tp1, tp2, tp3,
                         strength, size, score=None, trend=None, reasons=None, emoji="⚡️"):
    """
    ⚡️/🚀 SCALP ENTRY CONFIRMED [TF] 🟢/🔴 SIDE
    """
    label = "SCALP" if timeframe.lower() in ["5m", "15m"] else "SIGNAL"
    side_emoji = "🟢" if side == "LONG" else "🔴"
    
    score_display = f"Score:    {score}/10" if score else ""
    trend_display = f"Trend:    {trend}" if trend else ""
    
    code_part = (
        f"Trigger:  Momentum Exit\n"
        f"Entry:    {fmt_price(entry)}\n"
        f"SL:       {fmt_price(sl)}\n\n"
        f"TP1:      {fmt_price(tp1)} (30%)\n"
        f"TP2:      {fmt_price(tp2)} (40%)\n"
        f"TP3:      {fmt_price(tp3)} (30%)\n"
        f"──────────\n"
        f"{score_display}\n"
        f"{trend_display}\n"
    )
    if reasons:
        confl_str = ", ".join(reasons)
        code_part += f"Confluence: {confl_str}\n"

    msg = (
        f"<b>{emoji} {label} ENTRY CONFIRMED</b> [{timeframe.upper()}]\n"
        f"<b>{side_emoji} {side}</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


def send_scalp_closed(timeframe, side, price, emoji="⚡️"):
    """
    ⚡️/🚀 SCALP WINDOW CLOSED [TF] 🟢/🔴 SIDE
    """
    label = "SCALP" if timeframe.lower() in ["5m", "15m"] else "SIGNAL"
    side_emoji = "🟢" if side == "LONG" else "🔴"
    code_part = (
        f"Momentum: Zone Exit\n"
        f"Price:    {fmt_price(price)}"
    )
    
    msg = (
        f"<b>{emoji} {label} WINDOW CLOSED</b> [{timeframe.upper()}]\n"
        f"<b>{side_emoji} {side}</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


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

def get_daily_levels_html(date_str, daily_open, resistance, resistance_pct,
                          support, support_pct, volatility, volatility_pct,
                          critical_high, critical_low, indicators=None):
    """Generate the HTML for daily levels message."""
    indicator_part = ""
    if indicators:
        btc_d = indicators.get("BTC.D_change", 0)
        dxy = indicators.get("DXY_change", 0)
        indicator_part = (
            f"\n"
            f"<b>🌍 GLOBAL CONTEXT</b>\n"
            f"<blockquote>"
            f"• BTC.D: {'+' if btc_d >= 0 else ''}{btc_d:.2f}%\n"
            f"• DXY Proxy: {'+' if dxy >= 0 else ''}{dxy:.2f}%"
            f"</blockquote>\n"
            f"\n"
        )

    quote = (
        f"Level            Value\n"
        f"🟥 Resistance     {fmt_price(resistance)}  ({resistance_pct:.2f}%)\n"
        f"🟩 Support        {fmt_price(support)}  ({support_pct:.2f}%)\n"
        f"🟨 Volatility     {fmt_price(volatility)}   ({volatility_pct:.2f}%)\n"
        f"🚨 Critical High  {fmt_price(critical_high)}\n"
        f"🚨 Critical Low   {fmt_price(critical_low)}"
    )
    msg = (
        f"<b>📊 DAILY LEVELS</b>\n"
        f"<blockquote>"
        f"• Date: <i>{date_str}</i>\n"
        f"• DO:   {fmt_price(daily_open)}\n"
        f"</blockquote>\n"
        f"{indicator_part}"
        f"<pre>{quote}</pre>"
    )
    return msg

def send_daily_levels(date_str, daily_open, resistance, resistance_pct,
                      support, support_pct, volatility, volatility_pct,
                      critical_high, critical_low, indicators=None, chart_path=None):
    """
    📊 DAILY LEVELS
    """
    msg = get_daily_levels_html(
        date_str, daily_open, resistance, resistance_pct,
        support, support_pct, volatility, volatility_pct,
        critical_high, critical_low, indicators
    )
    
    if chart_path:
        resp = send_photo(chart_path, caption=msg)
        return {"response": resp, "html": msg}
    else:
        resp = send(msg, parse_mode="HTML")
        return {"response": resp, "html": msg}
        
# ═══════════════════════════════════════════════════════════════
# PERFORMANCE SUMMARY
# ═══════════════════════════════════════════════════════════════

def send_performance_summary(stats):
    """Send daily signal performance recap."""
    if not stats or stats["total"] == 0:
        return

    formatted_stats = (
        f"TP1 Hit:  {stats['tp1_hits']}\n"
        f"TP2 Hit:  {stats['tp2_hits']}\n"
        f"TP3 Hit:  {stats['tp3_hits']}\n"
        f"SL Hit:   {stats['sl_hits']}\n"
        f"Open:     {stats['still_open']}\n"
        f"───────────────────\n"
        f"Win Rate: {stats['win_rate']:.1f}%"
    )

    msg = (
        f"📊 SIGNAL PERFORMANCE\n"
        f"\n"
        f"Total Signals: {stats['total']}\n"
        f"<pre>{formatted_stats}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# PRICE APPROACHING LEVEL
# ═══════════════════════════════════════════════════════════════

def send_approaching_level(level_name, level_price, current_price, distance_pct):
    """Alert when price is within threshold of a key level."""
    direction = "above" if current_price > level_price else "below"
    
    code_part = (
        f"Level:    {fmt_price(level_price)}\n"
        f"Price:    {fmt_price(current_price)}\n"
        f"Distance: {distance_pct:.2f}% {direction}"
    )
    
    msg = (
        f"⚠️ PRICE APPROACHING TO {level_name.upper()} LEVEL\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# FUNDING RATE ALERT
# ═══════════════════════════════════════════════════════════════

def send_funding_alert(rate, direction):
    """Alert when funding rate is extreme."""
    rate_pct = rate * 100
    emoji = "🟢" if direction == "POSITIVE" else "🔴"
    
    code_part = (
        f"{emoji} Rate: {rate_pct:.4f}%\n"
        f"Direction: {direction}\n"
        f"Note: High funding = potential reversal"
    )
    
    msg = (
        f"💰 EXTREME FUNDING RATE\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# VOLUME SPIKE
# ═══════════════════════════════════════════════════════════════

def send_volume_spike(tf, current_vol, avg_vol, multiplier, price):
    """Alert when volume is abnormally high."""
    code_part = (
        f"Volume:     {current_vol:,.0f}\n"
        f"Average:    {avg_vol:,.0f}\n"
        f"Multiplier: {multiplier:.1f}x\n"
        f"Price:      {fmt_price(price)}"
    )
    
    msg = (
        f"📈 VOLUME SPIKE [{tf.upper()}]\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# SESSION ALERTS
# ═══════════════════════════════════════════════════════════════

def get_session_open_html(session_name, open_price, current_price=None, history=None, high=None, low=None):
    """Generate the HTML for session open message."""
    is_mid = current_price is not None and abs(current_price - open_price) > 0.0001
    
    lines = [f"Open:   {fmt_price(open_price)}"]
    
    if is_mid:
        change = current_price - open_price
        pct = (change / open_price) * 100 if open_price else 0
        sign = "+" if change >= 0 else ""
        if high: lines.append(f"High:   {fmt_price(high)}")
        if low:  lines.append(f"Low:    {fmt_price(low)}")
        lines.append(f"Now:    {fmt_price(current_price)}")
        lines.append(f"Change: {sign}{fmt_price(change)} ({sign}{pct:.2f}%)")

    code_part = "\n".join(lines)
    
    msg = (
        f"<b>🕐 {session_name} SESSION OPENED</b>\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )
    
    if history:
        msg += f"\n\n{history}"
    return msg

def send_session_open(session_name, open_price, current_price=None, history=None, high=None, low=None, chart_path=None):
    """Send alert when a session opens or bot starts mid-session."""
    msg = get_session_open_html(session_name, open_price, current_price, history, high, low)
        
    if chart_path and os.path.exists(chart_path):
        resp = send_photo(chart_path, caption=msg)
        return {"response": resp, "html": msg}
    else:
        send(msg, parse_mode="HTML")
        return None


def send_session_summary(session_name, price_open, price_close, signals_count, levels_tested, history=None, high=None, low=None, chart_path=None):
    """Send session recap at session close."""
    change = price_close - price_open
    change_pct = (change / price_open) * 100 if price_open else 0
    direction = "+" if change >= 0 else ""

    lines = [
        f"Open:   {fmt_price(price_open)}",
        f"High:   {fmt_price(high)}" if high else None,
        f"Low:    {fmt_price(low)}" if low else None,
        f"Close:  {fmt_price(price_close)}",
        f"Change: {direction}{fmt_price(change)} ({direction}{change_pct:.2f}%)",
        f"───────────────────",
        f"Levels Tested: {levels_tested}"
    ]
    code_part = "\n".join([l for l in lines if l])

    msg = (
        f"<b>🕐 {session_name} SESSION CLOSED</b>\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )
    
    if history:
        msg += f"\n\n{history}"
        
    if chart_path and os.path.exists(chart_path):
        return send_photo(chart_path, caption=msg)
    else:
        send(msg, parse_mode="HTML")
        return None


# ═══════════════════════════════════════════════════════════════
# ALERT BATCHING
# ═══════════════════════════════════════════════════════════════

def send_batched_alerts(alerts):
    """Send multiple alerts as a single message."""
    if not alerts:
        return

    lines = []
    for i, alert in enumerate(alerts, 1):
        lines.append(f"{i}. {alert['type']}")
        if "tf" in alert:
            lines.append(f"   TF:    {alert['tf'].upper()}")
        if "side" in alert:
            lines.append(f"   Side:  {alert['side']}")
        if "price" in alert:
            lines.append(f"   Price: {fmt_price(alert['price'])}")
        if "note" in alert:
            lines.append(f"   {alert['note']}")
        lines.append("")  # Empty line between alerts

    code_part = "\n".join(lines).strip()
    
    msg = (
        f"📑 ALERT BATCH ({len(alerts)} signals)\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )

    send(msg, parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════
# STARTUP / DEBUG
# ═══════════════════════════════════════════════════════════════

def send_startup():
    """Send a startup notification."""
    pass