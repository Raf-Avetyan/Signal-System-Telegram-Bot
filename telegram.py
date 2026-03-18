import os
import json
import requests
from config import BOT_TOKEN, CHAT_ID, SYMBOL, PRIVATE_CHAT_ID

API_URL_MSG   = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
API_URL_PHOTO = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
API_URL_EDIT_MEDIA = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageMedia"
API_URL_EDIT_TEXT  = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
API_URL_UPDATES    = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"


def get_updates(offset=None):
    """Fetch new messages from Telegram."""
    params = {"timeout": 1}
    if offset:
        params["offset"] = offset
    try:
        resp = requests.get(API_URL_UPDATES, params=params, timeout=15)
        return resp.json() if resp.status_code == 200 else None
    except Exception as e:
        print(f"[TG ERROR] get_updates failed: {e}")
        return None


def send(text, parse_mode=None, chat_id=None, reply_markup=None, reply_to_message_id=None):
    """Send a message via Telegram Bot API."""
    target_chat = chat_id if chat_id else CHAT_ID
    try:
        payload = {
            "chat_id": target_chat,
            "text":    text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if reply_to_message_id:
            payload["reply_to_message_id"] = reply_to_message_id
            
        resp = requests.post(API_URL_MSG, data=payload)
        if not resp.ok:
            print(f"[TG ERROR] {resp.status_code}: {resp.text}")
            return None
        return resp.json()
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return None

def send_tp2_hit_congrats(chat_id, message_id, tf):
    """Send a reply for hitting TP2."""
    import random
    messages = [
        f"⚡️ <b>TARGET 2 SMACKED!</b> [{tf}] Moving fast! Final goal in sight. 🚀",
        f"💹 <b>TP2 REACHED!</b> [{tf}] Profits secured. Riding to the end! ✅",
        f"🔥 <b>MID-TARGET HIT!</b> [{tf}] 2/3 TPs done. Pure momentum! 💰",
    ]
    txt = random.choice(messages)
    return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

def send_tp3_hit_congrats(chat_id, message_id, tf):
    """Send a congratulatory reply for hitting TP3."""
    import random
    messages = [
        f"🎯 <b>TP3 HIT!</b> [{tf}] All targets achieved. Incredible trade! 🔥",
        f"🚀 <b>BOOM! TP3 SMASHED!</b> [{tf}] The trend was our friend today! 💰",
        f"💎 <b>GOLDEN SIGNAL!</b> [{tf}] TP3 reached. Max profit secured! 💹",
        f"📊 <b>PERFECT TRADE!</b> [{tf}] 3/3 Targets Hit. Pure accuracy. ✅",
    ]
    txt = random.choice(messages)
    return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

def send_breakeven_alert(chat_id, message_id, tf):
    """Send a reply when price returns to entry after TPs hit."""
    txt = f"📉 <b>REVERSAL ALERT!</b> [{tf}] Price returned to Entry level after hitting targets. Signal closed at Breakeven. ⚖️"
    return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

def edit_message_text(message_id, text, chat_id=None, parse_mode="HTML"):
    """Edit an existing text message."""
    target_chat = chat_id if chat_id else CHAT_ID
    try:
        payload = {
            "chat_id": target_chat,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode
        }
        resp = requests.post(API_URL_EDIT_TEXT, data=payload)
        if not resp.ok:
            # If message not modified, ignore
            if "message is not modified" in resp.text:
                return None
            print(f"[TG ERROR] Edit Text {resp.status_code}: {resp.text}")
            return None
        return resp.json()
    except Exception as e:
        print(f"[TG ERROR] Edit Text: {e}")
        return None


def send_photo(photo_path, caption=None, chat_id=None):
    """Send a photo via Telegram Bot API."""
    target_chat = chat_id if chat_id else CHAT_ID
    try:
        with open(photo_path, 'rb') as f:
            files = {'photo': f}
            payload = {'chat_id': target_chat}
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


def edit_message_media(message_id, photo_path, caption=None, chat_id=None):
    """Edit the photo of an existing message."""
    target_chat = chat_id if chat_id else CHAT_ID
    try:
        with open(photo_path, 'rb') as f:
            media = {
                "type": "photo",
                "media": "attach://photo",
                "caption": caption if caption else "",
                "parse_mode": "HTML"
            }
            payload = {
                "chat_id": target_chat,
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
    """Format price with comma separator, 0 decimals."""
    if price is None: return "0"
    return f"{abs(price):,.0f}" if price != 0 else "0"


# ═══════════════════════════════════════════════════════════════
# LIQUIDITY SWEEP
# ═══════════════════════════════════════════════════════════════

def send_liquidity_sweep(side, level, price, points, strength, note="", chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


# ═══════════════════════════════════════════════════════════════
# VOLATILITY ZONE TOUCH
# ═══════════════════════════════════════════════════════════════

def send_volatility_touch(side, level, price, points, strength, note="", chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


# ═══════════════════════════════════════════════════════════════
# SCALP SYSTEM
# ═══════════════════════════════════════════════════════════════

def send_scalp_open(timeframe, side, price, emoji="⚡️", chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


def send_scalp_closed(timeframe, side, price, emoji="⚡️", chat_id=None):
    """
    ❌ SCALP WINDOW CLOSED [TF] — no confirmation
    """
    label = "SCALP" if timeframe.lower() in ["5m", "15m"] else "SIGNAL"
    side_emoji = "🟢" if side == "LONG" else "🔴"
    code_part = (
        f"Momentum: Window Expired\n"
        f"Price:    {fmt_price(price)}"
    )
    msg = (
        f"<b>❌ {label} WINDOW CLOSED</b> [{timeframe.upper()}]\n"
        f"<b>{side_emoji} {side}</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML", chat_id=chat_id)


def send_scalp_prepare(timeframe, side, points=None, strength=None, emoji="⚡️", chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


# ─── Formatting Helpers ─────────────────────────────────────
def fmt_hit(is_hit):
    """Return a checkmark if level was hit."""
    return " ✅" if is_hit else ""

def get_signal_levels_code(entry, sl, tp1, tp2, tp3, status="OPEN", tp1_h=False, tp2_h=False, tp3_h=False, sl_h=False):
    """Format the levels block with hit markers."""
    # Symbols based on state
    sl_mark = " ❌" if sl_h else ""
    tp1_mark = fmt_hit(tp1_h)
    tp2_mark = fmt_hit(tp2_h)
    tp3_mark = fmt_hit(tp3_h)
    
    lines = [
        f"Entry:  {fmt_price(entry)}",
        f"SL:     {fmt_price(sl)}{sl_mark}",
        f"",
        f"TP1:    {fmt_price(tp1)}{tp1_mark}",
        f"TP2:    {fmt_price(tp2)}{tp2_mark}",
        f"TP3:    {fmt_price(tp3)}{tp3_mark}"
    ]
    
    return "\n".join(lines)

def get_signal_html(signal_type, side, timeframe, entry, sl, tp1, tp2, tp3,
                    status="OPEN", tp1_h=False, tp2_h=False, tp3_h=False, sl_h=False,
                    score=None, trend=None, indicators=None, reasons=None, size=None):
    """Generate HTML for Scalp, Strong, or Extreme signals."""
    side_emoji = "🟢" if side == "LONG" else "🔴"
    
    # 1. Header
    if signal_type == "SCALP":
        emoji = "🚀" if timeframe.lower() in ["1h", "4h"] else "⚡️"
        label = "SCALP" if timeframe.lower() in ["5m", "15m"] else "SIGNAL"
        header = f"<b>{emoji} {label} ENTRY CONFIRMED</b> [{timeframe.upper()}]\n"
        header += f"<b>{side_emoji} {side}</b>\n"
        header += f"\n"
    elif signal_type == "STRONG":
        emoji = "✅"
        header = f"<b>{emoji} STRONG {side} CONFLUENCE</b>\n"
        header += f"<b>{side_emoji} Market Divergence Detected [{timeframe}]</b>\n\n"
    elif signal_type == "EXTREME":
        emoji = "🔥"
        header = f"<b>{emoji} EXTREME {side} CONFLUENCE</b>\n"
        header += f"<b>{side_emoji} High-Alpha Setup Identified [{timeframe}]</b>\n\n"
    else:
        header = f"<b>🔔 {signal_type} {side}</b>\n"

    # 2. Score/Trend (for Scalps) or Confluence Details (for Strong/Extreme)
    details = ""
    if signal_type == "SCALP":
        score_display = f"Score:   {score}/10\n" if score else ""
        trend_display = f"Trend:   {trend}\n" if trend else ""
        reasons_display = f"Confl:   {', '.join(reasons)}\n" if reasons else ""
        size_display = f"Size:    {size}%\n" if size is not None else ""
        details = f"<pre>Trigger: Momentum Exit\n{score_display}{trend_display}{reasons_display}{size_display}</pre>\n"
    else:
        # Strong/Extreme
        num_systems = len(indicators) if indicators else 0
        total_points = sum(ind['points'] for ind in indicators) if indicators else 0
        size_display = f"\n<b>Risk Size:</b> {size}%" if size is not None else ""
        details = (
            f"<b>Confluence:</b> {num_systems} Systems Agree\n"
            f"<b>Total Weight:</b> {total_points} Points"
            f"{size_display}\n"
        )

    # 3. Levels
    levels_code = get_signal_levels_code(entry, sl, tp1, tp2, tp3, status, tp1_h, tp2_h, tp3_h, sl_h)
    
    msg = header + details + f"\n<b>⚡️ TRADE LEVELS</b>\n<pre>{levels_code}</pre>\n"

    # 3b. Status Banner (Bold & Outside <pre>)
    if status == "OPEN":
        msg += f"\n<b>🔵 ACTIVE POSITION</b>"
    elif status == "TP1":
        msg += f"\n<b>🔵 ACTIVE POSITION (TP1 ✅)</b>"
    elif status == "TP2":
        msg += f"\n<b>🔵 ACTIVE POSITION (TP2 ✅)</b>"
    elif status == "TP3":
        msg += f"\n<b>💰 ALL TARGETS HIT</b>"
    elif status == "SL":
        msg += f"\n<b>❌ STOP LOSS HIT</b>"
    elif status == "CLOSED":
        msg += f"\n<b>🛡 CLOSED AFTER TP</b>"

    # 4. Matched Strategies (for Strong/Extreme)
    if (signal_type in ["STRONG", "EXTREME"]) and indicators:
        ind_lines = []
        for ind in indicators:
            name = ind['name'].replace("Ponch_", "").replace("_", " ")
            sig = ind['signal'].replace("ENTRY ", "")
            ind_lines.append(f"• {name}: {sig} (+{ind['points']})")
        
        msg += f"\n\n<b>Matched Strategies:</b>\n<pre>" + "\n".join(ind_lines) + "</pre>"

    return msg


def send_scalp_confirmed(timeframe, side, entry, sl, tp1, tp2, tp3,
                         strength, size, score=None, trend=None, reasons=None, chat_id=None):
    """⚡️/🚀 SCALP ENTRY CONFIRMED"""
    html = get_signal_html("SCALP", side, timeframe, entry, sl, tp1, tp2, tp3,
                           score=score, trend=trend, reasons=reasons, size=size)
    return send(html, parse_mode="HTML", chat_id=chat_id)


def send_strong(side, total_points, confirmations, indicators_list, price=None, sl=None, tp1=None, tp2=None, tp3=None, size=None, chat_id=None):
    """✅ STRONG CONFLUENCE"""
    tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators_list)))
    tf_summary = ", ".join(tfs)

    html = get_signal_html("STRONG", side, tf_summary, price, sl, tp1, tp2, tp3,
                           indicators=indicators_list, size=size)
    return send(html, parse_mode="HTML", chat_id=chat_id)


def send_extreme(side, total_points, confirmations, indicators_list, price=None, sl=None, tp1=None, tp2=None, tp3=None, size=None, chat_id=None):
    """🔥 EXTREME CONFLUENCE"""
    tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators_list)))
    tf_summary = ", ".join(tfs)

    html = get_signal_html("EXTREME", side, tf_summary, price, sl, tp1, tp2, tp3,
                           indicators=indicators_list, size=size)
    return send(html, parse_mode="HTML", chat_id=chat_id)


def update_signal_message(chat_id, msg_id, sig_data):
    """Edit the original signal message with updated hit markers."""
    # 'meta' contains original info like indicators or reasons
    meta = sig_data.get("meta", {})
    
    # Reconstruct timeframe summary for confluence signals
    tf_val = sig_data["tf"]
    indicators = meta.get("indicators")
    if tf_val == "Confluence" and indicators:
        tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators)))
        tf_val = ", ".join(tfs)

    html = get_signal_html(
        signal_type=sig_data.get("type", "SCALP"),
        side=sig_data["side"],
        timeframe=tf_val,
        entry=sig_data["entry"],
        sl=sig_data["sl"],
        tp1=sig_data["tp1"],
        tp2=sig_data["tp2"],
        tp3=sig_data["tp3"],
        status=sig_data["status"],
        tp1_h=sig_data["tp1_hit"],
        tp2_h=sig_data["tp2_hit"],
        tp3_h=sig_data["tp3_hit"],
        sl_h=sig_data["sl_hit"],
        score=meta.get("score"),
        trend=meta.get("trend"),
        indicators=indicators,
        reasons=meta.get("reasons"),
        size=meta.get("size"),
    )
    return edit_message_text(msg_id, html, chat_id=chat_id)


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
        f"<b>📊 {SYMBOL} DAILY LEVELS</b>\n"
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
                      critical_high, critical_low, indicators=None, chart_path=None, chat_id=None):
    """
    📊 DAILY LEVELS
    """
    msg = get_daily_levels_html(
        date_str, daily_open, resistance, resistance_pct,
        support, support_pct, volatility, volatility_pct,
        critical_high, critical_low, indicators
    )
    
    if chart_path:
        resp = send_photo(chart_path, caption=msg, chat_id=chat_id)
        return {"response": resp, "html": msg}
    else:
        resp = send(msg, parse_mode="HTML", chat_id=chat_id)
        return {"response": resp, "html": msg}
        
# ═══════════════════════════════════════════════════════════════
# PERFORMANCE SUMMARY
# ═══════════════════════════════════════════════════════════════

def send_performance_summary(stats, chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


# ═══════════════════════════════════════════════════════════════
# PRICE APPROACHING LEVEL
# ═══════════════════════════════════════════════════════════════

def send_approaching_level(level_name, level_price, current_price, distance_pct, chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


# ═══════════════════════════════════════════════════════════════
# FUNDING RATE ALERT
# ═══════════════════════════════════════════════════════════════

def send_funding_alert(rate, direction, chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


def send_oi_divergence(price_change, oi_change, note, chat_id=None):
    """⚠️ OI DIVERGENCE (Private)"""
    msg = (
        f"⚠️ <b>OPEN INTEREST DIVERGENCE</b>\n"
        f"\n"
        f"Price Change: <b>{price_change:+.2f}%</b>\n"
        f"OI Change:    <b>{oi_change:+.2f}%</b>\n"
        f"\n"
        f"<b>Logic:</b> {note}"
    )
    send(msg, parse_mode="HTML", chat_id=chat_id)

def send_squeeze_alert(total_liq, price, chat_id=None):
    """🚨 LIQUIDATION SQUEEZE (Private)"""
    msg = (
        f"🚨 <b>LIQUIDATION SQUEEZE ALERT</b>\n"
        f"\n"
        f"Total Liquidated: <b>${total_liq/1_000_000:.1f}M</b>\n"
        f"Current Price:    <b>{fmt_price(price)}</b>\n"
        f"\n"
        f"<i>Significant volatility expected as positions are washed out.</i>"
    )
    send(msg, parse_mode="HTML", chat_id=chat_id)

# ═══════════════════════════════════════════════════════════════
# VOLUME SPIKE
# ═══════════════════════════════════════════════════════════════

def send_volume_spike(tf, current_vol, avg_vol, multiplier, price, chat_id=None):
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
    send(msg, parse_mode="HTML", chat_id=chat_id)


def send_market_alert(pct_change, duration_hours, start_price, end_price, chat_id=None):
    """🚨 BTC FAST MOVE ALERT"""
    direction = "UP" if pct_change >= 0 else "DOWN"
    emoji = "🚀" if pct_change >= 0 else "📉"
    
    msg = (
        f"🚨 <b>MARKET ALERT</b>\n"
        f"\n"
        f"• BTC just moved {direction} {abs(pct_change):.1f}% in the last {duration_hours} hours {emoji}\n"
        f"Price went from ${fmt_price(start_price)} to ${fmt_price(end_price)}."
    )
    send(msg, parse_mode="HTML", chat_id=chat_id)


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

def send_session_open(session_name, open_price, current_price=None, history=None, high=None, low=None, chart_path=None, chat_id=None):
    """Send alert when a session opens or bot starts mid-session."""
    msg = get_session_open_html(session_name, open_price, current_price, history, high, low)
        
    if chart_path and os.path.exists(chart_path):
        resp = send_photo(chart_path, caption=msg, chat_id=chat_id)
        return {"response": resp, "html": msg}
    else:
        send(msg, parse_mode="HTML", chat_id=chat_id)
        return None


def send_session_summary(session_name, price_open, price_close, signals_count, levels_tested, history=None, high=None, low=None, chart_path=None, chat_id=None):
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
        return send_photo(chart_path, caption=msg, chat_id=chat_id)
    else:
        send(msg, parse_mode="HTML", chat_id=chat_id)
        return None


# ═══════════════════════════════════════════════════════════════
# ALERT BATCHING
# ═══════════════════════════════════════════════════════════════

def send_batched_alerts(alerts, chat_id=None):
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
        f"🔔 ALERT BATCH ({len(alerts)} signals)\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )

    send(msg, parse_mode="HTML", chat_id=chat_id)

def send_performance_summary(stats, chat_id=None):
    """📢 DAILY PERFORMANCE SUMMARY (Public)"""
    score_emoji = "🏆" if stats["win_rate"] >= 70 else "📈"
    
    code_part = (
        f"Total Signals: {stats['total']}\n"
        f"TP1 Hits:      {stats['tp1_hits']}\n"
        f"TP2 Hits:      {stats['tp2_hits']}\n"
        f"TP3 Hits:      {stats['tp3_hits']}\n"
        f"SL Hits:       {stats['sl_hits']}\n"
        f"Win Rate:      {stats['win_rate']:.1f}%"
    )
    
    msg = (
        f"{score_emoji} <b>DAILY PERFORMANCE RECAP</b>\n"
        f"\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML", chat_id=chat_id)

# ═══════════════════════════════════════════════════════════════
# STARTUP / DEBUG
# ═══════════════════════════════════════════════════════════════

def send_startup():
    """Send a startup notification."""
    pass