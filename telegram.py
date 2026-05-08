import os
import json
import requests
from config import BOT_TOKEN, SYMBOL, CHAT_ID, PRIVATE_EXEC_CHAT_ID, SIGNAL_CHAT_ID, SIGNAL_TRADING_THREAD_ID

API_URL_MSG   = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
API_URL_PHOTO = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
API_URL_EDIT_MEDIA = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageMedia"
API_URL_EDIT_TEXT  = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
API_URL_UPDATES    = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
API_URL_COMMANDS   = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"
API_URL_DELETE     = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
API_URL_CHAT_MEMBER = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"


def _default_public_chat_id():
    return SIGNAL_CHAT_ID or None


def _default_thread_id_for_chat(chat_id):
    target_chat = str(chat_id or "").strip()
    signal_chat = str(SIGNAL_CHAT_ID or "").strip()
    if target_chat and signal_chat and target_chat == signal_chat and int(SIGNAL_TRADING_THREAD_ID or 0) > 0:
        return int(SIGNAL_TRADING_THREAD_ID)
    return None


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


def send(text, parse_mode=None, chat_id=None, reply_markup=None, reply_to_message_id=None, message_thread_id=None):
    """Send a message via Telegram Bot API."""
    target_chat = chat_id if chat_id else _default_public_chat_id()
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
        if message_thread_id is None:
            message_thread_id = _default_thread_id_for_chat(target_chat)
        if message_thread_id:
            payload["message_thread_id"] = message_thread_id
            
        resp = requests.post(API_URL_MSG, data=payload)
        if not resp.ok:
            print(f"[TG ERROR] {resp.status_code}: {resp.text}")
            return None
        return resp.json()
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return None


def delete_message(chat_id, message_id):
    """Delete a Telegram message."""
    try:
        resp = requests.post(
            API_URL_DELETE,
            data={"chat_id": chat_id, "message_id": message_id},
            timeout=15,
        )
        if not resp.ok:
            print(f"[TG ERROR] deleteMessage {resp.status_code}: {resp.text}")
            return False
        payload = resp.json()
        return bool(payload.get("ok"))
    except Exception as e:
        print(f"[TG ERROR] deleteMessage failed: {e}")
        return False


def get_chat_member(chat_id, user_id):
    """Fetch Telegram chat member info."""
    try:
        resp = requests.get(
            API_URL_CHAT_MEMBER,
            params={"chat_id": chat_id, "user_id": user_id},
            timeout=15,
        )
        if not resp.ok:
            print(f"[TG ERROR] getChatMember {resp.status_code}: {resp.text}")
            return None
        payload = resp.json()
        if not payload.get("ok"):
            print(f"[TG ERROR] getChatMember failed: {payload}")
            return None
        return payload.get("result") or None
    except Exception as e:
        print(f"[TG ERROR] getChatMember exception: {e}")
        return None


def set_bot_commands(commands=None):
    """Register visible Telegram slash commands for this bot."""
    if not BOT_TOKEN:
        return False

    if commands is None:
        commands = [
            {"command": "scenarios", "description": "Show BTC scenarios"},
            {"command": "intraday", "description": "Show short-term BTC plans"},
            {"command": "analytics", "description": "Show signal analytics"},
        ]

    scopes = [
        {"type": "default"},
        {"type": "all_chat_administrators"},
        {"type": "all_group_chats"},
        {"type": "all_private_chats"},
    ]
    scoped_chat_id = SIGNAL_CHAT_ID or PRIVATE_EXEC_CHAT_ID or CHAT_ID
    if scoped_chat_id:
        scopes.extend([
            {"type": "chat", "chat_id": str(scoped_chat_id)},
            {"type": "chat_administrators", "chat_id": str(scoped_chat_id)},
        ])

    ok = True
    for scope in scopes:
        try:
            payload = {
                "commands": json.dumps(commands),
                "scope": json.dumps(scope),
            }
            resp = requests.post(API_URL_COMMANDS, data=payload, timeout=15)
            if not resp.ok:
                print(f"[TG ERROR] setMyCommands {scope['type']}: {resp.status_code} {resp.text}")
                ok = False
        except Exception as e:
            print(f"[TG ERROR] setMyCommands {scope['type']} failed: {e}")
            ok = False
    return ok


def send_execution_notice(title, lines=None, chat_id=None, icon="🔐"):
    """Send a private execution/update notice to the execution channel."""
    target_chat = chat_id if chat_id else (PRIVATE_EXEC_CHAT_ID or SIGNAL_CHAT_ID or None)
    body = ""
    if lines:
        clean_lines = [str(line) for line in lines if str(line).strip()]
        if clean_lines:
            body = "\n<pre>" + "\n".join(clean_lines) + "</pre>"
    msg = f"{icon} <b>{title}</b>{body}"
    return send(msg, parse_mode="HTML", chat_id=target_chat)

def send_tp1_hit_congrats(
    chat_id, message_id, tf, side=None, lock_price=None,
    entry=None, sl=None, tp1=None, tp2=None, size=None
):
    """Send a reply for hitting TP1 without changing the stop yet."""
    import random
    messages = [
        f"🟢 <b>TP1 HIT!</b> [{tf}] First target reached cleanly.",
        f"✅ <b>FIRST TARGET CLEARED!</b> [{tf}] Good reaction so far.",
        f"📈 <b>TP1 REACHED</b> [{tf}] The first target was filled.",
    ]
    txt = random.choice(messages)
    return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

def send_tp2_hit_congrats(
    chat_id, message_id, tf, side=None, lock_price=None,
    entry=None, sl=None, tp1=None, tp2=None, size=None, single_full=False
):
    """Send a reply for hitting TP2 or the only active fallback take profit."""
    import random
    if single_full:
        messages = [
            f"🎯 <b>TAKE PROFIT HIT!</b> [{tf}] The full position was closed.",
            f"✅ <b>FULL TAKE PROFIT FILLED!</b> [{tf}] The trade is finished in profit.",
            f"💰 <b>TARGET HIT!</b> [{tf}] The whole position was closed at take profit.",
        ]
        txt = random.choice(messages)
        return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

    messages = [
        f"⚡️ <b>TARGET 2 SMACKED!</b> [{tf}] The runner is protected now.",
        f"💹 <b>TP2 REACHED!</b> [{tf}] The stop was moved to protected breakeven.",
        f"🛡 <b>PROTECTION ON</b> [{tf}] TP2 was reached and the trade is locked.",
    ]
    txt = random.choice(messages)
    if lock_price is not None:
        side_txt = side if side else "POSITION"
        txt += (
            f"\n\n\U0001F6E1 <b>Safety Update</b>\n"
            f"Set SL to <b>protected breakeven</b> for <b>{side_txt}</b> at <b>{fmt_price(lock_price)}</b>"
        )
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
    txt = f"📉 <b>REVERSAL ALERT!</b> [{tf}] Price returned to the protected breakeven level after hitting targets. Signal closed safely. ⚖️"
    return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

def send_profit_sl_alert(chat_id, message_id, tf):
    """Send a reply when protected stop is hit in profit after targets."""
    txt = (
        f"🛡 <b>PROTECTED EXIT!</b> [{tf}] Stop-loss was hit in <b>profit</b> "
        f"after targets. Trade closed safely with locked gains. ✅"
    )
    return send(txt, parse_mode="HTML", chat_id=chat_id, reply_to_message_id=message_id)

def edit_message_text(message_id, text, chat_id=None, parse_mode="HTML"):
    """Edit an existing text message."""
    target_chat = chat_id if chat_id else _default_public_chat_id()
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


def send_photo(photo_path, caption=None, chat_id=None, message_thread_id=None):
    """Send a photo via Telegram Bot API."""
    target_chat = chat_id if chat_id else _default_public_chat_id()
    try:
        with open(photo_path, 'rb') as f:
            files = {'photo': f}
            payload = {'chat_id': target_chat}
            if message_thread_id is None:
                message_thread_id = _default_thread_id_for_chat(target_chat)
            if message_thread_id:
                payload["message_thread_id"] = message_thread_id
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
    target_chat = chat_id if chat_id else _default_public_chat_id()
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


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# LIQUIDITY SWEEP
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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


def send_liquidity_pool_alert(timeframe, side, level_price, size_usd, probability_pct, distance_pct, current_price, chat_id=None):
    """
    Alert about large visible order-book liquidity pool likely to be swept.
    side LONG -> pool above price, SHORT -> pool below price.
    """
    side_emoji = "🟢" if side == "LONG" else "🔴"
    code_part = (
        f"Timeframe:   {timeframe}\n"
        f"Direction:   {side}\n"
        f"Pool Price:  {fmt_price(level_price)}\n"
        f"Current:     {fmt_price(current_price)}\n"
        f"Pool Size:   ${size_usd:,.0f}\n"
        f"Distance:    {distance_pct:.2f}%\n"
        f"Sweep Prob:  {probability_pct:.0f}%"
    )
    msg = (
        f"<b>🧲 Liquidity Pool Alert</b>\n"
        f"<b>{side_emoji} {side}</b>\n"
        f"<pre>{code_part}</pre>"
    )
    send(msg, parse_mode="HTML", chat_id=chat_id)


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# VOLATILITY ZONE TOUCH
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# SCALP SYSTEM
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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


def send_scalp_closed(timeframe, side, price, emoji="❌", chat_id=None):
    """
    ❌ SCALP WINDOW CLOSED [TF] - no confirmation
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


def send_scalp_prepare(timeframe, side, points=None, strength=None, emoji="⚠️", chat_id=None):
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


# --- Formatting Helpers -------------------------------------
def fmt_hit(is_hit):
    """Return a checkmark if level was hit."""
    return " ✅" if is_hit else ""

def get_signal_levels_code(entry, sl, tp1, tp2, tp3, status="OPEN", tp1_h=False, tp2_h=False, tp3_h=False, sl_h=False, initial_sl=None):
    """Format the levels block with hit markers."""
    tp1_mark = fmt_hit(tp1_h)
    tp2_mark = fmt_hit(tp2_h)
    tp3_mark = fmt_hit(tp3_h)
    initial_sl = sl if initial_sl is None else initial_sl
    moved_sl = abs(float(sl) - float(initial_sl)) > 1e-9
    initial_sl_mark = ""
    lock_mark = " ✅" if (sl_h and moved_sl) else ""
    if sl_h and not moved_sl:
        initial_sl_mark = " ✅"

    lines = [
        f"Entry:  {fmt_price(entry)}",
        f"SL:     {fmt_price(initial_sl)}{initial_sl_mark}",
        f"",
        f"TP1:    {fmt_price(tp1)}{tp1_mark}",
        f"TP2:    {fmt_price(tp2)}{tp2_mark}",
        f"TP3:    {fmt_price(tp3)}{tp3_mark}"
    ]
    if moved_sl:
        lines.insert(2, f"Lock:   {fmt_price(sl)}{lock_mark}")
        lines.insert(3, f"")

    return "\n".join(lines)

def get_signal_html(signal_type, side, timeframe, entry, sl, tp1, tp2, tp3,
                    status="OPEN", tp1_h=False, tp2_h=False, tp3_h=False, sl_h=False,
                    score=None, trend=None, indicators=None, reasons=None, size=None,
                    tp_liq_prob=None, tp_liq_usd=None, tp_liq_target=None,
                    trigger_label=None, initial_sl=None):
    """Generate HTML for Scalp, Strong, or Extreme signals."""
    side_emoji = "\U0001F7E2" if side == "LONG" else "\U0001F534"

    if signal_type == "SCALP":
        trigger_name = str(trigger_label or "")
        if trigger_name == "Smart Money Liquidity":
            header = f"<b>\U0001F3E6 SMART MONEY ENTRY CONFIRMED</b> [{timeframe.upper()}]\n"
        else:
            emoji = "\U0001F680" if timeframe.lower() in ["1h", "4h"] else "\u26A1\uFE0F"
            label = "SCALP" if timeframe.lower() in ["5m", "15m"] else "SIGNAL"
            header = f"<b>{emoji} {label} ENTRY CONFIRMED</b> [{timeframe.upper()}]\n"
        header += f"<b>{side_emoji} {side}</b>\n\n"
    elif signal_type == "STRONG":
        emoji = "\u2705"
        header = f"<b>{emoji} STRONG {side} CONFLUENCE</b>\n"
        header += f"<b>{side_emoji} Market Divergence Detected [{timeframe}]</b>\n\n"
    elif signal_type == "EXTREME":
        emoji = "\U0001F525"
        header = f"<b>{emoji} EXTREME {side} CONFLUENCE</b>\n"
        header += f"<b>{side_emoji} High-Alpha Setup Identified [{timeframe}]</b>\n\n"
    else:
        header = f"<b>\U0001F514 {signal_type} {side}</b>\n"

    details = ""
    if signal_type == "SCALP":
        trigger_name = trigger_label or "Momentum Exit"
        score_display = f"Score:   {score}/10\n" if score else ""
        trend_display = f"Trend:   {trend}\n" if trend else ""
        reasons_label = "Model" if trigger_name == "Smart Money Liquidity" else "Confl"
        reasons_display = f"{reasons_label}:   {', '.join(reasons)}\n" if reasons else ""
        size_display = f"Size:    {size}%\n" if size is not None else ""
        liq_display = ""
        if tp_liq_prob is not None and tp_liq_usd is not None and tp_liq_target:
            liq_display = f"TP Liq:  {tp_liq_target} ${tp_liq_usd/1e6:.1f}M | Prob {tp_liq_prob:.0f}%\n"
        details = f"<pre>Trigger: {trigger_name}\n{score_display}{trend_display}{reasons_display}{size_display}{liq_display}</pre>\n"
    else:
        num_systems = len(indicators) if indicators else 0
        total_points = sum(ind['points'] for ind in indicators) if indicators else 0
        size_display = f"\n<b>Risk Size:</b> {size}%" if size is not None else ""
        liq_display = ""
        if tp_liq_prob is not None and tp_liq_usd is not None and tp_liq_target:
            liq_display = f"\n<b>TP Liquidity:</b> {tp_liq_target} ${tp_liq_usd/1e6:.1f}M | Prob {tp_liq_prob:.0f}%"
        details = (
            f"<b>Confluence:</b> {num_systems} Systems Agree\n"
            f"<b>Total Weight:</b> {total_points} Points"
            f"{size_display}"
            f"{liq_display}\n"
        )

    levels_code = get_signal_levels_code(entry, sl, tp1, tp2, tp3, status, tp1_h, tp2_h, tp3_h, sl_h, initial_sl=initial_sl)
    msg = header + details + f"\n<b>\u26A1\uFE0F TRADE LEVELS</b>\n<pre>{levels_code}</pre>\n"

    if status == "OPEN":
        msg += f"\n<b>\U0001F535 POSITION OPEN</b>"
    elif status == "TP1":
        msg += f"\n<b>\U0001F535 POSITION OPEN (TP1 \u2705)</b>"
    elif status == "TP2":
        msg += f"\n<b>\U0001F535 POSITION OPEN (TP2 \u2705)</b>"
    elif status == "TP3":
        msg += f"\n<b>\U0001F4B0 ALL TARGETS HIT</b>"
    elif status == "SL":
        msg += f"\n<b>\u274C STOP LOSS HIT</b>"
    elif status == "PROFIT_SL":
        msg += f"\n<b>\U0001F6E1 STOP HIT IN PROFIT</b>"
    elif status == "ENTRY_CLOSE":
        msg += f"\n<b>\U0001F7E1 CLOSED AT BREAKEVEN | NOT ACTIVE</b>"
    elif status == "CLOSED":
        msg += f"\n<b>\U0001F6E1 CLOSED AFTER TP | NOT ACTIVE</b>"

    if (signal_type in ["STRONG", "EXTREME"]) and indicators:
        ind_lines = []
        for ind in indicators:
            name = ind['name'].replace("Ponch_", "").replace("_", " ")
            sig = ind['signal'].replace("ENTRY ", "")
            ind_lines.append(f"- {name}: {sig} (+{ind['points']})")

        msg += f"\n\n<b>Matched Strategies:</b>\n<pre>" + "\n".join(ind_lines) + "</pre>"

    return msg


def send_scalp_confirmed(timeframe, side, entry, sl, tp1, tp2, tp3,
                         strength, size, score=None, trend=None, reasons=None, chat_id=None,
                         tp_liq_prob=None, tp_liq_usd=None, tp_liq_target=None,
                         trigger_label=None, initial_sl=None, message_thread_id=None):
    """⚡️/🚀 SCALP ENTRY CONFIRMED"""
    html = get_signal_html("SCALP", side, timeframe, entry, sl, tp1, tp2, tp3,
                           score=score, trend=trend, reasons=reasons, size=size,
                           tp_liq_prob=tp_liq_prob, tp_liq_usd=tp_liq_usd, tp_liq_target=tp_liq_target,
                           trigger_label=trigger_label, initial_sl=sl)
    return send(html, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)


def send_strong(side, total_points, confirmations, indicators_list, price=None, sl=None, tp1=None, tp2=None, tp3=None, size=None, chat_id=None,
                tp_liq_prob=None, tp_liq_usd=None, tp_liq_target=None, message_thread_id=None):
    """✅ STRONG CONFLUENCE"""
    tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators_list)))
    tf_summary = ", ".join(tfs)

    html = get_signal_html("STRONG", side, tf_summary, price, sl, tp1, tp2, tp3,
                           indicators=indicators_list, size=size,
                           tp_liq_prob=tp_liq_prob, tp_liq_usd=tp_liq_usd, tp_liq_target=tp_liq_target)
    return send(html, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)


def send_extreme(side, total_points, confirmations, indicators_list, price=None, sl=None, tp1=None, tp2=None, tp3=None, size=None, chat_id=None,
                 tp_liq_prob=None, tp_liq_usd=None, tp_liq_target=None, message_thread_id=None):
    """🔥 EXTREME CONFLUENCE"""
    tfs = sorted(list(set(ind.get('tf', 'N/A') for ind in indicators_list)))
    tf_summary = ", ".join(tfs)

    html = get_signal_html("EXTREME", side, tf_summary, price, sl, tp1, tp2, tp3,
                           indicators=indicators_list, size=size,
                           tp_liq_prob=tp_liq_prob, tp_liq_usd=tp_liq_usd, tp_liq_target=tp_liq_target)
    return send(html, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)


def update_signal_message(chat_id, msg_id, sig_data):
    """Edit the original signal message with updated hit markers."""
    # 'meta' contains original info like indicators or reasons
    meta = sig_data.get("meta", {})
    execution = sig_data.get("execution") or {}
    missing_tp_indices = {
        int(i) for i in (execution.get("missing_tp_indices") or [])
        if str(i).strip().isdigit()
    }
    tp1_h = bool(sig_data.get("tp1_hit"))
    tp2_h = bool(sig_data.get("tp2_hit"))
    tp3_h = bool(sig_data.get("tp3_hit"))
    if tp2_h and 1 not in missing_tp_indices:
        tp1_h = True
    if tp3_h and 1 not in missing_tp_indices:
        tp1_h = True
    if tp3_h and 2 not in missing_tp_indices:
        tp2_h = True

    effective_status = str(sig_data.get("status", "OPEN") or "OPEN").upper()
    entry = float(sig_data.get("entry", 0) or 0)
    sl = float(sig_data.get("sl", 0) or 0)
    initial_sl = float(sig_data.get("initial_sl", sl) or sl)
    if effective_status == "SL" and tp1_h:
        if abs(sl - entry) < 1e-9:
            effective_status = "ENTRY_CLOSE"
        elif abs(sl - initial_sl) > 1e-9:
            effective_status = "PROFIT_SL"
    
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
        initial_sl=sig_data.get("initial_sl", sig_data["sl"]),
        tp1=sig_data["tp1"],
        tp2=sig_data["tp2"],
        tp3=sig_data["tp3"],
        status=effective_status,
        tp1_h=tp1_h,
        tp2_h=tp2_h,
        tp3_h=tp3_h,
        sl_h=(effective_status in {"SL", "PROFIT_SL", "ENTRY_CLOSE"} or bool(sig_data.get("sl_hit"))),
        score=meta.get("score"),
        trend=meta.get("trend"),
        indicators=indicators,
        reasons=meta.get("reasons"),
        size=sig_data.get("signal_size_pct", meta.get("size")),
        tp_liq_prob=meta.get("tp_liq_prob"),
        tp_liq_usd=meta.get("tp_liq_usd"),
        tp_liq_target=meta.get("tp_liq_target"),
        trigger_label=meta.get("trigger"),
    )
    return edit_message_text(msg_id, html, chat_id=chat_id)


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# DAILY LEVELS REPORT
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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
                      critical_high, critical_low, indicators=None, chart_path=None, chat_id=None, message_thread_id=None):
    """
    📊 DAILY LEVELS
    """
    msg = get_daily_levels_html(
        date_str, daily_open, resistance, resistance_pct,
        support, support_pct, volatility, volatility_pct,
        critical_high, critical_low, indicators
    )
    
    if chart_path:
        resp = send_photo(chart_path, caption=msg, chat_id=chat_id, message_thread_id=message_thread_id)
        return {"response": resp, "html": msg}
    else:
        resp = send(msg, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)
        return {"response": resp, "html": msg}
        
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# PERFORMANCE SUMMARY
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

def send_performance_summary(stats, chat_id=None, message_thread_id=None):
    """Send daily signal performance recap."""
    if not stats or stats["total"] == 0:
        return

    insight_lines = []
    best_tf = stats.get("best_timeframe")
    worst_tf = stats.get("worst_timeframe")
    best_strategy = stats.get("best_strategy")
    worst_strategy = stats.get("worst_strategy")
    if best_tf:
        insight_lines.append(
            f"Best TF:   {best_tf['name']} ({float(best_tf['win_rate']):.1f}% / {int(best_tf['trades'])} trades)"
        )
    if worst_tf:
        insight_lines.append(
            f"Worst TF:  {worst_tf['name']} ({float(worst_tf['win_rate']):.1f}% / {int(worst_tf['trades'])} trades)"
        )
    if best_strategy:
        insight_lines.append(
            f"Best Mod:  {best_strategy['name']} ({float(best_strategy['win_rate']):.1f}% / {int(best_strategy['trades'])} trades)"
        )
    if worst_strategy:
        insight_lines.append(
            f"Worst Mod: {worst_strategy['name']} ({float(worst_strategy['win_rate']):.1f}% / {int(worst_strategy['trades'])} trades)"
        )

    formatted_stats = (
        f"TP1 Hit:  {stats['tp1_hits']}\n"
        f"TP2 Hit:  {stats['tp2_hits']}\n"
        f"TP3 Hit:  {stats['tp3_hits']}\n"
        f"SL Hit:   {stats['sl_hits']}\n"
        f"Open:     {stats['still_open']}\n"
        f"-------------------\n"
        f"Win Rate: {stats['win_rate']:.1f}%"
    )
    if insight_lines:
        formatted_stats += "\n" + "\n".join(insight_lines)

    msg = (
        f"📊 SIGNAL PERFORMANCE\n"
        f"\n"
        f"Total Signals: {stats['total']}\n"
        f"<pre>{formatted_stats}</pre>"
    )
    send(msg, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# PRICE APPROACHING LEVEL
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# FUNDING RATE ALERT
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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

# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# VOLUME SPIKE
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# SESSION ALERTS
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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

def send_session_open(session_name, open_price, current_price=None, history=None, high=None, low=None, chart_path=None, chat_id=None, message_thread_id=None):
    """Send alert when a session opens or bot starts mid-session."""
    msg = get_session_open_html(session_name, open_price, current_price, history, high, low)
        
    if chart_path and os.path.exists(chart_path):
        resp = send_photo(chart_path, caption=msg, chat_id=chat_id, message_thread_id=message_thread_id)
        return {"response": resp, "html": msg}
    else:
        send(msg, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)
        return None


def send_session_summary(session_name, price_open, price_close, signals_count, levels_tested, history=None, high=None, low=None, chart_path=None, chat_id=None, message_thread_id=None):
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
        f"-------------------",
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
        return send_photo(chart_path, caption=msg, chat_id=chat_id, message_thread_id=message_thread_id)
    else:
        send(msg, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)
        return None


# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# ALERT BATCHING
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

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

def send_performance_summary(stats, chat_id=None, message_thread_id=None):
    """📣 DAILY PERFORMANCE SUMMARY (Public)"""
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
    send(msg, parse_mode="HTML", chat_id=chat_id, message_thread_id=message_thread_id)

# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
# STARTUP / DEBUG
# в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ

def send_startup():
    """Send a startup notification."""
    pass
