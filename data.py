# ─── PONCH DATA FETCHER (OKX V5 REST API) ────────────────────

import json
import base64
import pandas as pd
import requests
import time
from urllib.parse import quote
from config import SYMBOL, KLINE_LIMITS

OKX_BASE = "https://www.okx.com"
KLINES_URL = f"{OKX_BASE}/api/v5/market/candles"

# Map Binance intervals to OKX V5 intervals
INTERVAL_MAP = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1M": "1M"
}

def fetch_klines(symbol=SYMBOL, interval="1h", limit=None):
    """
    Fetch OHLCV klines from OKX V5 public API.
    Returns a pandas DataFrame with columns: Open, High, Low, Close, Volume
    and a DatetimeIndex (UTC).
    """
    if limit is None:
        limit = KLINE_LIMITS.get(interval, 500)

    # Use OKX Perpetual Swap (matches TradingView Perp charts better)
    okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
    okx_interval = INTERVAL_MAP.get(interval, "1H")

    params = {
        "instId": okx_symbol,
        "bar": okx_interval,
        "limit": limit,
    }

    data = []
    for attempt in range(3):
        try:
            resp = requests.get(KLINES_URL, params=params, timeout=15)
            resp.raise_for_status()
            res_json = resp.json()
            if res_json.get("code") == "0":
                data = res_json.get("data", [])
            else:
                print(f"[ERROR] OKX API returned error: {res_json.get('msg')}")
            break
        except Exception as e:
            if attempt == 2:
                print(f"[ERROR] Failed to fetch {interval} klines: {e}")
                return pd.DataFrame()
            time.sleep(2)

    if not data:
        return pd.DataFrame()

    # OKX klines array: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    # Data is returned from newest to oldest, so we reverse it
    df = pd.DataFrame(data, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume", "VolCcy", "VolCcyQuote", "Confirm"
    ])
    df = df.iloc[::-1].reset_index(drop=True)

    # Convert types
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = df[col].astype(float)

    # Set datetime index (UTC) - OKX OpenTime is string timestamp in ms
    df.index = pd.to_datetime(df["OpenTime"].astype(float), unit="ms", utc=True)
    df.index.name = "Datetime"

    # Keep only OHLCV
    df = df[["Open", "High", "Low", "Close", "Volume"]]

    return df


def fetch_all_timeframes(symbol=SYMBOL, timeframes=None):
    """
    Fetch klines for multiple timeframes in parallel.
    Returns dict: {timeframe: DataFrame}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if timeframes is None:
        timeframes = list(KLINE_LIMITS.keys())

    data = {}
    
    # OKX Public API is quite generous, parallesim should be fine
    with ThreadPoolExecutor(max_workers=len(timeframes)) as executor:
        future_to_tf = {executor.submit(fetch_klines, symbol, tf): tf for tf in timeframes}
        for future in as_completed(future_to_tf):
            tf = future_to_tf[future]
            try:
                df = future.result()
                if not df.empty:
                    data[tf] = df
                    # print(f"  ✓ {tf}: {len(df)} candles")
                else:
                    print(f"  ✗ {tf}: FAILED (Empty)")
            except Exception as e:
                print(f"  ✗ {tf}: EXCEPTION {e}")

    return data


def fetch_daily(symbol=SYMBOL, limit=120):
    """Convenience: fetch daily data for levels calculation."""
    return fetch_klines(symbol=symbol, interval="1d", limit=limit)


def fetch_trading_economics_calendar(api_key, countries="united states", importance=3):
    """
    Fetch live economic calendar events from Trading Economics.
    Returns a list of event dicts or [] on failure.
    """
    if not api_key:
        return []

    countries_path = quote(str(countries).strip(), safe=",")
    url = f"https://api.tradingeconomics.com/calendar/country/{countries_path}"
    params = {
        "c": api_key,
        "importance": int(importance),
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[NEWS ERROR] TradingEconomics fetch failed: {e}")
        return []


def parse_gemini_trade_instruction(api_key, model, user_text, context_text):
    """
    Use Gemini to convert a free-form trade-control message into structured JSON.
    Returns a dict or None on failure.
    """
    if not api_key or not model or not user_text:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    prompt = (
        "You are a parser for a live crypto trading bot. "
        "Convert the user's message into a single JSON object only. "
        "Do not include markdown.\n\n"
        "Allowed actions: status, open_signal, open_manual, move_sl, move_sl_entry, move_all_sl_entry, set_tp, close_full, close_partial, close_all_positions, cancel_tp, cancel_all_positions_tps, unsupported.\n"
        "JSON schema:\n"
        "{"
        "\"action\": string,"
        "\"signal_id\": string|null,"
        "\"symbol\": string|null,"
        "\"side\": \"LONG\"|\"SHORT\"|null,"
        "\"tf\": \"5m\"|\"15m\"|\"1h\"|\"4h\"|null,"
        "\"preset\": \"safe\"|\"aggressive\"|\"runner\"|null,"
        "\"tp_index\": 1|2|3|null,"
        "\"price\": number|null,"
        "\"margin_usd\": number|null,"
        "\"leverage\": number|null,"
        "\"tp1\": number|null,"
        "\"tp2\": number|null,"
        "\"tp3\": number|null,"
        "\"sl\": number|null,"
        "\"fraction\": number|null,"
        "\"reason\": string,"
        "\"confidence\": number"
        "}\n\n"
        "Rules:\n"
        "- Use action=open_signal only if the user clearly wants to open a tracked signal.\n"
        "- Use action=open_manual if the user wants a fresh manual market position.\n"
        "- Use move_sl_entry for breakeven/entry stop moves on one position.\n"
        "- Use move_all_sl_entry when the user wants breakeven/entry stop moves on all matching positions.\n"
        "- For close half/30 percent etc, return close_partial with fraction from 0 to 1.\n"
        "- For close all positions / close everything, use close_all_positions.\n"
        "- For close all longs / close all shorts, use close_all_positions with side set when possible.\n"
        "- For set tp1/tp2/tp3, fill tp_index and price.\n"
        "- For cancel all take profits, use action cancel_tp with tp_index null.\n"
        "- For cancel all take profits on every open position, use cancel_all_positions_tps.\n"
        "- If the user names a symbol like BTCUSDT, ETHUSDT, SOLUSDT, keep it in symbol.\n"
        "- If the user asks for a safe, aggressive, or runner style, keep it in preset.\n"
        "- If user asks for something dangerous or unclear, use unsupported.\n"
        "- Prefer signal_id if the user mentions one.\n"
        "- If no signal_id is given, preserve side/tf hints when present.\n\n"
        f"Active signal context:\n{context_text}\n\n"
        f"User message:\n{user_text}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }
    try:
        resp = requests.post(url, params={"key": api_key}, json=payload, timeout=25)
        resp.raise_for_status()
        data = resp.json()
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        if not text:
            return None
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            text = text.replace("json", "", 1).strip()
        try:
            return json.loads(text)
        except Exception:
            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                return json.loads(text[start:end + 1])
    except Exception as e:
        print(f"[GEMINI ERROR] Trade instruction parse failed: {e}")
    return None


def ask_gemini_trade_question(api_key, model, user_text, context_text):
    """
    Use Gemini to answer a plain-language question about the bot, trades, or controls.
    Returns a short plain-text answer or None on failure.
    """
    if not api_key or not model or not user_text:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    prompt = (
        "You are an assistant inside a live crypto trading bot's private execution chat. "
        "The user may ask about positions, bot behavior, controls, how a requested action would work, or may just chat casually. "
        "Answer briefly, clearly, and in simple human language. "
        "Sound like a helpful assistant, not a system alert. "
        "You may use an occasional fitting emoji to keep the conversation alive, but keep it light. "
        "If the answer depends on the current tracked positions, use the provided context. "
        "If the user is just greeting you or talking casually, answer naturally and warmly like a normal assistant. "
        "Do not invent trades or exchange state that is not in the context. "
        "Do not output markdown tables or JSON. "
        "Prefer short paragraphs or short bullet points only when helpful.\n\n"
        f"Active signal context:\n{context_text}\n\n"
        f"User question:\n{user_text}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
        },
    }
    try:
        resp = requests.post(url, params={"key": api_key}, json=payload, timeout=25)
        resp.raise_for_status()
        data = resp.json()
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        text = str(text or "").strip()
        return text or None
    except Exception as e:
        print(f"[GEMINI ERROR] Trade question answer failed: {e}")
    return None


def ask_gemini_trade_question_with_image(api_key, model, user_text, context_text, image_bytes, mime_type="image/jpeg"):
    """
    Use Gemini to answer a plain-language question with an attached screenshot/image.
    Returns a short plain-text answer or None on failure.
    """
    if not api_key or not model or not user_text or not image_bytes:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    prompt = (
        "You are an assistant inside a live crypto trading bot's private execution chat. "
        "The user has attached a trading screenshot or chart image. "
        "Explain clearly what you can see in the image, what matters most, and what you would do next. "
        "Be honest about uncertainty and do not pretend the screenshot alone is enough to execute a trade. "
        "If the user asks for action, you can suggest the next text command they should send. "
        "Answer in simple, natural language. "
        "Do not output markdown tables or JSON.\n\n"
        f"Active signal context:\n{context_text}\n\n"
        f"User request about the image:\n{user_text}"
    )
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {
                    "inlineData": {
                        "mimeType": mime_type or "image/jpeg",
                        "data": base64.b64encode(image_bytes).decode("ascii"),
                    }
                },
            ]
        }],
        "generationConfig": {
            "temperature": 0.2,
        },
    }
    try:
        resp = requests.post(url, params={"key": api_key}, json=payload, timeout=40)
        resp.raise_for_status()
        data = resp.json()
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        text = str(text or "").strip()
        return text or None
    except Exception as e:
        print(f"[GEMINI ERROR] Trade image question failed: {e}")
    return None


def fetch_weekly(symbol=SYMBOL, limit=52):
    """Convenience: fetch weekly data."""
    return fetch_klines(symbol=symbol, interval="1w", limit=limit)


def fetch_monthly(symbol=SYMBOL, limit=12):
    """Convenience: fetch monthly data."""
    return fetch_klines(symbol=symbol, interval="1M", limit=limit)


def fetch_funding_rate(symbol=SYMBOL):
    """
    Fetch current funding rate from OKX perpetual swap.
    Returns float (e.g. 0.0001 = 0.01%) or None on failure.
    """
    okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
    url = f"{OKX_BASE}/api/v5/public/funding-rate"
    params = {"instId": okx_symbol}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == "0" and data.get("data"):
            rate = float(data["data"][0].get("fundingRate", 0))
            return rate
    except Exception as e:
        print(f"[ERROR] Failed to fetch funding rate: {e}")
    return None


def fetch_open_interest(symbol=SYMBOL):
    """
    Fetch current open interest for the perpetual swap.
    Returns float (number of contracts) or None.
    """
    okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
    url = f"{OKX_BASE}/api/v5/public/open-interest"
    params = {"instId": okx_symbol}

    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data.get("code") == "0" and data.get("data"):
            oi = float(data["data"][0].get("oi", 0))
            return oi
    except Exception as e:
        print(f"[ERROR] Failed to fetch OI: {e}")
    return None


def fetch_liquidations(symbol=SYMBOL):
    """
    Fetch recent liquidation history for the instrument.
    Aggregates the total USD value of liquidations in the last block.
    """
    okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
    url = f"{OKX_BASE}/api/v5/public/liquidation-orders"
    # instType=SWAP, mgnMode=cross/isolated
    params = {
        "instType": "SWAP",
        "instId": okx_symbol,
        "state": "filled",
        "limit": 50
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        if data.get("code") == "0" and data.get("data"):
            total_usd = 0
            for order in data["data"]:
                # posSide: long/short
                sz = float(order.get("sz", 0))
                price = float(order.get("bkPx", 0)) # Bankruptcy price
                total_usd += sz * price
            return total_usd
    except Exception as e:
        # print(f"[DEBUG] No recent liquidations found or error: {e}")
        pass
    return 0


def fetch_order_book(symbol=SYMBOL, depth=400):
    """
    Fetch current L2 order book from OKX.
    Returns {"bids": [[px, sz], ...], "asks": [[px, sz], ...]} or None on failure.
    """
    okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
    url = f"{OKX_BASE}/api/v5/market/books"
    params = {
        "instId": okx_symbol,
        "sz": min(max(int(depth), 1), 400),
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != "0" or not payload.get("data"):
            return None
        book = payload["data"][0]
        bids = []
        asks = []
        for row in book.get("bids", []):
            if len(row) >= 2:
                bids.append([float(row[0]), float(row[1])])
        for row in book.get("asks", []):
            if len(row) >= 2:
                asks.append([float(row[0]), float(row[1])])
        return {"bids": bids, "asks": asks}
    except Exception as e:
        print(f"[ERROR] Failed to fetch order book: {e}")
        return None


def fetch_global_indicators():
    """
    Fetch BTC Dominance and DXY Index approximate values.
    Uses EUR-USD index as a proxy for DXY.
    """
    indicators = {
        "BTC.D_change": 0.0,
        "DXY_change": 0.0
    }
    
    try:
        # 1. BTC Dominance Proxy: BTC Index vs ETH Index
        btc_resp = requests.get(f"{OKX_BASE}/api/v5/market/ticker", params={"instId": "BTC-USDT"})
        eth_resp = requests.get(f"{OKX_BASE}/api/v5/market/ticker", params={"instId": "ETH-USDT"})
        
        if btc_resp.ok and eth_resp.ok:
            btc_data = btc_resp.json()["data"][0]
            eth_data = eth_resp.json()["data"][0]
            
            btc_24h = float(btc_data["last"]) / float(btc_data["open24h"]) - 1
            eth_24h = float(eth_data["last"]) / float(eth_data["open24h"]) - 1
            
            indicators["BTC.D_change"] = (btc_24h - eth_24h) * 100

        # 2. DXY Proxy: Inverse of EUR-USD index daily change
        # DXY is ~57% EUR, so -EURUSD change is a strong proxy.
        dxy_resp = requests.get(f"{OKX_BASE}/api/v5/market/index-candles", 
                                params={"instId": "EUR-USD", "bar": "1D", "limit": "1"})
        
        if dxy_resp.ok and dxy_resp.json()["data"]:
            day_data = dxy_resp.json()["data"][0]
            # [ts, open, high, low, close, confirm]
            eur_open = float(day_data[1])
            eur_curr = float(day_data[4])
            
            if eur_open > 0:
                eur_change = (eur_curr / eur_open) - 1
                indicators["DXY_change"] = -eur_change * 100
            
    except Exception as e:
        print(f"[ERROR] Global indicators: {e}")
        
    return indicators


def fetch_last_price(symbol=SYMBOL):
    """
    Fetch the latest traded swap price from OKX.
    Returns float or None on failure.
    """
    okx_symbol = symbol.replace("USDT", "-USDT-SWAP")
    url = f"{OKX_BASE}/api/v5/market/ticker"
    params = {"instId": okx_symbol}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") == "0" and payload.get("data"):
            return float(payload["data"][0].get("last", 0) or 0)
    except Exception as e:
        print(f"[ERROR] Failed to fetch last price: {e}")
    return None
