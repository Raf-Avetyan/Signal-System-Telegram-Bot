# ─── PONCH DATA FETCHER (OKX V5 REST API) ────────────────────

import pandas as pd
import requests
import time
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

    # OKX symbol format for spot is usually BTC-USDT
    okx_symbol = symbol.replace("USDT", "-USDT")
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
    Fetch klines for multiple timeframes.
    Returns dict: {timeframe: DataFrame}
    """
    if timeframes is None:
        timeframes = list(KLINE_LIMITS.keys())

    data = {}
    for tf in timeframes:
        df = fetch_klines(symbol=symbol, interval=tf)
        if not df.empty:
            data[tf] = df
            print(f"  ✓ {tf}: {len(df)} candles")
        else:
            print(f"  ✗ {tf}: FAILED")

    return data


def fetch_daily(symbol=SYMBOL, limit=120):
    """Convenience: fetch daily data for levels calculation."""
    return fetch_klines(symbol=symbol, interval="1d", limit=limit)


def fetch_weekly(symbol=SYMBOL, limit=52):
    """Convenience: fetch weekly data."""
    return fetch_klines(symbol=symbol, interval="1w", limit=limit)


def fetch_monthly(symbol=SYMBOL, limit=12):
    """Convenience: fetch monthly data."""
    return fetch_klines(symbol=symbol, interval="1M", limit=limit)
