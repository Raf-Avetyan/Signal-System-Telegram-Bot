# ─── CTLT Data Fetcher (Bybit V5 REST API) ────────────────────

import pandas as pd
import requests
import time
from config import SYMBOL, KLINE_LIMITS

BYBIT_BASE = "https://api.bybit.com"
KLINES_URL = f"{BYBIT_BASE}/v5/market/kline"

# Map Binance intervals to Bybit V5 intervals
INTERVAL_MAP = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M"
}

def fetch_klines(symbol=SYMBOL, interval="1h", limit=None):
    """
    Fetch OHLCV klines from Bybit V5 public API.
    Returns a pandas DataFrame with columns: Open, High, Low, Close, Volume
    and a DatetimeIndex (UTC).
    """
    if limit is None:
        limit = KLINE_LIMITS.get(interval, 500)

    # Convert binance format (e.g. "1h") to bybit format (e.g. "60")
    bybit_interval = INTERVAL_MAP.get(interval, "60")

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": bybit_interval,
        "limit": limit,
    }

    data = []
    for attempt in range(3):
        try:
            resp = requests.get(KLINES_URL, params=params, timeout=15)
            resp.raise_for_status()
            res_json = resp.json()
            if res_json.get("retCode") == 0:
                data = res_json.get("result", {}).get("list", [])
            else:
                print(f"[ERROR] Bybit API returned error: {res_json.get('retMsg')}")
            break
        except Exception as e:
            if attempt == 2:
                print(f"[ERROR] Failed to fetch {interval} klines: {e}")
                return pd.DataFrame()
            time.sleep(2)

    if not data:
        return pd.DataFrame()

    # Bybit klines array: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
    # Data is returned from newest to oldest, so we reverse it
    df = pd.DataFrame(data, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume", "Turnover"
    ])
    df = df.iloc[::-1].reset_index(drop=True)

    # Convert types
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = df[col].astype(float)

    # Set datetime index (UTC) - Bybit OpenTime is string timestamp in ms
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
