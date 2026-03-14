# ─── CTLT Data Fetcher (Binance REST API) ────────────────────

import pandas as pd
import requests
import time
from config import SYMBOL, KLINE_LIMITS

BINANCE_BASE = "https://api.binance.com"
KLINES_URL   = f"{BINANCE_BASE}/api/v3/klines"


def fetch_klines(symbol=SYMBOL, interval="1h", limit=None):
    """
    Fetch OHLCV klines from Binance public API.
    Returns a pandas DataFrame with columns: Open, High, Low, Close, Volume
    and a DatetimeIndex (UTC).
    """
    if limit is None:
        limit = KLINE_LIMITS.get(interval, 500)

    params = {
        "symbol":   symbol,
        "interval": interval,
        "limit":    limit,
    }

    for attempt in range(3):
        try:
            resp = requests.get(KLINES_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt == 2:
                print(f"[ERROR] Failed to fetch {interval} klines: {e}")
                return pd.DataFrame()
            time.sleep(2)

    df = pd.DataFrame(data, columns=[
        "OpenTime", "Open", "High", "Low", "Close", "Volume",
        "CloseTime", "QuoteVolume", "Trades", "TakerBuyBase",
        "TakerBuyQuote", "Ignore"
    ])

    # Convert types
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        df[col] = df[col].astype(float)

    # Set datetime index (UTC)
    df.index = pd.to_datetime(df["OpenTime"], unit="ms", utc=True)
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
