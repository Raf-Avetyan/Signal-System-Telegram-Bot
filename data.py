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
