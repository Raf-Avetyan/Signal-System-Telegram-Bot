import pandas as pd
import numpy as np
import yfinance as yf
import requests
import time

# ─── TELEGRAM CONFIG ─────────────────────────────
BOT_TOKEN = "8794350463:AAG8i7Htj4P7bMcukrmAADop3BjgnOSWpHo"
CHAT_ID   = "5522646948"

def send_signal(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

# ─── PARAMETERS ─────────────────────────────────
# EMA / ATR
ema1_len = 9
ema2_len = 21
ema3_len = 55
atr_len  = 14
mult1 = 1.0
mult2 = 2.5
mult3 = 5.0

# Levels
adr_len = 14

# ─── HELPER FUNCTIONS ───────────────────────────
def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def atr(df, length):
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(length).mean()

# ─── FETCH DATA ─────────────────────────────────
def fetch_data(symbol="BTC-USD", interval="1m", period="1d"):
    df = yf.download(symbol, interval=interval, period=period)
    # Flatten MultiIndex columns (yfinance returns ('Close','BTC-USD') tuples)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    df.dropna(inplace=True)
    df.index = pd.to_datetime(df.index).tz_convert(None)  # Remove tz info
    return df

# ─── CALCULATE CHANNELS ─────────────────────────
def calculate_channels(df):
    df['EMA1'] = ema(df['Close'], ema1_len)
    df['EMA2'] = ema(df['Close'], ema2_len)
    df['EMA3'] = ema(df['Close'], ema3_len)
    df['ATR']  = atr(df, atr_len)
    
    df['InnerUp'] = df['EMA1'] + df['ATR'] * mult1
    df['InnerDn'] = df['EMA1'] - df['ATR'] * mult1
    df['MidUp']   = df['EMA2'] + df['ATR'] * mult2
    df['MidDn']   = df['EMA2'] - df['ATR'] * mult2
    df['OuterUp'] = df['EMA3'] + df['ATR'] * mult3
    df['OuterDn'] = df['EMA3'] - df['ATR'] * mult3
    
    return df

# ─── LEVELS ────────────────────────────────────
def calculate_levels(df):
    # Resample Open prices
    daily_open = df['Open'].resample('1D').first()
    weekly_open = df['Open'].resample('1W').first()
    monthly_open = df['Open'].resample('1ME').first()  # Month-End

    # Forward-fill to match df index
    df['DO'] = daily_open.reindex(df.index, method='ffill')
    df['WO'] = weekly_open.reindex(df.index, method='ffill')
    df['MO'] = monthly_open.reindex(df.index, method='ffill')

    # Previous Day High/Low
    df['PDH'] = df['High'].shift(1)
    df['PDL'] = df['Low'].shift(1)

    # Pump/Dump
    df['AvgPump'] = (df['High'] - df['Open']).rolling(adr_len).mean()
    df['AvgDump'] = (df['Open'] - df['Low']).rolling(adr_len).mean()
    df['PumpLvl'] = df['DO'] + df['AvgPump']
    df['DumpLvl'] = df['DO'] - df['AvgDump']

    # Ensure numeric types
    for col in ['DO', 'WO', 'MO', 'PDH', 'PDL', 'PumpLvl', 'DumpLvl']:
        df[col] = df[col].astype(float)

    return df

# ─── SIGNAL LOGIC ──────────────────────────────
def check_signals(df):
    df['BullPts'] = ((df['Close'] > df['DO']).astype(int) +
                     (df['Close'] > df['WO']).astype(int) +
                     (df['Close'] > df['MO']).astype(int))
    
    df['BearPts'] = ((df['Close'] < df['DO']).astype(int) +
                     (df['Close'] < df['WO']).astype(int) +
                     (df['Close'] < df['MO']).astype(int))
    
    long_cross  = ((df['Low'] < df['OuterDn']) |
                   (df['Close'] < df['MidDn']) |
                   ((df['Close'] < df['InnerDn']) & (df['BullPts'] >= 1)))
    
    short_cross = ((df['High'] > df['OuterUp']) |
                   (df['Close'] > df['MidUp']) |
                   ((df['Close'] > df['InnerUp']) & (df['BearPts'] >= 1)))
    
    if long_cross.iloc[-1]:
        send_signal("🚀 LONG signal detected!")
    if short_cross.iloc[-1]:
        send_signal("🔻 SHORT signal detected!")

# ─── LIVE LOOP ─────────────────────────────────
def run_live(symbol="BTC-USD"):
    while True:
        df = fetch_data(symbol)
        df = calculate_channels(df)
        df = calculate_levels(df)
        check_signals(df)
        time.sleep(60)  # adjust per interval

# ─── RUN ──────────────────────────────────────
if __name__ == "__main__":
    run_live("BTC-USD")