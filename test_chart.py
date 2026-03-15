# ─── Chart Verification Script ───────────────────────────────

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from charting import generate_daily_levels_chart
import os

def test_chart():
    print("Testing Perfectly Framed Daily Levels Chart Generation...")
    
    # 1. Create Mock Data (48 candles)
    num_candles = 48
    dates = [datetime.now() - timedelta(hours=i) for i in range(num_candles)]
    dates.reverse()
    
    df = pd.DataFrame({
        "Open": np.random.normal(71000, 100, num_candles),
        "High": np.random.normal(71100, 100, num_candles),
        "Low": np.random.normal(70900, 100, num_candles),
        "Close": np.random.normal(71000, 100, num_candles),
        "Volume": np.random.randint(100, 1000, num_candles)
    }, index=pd.to_datetime(dates, utc=True))
    
    # 2. Mock Levels
    levels = {
        "DO": 71000.0,
        "PDH": 71500.0,
        "PDL": 70500.0,
        "Pump": 72500.0,
        "Dump": 69500.0,
        "PumpMax": 74000.0,
        "DumpMax": 68000.0
    }
    
    # 3. Generate Chart
    output_fn = "perfect_framed_chart.png"
    path = generate_daily_levels_chart(df, levels, output_path=output_fn)
    
    if path and os.path.exists(path):
        print(f"✅ Success! Chart saved to: {path}")
    else:
        print("❌ Failed to generate chart.")

if __name__ == "__main__":
    test_chart()
