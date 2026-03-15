
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from charting import generate_daily_levels_chart

# Create mock data
dates = [datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=i) for i in range(48)]
dates.reverse()

data = {
    "Open": np.random.uniform(70000, 72000, 48),
    "High": np.random.uniform(71000, 73000, 48),
    "Low": np.random.uniform(69000, 71000, 48),
    "Close": np.random.uniform(70000, 72000, 48),
    "Volume": np.random.uniform(100, 1000, 48)
}
df = pd.DataFrame(data, index=dates)

levels = {
    "DO": 71000,
    "Pump": 72500,
    "Dump": 69500,
    "PumpMax": 73500,
    "DumpMax": 68500
}

path = generate_daily_levels_chart(df, levels, output_path="test_session_chart.png")
if path:
    print(f"Chart generated at: {path}")
else:
    print("Failed to generate chart")
