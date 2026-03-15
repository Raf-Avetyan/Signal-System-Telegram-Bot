# ─── Ponch Charting Utility ───────────────────────────────────

import pandas as pd
import mplfinance as mpf
import os

def generate_daily_levels_chart(df, levels, symbol="BTCUSDT", output_path="daily_chart.png"):
    """
    Generate a candlestick chart with daily levels as horizontal lines.
    """
    if df.empty or not levels:
        return None

    # Filter to last 48 candles (original resolution)
    plot_df = df.tail(48).copy()

    hlines = []
    hcolors = []
    hwidths = []
    
    # Required for scaling
    all_values = list(plot_df["High"]) + list(plot_df["Low"])

    # 1. DO (Daily Open) - White
    if "DO" in levels:
        val = levels["DO"]
        hlines.append(val)
        hcolors.append('#ffffff')
        hwidths.append(1.0)
        all_values.append(val)
    
    # 2. PDH/PDL (Previous Day High/Low) - Cyan
    if "PDH" in levels:
        val = levels["PDH"]
        hlines.append(val)
        hcolors.append('#00e5ff')
        hwidths.append(1.0)
        all_values.append(val)
    if "PDL" in levels:
        val = levels["PDL"]
        hlines.append(val)
        hcolors.append('#00e5ff')
        hwidths.append(1.0)
        all_values.append(val)

    # 3. Main Resistance / Support (Pump/Dump) - Red/Green
    if "Pump" in levels:
        val = levels["Pump"]
        hlines.append(val)
        hcolors.append('#ff0000') # Pure Red
        hwidths.append(1.5)
        all_values.append(val)
    if "Dump" in levels:
        val = levels["Dump"]
        hlines.append(val)
        hcolors.append('#00ff00') # Pure Green
        hwidths.append(1.5)
        all_values.append(val)
        
    # 4. Critical High / Low (PumpMax / DumpMax) - Bold Red
    if "PumpMax" in levels:
        val = levels["PumpMax"]
        hlines.append(val)
        hcolors.append('#ff1744')
        hwidths.append(2.5)
        all_values.append(val)
    if "DumpMax" in levels:
        val = levels["DumpMax"]
        hlines.append(val)
        hcolors.append('#ff1744')
        hwidths.append(2.5)
        all_values.append(val)

    # Calculate Y-limits with 5% padding
    ymin = min(all_values)
    ymax = max(all_values)
    padding_y = (ymax - ymin) * 0.05
    ylim = (ymin - padding_y, ymax + padding_y)

    # Perfect Framing: Cut left empty space, add right space for labels
    num_candles = len(plot_df)
    xlim = (0, num_candles + 10) # Start exactly at first candle, end 10 units after

    # Style
    mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', edge='inherit', wick='inherit')
    s = mpf.make_mpf_style(marketcolors=mc, base_mpl_style='dark_background', gridcolor='#2b2b2b', facecolor='#131722')

    hlines_dict = dict(hlines=hlines, colors=hcolors, linestyle='--', linewidths=hwidths)

    try:
        # Use returnfig to add labels
        fig, axlist = mpf.plot(plot_df, 
                               type='candle', 
                               style=s, 
                               hlines=hlines_dict,
                               title=f"\n{symbol} Daily Levels",
                               ylabel='Price',
                               datetime_format='%H:%M',
                               tight_layout=True,
                               savefig=output_path,
                               figratio=(16,9),
                               figscale=1.5,
                               ylim=ylim,
                               xlim=xlim,
                               returnfig=True)
        
        # Add text labels on the padded right side
        ax = axlist[0]
        label_x = num_candles + 1
        
        label_map = {
            "DO": ("DO", '#ffffff'),
            "PDH": ("PDH", '#00e5ff'),
            "PDL": ("PDL", '#00e5ff'),
            "Pump": ("RES", '#ff0000'),
            "Dump": ("SUP", '#00ff00'),
            "PumpMax": ("CRIT H", '#ff1744'),
            "DumpMax": ("CRIT L", '#ff1744')
        }

        for key, (label, color) in label_map.items():
            if key in levels:
                ax.text(label_x, levels[key], f"  {label}", 
                        color=color, va='center', fontsize=9, fontweight='bold')

        fig.savefig(output_path)
        return os.path.abspath(output_path)
    except Exception as e:
        print(f"[CHART ERROR] {e}")
        return None
