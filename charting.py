# ─── Ponch Charting Utility ───────────────────────────────────

import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import os

def generate_daily_levels_chart(df, levels, symbol="BTCUSDT", timeframe="1H", output_path="daily_chart.png"):
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

    # 1. Resistance (Pump) - Red
    if "Pump" in levels:
        val = levels["Pump"]
        hlines.append(val)
        hcolors.append('#ff0000')
        hwidths.append(1.5)
        all_values.append(val)

    # 2. Support (Dump) - Green
    if "Dump" in levels:
        val = levels["Dump"]
        hlines.append(val)
        hcolors.append('#00ff00')
        hwidths.append(1.5)
        all_values.append(val)
        
    # 3. Critical High (PumpMax) - Bold Red
    if "PumpMax" in levels:
        val = levels["PumpMax"]
        hlines.append(val)
        hcolors.append('#ff1744')
        hwidths.append(2.5)
        all_values.append(val)

    # 4. Critical Low (DumpMax) - Bold Red
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

    # Framing: Cut left, add right space for labels
    num_candles = len(plot_df)
    xlim = (0, num_candles + 14)

    # Style
    mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', edge='inherit', wick='inherit')
    s = mpf.make_mpf_style(marketcolors=mc, base_mpl_style='dark_background', gridcolor='#2b2b2b', facecolor='#131722')

    hlines_dict = dict(hlines=hlines, colors=hcolors, linestyle='--', linewidths=hwidths)

    try:
        fig, axlist = mpf.plot(plot_df, 
                               type='candle', 
                               style=s, 
                               hlines=hlines_dict,
                               title=f"\n{symbol} · {timeframe.upper()} Daily Levels",
                               ylabel='Price',
                               datetime_format='%H:%M',
                               tight_layout=True,
                               savefig=output_path,
                               figratio=(16,9),
                               figscale=1.5,
                               ylim=ylim,
                               xlim=xlim,
                               returnfig=True)
        
        ax = axlist[0]
        label_x = num_candles + 1

        # --- Volatility zone (shaded area between Pump and Dump) ---
        if "Pump" in levels and "Dump" in levels:
            ax.axhspan(levels["Dump"], levels["Pump"], 
                       alpha=0.08, color='#ffab00', zorder=0)
            # Add VOLATILITY ZONE label in the middle of the shaded area
            mid_vol = (levels["Pump"] + levels["Dump"]) / 2
            ax.text(label_x, mid_vol, "  VOLATILITY ZONE", 
                    color='#ffab00', va='center', fontsize=8, fontstyle='italic', alpha=0.7)

        # --- Full text labels ---
        label_map = {
            "Pump": ("RESISTANCE", '#ff0000'),
            "Dump": ("SUPPORT", '#00ff00'),
            "PumpMax": ("CRITICAL HIGH", '#ff1744'),
            "DumpMax": ("CRITICAL LOW", '#ff1744'),
        }

        for key, (label, color) in label_map.items():
            if key in levels:
                price = levels[key]
                ax.text(label_x, price, f"{label} ({price:,.2f})", 
                        color=color, va='center', fontsize=9, fontweight='bold')

        fig.subplots_adjust(left=0.06, right=0.98)
        fig.savefig(output_path, bbox_inches='tight', pad_inches=0.1)
        plt.close(fig)
        return os.path.abspath(output_path)
    except Exception as e:
        print(f"[CHART ERROR] {e}")
        return None
