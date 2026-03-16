# ─── Ponch Charting Utility ───────────────────────────────────

import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import os

from config import SESSIONS, get_adjusted_sessions

def generate_daily_levels_chart(df, levels, symbol="BTCUSDT", timeframe="1H", output_path="daily_chart.png", show_sessions=True, session_stats=None):
    """
    Generate a candlestick chart with daily levels as horizontal lines.
    session_stats: { session_name: {"high": float, "low": float} or {"high": float, "low": float, "id": str} }
    """
    if df.empty or not levels:
        return None

    # Filter to last 48 candles (original resolution)
    plot_df = df.tail(48).copy()
    
    # ... (skipping unchanged code for brevity in instruction, but I'll provide full block)
    # Actually I should provide the full modified block to avoid errors.

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

        # --- Session Vertical Lines & H/L Labels ---
        if show_sessions:
            session_colors = {
                "ASIA": "#3d5afe",   # Blue
                "LONDON": "#ff9100", # Orange
                "NY": "#00e676"      # Green
            }
            
            # Get adjusted sessions for the date of the last candle
            last_ts = plot_df.index[-1]
            sessions = get_adjusted_sessions(last_ts)
            plot_df["float_hour"] = plot_df.index.hour + plot_df.index.minute / 60.0

            for s_name, times in sessions.items():
                s_open = times["open"]
                s_close = times["close"]
                color = session_colors.get(s_name, "#ffffff")
                
                # Find indices for this session within the plotted DF
                if s_open < s_close:
                    mask = (plot_df["float_hour"] >= s_open - 0.01) & (plot_df["float_hour"] < s_close - 0.01)
                else: # Crosses midnight
                    mask = (plot_df["float_hour"] >= s_open - 0.01) | (plot_df["float_hour"] < s_close - 0.01)
                
                all_session_candles = plot_df[mask]
                if all_session_candles.empty:
                    continue

                # --- FIX: Only use the MOST RECENT day's session ---
                last_date = all_session_candles.index[-1].date()
                session_df = all_session_candles[all_session_candles.index.date == last_date]
                
                if not session_df.empty:
                    # 1. Vertical line at Open
                    open_ts = session_df.index[0]
                    # Check if this candle is the exact open
                    candle_f_h = open_ts.hour + open_ts.minute / 60.0
                    if abs(candle_f_h - s_open) < 0.01:
                        idx_open = plot_df.index.get_loc(open_ts)
                        ax.axvline(idx_open, color=color, linestyle='--', alpha=0.7, linewidth=1.5)
                        ax.text(idx_open, ymax + padding_y*0.1, f" {s_name}", 
                                color=color, fontsize=11, fontweight='bold', ha='left')

                    # 2. Session High/Low Labels
                    # Priority: Use provided session_stats (live 5s tracking) if available
                    # otherwise fallback to candle-based max/min
                    s_high = session_df["High"].max()
                    s_low = session_df["Low"].min()
                    ts_high = session_df["High"].idxmax()
                    ts_low = session_df["Low"].idxmin()
                    
                    if session_stats and s_name in session_stats:
                        live_high = session_stats[s_name].get("high")
                        live_low = session_stats[s_name].get("low")
                        if live_high: s_high = max(s_high, live_high)
                        if live_low: s_low = min(s_low, live_low)

                    idx_high = plot_df.index.get_loc(ts_high)
                    idx_low = plot_df.index.get_loc(ts_low)
                    
                    short_name = s_name[:2]
                    # Use :,.2f if there are decimals or just :,.0f if clean
                    h_fmt = f"{s_high:,.2f}" if s_high % 1 != 0 else f"{s_high:,.0f}"
                    l_fmt = f"{s_low:,.2f}" if s_low % 1 != 0 else f"{s_low:,.0f}"
                    
                    ax.text(idx_high, s_high, f"{short_name}(H) {h_fmt}", color=color, 
                            fontsize=9, ha='center', va='bottom', fontweight='bold',
                            bbox=dict(facecolor='#131722', alpha=0.8, pad=1, edgecolor=color, linewidth=0.5))
                    ax.text(idx_low, s_low, f"{short_name}(L) {l_fmt}", color=color, 
                            fontsize=9, ha='center', va='top', fontweight='bold',
                            bbox=dict(facecolor='#131722', alpha=0.8, pad=1, edgecolor=color, linewidth=0.5))


        fig.subplots_adjust(left=0.06, right=0.98)
        fig.savefig(output_path, bbox_inches='tight', pad_inches=0.1)
        plt.close(fig)
        return os.path.abspath(output_path)
    except Exception as e:
        print(f"[CHART ERROR] {e}")
        return None
