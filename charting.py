import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import pandas as pd

from config import get_adjusted_sessions


def _ponch_chart_style():
    mc = mpf.make_marketcolors(up="#20c997", down="#ff6b6b", edge="inherit", wick="inherit")
    return mpf.make_mpf_style(
        marketcolors=mc,
        base_mpl_style="dark_background",
        gridcolor="#2b2b2b",
        facecolor="#101722",
    )


def _draw_level_labels(ax, label_x, labeled_levels, show_price=True):
    for price, label, color in labeled_levels:
        text = f" {label} {price:,.2f}" if show_price else f" {label}"
        ax.text(
            label_x,
            price,
            text,
            color=color,
            va="center",
            fontsize=8.5,
            fontweight="bold",
            bbox=dict(facecolor="#101722", alpha=0.78, pad=1.0, edgecolor=color, linewidth=0.6),
            zorder=4,
        )


def _compact_money(value):
    try:
        value = float(value or 0)
    except Exception:
        value = 0.0
    if abs(value) >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if abs(value) >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"${value/1_000:.0f}K"
    return f"${value:.0f}"


def _draw_segment_line(ax, y, x_start, x_end, color, lw=2.0, ls="-"):
    ax.hlines(y, x_start, x_end, colors=color, linewidth=lw, linestyles=ls, zorder=3)


def _synthetic_heatmap_history(rows, steps=72):
    ordered = [row for row in (rows or []) if float(row.get("price") or 0) > 0]
    if not ordered:
        return []
    ordered = sorted(ordered, key=lambda row: float(row.get("size_usd") or 0), reverse=True)[:60]
    steps = int(max(steps, 12))
    history = [{"ts": step, "rows": []} for step in range(steps)]
    size_values = [max(float(row.get("size_usd") or 0), 1.0) for row in ordered]
    min_size = min(size_values)
    max_size = max(size_values)
    for idx, row in enumerate(ordered):
        size_usd = max(float(row.get("size_usd") or 0), 1.0)
        if max_size > min_size:
            norm = (size_usd - min_size) / (max_size - min_size)
        else:
            norm = 0.5
        seg_count = 1 if norm >= 0.70 else (2 if norm >= 0.35 else 3)
        for seg_idx in range(seg_count):
            base_len = int(steps * (0.18 + 0.52 * norm))
            seg_len = max(8, min(steps, base_len - seg_idx * max(2, int(base_len * 0.18))))
            max_start = max(0, steps - seg_len)
            seed = (idx + 3) * 17 + (seg_idx + 1) * 29
            start = seed % (max_start + 1 if max_start > 0 else 1)
            end = min(steps, start + seg_len)
            for step in range(start, end):
                mid = (start + end - 1) / 2.0
                if end - start > 1:
                    dist_from_mid = abs(step - mid) / max((end - start) / 2.0, 1.0)
                else:
                    dist_from_mid = 0.0
                intensity = 0.78 + (1.0 - dist_from_mid) * 0.22
                price_jitter = ((((idx * 5) + (seg_idx * 3) + step) % 3) - 1) * 0.00003
                history[step]["rows"].append(
                    {
                        "price": float(row.get("price") or 0) * (1.0 + price_jitter),
                        "size_usd": max(size_usd * intensity, 1.0),
                        "distance_pct": float(row.get("distance_pct") or 0),
                        "zone_side": row.get("zone_side"),
                        "bucket": row.get("bucket"),
                    }
                )
    return history


def generate_signal_setup_chart(
    df,
    *,
    side,
    entry,
    sl,
    tp1,
    tp2,
    tp3,
    symbol="BTCUSDT",
    timeframe="15m",
    output_path="signal_setup.png",
    title="Signal Setup",
    close_price=None,
    event_label=None,
):
    if df is None or df.empty:
        return None

    plot_df = df.tail(120).copy()
    levels = [float(entry), float(sl), float(tp1), float(tp2), float(tp3)]
    all_values = list(plot_df["High"]) + list(plot_df["Low"]) + levels
    if close_price is not None:
        all_values.append(float(close_price))

    ymin = min(all_values)
    ymax = max(all_values)
    padding_y = max((ymax - ymin) * 0.08, float(entry) * 0.0025)
    ylim = (ymin - padding_y, ymax + padding_y)
    num_candles = len(plot_df)
    xlim = (0, num_candles + 14)

    lines = [float(entry), float(sl), float(tp1), float(tp2), float(tp3)]
    colors = ["#4dabf7", "#ff6b6b", "#69db7c", "#ffd43b", "#f06595"]
    widths = [2.0, 1.8, 1.5, 1.5, 1.5]
    styles = ["-", "-", "-", "-", "-"]
    labels = [
        (float(entry), "ENTRY", "#4dabf7"),
        (float(sl), "SL", "#ff6b6b"),
        (float(tp1), "TP1", "#69db7c"),
        (float(tp2), "TP2", "#ffd43b"),
        (float(tp3), "TP3", "#f06595"),
    ]
    if close_price is not None:
        lines.append(float(close_price))
        colors.append("#ffffff")
        widths.append(1.3)
        styles.append("--")
        labels.append((float(close_price), str(event_label or "NOW").upper(), "#ffffff"))

    try:
        fig, axlist = mpf.plot(
            plot_df,
            type="candle",
            style=_ponch_chart_style(),
            title=f"\n{symbol} · {timeframe.upper()} {title}",
            ylabel="Price",
            datetime_format="%d %H:%M",
            tight_layout=True,
            savefig=output_path,
            figratio=(16, 9),
            figscale=1.35,
            ylim=ylim,
            xlim=xlim,
            returnfig=True,
        )
        ax = axlist[0]
        seg_start = max(12, int(num_candles * 0.60))
        seg_end = num_candles + 1.8
        label_x = num_candles + 2.4

        if str(side or "").upper() == "LONG":
            ax.axhspan(float(entry), float(tp1), alpha=0.04, color="#69db7c", zorder=0)
        else:
            ax.axhspan(float(tp1), float(entry), alpha=0.04, color="#ff8787", zorder=0)

        for price, color, width, style in zip(lines, colors, widths, styles):
            _draw_segment_line(ax, price, seg_start, seg_end, color, lw=width, ls=style)

        ax.text(
            0.02,
            0.98,
            str(title or "SETUP").upper(),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=12,
            fontweight="bold",
            color="#e9ecef",
            bbox=dict(facecolor="#101722", alpha=0.82, pad=3.0, edgecolor="#343a40", linewidth=0.8),
            zorder=5,
        )
        _draw_level_labels(ax, label_x, labels)
        fig.subplots_adjust(left=0.06, right=0.98)
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        plt.close(fig)
        return os.path.abspath(output_path)
    except Exception as e:
        print(f"[CHART ERROR] signal setup chart failed: {e}")
        return None


def generate_liquidation_map_chart(
    df,
    *,
    current_price,
    horizon_rows,
    heatmap_rows=None,
    heatmap_history=None,
    symbol="BTCUSDT",
    timeframe="15m",
    output_path="liquidation_map.png",
):
    if df is None or df.empty:
        return None

    plot_df = df.tail(96).copy()
    dense_rows = [row for row in list(heatmap_rows or []) if float(row.get("price") or 0) > 0]
    if not dense_rows:
        for row in horizon_rows or []:
            upside = row.get("upside")
            downside = row.get("downside")
            if upside:
                dense_rows.append(
                    {
                        "price": float(upside),
                        "size_usd": float((row.get("upside_zone") or {}).get("size_usd") or 0),
                        "zone_side": "short_liq",
                        "bucket": "mid",
                    }
                )
            if downside:
                dense_rows.append(
                    {
                        "price": float(downside),
                        "size_usd": float((row.get("downside_zone") or {}).get("size_usd") or 0),
                        "zone_side": "long_liq",
                        "bucket": "mid",
                    }
                )

    labels = [(float(current_price), "PRICE", "#ffffff")]
    top_above = [row for row in dense_rows if float(row.get("price") or 0) > float(current_price)]
    top_below = [row for row in dense_rows if float(row.get("price") or 0) < float(current_price)]
    top_above = sorted(top_above, key=lambda row: float(row.get("size_usd") or 0), reverse=True)[:2]
    top_below = sorted(top_below, key=lambda row: float(row.get("size_usd") or 0), reverse=True)[:2]
    selected_rows = top_above + top_below

    focus_values = list(plot_df["High"]) + list(plot_df["Low"]) + [float(current_price)]
    for row in selected_rows:
        px = float(row.get("price") or 0)
        if px > 0:
            focus_values.append(px)
    ymin = min(focus_values)
    ymax = max(focus_values)
    padding_y = max((ymax - ymin) * 0.16, float(current_price) * 0.0028)
    ylim = (ymin - padding_y, ymax + padding_y)
    num_candles = len(plot_df)
    x_padding = 18
    xlim = (0, num_candles + x_padding)

    try:
        fig, axlist = mpf.plot(
            plot_df,
            type="candle",
            style=_ponch_chart_style(),
            title=f"\n{symbol} · {timeframe.upper()} Liquidation Map",
            ylabel="Price",
            datetime_format="%d %H:%M",
            tight_layout=True,
            savefig=output_path,
            figratio=(14, 11),
            figscale=1.35,
            ylim=ylim,
            xlim=xlim,
            returnfig=True,
        )
        ax = axlist[0]
        ax.set_facecolor("#2b0047")
        ax.grid(True, alpha=0.10, color="#323232")
        label_x = num_candles + 2.4
        x_end = num_candles + 2.0
        palette_above = ["#ffe066", "#ffd43b"]
        palette_below = ["#74c0fc", "#5cdbd3"]
        for idx, row in enumerate(top_above):
            price = float(row.get("price") or 0)
            size_usd = float(row.get("size_usd") or 0)
            color = palette_above[min(idx, len(palette_above) - 1)]
            _draw_segment_line(ax, price, max(10, int(num_candles * 0.58)), x_end, color, lw=1.8, ls="--")
            labels.append((price, f"UP {price:,.0f} | {_compact_money(size_usd)}", color))
        for idx, row in enumerate(top_below):
            price = float(row.get("price") or 0)
            size_usd = float(row.get("size_usd") or 0)
            color = palette_below[min(idx, len(palette_below) - 1)]
            _draw_segment_line(ax, price, max(10, int(num_candles * 0.58)), x_end, color, lw=1.8, ls="--")
            labels.append((price, f"DN {price:,.0f} | {_compact_money(size_usd)}", color))

        _draw_segment_line(ax, float(current_price), max(10, int(num_candles * 0.54)), x_end, "#ffffff", lw=1.6, ls="--")
        _draw_level_labels(ax, label_x, labels, show_price=False)
        fig.subplots_adjust(left=0.06, right=0.98)
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        plt.close(fig)
        return os.path.abspath(output_path)
    except Exception as e:
        print(f"[CHART ERROR] liquidation map chart failed: {e}")
        return None


def generate_daily_levels_chart(df, levels, symbol="BTCUSDT", timeframe="1H", output_path="daily_chart.png", show_sessions=True, session_stats=None):
    """
    Generate a candlestick chart with daily levels as horizontal lines.
    session_stats: { session_name: {"high": float, "low": float} or {"high": float, "low": float, "id": str} }
    """
    if df.empty or not levels:
        return None

    plot_df = df.tail(48).copy()

    hlines = []
    hcolors = []
    hwidths = []
    all_values = list(plot_df["High"]) + list(plot_df["Low"])

    if "Pump" in levels:
        val = levels["Pump"]
        hlines.append(val)
        hcolors.append("#ff0000")
        hwidths.append(1.5)
        all_values.append(val)

    if "Dump" in levels:
        val = levels["Dump"]
        hlines.append(val)
        hcolors.append("#00ff00")
        hwidths.append(1.5)
        all_values.append(val)

    if "PumpMax" in levels:
        val = levels["PumpMax"]
        hlines.append(val)
        hcolors.append("#ff1744")
        hwidths.append(2.5)
        all_values.append(val)

    if "DumpMax" in levels:
        val = levels["DumpMax"]
        hlines.append(val)
        hcolors.append("#ff1744")
        hwidths.append(2.5)
        all_values.append(val)

    ymin = min(all_values)
    ymax = max(all_values)
    padding_y = (ymax - ymin) * 0.05
    ylim = (ymin - padding_y, ymax + padding_y)

    num_candles = len(plot_df)
    xlim = (0, num_candles + 14)

    mc = mpf.make_marketcolors(up="#26a69a", down="#ef5350", edge="inherit", wick="inherit")
    s = mpf.make_mpf_style(marketcolors=mc, base_mpl_style="dark_background", gridcolor="#2b2b2b", facecolor="#131722")
    hlines_dict = dict(hlines=hlines, colors=hcolors, linestyle="--", linewidths=hwidths)

    try:
        fig, axlist = mpf.plot(
            plot_df,
            type="candle",
            style=s,
            hlines=hlines_dict,
            title=f"\n{symbol} · {timeframe.upper()} Daily Levels",
            ylabel="Price",
            datetime_format="%H:%M",
            tight_layout=True,
            savefig=output_path,
            figratio=(16, 9),
            figscale=1.5,
            ylim=ylim,
            xlim=xlim,
            returnfig=True,
        )

        ax = axlist[0]
        label_x = num_candles + 1

        if "Pump" in levels and "Dump" in levels:
            ax.axhspan(levels["Dump"], levels["Pump"], alpha=0.08, color="#ffab00", zorder=0)
            mid_vol = (levels["Pump"] + levels["Dump"]) / 2
            ax.text(label_x, mid_vol, "  VOLATILITY ZONE", color="#ffab00", va="center", fontsize=8, fontstyle="italic", alpha=0.7)

        label_map = {
            "Pump": ("RESISTANCE", "#ff0000"),
            "Dump": ("SUPPORT", "#00ff00"),
            "PumpMax": ("CRITICAL HIGH", "#ff1744"),
            "DumpMax": ("CRITICAL LOW", "#ff1744"),
        }

        for key, (label, color) in label_map.items():
            if key in levels:
                price = levels[key]
                ax.text(label_x, price, f"{label} ({price:,.2f})", color=color, va="center", fontsize=9, fontweight="bold")

        if show_sessions:
            session_colors = {
                "ASIA": "#3d5afe",
                "LONDON": "#ff9100",
                "NY": "#00e676",
            }

            last_ts = plot_df.index[-1]
            sessions = get_adjusted_sessions(last_ts)
            plot_df["float_hour"] = plot_df.index.hour + plot_df.index.minute / 60.0

            for s_name, times in sessions.items():
                s_open = times["open"]
                s_close = times["close"]
                color = session_colors.get(s_name, "#ffffff")

                if s_open < s_close:
                    mask = (plot_df["float_hour"] >= s_open - 0.01) & (plot_df["float_hour"] < s_close - 0.01)
                else:
                    mask = (plot_df["float_hour"] >= s_open - 0.01) | (plot_df["float_hour"] < s_close - 0.01)

                all_session_candles = plot_df[mask]
                if all_session_candles.empty:
                    continue

                last_date = all_session_candles.index[-1].date()
                session_df = all_session_candles[all_session_candles.index.date == last_date]

                if not session_df.empty:
                    open_ts = session_df.index[0]
                    candle_f_h = open_ts.hour + open_ts.minute / 60.0
                    if abs(candle_f_h - s_open) < 0.01:
                        idx_open = plot_df.index.get_loc(open_ts)
                        ax.axvline(idx_open, color=color, linestyle="--", alpha=0.7, linewidth=1.5)
                        ax.text(idx_open, ymax + padding_y * 0.1, f" {s_name}", color=color, fontsize=11, fontweight="bold", ha="left")

                    s_high = session_df["High"].max()
                    s_low = session_df["Low"].min()
                    ts_high = session_df["High"].idxmax()
                    ts_low = session_df["Low"].idxmin()

                    if session_stats and s_name in session_stats:
                        live_high = session_stats[s_name].get("high")
                        live_low = session_stats[s_name].get("low")
                        if live_high:
                            s_high = max(s_high, live_high)
                        if live_low:
                            s_low = min(s_low, live_low)

                    idx_high = plot_df.index.get_loc(ts_high)
                    idx_low = plot_df.index.get_loc(ts_low)

                    short_name = s_name[:2]
                    h_fmt = f"{s_high:,.2f}" if s_high % 1 != 0 else f"{s_high:,.0f}"
                    l_fmt = f"{s_low:,.2f}" if s_low % 1 != 0 else f"{s_low:,.0f}"

                    ax.text(
                        idx_high,
                        s_high,
                        f"{short_name}(H) {h_fmt}",
                        color=color,
                        fontsize=9,
                        ha="center",
                        va="bottom",
                        fontweight="bold",
                        bbox=dict(facecolor="#131722", alpha=0.8, pad=1, edgecolor=color, linewidth=0.5),
                    )
                    ax.text(
                        idx_low,
                        s_low,
                        f"{short_name}(L) {l_fmt}",
                        color=color,
                        fontsize=9,
                        ha="center",
                        va="top",
                        fontweight="bold",
                        bbox=dict(facecolor="#131722", alpha=0.8, pad=1, edgecolor=color, linewidth=0.5),
                    )

        fig.subplots_adjust(left=0.06, right=0.98)
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        plt.close(fig)
        return os.path.abspath(output_path)
    except Exception as e:
        print(f"[CHART ERROR] {e}")
        return None
