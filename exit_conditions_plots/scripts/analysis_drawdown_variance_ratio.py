"""
Analysis 1: Drawdown Detection + Variance Ratio Overlay

For both the portfolio and SPTM:
- Identify all drawdown periods (peak-to-trough declines)
- Compute variance ratios at multiple (short, long) timescale pairs
- Overlay the variance ratio on the drawdown chart
- Check: did variance ratios spike BEFORE or AT the onset of drawdowns?

Outputs:
- Drawdown timeseries with variance ratio overlay (per series, per timescale pair)
- Summary table: for each major drawdown, when did the VR signal first fire vs when did the drawdown begin?
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import os

# ---------------------------------------------------------------------------
# Load & prepare
# ---------------------------------------------------------------------------
DATA_PATH = "exit_conditions_data/most_recent_backtest_20260326.csv"
OUT_DIR = "exit_conditions_plots"
os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(DATA_PATH, parse_dates=["date"])
df.sort_values("date", inplace=True)
df.reset_index(drop=True, inplace=True)

df["portfolio_return"] = df["portfolio_value"].pct_change()
df["sptm_return"] = df["sptm_value"].pct_change()

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def rolling_variance(returns, window):
    """Rolling variance (population) of returns w.r.t. rolling mean."""
    return returns.rolling(window=window).apply(
        lambda x: np.mean((x - np.mean(x)) ** 2), raw=True
    )

def compute_drawdown(values):
    """Compute drawdown series: 0 at peaks, negative during drawdowns."""
    cummax = values.cummax()
    drawdown = (values - cummax) / cummax
    return drawdown

def find_drawdown_periods(drawdown_series, threshold=-0.05):
    """
    Find contiguous drawdown periods deeper than threshold.
    Returns list of dicts: {start, trough, end, depth}.
    """
    in_dd = drawdown_series < threshold
    periods = []
    started = False
    for i in range(len(drawdown_series)):
        if in_dd.iloc[i] and not started:
            started = True
            start_idx = i
        elif not in_dd.iloc[i] and started:
            started = False
            segment = drawdown_series.iloc[start_idx:i]
            trough_idx = segment.idxmin()
            periods.append({
                "start_idx": start_idx,
                "end_idx": i - 1,
                "trough_idx": trough_idx,
                "depth": drawdown_series.iloc[trough_idx],
            })
    # Handle case where we end inside a drawdown
    if started:
        segment = drawdown_series.iloc[start_idx:]
        trough_idx = segment.idxmin()
        periods.append({
            "start_idx": start_idx,
            "end_idx": len(drawdown_series) - 1,
            "trough_idx": trough_idx,
            "depth": drawdown_series.iloc[trough_idx],
        })
    return periods

# ---------------------------------------------------------------------------
# Variance ratio pairs to test
# ---------------------------------------------------------------------------
VR_PAIRS = [
    (5, 50),
    (5, 100),
    (10, 50),
    (10, 100),
    (20, 100),
    (20, 200),
]

# ---------------------------------------------------------------------------
# Compute everything
# ---------------------------------------------------------------------------

for series_name, ret_col, val_col in [
    ("Portfolio", "portfolio_return", "portfolio_value"),
    ("SPTM", "sptm_return", "sptm_value"),
]:
    print(f"\n{'='*60}")
    print(f"  {series_name} Analysis")
    print(f"{'='*60}")

    dd = compute_drawdown(df[val_col])
    df[f"{series_name.lower()}_drawdown"] = dd

    # Find major drawdown periods (> 5% decline)
    dd_periods = find_drawdown_periods(dd, threshold=-0.05)
    print(f"\nFound {len(dd_periods)} drawdown periods > 5%:")
    for j, p in enumerate(dd_periods):
        print(f"  DD #{j+1}: {df['date'].iloc[p['start_idx']].date()} to "
              f"{df['date'].iloc[p['end_idx']].date()}, "
              f"depth = {p['depth']:.2%}")

    # Compute variance ratios
    vr_data = {}
    for n_short, n_long in VR_PAIRS:
        var_short = rolling_variance(df[ret_col], n_short)
        var_long = rolling_variance(df[ret_col], n_long)
        vr = var_short / var_long
        col_name = f"vr_{n_short}_{n_long}"
        df[f"{series_name.lower()}_{col_name}"] = vr
        vr_data[(n_short, n_long)] = vr

    # Also compute 5-day mean return for the combined signal
    df[f"{series_name.lower()}_r5"] = df[ret_col].rolling(5).mean()

    # -------------------------------------------------------------------
    # Plot 1: Comprehensive overview — drawdown + all VR pairs
    # -------------------------------------------------------------------
    fig, axes = plt.subplots(len(VR_PAIRS) + 1, 1,
                              figsize=(16, 3 * (len(VR_PAIRS) + 1)),
                              sharex=True)

    # Top panel: drawdown
    ax = axes[0]
    ax.fill_between(df["date"], dd * 100, 0, color="red", alpha=0.3)
    ax.plot(df["date"], dd * 100, color="red", linewidth=0.6)
    ax.set_ylabel("Drawdown %")
    ax.set_title(f"{series_name} — Drawdown with Variance Ratio Signals")
    ax.axhline(y=-5, color="orange", linestyle="--", linewidth=0.5, alpha=0.7)
    ax.axhline(y=-10, color="red", linestyle="--", linewidth=0.5, alpha=0.7)

    # Shade drawdown periods
    for p in dd_periods:
        ax.axvspan(df["date"].iloc[p["start_idx"]],
                    df["date"].iloc[p["end_idx"]],
                    alpha=0.1, color="blue")

    # Lower panels: each VR pair
    for idx, (n_short, n_long) in enumerate(VR_PAIRS):
        ax = axes[idx + 1]
        vr = vr_data[(n_short, n_long)]
        ax.plot(df["date"], vr, linewidth=0.7, color="navy")
        ax.axhline(y=1.0, color="grey", linestyle="-", linewidth=0.5, alpha=0.5)
        ax.axhline(y=2.0, color="orange", linestyle="--", linewidth=0.7, alpha=0.8, label="κ=2.0")
        ax.axhline(y=2.5, color="red", linestyle="--", linewidth=0.7, alpha=0.8, label="κ=2.5")
        ax.axhline(y=3.0, color="darkred", linestyle="--", linewidth=0.7, alpha=0.8, label="κ=3.0")
        ax.set_ylabel(f"VR({n_short}/{n_long})")
        ax.set_ylim(0, min(vr.quantile(0.995) * 1.5, 10))

        # Shade drawdown periods
        for p in dd_periods:
            ax.axvspan(df["date"].iloc[p["start_idx"]],
                        df["date"].iloc[p["end_idx"]],
                        alpha=0.1, color="blue")

        if idx == 0:
            ax.legend(loc="upper right", fontsize=8)

    axes[-1].set_xlabel("Date")
    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    outpath = os.path.join(OUT_DIR, f"{series_name.lower()}_drawdown_vr_overview.png")
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"\nSaved {outpath}")

    # -------------------------------------------------------------------
    # Plot 2: Zoomed-in on each major drawdown period
    # -------------------------------------------------------------------
    for j, p in enumerate(dd_periods):
        # Extend the view window: 40 trading days before start, 20 after end
        view_start = max(0, p["start_idx"] - 40)
        view_end = min(len(df) - 1, p["end_idx"] + 20)
        mask = (df.index >= view_start) & (df.index <= view_end)
        sub = df[mask]

        fig, axes_z = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

        # Panel 1: Value + drawdown
        ax1 = axes_z[0]
        ax1.plot(sub["date"], sub[val_col], color="black", linewidth=1)
        ax1.set_ylabel(f"{series_name} Value")
        ax1.set_title(f"{series_name} Drawdown #{j+1}: "
                       f"{df['date'].iloc[p['start_idx']].date()} → "
                       f"{df['date'].iloc[p['end_idx']].date()} "
                       f"(depth: {p['depth']:.2%})")
        ax1_dd = ax1.twinx()
        ax1_dd.fill_between(sub["date"], sub[f"{series_name.lower()}_drawdown"] * 100,
                            0, color="red", alpha=0.2)
        ax1_dd.set_ylabel("Drawdown %", color="red")

        # Mark drawdown start
        ax1.axvline(x=df["date"].iloc[p["start_idx"]], color="orange",
                    linestyle="--", linewidth=1, label="DD start")
        ax1.legend(loc="upper left", fontsize=8)

        # Panel 2: Fast VR pairs (short window = 5)
        ax2 = axes_z[1]
        for n_short, n_long in VR_PAIRS:
            if n_short <= 10:
                col = f"{series_name.lower()}_vr_{n_short}_{n_long}"
                ax2.plot(sub["date"], sub[col], linewidth=0.9,
                         label=f"VR({n_short}/{n_long})")
        ax2.axhline(y=2.0, color="orange", linestyle="--", linewidth=0.7)
        ax2.axhline(y=2.5, color="red", linestyle="--", linewidth=0.7)
        ax2.axvline(x=df["date"].iloc[p["start_idx"]], color="orange",
                    linestyle="--", linewidth=1)
        ax2.set_ylabel("Variance Ratio (fast)")
        ax2.legend(loc="upper right", fontsize=8)

        # Panel 3: Slow VR pairs (short window = 20)
        ax3 = axes_z[2]
        for n_short, n_long in VR_PAIRS:
            if n_short >= 20:
                col = f"{series_name.lower()}_vr_{n_short}_{n_long}"
                ax3.plot(sub["date"], sub[col], linewidth=0.9,
                         label=f"VR({n_short}/{n_long})")
        ax3.axhline(y=2.0, color="orange", linestyle="--", linewidth=0.7)
        ax3.axhline(y=2.5, color="red", linestyle="--", linewidth=0.7)
        ax3.axvline(x=df["date"].iloc[p["start_idx"]], color="orange",
                    linestyle="--", linewidth=1)
        ax3.set_ylabel("Variance Ratio (slow)")
        ax3.set_xlabel("Date")
        ax3.legend(loc="upper right", fontsize=8)

        plt.tight_layout()
        outpath = os.path.join(OUT_DIR,
                               f"{series_name.lower()}_drawdown_{j+1}_zoom.png")
        plt.savefig(outpath, dpi=150)
        plt.close()
        print(f"Saved {outpath}")

    # -------------------------------------------------------------------
    # Early warning table: for each drawdown, when did VR first breach
    # threshold BEFORE the drawdown trough?
    # -------------------------------------------------------------------
    print(f"\n--- Early Warning Analysis for {series_name} ---")
    print(f"{'DD#':>4} {'Start':>12} {'Trough':>12} {'Depth':>8}  |  ", end="")
    for n_short, n_long in VR_PAIRS:
        print(f"VR({n_short}/{n_long})", end="  ")
    print()
    print("-" * (50 + 14 * len(VR_PAIRS)))

    for j, p in enumerate(dd_periods):
        start_date = df["date"].iloc[p["start_idx"]].date()
        trough_date = df["date"].iloc[p["trough_idx"]].date()

        print(f"{j+1:>4} {str(start_date):>12} {str(trough_date):>12} {p['depth']:>8.2%}  |  ", end="")

        for n_short, n_long in VR_PAIRS:
            col = f"{series_name.lower()}_vr_{n_short}_{n_long}"
            # Look in window: 20 days before DD start to DD trough
            look_start = max(0, p["start_idx"] - 20)
            look_end = p["trough_idx"]
            window = df.iloc[look_start:look_end + 1]

            # Find first day VR > 2.0 AND 5-day return < 0
            r5_col = f"{series_name.lower()}_r5"
            signal = (window[col] > 2.0) & (window[r5_col] < 0)
            if signal.any():
                first_fire = window.loc[signal.idxmax(), "date"].date()
                days_before_trough = (trough_date - first_fire).days
                print(f"{days_before_trough:>4}d early", end="  ")
            else:
                print(f"{'no sig':>10}", end="  ")
        print()

print("\n\nDone. Check exit_conditions_plots/ for all output.")
