"""
Analysis 3: Simulated Exit Signal Backtest

We simulate an overlay on the portfolio equity curve:
- "Baseline": the actual portfolio as-is (which already uses 200-day SMA defense)
- "VR overlay": when the variance-ratio signal fires on the portfolio, we assume
  the portfolio goes to cash (earns 0) until the signal clears, then re-enters.

This is an approximation — we can't re-run the full backtest with different logic,
but we CAN measure the marginal value of the VR signal by seeing how much drawdown
it would avoid vs how much upside it would sacrifice.

We also compare the VR signal timing against the existing 200-day SMA defensive
mode to see if VR would have triggered earlier.

Outputs:
- Equity curves: baseline vs VR-overlay for several parameter choices
- Timing comparison: VR signal vs 200-day SMA for each major event
- Performance summary table
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

def rolling_variance(returns, window):
    return returns.rolling(window=window).apply(
        lambda x: np.mean((x - np.mean(x)) ** 2), raw=True
    )

def compute_drawdown(values):
    cummax = values.cummax()
    return (values - cummax) / cummax

def max_drawdown(values):
    return compute_drawdown(values).min()

# ---------------------------------------------------------------------------
# Simulate VR overlay strategies
# ---------------------------------------------------------------------------
# We test several configurations. For each:
# - When VR signal fires AND r5 < 0 → go to cash (return = 0 that day)
# - Stay in cash until VR drops back below exit_clear threshold AND r5 > 0
# - Then re-enter (resume portfolio returns)

CONFIGS = [
    # (label, n_short, n_long, kappa_enter, kappa_exit, use_r5_filter)
    ("VR(5/50) κ=2.0 +R5", 5, 50, 2.0, 1.0, True),
    ("VR(5/100) κ=2.0 +R5", 5, 100, 2.0, 1.0, True),
    ("VR(10/100) κ=2.0 +R5", 10, 100, 2.0, 1.0, True),
    ("VR(20/100) κ=1.75 +R5", 20, 100, 1.75, 1.0, True),
    ("VR(20/200) κ=2.0 +R5", 20, 200, 2.0, 1.0, True),
    ("VR(5/100) κ=2.5 +R5", 5, 100, 2.5, 1.0, True),
    ("VR(5/50) κ=2.0 no filter", 5, 50, 2.0, 1.0, False),
    ("VR(10/50) κ=2.0 no filter", 10, 50, 2.0, 1.0, False),
]

# Precompute all needed VR series and r5
vr_cache = {}
for _, n_short, n_long, _, _, _ in CONFIGS:
    key = (n_short, n_long)
    if key not in vr_cache:
        vr_cache[key] = rolling_variance(df["portfolio_return"], n_short) / \
                        rolling_variance(df["portfolio_return"], n_long)

df["r5"] = df["portfolio_return"].rolling(5).mean()

results_summary = []

for label, n_short, n_long, kappa_enter, kappa_exit, use_r5 in CONFIGS:
    vr = vr_cache[(n_short, n_long)]

    # Simulate the overlay
    in_cash = False
    overlay_value = [df["portfolio_value"].iloc[0]]
    signal_state = [False]
    days_in_cash = 0

    for i in range(1, len(df)):
        vr_val = vr.iloc[i] if pd.notna(vr.iloc[i]) else 0
        r5_val = df["r5"].iloc[i] if pd.notna(df["r5"].iloc[i]) else 0
        daily_ret = df["portfolio_return"].iloc[i] if pd.notna(df["portfolio_return"].iloc[i]) else 0

        if not in_cash:
            # Check entry into cash
            fire = vr_val > kappa_enter
            if use_r5:
                fire = fire and (r5_val < 0)

            if fire:
                in_cash = True
                # On the day signal fires, we already took the day's return
                # (signal is computed from today's data, so we exit at close)
                overlay_value.append(overlay_value[-1] * (1 + daily_ret))
            else:
                overlay_value.append(overlay_value[-1] * (1 + daily_ret))
        else:
            days_in_cash += 1
            # Check exit from cash (re-enter)
            clear = vr_val < kappa_exit
            if use_r5:
                clear = clear and (r5_val >= 0)

            if clear:
                in_cash = False
                # Re-enter: no return on this day (buying back in at close)
                overlay_value.append(overlay_value[-1])
            else:
                # In cash: no return
                overlay_value.append(overlay_value[-1])

        signal_state.append(in_cash)

    overlay_series = pd.Series(overlay_value, index=df.index)
    signal_series = pd.Series(signal_state, index=df.index)

    # Performance metrics
    baseline_final = df["portfolio_value"].iloc[-1]
    overlay_final = overlay_series.iloc[-1]
    baseline_return = (baseline_final / df["portfolio_value"].iloc[0] - 1) * 100
    overlay_return = (overlay_final / overlay_series.iloc[0] - 1) * 100
    baseline_maxdd = max_drawdown(df["portfolio_value"]) * 100
    overlay_maxdd = max_drawdown(overlay_series) * 100
    pct_time_in_cash = signal_series.mean() * 100

    results_summary.append({
        "config": label,
        "baseline_return": baseline_return,
        "overlay_return": overlay_return,
        "return_diff": overlay_return - baseline_return,
        "baseline_maxdd": baseline_maxdd,
        "overlay_maxdd": overlay_maxdd,
        "maxdd_improvement": overlay_maxdd - baseline_maxdd,
        "pct_time_in_cash": pct_time_in_cash,
        "days_in_cash": days_in_cash,
    })

    df[f"overlay_{label}"] = overlay_series
    df[f"signal_{label}"] = signal_series

# ---------------------------------------------------------------------------
# Also add the 200-day SMA baseline signal for comparison
# ---------------------------------------------------------------------------
sma_in_cash = False
sma_overlay = [df["portfolio_value"].iloc[0]]
sma_state = [False]
sma_days_cash = 0

for i in range(1, len(df)):
    daily_ret = df["portfolio_return"].iloc[i] if pd.notna(df["portfolio_return"].iloc[i]) else 0
    mh = df["market_health"].iloc[i]

    if not sma_in_cash:
        if mh == False or (isinstance(mh, str) and mh.lower() == "false"):
            sma_in_cash = True
            sma_overlay.append(sma_overlay[-1] * (1 + daily_ret))
        else:
            sma_overlay.append(sma_overlay[-1] * (1 + daily_ret))
    else:
        sma_days_cash += 1
        if mh == True or (isinstance(mh, str) and mh.lower() == "true"):
            sma_in_cash = False
            sma_overlay.append(sma_overlay[-1])
        else:
            sma_overlay.append(sma_overlay[-1])

    sma_state.append(sma_in_cash)

sma_series = pd.Series(sma_overlay, index=df.index)
sma_signal = pd.Series(sma_state, index=df.index)

sma_return = (sma_series.iloc[-1] / sma_series.iloc[0] - 1) * 100
sma_maxdd = max_drawdown(sma_series) * 100

results_summary.append({
    "config": "200-day SMA (current)",
    "baseline_return": (df["portfolio_value"].iloc[-1] / df["portfolio_value"].iloc[0] - 1) * 100,
    "overlay_return": sma_return,
    "return_diff": sma_return - (df["portfolio_value"].iloc[-1] / df["portfolio_value"].iloc[0] - 1) * 100,
    "baseline_maxdd": max_drawdown(df["portfolio_value"]) * 100,
    "overlay_maxdd": sma_maxdd,
    "maxdd_improvement": sma_maxdd - max_drawdown(df["portfolio_value"]) * 100,
    "pct_time_in_cash": sma_signal.mean() * 100,
    "days_in_cash": sma_days_cash,
})

# ---------------------------------------------------------------------------
# Print results table
# ---------------------------------------------------------------------------
print("\n" + "=" * 130)
print("  EXIT SIGNAL BACKTEST COMPARISON")
print("=" * 130)
print(f"{'Config':<30} {'Base Ret%':>10} {'Overlay Ret%':>13} {'Δ Ret%':>8} "
      f"{'Base MaxDD%':>12} {'Ovl MaxDD%':>12} {'Δ MaxDD%':>10} {'% in Cash':>10} {'Days Cash':>10}")
print("-" * 130)

for r in results_summary:
    print(f"{r['config']:<30} {r['baseline_return']:>9.1f}% {r['overlay_return']:>12.1f}% "
          f"{r['return_diff']:>+7.1f}% {r['baseline_maxdd']:>11.1f}% "
          f"{r['overlay_maxdd']:>11.1f}% {r['maxdd_improvement']:>+9.1f}% "
          f"{r['pct_time_in_cash']:>9.1f}% {r['days_in_cash']:>10}")

# Save to CSV
pd.DataFrame(results_summary).to_csv(
    os.path.join(OUT_DIR, "exit_signal_backtest_results.csv"), index=False)
print(f"\nSaved exit_signal_backtest_results.csv")

# ---------------------------------------------------------------------------
# Plot: Equity curves comparison (top 4 + SMA + baseline)
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(2, 1, figsize=(16, 12))

# Top panel: equity curves
ax = axes[0]
ax.plot(df["date"], df["portfolio_value"], color="black", linewidth=1.2, label="Baseline (actual)")
ax.plot(df["date"], sma_series, color="grey", linewidth=1, linestyle="--", label="200-day SMA overlay")

colors = plt.cm.tab10(np.linspace(0, 1, len(CONFIGS)))
for idx, (label, *_) in enumerate(CONFIGS[:5]):
    col = f"overlay_{label}"
    ax.plot(df["date"], df[col], linewidth=0.8, color=colors[idx], alpha=0.8, label=label)

ax.set_title("Exit Signal Overlay Backtest — Equity Curves")
ax.set_ylabel("Portfolio Value ($)")
ax.legend(fontsize=7, loc="upper left")
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

# Bottom panel: drawdown comparison
ax = axes[1]
ax.fill_between(df["date"], compute_drawdown(df["portfolio_value"]) * 100, 0,
                color="red", alpha=0.2, label="Baseline drawdown")
ax.plot(df["date"], compute_drawdown(sma_series) * 100,
        color="grey", linewidth=0.8, linestyle="--", label="200-day SMA")

for idx, (label, *_) in enumerate(CONFIGS[:5]):
    col = f"overlay_{label}"
    ax.plot(df["date"], compute_drawdown(df[col]) * 100,
            linewidth=0.7, color=colors[idx], alpha=0.8, label=label)

ax.set_title("Drawdown Comparison")
ax.set_ylabel("Drawdown %")
ax.set_xlabel("Date")
ax.legend(fontsize=7, loc="lower left")
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

plt.tight_layout()
outpath = os.path.join(OUT_DIR, "exit_signal_backtest_equity.png")
plt.savefig(outpath, dpi=150)
plt.close()
print(f"Saved {outpath}")

# ---------------------------------------------------------------------------
# Plot: Signal timing comparison — when did VR fire vs when did SMA turn off?
# For the big drawdowns only.
# ---------------------------------------------------------------------------
# Identify periods where SMA went defensive
sma_transitions = []
was_def = False
for i in range(1, len(df)):
    mh = df["market_health"].iloc[i]
    is_bad = (mh == False) or (isinstance(mh, str) and mh.lower() == "false")
    if is_bad and not was_def:
        sma_transitions.append({"start_idx": i, "start_date": df["date"].iloc[i]})
    elif not is_bad and was_def:
        sma_transitions[-1]["end_idx"] = i
        sma_transitions[-1]["end_date"] = df["date"].iloc[i]
    was_def = is_bad

# For each SMA defensive period, find when the best VR configs fired
print("\n" + "=" * 100)
print("  SIGNAL TIMING COMPARISON: VR vs 200-day SMA")
print("=" * 100)
print(f"{'SMA Defense Start':<20} {'SMA End':<20} | ", end="")
best_configs = [CONFIGS[0], CONFIGS[1], CONFIGS[2], CONFIGS[4]]
for label, *_ in best_configs:
    print(f"{label:<25}", end="")
print()
print("-" * 120)

for t in sma_transitions:
    start = t["start_date"]
    end = t.get("end_date", df["date"].iloc[-1])
    print(f"{str(start.date()):<20} {str(end.date()):<20} | ", end="")

    # Look 40 days before SMA start for VR fires
    look_start = max(0, t["start_idx"] - 40)
    for label, n_short, n_long, kappa_enter, _, use_r5 in best_configs:
        vr = vr_cache[(n_short, n_long)]
        window = df.iloc[look_start:t["start_idx"] + 20]
        vr_window = vr.iloc[look_start:t["start_idx"] + 20]
        r5_window = df["r5"].iloc[look_start:t["start_idx"] + 20]

        fire = vr_window > kappa_enter
        if use_r5:
            fire = fire & (r5_window < 0)

        if fire.any():
            first_fire_idx = fire.idxmax()
            first_fire_date = df["date"].iloc[first_fire_idx]
            days_diff = (start - first_fire_date).days
            if days_diff >= 0:
                print(f"{days_diff:>3}d earlier            ", end="")
            else:
                print(f"{-days_diff:>3}d later              ", end="")
        else:
            print(f"{'no signal':<25}", end="")
    print()

# ---------------------------------------------------------------------------
# Zoomed plot on major drawdown periods
# ---------------------------------------------------------------------------
major_drawdowns = [
    ("2018 Q4 Sell-off", "2018-09-01", "2019-04-01"),
    ("COVID-19 Crash", "2020-01-15", "2020-07-01"),
    ("2022 Bear Market", "2021-11-01", "2023-01-01"),
    ("2025 Drawdown", "2025-01-15", "2025-06-01"),
]

for dd_name, start_str, end_str in major_drawdowns:
    mask = (df["date"] >= start_str) & (df["date"] <= end_str)
    sub = df[mask]
    if len(sub) == 0:
        continue

    fig, ax = plt.subplots(figsize=(14, 7))

    # Normalize to 100 at start
    base_start = sub["portfolio_value"].iloc[0]
    ax.plot(sub["date"], sub["portfolio_value"] / base_start * 100,
            color="black", linewidth=1.5, label="Baseline")

    sma_sub = sma_series[mask]
    sma_start = sma_sub.iloc[0]
    ax.plot(sub["date"], sma_sub / sma_start * 100,
            color="grey", linewidth=1.2, linestyle="--", label="200-day SMA")

    for idx, (label, *_) in enumerate(CONFIGS[:5]):
        col = f"overlay_{label}"
        ovl_sub = sub[col]
        ovl_start = ovl_sub.iloc[0]
        ax.plot(sub["date"], ovl_sub / ovl_start * 100,
                linewidth=0.9, color=colors[idx], alpha=0.85, label=label)

    ax.set_title(f"Exit Signal Comparison — {dd_name}")
    ax.set_ylabel("Normalized Value (100 = start)")
    ax.set_xlabel("Date")
    ax.legend(fontsize=7, loc="best")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45)
    plt.tight_layout()

    safe_name = dd_name.replace(" ", "_").replace("-", "").lower()
    outpath = os.path.join(OUT_DIR, f"exit_zoom_{safe_name}.png")
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"Saved {outpath}")

print("\nDone.")
