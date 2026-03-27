"""
Analysis 2: False Positive Rate across Parameter Grid

For each (n_short, n_long, kappa) combination:
- Count how often the combined signal fires (VR > kappa AND r_bar_5 < 0)
- For each signal fire, measure what happens over the next N days:
  - Forward 5/10/20-day return
  - Max drawdown over next 20 days
- Classify each fire as:
  - True positive: forward max drawdown > 5% (a real drawdown followed)
  - False positive: forward max drawdown < 5% (we would have exited unnecessarily)
- Report: signal rate, true positive rate, false positive rate, avg avoided drawdown

Outputs:
- Summary heatmaps for each metric
- CSV of all results
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
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
# Parameters
# ---------------------------------------------------------------------------
VR_PAIRS = [
    (5, 50), (5, 100),
    (10, 50), (10, 100),
    (20, 100), (20, 200),
]
KAPPAS = [1.5, 1.75, 2.0, 2.25, 2.5, 3.0, 3.5, 4.0]
FORWARD_WINDOWS = [5, 10, 20, 40]
DD_THRESHOLD = -0.05  # 5% drawdown = "true positive"

def rolling_variance(returns, window):
    return returns.rolling(window=window).apply(
        lambda x: np.mean((x - np.mean(x)) ** 2), raw=True
    )

# ---------------------------------------------------------------------------
# Compute forward-looking metrics (for evaluation only, not for trading)
# ---------------------------------------------------------------------------
for fw in FORWARD_WINDOWS:
    df[f"portfolio_fwd_{fw}d"] = df["portfolio_value"].pct_change(fw).shift(-fw)
    df[f"sptm_fwd_{fw}d"] = df["sptm_value"].pct_change(fw).shift(-fw)

# Forward max drawdown over next 20 days
def forward_max_drawdown(values, window=20):
    """For each day t, compute the max drawdown from t over the next `window` days."""
    result = pd.Series(np.nan, index=values.index)
    vals = values.values
    for i in range(len(vals) - window):
        future = vals[i:i + window + 1]
        peak = future[0]
        trough = np.min(future)
        result.iloc[i] = (trough - peak) / peak
    return result

df["portfolio_fwd_maxdd_20"] = forward_max_drawdown(df["portfolio_value"], 20)
df["portfolio_fwd_maxdd_40"] = forward_max_drawdown(df["portfolio_value"], 40)
df["sptm_fwd_maxdd_20"] = forward_max_drawdown(df["sptm_value"], 20)
df["sptm_fwd_maxdd_40"] = forward_max_drawdown(df["sptm_value"], 40)

# 5-day mean returns
df["portfolio_r5"] = df["portfolio_return"].rolling(5).mean()
df["sptm_r5"] = df["sptm_return"].rolling(5).mean()

# ---------------------------------------------------------------------------
# Grid search
# ---------------------------------------------------------------------------
results = []

for series_name, ret_col, r5_col, fwd_prefix in [
    ("Portfolio", "portfolio_return", "portfolio_r5", "portfolio"),
    ("SPTM", "sptm_return", "sptm_r5", "sptm"),
]:
    for n_short, n_long in VR_PAIRS:
        var_short = rolling_variance(df[ret_col], n_short)
        var_long = rolling_variance(df[ret_col], n_long)
        vr = var_short / var_long

        for kappa in KAPPAS:
            # Combined signal: VR > kappa AND 5-day mean return < 0
            signal = (vr > kappa) & (df[r5_col] < 0)
            # Also test VR-only (no return filter)
            signal_vr_only = vr > kappa

            for sig_name, sig in [("VR+R5<0", signal), ("VR_only", signal_vr_only)]:
                valid = signal.dropna()
                n_total = len(valid)
                n_fires = sig.sum()
                fire_rate = n_fires / n_total if n_total > 0 else 0

                if n_fires == 0:
                    results.append({
                        "series": series_name,
                        "signal_type": sig_name,
                        "n_short": n_short,
                        "n_long": n_long,
                        "kappa": kappa,
                        "n_fires": 0,
                        "fire_rate_pct": 0,
                        "tp_rate": np.nan,
                        "fp_rate": np.nan,
                        "avg_fwd_5d": np.nan,
                        "avg_fwd_10d": np.nan,
                        "avg_fwd_20d": np.nan,
                        "avg_fwd_maxdd_20": np.nan,
                        "avg_fwd_maxdd_40": np.nan,
                        "median_fwd_maxdd_20": np.nan,
                    })
                    continue

                fire_days = df[sig].index

                # Deduplicate: only count first fire in a cluster (within 5 days)
                deduped = []
                last = -999
                for d in fire_days:
                    if d - last > 5:
                        deduped.append(d)
                    last = d
                deduped_fires = len(deduped)

                fwd_5d = df.loc[fire_days, f"{fwd_prefix}_fwd_5d"].dropna()
                fwd_10d = df.loc[fire_days, f"{fwd_prefix}_fwd_10d"].dropna()
                fwd_20d = df.loc[fire_days, f"{fwd_prefix}_fwd_20d"].dropna()
                fwd_maxdd_20 = df.loc[fire_days, f"{fwd_prefix}_fwd_maxdd_20"].dropna()
                fwd_maxdd_40 = df.loc[fire_days, f"{fwd_prefix}_fwd_maxdd_40"].dropna()

                tp = (fwd_maxdd_20 < DD_THRESHOLD).sum()
                fp = (fwd_maxdd_20 >= DD_THRESHOLD).sum()
                tp_rate = tp / (tp + fp) if (tp + fp) > 0 else np.nan
                fp_rate = fp / (tp + fp) if (tp + fp) > 0 else np.nan

                results.append({
                    "series": series_name,
                    "signal_type": sig_name,
                    "n_short": n_short,
                    "n_long": n_long,
                    "kappa": kappa,
                    "n_fires": int(n_fires),
                    "n_fires_deduped": deduped_fires,
                    "fire_rate_pct": fire_rate * 100,
                    "tp_rate": tp_rate,
                    "fp_rate": fp_rate,
                    "avg_fwd_5d": fwd_5d.mean() * 100,
                    "avg_fwd_10d": fwd_10d.mean() * 100,
                    "avg_fwd_20d": fwd_20d.mean() * 100,
                    "avg_fwd_maxdd_20": fwd_maxdd_20.mean() * 100,
                    "avg_fwd_maxdd_40": fwd_maxdd_40.mean() * 100,
                    "median_fwd_maxdd_20": fwd_maxdd_20.median() * 100,
                })

results_df = pd.DataFrame(results)
results_df.to_csv(os.path.join(OUT_DIR, "false_positive_grid_results.csv"), index=False)
print("Saved false_positive_grid_results.csv")

# ---------------------------------------------------------------------------
# Print summary tables
# ---------------------------------------------------------------------------
for series_name in ["Portfolio", "SPTM"]:
    for sig_name in ["VR+R5<0", "VR_only"]:
        sub = results_df[(results_df["series"] == series_name) &
                         (results_df["signal_type"] == sig_name)]
        if sub.empty:
            continue

        print(f"\n{'='*80}")
        print(f"  {series_name} — Signal: {sig_name}")
        print(f"{'='*80}")
        print(f"{'VR pair':>10} {'kappa':>6} {'fires':>6} {'dedup':>6} "
              f"{'fire%':>7} {'TP%':>6} {'FP%':>6} "
              f"{'fwd5d':>7} {'fwd10d':>8} {'fwd20d':>8} "
              f"{'maxDD20':>8} {'maxDD40':>8}")
        print("-" * 105)
        for _, row in sub.iterrows():
            print(f"  {row['n_short']:>2}/{row['n_long']:<3} "
                  f"{row['kappa']:>6.2f} "
                  f"{row.get('n_fires', 0):>6} "
                  f"{row.get('n_fires_deduped', ''):>6} "
                  f"{row['fire_rate_pct']:>6.2f}% "
                  f"{row['tp_rate']*100 if pd.notna(row['tp_rate']) else 0:>5.1f}% "
                  f"{row['fp_rate']*100 if pd.notna(row['fp_rate']) else 0:>5.1f}% "
                  f"{row['avg_fwd_5d']:>6.2f}% " if pd.notna(row['avg_fwd_5d']) else "",
                  end="")
            if pd.notna(row.get('avg_fwd_10d')):
                print(f"{row['avg_fwd_10d']:>7.2f}% "
                      f"{row['avg_fwd_20d']:>7.2f}% "
                      f"{row['avg_fwd_maxdd_20']:>7.2f}% "
                      f"{row['avg_fwd_maxdd_40']:>7.2f}%")
            else:
                print()

# ---------------------------------------------------------------------------
# Heatmaps: for the combined signal (VR+R5<0), plot TP rate and avg forward
# max drawdown as heatmaps over (VR pair, kappa)
# ---------------------------------------------------------------------------
for series_name in ["Portfolio", "SPTM"]:
    sub = results_df[(results_df["series"] == series_name) &
                     (results_df["signal_type"] == "VR+R5<0")].copy()

    sub["vr_label"] = sub.apply(lambda r: f"{int(r['n_short'])}/{int(r['n_long'])}", axis=1)

    for metric, title, cmap, vmin, vmax, fmt in [
        ("tp_rate", "True Positive Rate (≥5% DD follows)", "RdYlGn", 0, 1, ".0%"),
        ("avg_fwd_maxdd_20", "Avg Forward 20d Max Drawdown (%)", "RdYlGn_r", None, 0, ".2f"),
        ("avg_fwd_maxdd_40", "Avg Forward 40d Max Drawdown (%)", "RdYlGn_r", None, 0, ".2f"),
        ("fire_rate_pct", "Signal Fire Rate (%)", "YlOrRd", 0, None, ".2f"),
        ("avg_fwd_5d", "Avg Forward 5d Return (%)", "RdYlGn", None, None, ".2f"),
        ("avg_fwd_20d", "Avg Forward 20d Return (%)", "RdYlGn", None, None, ".2f"),
    ]:
        pivot = sub.pivot_table(index="vr_label", columns="kappa", values=metric)
        # Reorder rows
        row_order = [f"{s}/{l}" for s, l in VR_PAIRS]
        pivot = pivot.reindex([r for r in row_order if r in pivot.index])

        fig, ax = plt.subplots(figsize=(10, 5))
        im = ax.imshow(pivot.values, cmap=cmap, aspect="auto",
                       vmin=vmin, vmax=vmax)
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels([f"κ={k}" for k in pivot.columns])
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels(pivot.index)
        ax.set_xlabel("Threshold (κ)")
        ax.set_ylabel("VR pair (short/long)")
        ax.set_title(f"{series_name} — {title}")

        # Annotate cells
        for i in range(len(pivot.index)):
            for j in range(len(pivot.columns)):
                val = pivot.values[i, j]
                if pd.notna(val):
                    if fmt == ".0%":
                        text = f"{val:.0%}"
                    else:
                        text = f"{val:{fmt}}"
                    ax.text(j, i, text, ha="center", va="center", fontsize=8,
                            color="black" if abs(val) < (vmax or 10) * 0.6 else "white")

        plt.colorbar(im, ax=ax)
        plt.tight_layout()
        safe_metric = metric.replace("/", "_")
        outpath = os.path.join(OUT_DIR,
                               f"{series_name.lower()}_heatmap_{safe_metric}.png")
        plt.savefig(outpath, dpi=150)
        plt.close()
        print(f"Saved {outpath}")

print("\nDone.")
