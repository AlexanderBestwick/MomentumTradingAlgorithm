"""
Plot portfolio and SPTM rolling volatility side by side for given lookback periods.
Optionally overlay daily or cumulative returns on either subplot.

Usage:
    python plot_compare_volatility.py 5 20 100
    python plot_compare_volatility.py 20 50 --returns portfolio
    python plot_compare_volatility.py 20 50 --returns both
    python plot_compare_volatility.py 20 50 --cumreturns sptm
    python plot_compare_volatility.py 20 50 --returns both --cumreturns both
    python plot_compare_volatility.py 20 --cumreturns both --signal 20 100 2.0
"""
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

parser = argparse.ArgumentParser(description="Plot rolling volatility for portfolio and SPTM.")
parser.add_argument("lookbacks", type=int, nargs="+", help="Lookback periods in days (e.g. 5 20 100)")
parser.add_argument("--returns", choices=["portfolio", "sptm", "both"], default=None,
                    help="Overlay daily returns on the corresponding subplot(s)")
parser.add_argument("--cumreturns", choices=["portfolio", "sptm", "both"], default=None,
                    help="Overlay cumulative returns on the corresponding subplot(s)")
parser.add_argument("--signal", nargs=3, metavar=("N_SHORT", "N_LONG", "KAPPA"), default=None,
                    help="Show VR sell signal as vertical lines. Args: n_short n_long kappa (e.g. 20 100 2.0). "
                         "Signal fires when VR(n_short/n_long) > kappa AND 5-day mean return < 0.")
parser.add_argument("--start", default=None, help="Start date for plot range (e.g. 2020-01-01)")
parser.add_argument("--end", default=None, help="End date for plot range (e.g. 2021-12-31)")
args = parser.parse_args()

lookbacks = args.lookbacks

# Load data
df = pd.read_csv("exit_conditions_data/most_recent_backtest_20260326.csv", parse_dates=["date"])
df.sort_values("date", inplace=True)
df.reset_index(drop=True, inplace=True)

# Compute daily returns
df["portfolio_return"] = df["portfolio_value"].pct_change()
df["sptm_return"] = df["sptm_value"].pct_change()

def rolling_variance_volatility(returns, window):
    """Rolling variance of returns w.r.t. the rolling mean over the lookback period."""
    return returns.rolling(window=window).apply(
        lambda x: np.mean((x - np.mean(x)) ** 2), raw=True
    )

show_portfolio_returns = args.returns in ("portfolio", "both")
show_sptm_returns = args.returns in ("sptm", "both")
show_portfolio_cumreturns = args.cumreturns in ("portfolio", "both")
show_sptm_cumreturns = args.cumreturns in ("sptm", "both")

# Compute cumulative returns
df["portfolio_cumreturn"] = (1 + df["portfolio_return"]).cumprod() - 1
df["sptm_cumreturn"] = (1 + df["sptm_return"]).cumprod() - 1

# Compute sell signal if requested
signal_dates = {"portfolio": [], "sptm": []}
if args.signal:
    sig_short, sig_long, sig_kappa = int(args.signal[0]), int(args.signal[1]), float(args.signal[2])
    for series_name, ret_col in [("portfolio", "portfolio_return"), ("sptm", "sptm_return")]:
        var_short = rolling_variance_volatility(df[ret_col], sig_short)
        var_long = rolling_variance_volatility(df[ret_col], sig_long)
        vr = var_short / var_long
        r5 = df[ret_col].rolling(5).mean()
        fire = (vr > sig_kappa) & (r5 < 0)
        # Deduplicate: only mark first fire in each cluster (>5 days apart)
        fire_idx = df.index[fire.fillna(False)]
        deduped = []
        last = -999
        for idx in fire_idx:
            if idx - last > 5:
                deduped.append(idx)
            last = idx
        signal_dates[series_name] = [df["date"].iloc[i] for i in deduped]

# Apply date range filter (after all rolling computations so edges are valid)
if args.start:
    df = df[df["date"] >= args.start]
if args.end:
    df = df[df["date"] <= args.end]
df.reset_index(drop=True, inplace=True)

# Filter signal dates to visible range
if args.signal:
    for key in signal_dates:
        signal_dates[key] = [d for d in signal_dates[key]
                             if (args.start is None or d >= pd.Timestamp(args.start))
                             and (args.end is None or d <= pd.Timestamp(args.end))]

fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

# Portfolio
for lb in lookbacks:
    vol = rolling_variance_volatility(df["portfolio_return"], lb)
    axes[0].plot(df["date"], vol, label=f"{lb}-day vol", linewidth=0.9)
if show_portfolio_returns or show_portfolio_cumreturns:
    ax_ret = axes[0].twinx()
    if show_portfolio_returns:
        ax_ret.plot(df["date"], df["portfolio_return"], color="grey", alpha=0.3, linewidth=0.5, label="Daily returns")
    if show_portfolio_cumreturns:
        ax_ret.plot(df["date"], df["portfolio_cumreturn"], color="blue", alpha=0.4, linewidth=0.7, label="Cumulative returns")
    ax_ret.set_ylabel("Returns")
    ax_ret.legend(loc="upper left")
for d in signal_dates["portfolio"]:
    axes[0].axvline(x=d, color="red", alpha=0.5, linewidth=0.7, linestyle="--")
if signal_dates["portfolio"]:
    axes[0].axvline(x=signal_dates["portfolio"][0], color="red", alpha=0.5,
                    linewidth=0.7, linestyle="--", label=f"Sell signal VR({sig_short}/{sig_long})>κ={sig_kappa}")
axes[0].set_title("Portfolio Returns — Rolling Volatility (Variance w.r.t. Mean)")
axes[0].set_ylabel("Variance")
axes[0].legend(title="Lookback", loc="upper right")

# SPTM
for lb in lookbacks:
    vol = rolling_variance_volatility(df["sptm_return"], lb)
    axes[1].plot(df["date"], vol, label=f"{lb}-day vol", linewidth=0.9)
if show_sptm_returns or show_sptm_cumreturns:
    ax_ret = axes[1].twinx()
    if show_sptm_returns:
        ax_ret.plot(df["date"], df["sptm_return"], color="grey", alpha=0.3, linewidth=0.5, label="Daily returns")
    if show_sptm_cumreturns:
        ax_ret.plot(df["date"], df["sptm_cumreturn"], color="blue", alpha=0.4, linewidth=0.7, label="Cumulative returns")
    ax_ret.set_ylabel("Returns")
    ax_ret.legend(loc="upper left")
for d in signal_dates["sptm"]:
    axes[1].axvline(x=d, color="red", alpha=0.5, linewidth=0.7, linestyle="--")
if signal_dates["sptm"]:
    axes[1].axvline(x=signal_dates["sptm"][0], color="red", alpha=0.5,
                    linewidth=0.7, linestyle="--", label=f"Sell signal VR({sig_short}/{sig_long})>κ={sig_kappa}")
axes[1].set_title("SPTM Returns — Rolling Volatility (Variance w.r.t. Mean)")
axes[1].set_ylabel("Variance")
axes[1].legend(title="Lookback", loc="upper right")

axes[1].set_xlabel("Date")
axes[1].xaxis.set_major_locator(mdates.YearLocator())
axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.xticks(rotation=45)
plt.tight_layout()

tag = "_".join(str(lb) for lb in lookbacks)
if args.returns:
    tag += f"_returns_{args.returns}"
if args.cumreturns:
    tag += f"_cumreturns_{args.cumreturns}"
if args.signal:
    tag += f"_signal_{sig_short}_{sig_long}_{sig_kappa}"
if args.start or args.end:
    tag += f"_{args.start or 'start'}_{args.end or 'end'}"
outpath = f"exit_conditions_plots/compare_volatility_{tag}.png"
plt.savefig(outpath, dpi=150)
plt.close()
print(f"Saved {outpath}")
