import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Load data
df = pd.read_csv("exit_conditions_data/most_recent_backtest_20260326.csv", parse_dates=["date"])
df.sort_values("date", inplace=True)
df.reset_index(drop=True, inplace=True)

# Compute daily returns
df["portfolio_return"] = df["portfolio_value"].pct_change()
df["sptm_return"] = df["sptm_value"].pct_change()

lookbacks = [5, 10, 20, 50, 100, 150, 200]

def rolling_variance_volatility(returns, window):
    """Rolling variance of returns w.r.t. the rolling mean over the lookback period."""
    rolling_mean = returns.rolling(window=window).mean()
    rolling_var = returns.rolling(window=window).apply(
        lambda x: np.mean((x - np.mean(x)) ** 2), raw=True
    )
    return rolling_var

# --- Plot 1: Portfolio rolling volatility ---
fig, ax = plt.subplots(figsize=(14, 7))
for lb in lookbacks:
    vol = rolling_variance_volatility(df["portfolio_return"], lb)
    ax.plot(df["date"], vol, label=f"{lb}-day", linewidth=0.9)

ax.set_title("Portfolio Returns — Rolling Volatility (Variance w.r.t. Mean)")
ax.set_xlabel("Date")
ax.set_ylabel("Variance")
ax.legend(title="Lookback")
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("exit_conditions_plots/portfolio_rolling_volatility.png", dpi=150)
plt.close()
print("Saved portfolio_rolling_volatility.png")

# --- Plot 2: SPTM rolling volatility ---
fig, ax = plt.subplots(figsize=(14, 7))
for lb in lookbacks:
    vol = rolling_variance_volatility(df["sptm_return"], lb)
    ax.plot(df["date"], vol, label=f"{lb}-day", linewidth=0.9)

ax.set_title("SPTM Returns — Rolling Volatility (Variance w.r.t. Mean)")
ax.set_xlabel("Date")
ax.set_ylabel("Variance")
ax.legend(title="Lookback")
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("exit_conditions_plots/sptm_rolling_volatility.png", dpi=150)
plt.close()
print("Saved sptm_rolling_volatility.png")
