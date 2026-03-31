# Backtest Database

This folder now holds a legacy database storage layer for backtest and live results.

The current default website flow publishes JSON files directly for the frontend instead of writing to a database.

## Files

- `BacktestStore.py`: schema creation, inserts, and query helpers
- `FundStore.py`: schema creation and query helpers for pooled funds, units, investors, and fund-level cash flows

## Tables

- `backtest_runs`
  - one row per backtest run
  - stores summary metrics and run parameters

- `backtest_timeseries`
  - one row per date within a backtest run
  - stores portfolio, benchmark, reserve, and trade-count series

- `live_runs`
  - one row per live worker execution
  - stores run status, account snapshots, and summary counts

- `live_run_actions`
  - one row per action/symbol touched in a live run

- `live_run_positions`
  - end-of-run live position snapshot

- `funds`
  - one row per pooled fund / vehicle

- `fund_investors`
  - one row per investor profile

- `fund_memberships`
  - which investors belong to which funds

- `fund_cash_flows`
  - pending and executed contributions / withdrawals

- `fund_nav_snapshots`
  - fund valuation snapshots with total units and unit price

- `fund_unit_ledger`
  - unit issuance / redemption entries per investor

- `fund_strategy_allocations`
  - sleeve weights for strategy modules inside a fund

## Default database path

By default the backtest database is written to:

- `Data/backtest_results.db`

That path is configured from [Backtesting.py](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/Backtesting.py).

## AWS / PostgreSQL

The storage layer also supports a PostgreSQL connection URL.

For the new pooled-fund / unit-accounting work, PostgreSQL is the recommended
production target rather than SQLite.

Examples:

- `postgresql://username:password@host:5432/database?sslmode=require`
- `sqlite:///Data/backtest_results.db`

Use:

- `database_url` in [Backtesting.py](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/Backtesting.py)
- `LIVE_DATABASE_URL` or `DATABASE_URL` in the ECS worker
