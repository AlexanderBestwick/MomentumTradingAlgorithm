# Fund And Unit Accounting

This package introduces a pooled-fund layer that sits above the existing trading
logic.

## Core model

- investors own `units` in a fund
- strategies trade inside the fund
- the broker account stays attached to the fund, not to individual investors

That separation makes later strategy expansion much easier:

- one `momentum` strategy can run today
- one `hedge` strategy can be added later
- both can roll up into the same fund NAV without changing investor balances

## Why units instead of percentages

Percentages are useful output, but they should not be your source of truth.

The durable state is:

- fund NAV
- total units outstanding
- each investor's unit balance

From there:

- `unit_price = NAV / total_units`
- `investor_value = investor_units * unit_price`
- `ownership_percent = investor_units / total_units`

## Current scope

This package does not replace the existing live worker yet.

It gives us:

- shared fund and investor dataclasses
- cash-flow batching at one valuation point
- unit issuance/redemption math
- unit-ledger entry generation
- an S3 or local-folder ledger store for request / execution / latest JSON files
- a CLI for recording confirmed manual contributions and withdrawals
- a live-worker hook that can reserve cash for outstanding redemptions before buys
- a pre-trade cash-raise step that trims positions proportionally if a confirmed withdrawal is larger than the free cash already in the account

## Ledger files

For each fund, the ledger now uses these buckets / prefixes:

- `requests/` for confirmed cash flows waiting to be executed or settled
- `executions/` for immutable unit-issuance / redemption records
- `latest/` for the current fund NAV, investor balances, and pending cash-flow summaries
- `investors/` for optional display-name metadata

## CLI flow

Record a confirmed transfer after the money actually reaches Alpaca:

```bash
python3 -m Funds.ManageCashFlows record \
  --fund-id main_fund \
  --investor-id alice \
  --display-name "Alice" \
  --flow-type contribution \
  --gross-amount 1000 \
  --fee-amount 8 \
  --bucket your-private-ledger-bucket
```

After a withdrawal has been paid out, mark it settled:

```bash
python3 -m Funds.ManageCashFlows settle \
  --fund-id main_fund \
  --request-id main_fund-alice-withdrawal-20260401T093000Z \
  --bucket your-private-ledger-bucket
```

## Worker integration

If you set these environment variables on the worker, it will process confirmed
cash flows before trading and then keep the latest ledger JSON refreshed after
the run:

- `FUND_LEDGER_ENABLED=true`
- `FUND_LEDGER_FUND_ID=main_fund`
- `FUND_LEDGER_BUCKET=your-private-ledger-bucket`
- `FUND_LEDGER_PREFIX=funds`
- `FUND_INITIAL_UNIT_PRICE=100`

Test-only override:

- `IGNORE_LEDGER_EFFECTIVE_AT=true` will execute all `confirmed` cash-flow requests on the next run, even if their `effective_at` is still in the future.

For local dry-runs, you can replace the bucket with:

- `FUND_LEDGER_ROOT=/path/to/local/folder`
