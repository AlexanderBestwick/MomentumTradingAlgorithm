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

The next integration step is to value the live portfolio before each scheduled
rebalance, process pending contributions/withdrawals, and only then run the
portfolio construction / execution step.
