# Published Site Data

This folder contains the shared JSON publishing layer for the website-facing data flow.

## Default output root

By default the publisher writes under:

- `frontend/data/`

That keeps the local static dashboard simple today and maps cleanly to an S3 upload step later.

## Backtest files

- `frontend/data/backtests/index.json`
  - recent backtest run summaries
- `frontend/data/backtests/latest.json`
  - latest full backtest payload
- `frontend/data/backtests/runs/<run_id>.json`
  - one full backtest payload per run
- `frontend/data/backtests/charts/<run_id>.png`
  - copied chart artifact
- `frontend/data/backtests/results/<run_id>.csv`
  - copied CSV artifact

## Live files

- `frontend/data/live/latest.json`
  - latest live run payload
- `frontend/data/live/history.json`
  - recent live run summaries
- `frontend/data/live/runs/<run_id>.json`
  - one full live run payload per execution

## Why this exists

This project now defaults to a JSON-first publishing flow instead of writing results into a database.

That keeps the website path cheap and simple:

- backtests and live runs publish JSON
- S3 can store those files later
- the frontend reads the published files directly

## Optional S3 sync

Both the backtest runner and live worker can optionally upload the published files to S3 after writing them locally.

Use:

- `S3_PUBLISH_ENABLED=true`
- `S3_BUCKET_NAME=momentum-run-data`
- `S3_PREFIX=` (optional)
- `AWS_REGION=eu-west-2`
