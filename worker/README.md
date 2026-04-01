# Trading Worker

This worker packages the existing strategy code for scheduled execution in AWS. Build from [Dockerfile.worker](/home/alexa/Documents/GitHub/MomentumTradingAlgorithm/Dockerfile.worker), and use command overrides when you want the same image to run a backtest entrypoint instead of the live worker.

## What it runs

The container entrypoint is [worker/run_live.py](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/worker/run_live.py), which calls [LiveRebalance.py](/home/alexa/Documents/GitHub/MomentumTradingAlgorithm/App/LiveRebalance.py) using environment-driven settings.

It keeps the current strategy behavior intact and packages the app-oriented module layout for scheduled execution.

## Local Docker test

Build the image from the repo root:

```bash
docker build -f Dockerfile.worker -t momentum-app .
```

Run it against your paper account:

```bash
docker run --rm --env-file .env momentum-app
```

Run the fund-flow backtest instead by overriding the command:

```bash
docker run --rm -v "$(pwd)/Data:/app/Data" --env-file .env momentum-app python BacktestingFundFlows.py
```

## Environment variables

Required:

- `ALPACA_ENV=paper` or `live`
- `ALPACA_PAPER_KEY`
- `ALPACA_PAPER_SECRET`
- `ALPACA_LIVE_KEY`
- `ALPACA_LIVE_SECRET`

Optional worker settings:

- `DEFENSIVE_MODE=cash` or `treasury_bonds`
- `DEFENSIVE_SYMBOL=SGOV`
- `RAW_RANK_CONSIDERATION_LIMIT=80`
- `MAX_POSITION_FRACTION=0.10`
- `CASH_BUFFER=10`
- `CASH_BUFFER_PERCENT=0.001`
- `CASH_BUFFER_MIN=10`
- `SAVE_OUTPUTS=true`
- `ENFORCE_LIVE_SAFEGUARDS=true`
- `IGNORE_ONCE_PER_DAY_CHECK=false`
- `IGNORE_LEDGER_EFFECTIVE_AT=false`
- `WITHDRAWAL_CASH_RAISE_BUFFER_PERCENT=0.001`
- `EXPORT_SITE_DATA=true`
- `SITE_DATA_ROOT=frontend/data`
- `LIVE_HISTORY_LIMIT=30`
- `S3_PUBLISH_ENABLED=true`
- `S3_PUBLISH_ALLOW_LOCAL=false`
- `S3_BUCKET_NAME=momentum-run-data`
- `S3_PREFIX=`
- `AWS_REGION=eu-west-2`
- `LIVE_RUN_SOURCE=ecs_worker`

`CASH_BUFFER` remains the fixed-dollar fallback. If you set `CASH_BUFFER_PERCENT`, the worker will instead use `max(CASH_BUFFER_MIN, equity * CASH_BUFFER_PERCENT)`. For example, `CASH_BUFFER_PERCENT=0.001` keeps back about `0.1%` of account equity, with `CASH_BUFFER_MIN` acting as the minimum floor.

`WITHDRAWAL_CASH_RAISE_BUFFER_PERCENT` applies only to withdrawal funding. For example, `0.001` tells the worker to try to sell about `0.1%` more than the current withdrawal cash shortfall so small fill differences are less likely to leave the account underfunded.

## AWS structure

Recommended first deployment shape:

- Website: deploy `frontend/` separately
- Worker: build this Docker image and run it on ECS Fargate
- Schedule: EventBridge Scheduler
- Secrets: Secrets Manager or ECS task secrets
- Logs: CloudWatch

## Typical AWS flow

1. Push this repo to GitHub.
2. Build `Dockerfile.worker`.
3. Push the image to ECR.
4. Create an ECS task that runs `python worker/run_live.py`.
5. Inject your paper-account secrets into the task.
6. Schedule the task weekly with EventBridge Scheduler.

## Important note

The worker image includes [Data/holdings-daily-us-en-sptm.csv](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/Data/holdings-daily-us-en-sptm.csv), but it intentionally excludes the rest of `Data/` from the Docker build context.
Live run results are now published as website-facing JSON under `frontend/data/live/`.
If `S3_PUBLISH_ENABLED=true`, the worker only uploads those files to S3 when it is running inside AWS/ECS.
Local runs skip the S3 upload by default, even if the flag is set in `.env`.
If you intentionally want a local run to upload, set `S3_PUBLISH_ALLOW_LOCAL=true`.
