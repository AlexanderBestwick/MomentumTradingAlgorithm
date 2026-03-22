# Trading Worker

This worker packages the existing strategy code for scheduled execution in AWS.

## What it runs

The container entrypoint is [worker/run_live.py](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/worker/run_live.py), which calls [FullRun.py](/c:/Users/alexa/Documents/GitHub/MomentumTradingAlgorithm/FullRun.py) using environment-driven settings.

It keeps your current code layout intact and adds a deployment wrapper around it.

## Local Docker test

Build the image from the repo root:

```powershell
docker build -f Dockerfile.worker -t momentum-worker .
```

Run it against your paper account:

```powershell
docker run --rm --env-file .env momentum-worker
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
- `SAVE_OUTPUTS=true`
- `ENFORCE_LIVE_SAFEGUARDS=true`

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
