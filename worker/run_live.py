import os
from pathlib import Path
import sys
from datetime import datetime, timezone


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from Config import get_alpaca_credentials
from FullRun import RunAll, build_live_clients
from SiteData.Publisher import (
    DEFAULT_LIVE_HISTORY_LIMIT,
    DEFAULT_SITE_DATA_ROOT,
    build_live_run_record,
    publish_live_run,
    upload_site_data_to_s3,
)


def _get_bool_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise RuntimeError(f"Environment variable {name} must be a boolean-like value.")


def _get_int_env(name, default):
    value = os.getenv(name)
    return default if value is None else int(value)


def _get_float_env(name, default):
    value = os.getenv(name)
    return default if value is None else float(value)


def _snapshot_account(trading_client):
    account = trading_client.get_account()
    return {
        "cash": float(account.cash),
        "portfolio_value": float(account.portfolio_value),
    }


def _snapshot_positions(trading_client):
    positions = []
    for position in trading_client.get_all_positions():
        positions.append(
            {
                "symbol": position.symbol,
                "qty": float(position.qty),
                "market_value": float(position.market_value),
            }
        )
    return positions


def main():
    credentials = get_alpaca_credentials()
    defensive_mode = os.getenv("DEFENSIVE_MODE", "cash").strip().lower()
    defensive_symbol = os.getenv("DEFENSIVE_SYMBOL", "SHY").strip().upper()
    raw_rank_limit = _get_int_env("RAW_RANK_CONSIDERATION_LIMIT", 100)
    max_position_fraction = _get_float_env("MAX_POSITION_FRACTION", 0.10)
    save_outputs = _get_bool_env("SAVE_OUTPUTS", True)
    enforce_live_safeguards = _get_bool_env("ENFORCE_LIVE_SAFEGUARDS", True)
    export_site_data = _get_bool_env("EXPORT_SITE_DATA", True)
    site_data_root = os.getenv("SITE_DATA_ROOT", str(DEFAULT_SITE_DATA_ROOT))
    live_history_limit = _get_int_env("LIVE_HISTORY_LIMIT", DEFAULT_LIVE_HISTORY_LIMIT)
    s3_publish_enabled = _get_bool_env("S3_PUBLISH_ENABLED", False)
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_prefix = os.getenv("S3_PREFIX", "")
    aws_region = os.getenv("AWS_REGION")
    live_run_source = os.getenv("LIVE_RUN_SOURCE", "ecs_worker")

    trading_client, data_client = build_live_clients()
    initial_account = _snapshot_account(trading_client)
    generated_at = datetime.now(timezone.utc)

    print(
        "Starting momentum worker with "
        f"ALPACA_ENV={credentials.environment}, "
        f"defensive_mode={defensive_mode}, "
        f"raw_rank_limit={raw_rank_limit}, "
        f"max_position_fraction={max_position_fraction:.2%}"
    )

    try:
        result = RunAll(
            trading_client=trading_client,
            data_client=data_client,
            save_outputs=save_outputs,
            defensive_mode=defensive_mode,
            defensive_symbol=defensive_symbol,
            raw_rank_consideration_limit=raw_rank_limit,
            max_position_fraction=max_position_fraction,
            enforce_live_safeguards=enforce_live_safeguards,
        )
    except Exception as exc:
        if export_site_data:
            live_run_record = build_live_run_record(
                generated_at=generated_at,
                environment=credentials.environment,
                trigger_source=live_run_source,
                initial_account=initial_account,
                final_account=_snapshot_account(trading_client),
                final_positions=_snapshot_positions(trading_client),
                error_detail=str(exc),
            )
            published_site_data = publish_live_run(
                live_run_record,
                site_data_root=site_data_root,
                max_runs=live_history_limit,
            )
            print(f"Published failed live run site data at {site_data_root}")
            if s3_publish_enabled:
                uploaded_paths = upload_site_data_to_s3(
                    published_site_data["paths"],
                    site_data_root=site_data_root,
                    bucket_name=s3_bucket_name,
                    prefix=s3_prefix,
                    aws_region=aws_region,
                )
                print(f"Uploaded {len(uploaded_paths)} failed live-run files to s3://{s3_bucket_name}")
        raise

    final_account = _snapshot_account(trading_client)
    final_positions = _snapshot_positions(trading_client)

    if export_site_data:
        live_run_record = build_live_run_record(
            result=result,
            generated_at=generated_at,
            environment=credentials.environment,
            trigger_source=live_run_source,
            initial_account=initial_account,
            final_account=final_account,
            final_positions=final_positions,
        )
        published_site_data = publish_live_run(
            live_run_record,
            site_data_root=site_data_root,
            max_runs=live_history_limit,
        )
        print(f"Published live run site data at {site_data_root} with run_id={live_run_record['id']}")
        if s3_publish_enabled:
            uploaded_paths = upload_site_data_to_s3(
                published_site_data["paths"],
                site_data_root=site_data_root,
                bucket_name=s3_bucket_name,
                prefix=s3_prefix,
                aws_region=aws_region,
            )
            print(f"Uploaded {len(uploaded_paths)} live-run files to s3://{s3_bucket_name}")

    print(
        "Momentum worker completed: "
        f"run_date={result['run_date']}, "
        f"market_health={result['market_health']}, "
        f"opened={len(result['opened'])}, "
        f"closed={len(result['closed'])}, "
        f"overrisked={len(result['overrisked'])}, "
        f"underrisked={len(result['underrisked'])}, "
        f"capped_sells={len(result['capped_sells'])}, "
        f"defensive_buys={len(result['defensive_buys'])}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Momentum worker failed: {exc}", file=sys.stderr)
        raise
