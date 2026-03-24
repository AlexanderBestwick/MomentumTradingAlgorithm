import os
from pathlib import Path
import sys
from datetime import datetime, timezone


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from Database.LiveRunStore import DEFAULT_LIVE_DATABASE_PATH, save_live_run_record
from Config import get_alpaca_credentials
from FullRun import RunAll, build_live_clients


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
    export_live_database = _get_bool_env("EXPORT_LIVE_DATABASE", True)
    live_database_url = os.getenv("LIVE_DATABASE_URL") or os.getenv("DATABASE_URL")
    live_database_path = os.getenv("LIVE_DATABASE_PATH", str(DEFAULT_LIVE_DATABASE_PATH))
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
        if export_live_database:
            save_live_run_record(
                generated_at=generated_at,
                environment=credentials.environment,
                trigger_source=live_run_source,
                database_path=live_database_path,
                database_url=live_database_url,
                initial_account=initial_account,
                final_account=_snapshot_account(trading_client),
                final_positions=_snapshot_positions(trading_client),
                error_detail=str(exc),
            )
            print(f"Live run failure saved to database at {live_database_url or live_database_path}")
        raise

    final_account = _snapshot_account(trading_client)
    final_positions = _snapshot_positions(trading_client)

    if export_live_database:
        run_id = save_live_run_record(
            result=result,
            generated_at=generated_at,
            environment=credentials.environment,
            trigger_source=live_run_source,
            database_path=live_database_path,
            database_url=live_database_url,
            initial_account=initial_account,
            final_account=final_account,
            final_positions=final_positions,
        )
        print(f"Live run saved to database at {live_database_url or live_database_path} with run_id={run_id}")

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
