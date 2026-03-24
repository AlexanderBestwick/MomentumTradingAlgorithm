import os
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

from alpaca.common.enums import Sort
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest, GetPortfolioHistoryRequest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from Config import get_alpaca_credentials
from FullRun import RunAll, build_live_clients
from SiteData.Publisher import (
    DEFAULT_LIVE_HISTORY_LIMIT,
    DEFAULT_SITE_DATA_ROOT,
    build_live_run_record,
    publish_error_event,
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
        "equity": float(getattr(account, "equity", account.portfolio_value)),
        "buying_power": None if getattr(account, "buying_power", None) is None else float(account.buying_power),
        "long_market_value": None if getattr(account, "long_market_value", None) is None else float(account.long_market_value),
    }


def _snapshot_positions(trading_client):
    positions = []
    for position in trading_client.get_all_positions():
        positions.append(
            {
                "symbol": position.symbol,
                "qty": float(position.qty),
                "market_value": float(position.market_value),
                "avg_entry_price": None if getattr(position, "avg_entry_price", None) is None else float(position.avg_entry_price),
                "current_price": None if getattr(position, "current_price", None) is None else float(position.current_price),
                "cost_basis": None if getattr(position, "cost_basis", None) is None else float(position.cost_basis),
                "unrealized_pl": None if getattr(position, "unrealized_pl", None) is None else float(position.unrealized_pl),
                "unrealized_plpc": None if getattr(position, "unrealized_plpc", None) is None else float(position.unrealized_plpc),
                "change_today": None if getattr(position, "change_today", None) is None else float(position.change_today),
            }
        )
    return positions


def _coerce_iso_datetime(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _serialize_portfolio_history(history):
    if history is None or not getattr(history, "timestamp", None):
        return None

    timestamps = [
        datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        for timestamp in history.timestamp
    ]
    cashflow = {}
    for activity_type, values in (getattr(history, "cashflow", {}) or {}).items():
        key = getattr(activity_type, "value", str(activity_type))
        cashflow[key] = [float(value) for value in values]

    return {
        "timestamps": timestamps,
        "equity": [float(value) for value in history.equity],
        "profit_loss": [float(value) for value in history.profit_loss],
        "profit_loss_pct": [None if value is None else float(value) for value in history.profit_loss_pct],
        "base_value": None if history.base_value is None else float(history.base_value),
        "timeframe": history.timeframe,
        "cashflow": cashflow,
    }


def _fetch_portfolio_history(trading_client):
    windows = {
        "1M": GetPortfolioHistoryRequest(period="1M", timeframe="1D"),
        "3M": GetPortfolioHistoryRequest(period="3M", timeframe="1D"),
        "1A": GetPortfolioHistoryRequest(period="1A", timeframe="1D"),
    }
    histories = {}
    for label, request in windows.items():
        try:
            history = trading_client.get_portfolio_history(request)
            serialized = _serialize_portfolio_history(history)
            if serialized:
                histories[label] = serialized
        except Exception as exc:
            print(f"Warning: failed to fetch {label} portfolio history: {exc}")

    total_fees_paid = None
    try:
        fee_history = trading_client.get_portfolio_history(
            GetPortfolioHistoryRequest(period="5A", timeframe="1D", cashflow_types="FEE,CFEE")
        )
        cashflow = getattr(fee_history, "cashflow", {}) or {}
        total_fees_paid = abs(
            sum(float(value) for values in cashflow.values() for value in values)
        )
    except Exception as exc:
        print(f"Warning: failed to fetch cumulative fee history: {exc}")

    return histories, total_fees_paid


def _fetch_recent_orders(trading_client, *, after_time, action_details):
    reason_lookup = {}
    for detail in action_details or []:
        reason_lookup[(detail.get("symbol"), detail.get("side"))] = detail

    try:
        orders = trading_client.get_orders(
            GetOrdersRequest(
                status=QueryOrderStatus.ALL,
                after=after_time - timedelta(minutes=2),
                until=datetime.now(timezone.utc),
                direction=Sort.DESC,
            )
        )
    except Exception as exc:
        print(f"Warning: failed to fetch recent orders: {exc}")
        return []

    serialized = []
    for order in orders:
        symbol = getattr(order, "symbol", None)
        side_value = getattr(getattr(order, "side", None), "value", str(getattr(order, "side", ""))).lower()
        detail = reason_lookup.get((symbol, side_value))
        serialized.append(
            {
                "id": str(getattr(order, "id", "")),
                "symbol": symbol,
                "side": side_value,
                "status": getattr(getattr(order, "status", None), "value", str(getattr(order, "status", ""))),
                "submitted_at": _coerce_iso_datetime(getattr(order, "submitted_at", None)),
                "filled_at": _coerce_iso_datetime(getattr(order, "filled_at", None)),
                "qty": None if getattr(order, "qty", None) is None else float(order.qty),
                "filled_qty": None if getattr(order, "filled_qty", None) is None else float(order.filled_qty),
                "filled_avg_price": None if getattr(order, "filled_avg_price", None) is None else float(order.filled_avg_price),
                "notional": None if getattr(order, "notional", None) is None else float(order.notional),
                "category": detail.get("category") if detail else None,
                "reason": detail.get("reason") if detail else None,
                "raw_rank": detail.get("raw_rank") if detail else None,
            }
        )

    return serialized


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
            portfolio_history, total_fees_paid = _fetch_portfolio_history(trading_client)
            recent_orders = _fetch_recent_orders(
                trading_client,
                after_time=generated_at,
                action_details=[],
            )
            live_run_record = build_live_run_record(
                generated_at=generated_at,
                environment=credentials.environment,
                trigger_source=live_run_source,
                initial_account=initial_account,
                final_account=_snapshot_account(trading_client),
                final_positions=_snapshot_positions(trading_client),
                portfolio_history=portfolio_history,
                recent_orders=recent_orders,
                total_fees_paid=total_fees_paid,
                error_detail=str(exc),
            )
            published_site_data = publish_live_run(
                live_run_record,
                site_data_root=site_data_root,
                max_runs=live_history_limit,
            )
            published_error_data = publish_error_event(
                generated_at=generated_at,
                source="live_worker",
                category="live_run_failed",
                title="Live worker run failed",
                message=str(exc),
                run_id=live_run_record["id"],
                site_data_root=site_data_root,
                context={
                    "environment": credentials.environment,
                    "trigger_source": live_run_source,
                },
            )
            print(f"Published failed live run site data at {site_data_root}")
            if s3_publish_enabled:
                uploaded_paths = upload_site_data_to_s3(
                    [*published_site_data["paths"], *published_error_data["paths"]],
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
        portfolio_history, total_fees_paid = _fetch_portfolio_history(trading_client)
        recent_orders = _fetch_recent_orders(
            trading_client,
            after_time=generated_at,
            action_details=result.get("action_details", []),
        )
        live_run_record = build_live_run_record(
            result=result,
            generated_at=generated_at,
            environment=credentials.environment,
            trigger_source=live_run_source,
            initial_account=initial_account,
            final_account=final_account,
            final_positions=final_positions,
            portfolio_history=portfolio_history,
            recent_orders=recent_orders,
            total_fees_paid=total_fees_paid,
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
