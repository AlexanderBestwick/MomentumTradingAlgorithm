import os
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone
import time

from alpaca.common.enums import Sort
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest, GetPortfolioHistoryRequest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from Config import get_alpaca_credentials
from App.LiveRebalance import RunAll, build_live_clients
from Funds.Accounting import DEFAULT_INITIAL_UNIT_PRICE
from Funds.FundCycle import (
    process_confirmed_cash_flows,
    raise_cash_for_shortfall,
    wrap_trading_client_with_cash_reserve,
    write_latest_ledger_state,
)
from Funds.LedgerStore import FundLedgerStore
from SiteData.Publisher import (
    DEFAULT_LIVE_HISTORY_LIMIT,
    DEFAULT_SITE_DATA_ROOT,
    LIVE_ERROR_SOURCES,
    build_live_run_record,
    publish_error_event,
    publish_live_run,
    resolve_error_events,
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


def _resolve_cash_buffer(account_snapshot):
    fixed_cash_buffer = _get_float_env("CASH_BUFFER", 10.0)
    raw_percent = os.getenv("CASH_BUFFER_PERCENT")

    if raw_percent is None:
        return fixed_cash_buffer, "fixed"

    cash_buffer_percent = float(raw_percent)
    cash_buffer_min = _get_float_env("CASH_BUFFER_MIN", fixed_cash_buffer)
    account_equity = float(account_snapshot["equity"])
    computed_cash_buffer = max(cash_buffer_min, account_equity * cash_buffer_percent)
    return computed_cash_buffer, f"percent({cash_buffer_percent:.4%})"


def _is_running_in_aws():
    aws_markers = (
        os.getenv("ECS_CONTAINER_METADATA_URI_V4", "").strip(),
        os.getenv("ECS_CONTAINER_METADATA_URI", "").strip(),
        os.getenv("AWS_EXECUTION_ENV", "").strip(),
    )
    return any(aws_markers)


def _should_upload_site_data_to_s3():
    if not _get_bool_env("S3_PUBLISH_ENABLED", False):
        return False, "disabled"

    if _get_bool_env("S3_PUBLISH_ALLOW_LOCAL", False):
        return True, "local_override"

    if _is_running_in_aws():
        return True, "aws_runtime"

    return False, "local_runtime"


def _build_fund_ledger_store():
    enabled = _get_bool_env("FUND_LEDGER_ENABLED", False)
    bucket_name = os.getenv("FUND_LEDGER_BUCKET", "").strip()
    root_path = os.getenv("FUND_LEDGER_ROOT", "").strip()

    if not enabled and not bucket_name and not root_path:
        return None

    fund_id = (
        os.getenv("FUND_LEDGER_FUND_ID", "").strip()
        or os.getenv("FUND_ID", "").strip()
    )
    if not fund_id:
        raise RuntimeError(
            "Fund ledger support requires FUND_LEDGER_FUND_ID or FUND_ID to be set."
        )

    return FundLedgerStore(
        fund_id=fund_id,
        bucket_name=bucket_name or None,
        prefix=os.getenv("FUND_LEDGER_PREFIX", "funds"),
        root_path=root_path or None,
        aws_region=os.getenv("AWS_REGION", "").strip() or None,
    )


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
    ignore_once_per_day_check = _get_bool_env("IGNORE_ONCE_PER_DAY_CHECK", False)
    ignore_ledger_effective_at = _get_bool_env("IGNORE_LEDGER_EFFECTIVE_AT", False)
    export_site_data = _get_bool_env("EXPORT_SITE_DATA", True)
    site_data_root = os.getenv("SITE_DATA_ROOT", str(DEFAULT_SITE_DATA_ROOT))
    live_history_limit = _get_int_env("LIVE_HISTORY_LIMIT", DEFAULT_LIVE_HISTORY_LIMIT)
    s3_publish_enabled, s3_publish_reason = _should_upload_site_data_to_s3()
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_prefix = os.getenv("S3_PREFIX", "")
    aws_region = os.getenv("AWS_REGION")
    live_run_source = os.getenv("LIVE_RUN_SOURCE", "ecs_worker")
    fund_initial_unit_price = _get_float_env("FUND_INITIAL_UNIT_PRICE", DEFAULT_INITIAL_UNIT_PRICE)
    withdrawal_cash_raise_buffer_percent = _get_float_env(
        "WITHDRAWAL_CASH_RAISE_BUFFER_PERCENT",
        0.001,
    )

    broker_trading_client, data_client = build_live_clients()
    strategy_trading_client = broker_trading_client
    initial_account = _snapshot_account(broker_trading_client)
    cash_buffer, cash_buffer_mode = _resolve_cash_buffer(initial_account)
    generated_at = datetime.now(timezone.utc)
    fund_ledger_store = _build_fund_ledger_store()
    fund_cycle_summary = None

    print(
        "Starting momentum worker with "
        f"ALPACA_ENV={credentials.environment}, "
        f"defensive_mode={defensive_mode}, "
        f"raw_rank_limit={raw_rank_limit}, "
        f"max_position_fraction={max_position_fraction:.2%}, "
        f"cash_buffer={cash_buffer:.2f}, "
        f"cash_buffer_mode={cash_buffer_mode}, "
        f"withdrawal_cash_raise_buffer_percent={withdrawal_cash_raise_buffer_percent:.4%}"
    )
    if os.getenv("S3_PUBLISH_ENABLED") is not None:
        print(
            "Site-data S3 upload mode: "
            f"enabled={s3_publish_enabled}, reason={s3_publish_reason}"
        )
    if ignore_once_per_day_check:
        print("Warning: duplicate-run protection is disabled for this run.")
    if ignore_ledger_effective_at:
        print("Warning: IGNORE_LEDGER_EFFECTIVE_AT is enabled. All confirmed cash flows will execute on this run.")

    try:
        if fund_ledger_store is not None:
            fund_cycle_summary = process_confirmed_cash_flows(
                fund_ledger_store,
                valued_at=generated_at,
                gross_asset_value=initial_account["equity"],
                cash_value=initial_account["cash"],
                initial_unit_price=fund_initial_unit_price,
                source_run_id=f"{generated_at:%Y%m%dT%H%M%S}",
                note=f"Processed by {live_run_source}",
                ignore_effective_at=ignore_ledger_effective_at,
            )
            active_fund_state = fund_cycle_summary["latest_state"]
            cash_shortfall = float(active_fund_state["fund_nav"].get("cash_shortfall", 0.0) or 0.0)

            if cash_shortfall > 0:
                print(
                    "Outstanding withdrawal shortfall detected: "
                    f"{cash_shortfall:.2f}. Selling positions proportionally before new buys."
                )
                cash_raise_summary = raise_cash_for_shortfall(
                    broker_trading_client,
                    cash_shortfall,
                    oversell_buffer_percent=withdrawal_cash_raise_buffer_percent,
                )
                fund_cycle_summary["cash_raise"] = cash_raise_summary

                wait_seconds = _get_float_env("FUND_LEDGER_SHORTFALL_WAIT_SECONDS", 2.0)
                if cash_raise_summary["submitted_orders"] and wait_seconds > 0:
                    time.sleep(wait_seconds)

                refreshed_account = _snapshot_account(broker_trading_client)
                active_fund_state = write_latest_ledger_state(
                    fund_ledger_store,
                    valued_at=datetime.now(timezone.utc),
                    gross_asset_value=refreshed_account["equity"],
                    cash_value=refreshed_account["cash"],
                    initial_unit_price=fund_initial_unit_price,
                    source_run_id=f"{generated_at:%Y%m%dT%H%M%S}",
                    note=f"Refreshed after cash raise by {live_run_source}",
                )
                fund_cycle_summary["latest_state"] = active_fund_state

                remaining_shortfall = float(active_fund_state["fund_nav"].get("cash_shortfall", 0.0) or 0.0)
                if remaining_shortfall > 0:
                    print(
                        "Warning: cash shortfall remains after pre-trade liquidation: "
                        f"{remaining_shortfall:.2f}"
                    )

            reserved_cash = float(active_fund_state.get("cash_reserve", 0.0) or 0.0)
            strategy_trading_client = wrap_trading_client_with_cash_reserve(
                broker_trading_client,
                reserved_cash,
            )
            print(
                "Processed fund ledger with "
                f"executed_count={fund_cycle_summary['executed_count']}, "
                f"cash_reserve={reserved_cash:.2f}"
            )

        result = RunAll(
            trading_client=strategy_trading_client,
            data_client=data_client,
            save_outputs=save_outputs,
            defensive_mode=defensive_mode,
            defensive_symbol=defensive_symbol,
            raw_rank_consideration_limit=raw_rank_limit,
            max_position_fraction=max_position_fraction,
            cash_buffer=cash_buffer,
            enforce_live_safeguards=enforce_live_safeguards,
            ignore_once_per_day_check=ignore_once_per_day_check,
        )
    except Exception as exc:
        if export_site_data:
            portfolio_history, total_fees_paid = _fetch_portfolio_history(broker_trading_client)
            recent_orders = _fetch_recent_orders(
                broker_trading_client,
                after_time=generated_at,
                action_details=[],
            )
            live_run_record = build_live_run_record(
                generated_at=generated_at,
                environment=credentials.environment,
                trigger_source=live_run_source,
                initial_account=initial_account,
                final_account=_snapshot_account(broker_trading_client),
                final_positions=_snapshot_positions(broker_trading_client),
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

    final_account = _snapshot_account(broker_trading_client)
    final_positions = _snapshot_positions(broker_trading_client)

    if fund_ledger_store is not None:
        latest_fund_state = write_latest_ledger_state(
            fund_ledger_store,
            valued_at=datetime.now(timezone.utc),
            gross_asset_value=final_account["equity"],
            cash_value=final_account["cash"],
            initial_unit_price=fund_initial_unit_price,
            source_run_id=f"{generated_at:%Y%m%dT%H%M%S}",
            note=f"Refreshed after {live_run_source}",
        )
        result["fund_cycle"] = fund_cycle_summary
        result["fund_latest_state"] = latest_fund_state["fund_nav"]
        print(
            "Updated fund ledger latest state with "
            f"unit_price={latest_fund_state['fund_nav']['unit_price']:.4f}, "
            f"total_units={latest_fund_state['fund_nav']['total_units']:.6f}"
        )

    if export_site_data:
        portfolio_history, total_fees_paid = _fetch_portfolio_history(broker_trading_client)
        recent_orders = _fetch_recent_orders(
            broker_trading_client,
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
        resolved_error_data = resolve_error_events(
            resolved_at=datetime.now(timezone.utc),
            site_data_root=site_data_root,
            sources=LIVE_ERROR_SOURCES,
        )
        print(f"Published live run site data at {site_data_root} with run_id={live_run_record['id']}")
        if s3_publish_enabled:
            uploaded_paths = upload_site_data_to_s3(
                [*published_site_data["paths"], *resolved_error_data["paths"]],
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
