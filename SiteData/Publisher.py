import json
import mimetypes
import shutil
from datetime import date, datetime
from pathlib import Path


DEFAULT_SITE_DATA_ROOT = Path("frontend/data")
DEFAULT_BACKTEST_HISTORY_LIMIT = 12
DEFAULT_LIVE_HISTORY_LIMIT = 30
DEFAULT_ERROR_HISTORY_LIMIT = 40
SCHEMA_VERSION = 1


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _read_json(path, default):
    path = Path(path)
    if not path.exists():
        return default

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default), encoding="utf-8")


def _append_if_present(paths, value):
    if value is not None:
        paths.append(Path(value))


def _copy_artifact(source_path, target_path):
    if not source_path:
        return None

    source_path = Path(source_path)
    if not source_path.exists():
        return None

    target_path = Path(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    return target_path


def _site_url(site_data_root, target_path):
    relative = Path(target_path).relative_to(Path(site_data_root)).as_posix()
    return f"./data/{relative}"


def _sort_runs_desc(runs):
    return sorted(runs, key=lambda run: run.get("generated_at", ""), reverse=True)


def _normalize_symbol_list(values):
    if values is None:
        return []
    if isinstance(values, set):
        values = sorted(values)
    return [str(value) for value in values]


def _normalized_prefix(prefix):
    prefix = (prefix or "").strip().strip("/")
    return prefix


def _guess_content_type(path):
    content_type, _ = mimetypes.guess_type(str(path))
    return content_type or "application/octet-stream"


def upload_site_data_to_s3(
    published_paths,
    *,
    site_data_root=DEFAULT_SITE_DATA_ROOT,
    bucket_name,
    prefix="",
    aws_region=None,
):
    if not bucket_name:
        raise RuntimeError("S3 bucket name is required when S3 publishing is enabled.")

    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "S3 publishing requires the 'boto3' package. Install it before enabling S3 publishing."
        ) from exc

    site_data_root = Path(site_data_root).resolve()
    prefix = _normalized_prefix(prefix)
    s3_client = boto3.client("s3", region_name=aws_region or None)

    uploaded = []
    seen = set()
    for path in published_paths:
        path = Path(path)
        if not path.exists():
            continue

        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)

        relative = resolved.relative_to(site_data_root).as_posix()
        key = f"{prefix}/{relative}" if prefix else relative
        extra_args = {"ContentType": _guess_content_type(resolved)}
        s3_client.upload_file(str(resolved), bucket_name, key, ExtraArgs=extra_args)
        uploaded.append(f"s3://{bucket_name}/{key}")

    return uploaded


def publish_backtest_run(
    backtest_record,
    *,
    site_data_root=DEFAULT_SITE_DATA_ROOT,
    chart_path=None,
    results_path=None,
    max_runs=DEFAULT_BACKTEST_HISTORY_LIMIT,
):
    site_data_root = Path(site_data_root)
    run_id = backtest_record["id"]
    published_paths = []

    detail_path = site_data_root / "backtests" / "runs" / f"{run_id}.json"
    latest_path = site_data_root / "backtests" / "latest.json"
    index_path = site_data_root / "backtests" / "index.json"

    chart_url = None
    copied_chart_path = _copy_artifact(
        chart_path,
        site_data_root / "backtests" / "charts" / f"{run_id}{Path(chart_path).suffix}" if chart_path else None,
    )
    if copied_chart_path is not None:
        chart_url = _site_url(site_data_root, copied_chart_path)
        _append_if_present(published_paths, copied_chart_path)

    results_url = None
    copied_results_path = _copy_artifact(
        results_path,
        site_data_root / "backtests" / "results" / f"{run_id}{Path(results_path).suffix}" if results_path else None,
    )
    if copied_results_path is not None:
        results_url = _site_url(site_data_root, copied_results_path)
        _append_if_present(published_paths, copied_results_path)

    detail_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "backtest_run",
        **backtest_record,
        "artifacts": {
            "chart_path": chart_url,
            "results_path": results_url,
        },
    }
    _write_json(detail_path, detail_payload)
    _write_json(latest_path, detail_payload)
    _append_if_present(published_paths, detail_path)
    _append_if_present(published_paths, latest_path)

    run_summary = {
        "id": backtest_record["id"],
        "generated_at": backtest_record["generated_at"],
        "period": backtest_record["period"],
        "summary": backtest_record["summary"],
        "detail_path": _site_url(site_data_root, detail_path),
        "chart_path": chart_url,
        "results_path": results_url,
    }

    current_index = _read_json(index_path, {"schema_version": SCHEMA_VERSION, "updated_at": None, "runs": []})
    runs = [run for run in current_index.get("runs", []) if run.get("id") != run_id]
    runs.insert(0, run_summary)
    runs = _sort_runs_desc(runs)[:max_runs]

    index_payload = {
        "schema_version": SCHEMA_VERSION,
        "updated_at": backtest_record["generated_at"],
        "latest_run_id": run_id,
        "runs": runs,
    }
    _write_json(index_path, index_payload)
    _append_if_present(published_paths, index_path)

    return {
        "index": index_payload,
        "paths": published_paths,
    }


def build_live_run_record(
    *,
    generated_at,
    environment,
    trigger_source,
    initial_account,
    final_account,
    final_positions,
    result=None,
    error_detail=None,
    portfolio_history=None,
    recent_orders=None,
    total_fees_paid=None,
):
    generated_at_iso = generated_at.isoformat() if isinstance(generated_at, datetime) else str(generated_at)
    run_date = None
    status = "failed" if error_detail else "completed"
    action_lists = {
        "opened": [],
        "closed": [],
        "overrisked": [],
        "underrisked": [],
        "capped_sells": [],
        "defensive_buys": [],
    }
    settings = {
        "defensive_mode": "cash",
        "defensive_symbol": "",
        "raw_rank_consideration_limit": None,
        "max_position_fraction": None,
        "is_risk_rebalance_day": False,
    }
    summary = {
        "market_health": None,
        "approved_count": 0,
        "opened_count": 0,
        "closed_count": 0,
        "overrisked_count": 0,
        "underrisked_count": 0,
        "capped_sells_count": 0,
        "defensive_buy_count": 0,
        "portfolio_value_change": float(final_account["portfolio_value"]) - float(initial_account["portfolio_value"]),
        "cash_change": float(final_account["cash"]) - float(initial_account["cash"]),
        "positions_final": len(final_positions),
        "defensive_mode": "cash",
        "defensive_symbol": "",
        "raw_rank_consideration_limit": None,
        "max_position_fraction": None,
        "final_portfolio_value": float(final_account["portfolio_value"]),
        "final_cash": float(final_account["cash"]),
        "reserve_percentage": (
            (float(final_account["cash"]) / float(final_account["portfolio_value"])) * 100.0
            if float(final_account["portfolio_value"])
            else 0.0
        ),
        "total_fees_paid": total_fees_paid,
    }

    if result is not None:
        run_date = result["run_date"].isoformat() if hasattr(result["run_date"], "isoformat") else str(result["run_date"])
        action_lists = {
            "opened": _normalize_symbol_list(result.get("opened")),
            "closed": _normalize_symbol_list(result.get("closed")),
            "overrisked": _normalize_symbol_list(result.get("overrisked")),
            "underrisked": _normalize_symbol_list(result.get("underrisked")),
            "capped_sells": _normalize_symbol_list(result.get("capped_sells")),
            "defensive_buys": _normalize_symbol_list(result.get("defensive_buys")),
        }
        settings.update(
            {
                "defensive_mode": result.get("defensive_mode", "cash"),
                "defensive_symbol": result.get("defensive_symbol", ""),
                "raw_rank_consideration_limit": result.get("raw_rank_consideration_limit"),
                "max_position_fraction": result.get("max_position_fraction"),
                "is_risk_rebalance_day": bool(result.get("is_risk_rebalance_day")),
            }
        )
        summary.update(
            {
                "market_health": bool(result.get("market_health")),
                "approved_count": int(result.get("approved_count", 0)),
                "opened_count": len(action_lists["opened"]),
                "closed_count": len(action_lists["closed"]),
                "overrisked_count": len(action_lists["overrisked"]),
                "underrisked_count": len(action_lists["underrisked"]),
                "capped_sells_count": len(action_lists["capped_sells"]),
                "defensive_buy_count": len(action_lists["defensive_buys"]),
                "defensive_mode": result.get("defensive_mode", "cash"),
                "defensive_symbol": result.get("defensive_symbol", ""),
                "raw_rank_consideration_limit": result.get("raw_rank_consideration_limit"),
                "max_position_fraction": result.get("max_position_fraction"),
            }
        )

    run_id = f"live_{generated_at_iso.replace(':', '').replace('-', '')}"
    portfolio_total = float(final_account["portfolio_value"]) if float(final_account["portfolio_value"]) else 0.0
    enriched_positions = []
    for position in final_positions:
        market_value = float(position.get("market_value", 0.0))
        enriched_positions.append(
            {
                **position,
                "weight_percent": (market_value / portfolio_total) * 100.0 if portfolio_total else 0.0,
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "live_run",
        "id": run_id,
        "generated_at": generated_at_iso,
        "run_date": run_date,
        "status": status,
        "environment": environment,
        "trigger_source": trigger_source,
        "settings": settings,
        "summary": summary,
        "initial_account": initial_account,
        "final_account": final_account,
        "final_positions": enriched_positions,
        "actions": action_lists,
        "action_details": result.get("action_details", []) if result is not None else [],
        "portfolio_history": portfolio_history or {},
        "recent_orders": recent_orders or [],
        "error_detail": error_detail,
    }


def publish_live_run(
    live_run_record,
    *,
    site_data_root=DEFAULT_SITE_DATA_ROOT,
    max_runs=DEFAULT_LIVE_HISTORY_LIMIT,
):
    site_data_root = Path(site_data_root)
    run_id = live_run_record["id"]
    published_paths = []

    detail_path = site_data_root / "live" / "runs" / f"{run_id}.json"
    latest_path = site_data_root / "live" / "latest.json"
    history_path = site_data_root / "live" / "history.json"

    _write_json(detail_path, live_run_record)
    _write_json(latest_path, live_run_record)
    _append_if_present(published_paths, detail_path)
    _append_if_present(published_paths, latest_path)

    history_entry = {
        "id": live_run_record["id"],
        "generated_at": live_run_record["generated_at"],
        "run_date": live_run_record["run_date"],
        "status": live_run_record["status"],
        "environment": live_run_record["environment"],
        "trigger_source": live_run_record["trigger_source"],
        "summary": live_run_record["summary"],
        "detail_path": _site_url(site_data_root, detail_path),
        "error_detail": live_run_record["error_detail"],
        "final_account": live_run_record["final_account"],
    }

    current_history = _read_json(history_path, {"schema_version": SCHEMA_VERSION, "updated_at": None, "runs": []})
    runs = [run for run in current_history.get("runs", []) if run.get("id") != run_id]
    runs.insert(0, history_entry)
    runs = _sort_runs_desc(runs)[:max_runs]

    history_payload = {
        "schema_version": SCHEMA_VERSION,
        "updated_at": live_run_record["generated_at"],
        "latest_run_id": run_id,
        "runs": runs,
    }
    _write_json(history_path, history_payload)
    _append_if_present(published_paths, history_path)

    return {
        "history": history_payload,
        "paths": published_paths,
    }


def publish_error_event(
    *,
    generated_at,
    source,
    title,
    message,
    site_data_root=DEFAULT_SITE_DATA_ROOT,
    severity="error",
    category="runtime_error",
    run_id=None,
    context=None,
    max_errors=DEFAULT_ERROR_HISTORY_LIMIT,
):
    site_data_root = Path(site_data_root)
    published_paths = []
    generated_at_iso = generated_at.isoformat() if isinstance(generated_at, datetime) else str(generated_at)
    history_path = site_data_root / "errors" / "history.json"
    event_id = f"{source}_{generated_at_iso.replace(':', '').replace('-', '')}"

    error_event = {
        "id": event_id,
        "generated_at": generated_at_iso,
        "source": source,
        "severity": severity,
        "category": category,
        "title": title,
        "message": message,
        "run_id": run_id,
        "context": context or {},
    }

    current_history = _read_json(history_path, {"schema_version": SCHEMA_VERSION, "updated_at": None, "errors": []})
    errors = [error for error in current_history.get("errors", []) if error.get("id") != event_id]
    errors.insert(0, error_event)
    errors = _sort_runs_desc(errors)[:max_errors]

    history_payload = {
        "schema_version": SCHEMA_VERSION,
        "updated_at": generated_at_iso,
        "latest_error_id": event_id,
        "errors": errors,
    }
    _write_json(history_path, history_payload)
    _append_if_present(published_paths, history_path)

    return {
        "history": history_payload,
        "event": error_event,
        "paths": published_paths,
    }
