import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo


NEW_YORK_TZ = ZoneInfo("America/New_York")
DEFAULT_LIVE_RUN_RECORDS_DIR = Path("Data/live_run_records")


@dataclass(frozen=True)
class LiveClockInfo:
    market_date: date
    timestamp_iso: str
    is_open: bool


def get_live_clock_info(trading_client):
    clock = trading_client.get_clock()
    timestamp = getattr(clock, "timestamp", None)

    if timestamp is None:
        raise RuntimeError("Alpaca clock did not return a timestamp.")

    if timestamp.tzinfo is None:
        market_timestamp = timestamp.replace(tzinfo=NEW_YORK_TZ)
    else:
        market_timestamp = timestamp.astimezone(NEW_YORK_TZ)

    return LiveClockInfo(
        market_date=market_timestamp.date(),
        timestamp_iso=market_timestamp.isoformat(),
        is_open=bool(getattr(clock, "is_open", False)),
    )


def ensure_market_is_open(live_clock_info):
    if live_clock_info.is_open:
        return

    raise RuntimeError(
        "Market is closed according to Alpaca clock "
        f"({live_clock_info.timestamp_iso}). Refusing to place live orders."
    )


def begin_live_run_record(
    run_date,
    *,
    live_clock_info,
    records_dir=DEFAULT_LIVE_RUN_RECORDS_DIR,
):
    records_dir = Path(records_dir)
    records_dir.mkdir(parents=True, exist_ok=True)
    record_path = records_dir / f"{run_date.isoformat()}.json"

    payload = {
        "run_date": run_date.isoformat(),
        "status": "started",
        "started_at": live_clock_info.timestamp_iso,
        "last_updated_at": live_clock_info.timestamp_iso,
    }

    try:
        descriptor = os.open(record_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        existing_status = "unknown"
        existing_time = "unknown"

        try:
            existing_payload = json.loads(record_path.read_text(encoding="utf-8"))
            existing_status = existing_payload.get("status", existing_status)
            existing_time = existing_payload.get("last_updated_at", existing_time)
        except (OSError, json.JSONDecodeError):
            pass

        raise RuntimeError(
            f"Live strategy already attempted on {run_date.isoformat()} "
            f"(status={existing_status}, last_updated_at={existing_time}). "
            "Refusing a second live run on the same market day."
        ) from exc

    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)

    return record_path


def finish_live_run_record(record_path, *, status, detail=None, summary=None, updated_at=None):
    record_path = Path(record_path)

    payload = {"status": status}
    if record_path.exists():
        try:
            payload = json.loads(record_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {"status": status}

    payload["status"] = status
    payload["last_updated_at"] = updated_at or payload.get("last_updated_at")

    if detail:
        payload["detail"] = detail

    if summary is not None:
        payload["summary"] = summary

    record_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
