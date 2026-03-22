import os
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from Config import get_alpaca_credentials
from FullRun import RunAll


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


def main():
    credentials = get_alpaca_credentials()
    defensive_mode = os.getenv("DEFENSIVE_MODE", "cash").strip().lower()
    defensive_symbol = os.getenv("DEFENSIVE_SYMBOL", "SGOV").strip().upper()
    raw_rank_limit = _get_int_env("RAW_RANK_CONSIDERATION_LIMIT", 80)
    max_position_fraction = _get_float_env("MAX_POSITION_FRACTION", 0.10)
    save_outputs = _get_bool_env("SAVE_OUTPUTS", True)
    enforce_live_safeguards = _get_bool_env("ENFORCE_LIVE_SAFEGUARDS", True)

    print(
        "Starting momentum worker with "
        f"ALPACA_ENV={credentials.environment}, "
        f"defensive_mode={defensive_mode}, "
        f"raw_rank_limit={raw_rank_limit}, "
        f"max_position_fraction={max_position_fraction:.2%}"
    )

    result = RunAll(
        save_outputs=save_outputs,
        defensive_mode=defensive_mode,
        defensive_symbol=defensive_symbol,
        raw_rank_consideration_limit=raw_rank_limit,
        max_position_fraction=max_position_fraction,
        enforce_live_safeguards=enforce_live_safeguards,
    )

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
