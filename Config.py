import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ENV_PATH = Path(".env")
_ENV_LOADED = False


@dataclass(frozen=True)
class AlpacaCredentials:
    environment: str
    key: str
    secret: str
    paper: bool


def load_local_env(env_path=DEFAULT_ENV_PATH):
    global _ENV_LOADED

    if _ENV_LOADED:
        return

    env_path = Path(env_path)
    if not env_path.exists():
        _ENV_LOADED = True
        return

    for line_number, raw_line in enumerate(env_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            raise ValueError(f"Invalid .env line {line_number}: missing '=' separator.")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            raise ValueError(f"Invalid .env line {line_number}: empty variable name.")

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)

    _ENV_LOADED = True


def get_alpaca_credentials():
    load_local_env()

    environment = os.getenv("ALPACA_ENV", "paper").strip().lower()
    if environment not in {"paper", "live"}:
        raise RuntimeError("ALPACA_ENV must be either 'paper' or 'live'.")

    key = os.getenv("ALPACA_KEY") or os.getenv(f"ALPACA_{environment.upper()}_KEY")
    secret = os.getenv("ALPACA_SECRET") or os.getenv(f"ALPACA_{environment.upper()}_SECRET")

    if not key or not secret:
        raise RuntimeError(
            "Missing Alpaca credentials. Set ALPACA_KEY/ALPACA_SECRET or the "
            f"mode-specific pair for ALPACA_ENV={environment!r}."
        )

    return AlpacaCredentials(
        environment=environment,
        key=key,
        secret=secret,
        paper=(environment == "paper"),
    )


def get_error_webhook_url():
    load_local_env()
    value = os.getenv("ERROR_WEBHOOK_URL", "").strip()
    return value or None
