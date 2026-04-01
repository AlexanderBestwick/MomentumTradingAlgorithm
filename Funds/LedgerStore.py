import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from Config import load_local_env


DEFAULT_LEDGER_PREFIX = "funds"
LEDGER_SCHEMA_VERSION = 1


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _coerce_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)

    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class FundLedgerStore:
    def __init__(
        self,
        *,
        fund_id,
        bucket_name=None,
        prefix=DEFAULT_LEDGER_PREFIX,
        root_path=None,
        aws_region=None,
        s3_client=None,
    ):
        self.fund_id = str(fund_id).strip()
        if not self.fund_id:
            raise ValueError("fund_id is required.")

        self.bucket_name = (bucket_name or "").strip() or None
        self.root_path = None if root_path is None else Path(root_path)
        self.prefix = str(prefix or DEFAULT_LEDGER_PREFIX).strip().strip("/")
        self.aws_region = (aws_region or "").strip() or None
        self._s3_client = s3_client

        if bool(self.bucket_name) == bool(self.root_path):
            raise ValueError("Specify exactly one of bucket_name or root_path.")

    @classmethod
    def from_env(cls, *, fund_id=None, require=False):
        load_local_env()

        bucket_name = os.getenv("FUND_LEDGER_BUCKET", "").strip() or None
        root_path = os.getenv("FUND_LEDGER_ROOT", "").strip() or None
        prefix = os.getenv("FUND_LEDGER_PREFIX", DEFAULT_LEDGER_PREFIX)
        aws_region = os.getenv("AWS_REGION", "").strip() or None
        resolved_fund_id = (
            fund_id
            or os.getenv("FUND_LEDGER_FUND_ID", "").strip()
            or os.getenv("FUND_ID", "").strip()
            or None
        )

        if not bucket_name and not root_path:
            if require:
                raise RuntimeError(
                    "Missing fund ledger location. Set FUND_LEDGER_BUCKET or FUND_LEDGER_ROOT."
                )
            return None

        if not resolved_fund_id:
            raise RuntimeError(
                "Missing fund identifier. Set FUND_LEDGER_FUND_ID/FUND_ID or pass fund_id explicitly."
            )

        return cls(
            fund_id=resolved_fund_id,
            bucket_name=bucket_name,
            prefix=prefix,
            root_path=root_path,
            aws_region=aws_region,
        )

    @property
    def base_prefix(self):
        return "/".join(part for part in [self.prefix, self.fund_id] if part)

    def _relative_path(self, relative_key):
        normalized = str(relative_key).strip().strip("/")
        if not normalized:
            raise ValueError("relative_key must be non-empty.")
        return normalized

    def _full_s3_key(self, relative_key):
        relative_key = self._relative_path(relative_key)
        return "/".join(part for part in [self.base_prefix, relative_key] if part)

    def _full_local_path(self, relative_key):
        relative_key = self._relative_path(relative_key)
        return self.root_path / self.base_prefix / relative_key

    def _get_s3_client(self):
        if self._s3_client is not None:
            return self._s3_client

        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError(
                "S3-backed fund ledgers require boto3. Install it before using FUND_LEDGER_BUCKET."
            ) from exc

        self._s3_client = boto3.client("s3", region_name=self.aws_region or None)
        return self._s3_client

    def write_json(self, relative_key, payload):
        payload_text = json.dumps(payload, indent=2, sort_keys=True, default=_json_default)

        if self.bucket_name:
            self._get_s3_client().put_object(
                Bucket=self.bucket_name,
                Key=self._full_s3_key(relative_key),
                Body=payload_text.encode("utf-8"),
                ContentType="application/json",
            )
            return

        target_path = self._full_local_path(relative_key)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(payload_text, encoding="utf-8")

    def read_json(self, relative_key, default=None):
        if self.bucket_name:
            client = self._get_s3_client()
            try:
                response = client.get_object(Bucket=self.bucket_name, Key=self._full_s3_key(relative_key))
            except client.exceptions.NoSuchKey:
                return default
            except Exception as exc:
                error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
                if error_code in {"NoSuchKey", "404", "NotFound"}:
                    return default
                raise
            return json.loads(response["Body"].read().decode("utf-8"))

        path = self._full_local_path(relative_key)
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def list_json(self, relative_prefix):
        relative_prefix = str(relative_prefix or "").strip().strip("/")
        items = []

        if self.bucket_name:
            client = self._get_s3_client()
            prefix = self._full_s3_key(relative_prefix) if relative_prefix else self.base_prefix
            if prefix and not prefix.endswith("/"):
                prefix = f"{prefix}/"
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for entry in page.get("Contents", []):
                    key = entry["Key"]
                    if not key.endswith(".json"):
                        continue
                    response = client.get_object(Bucket=self.bucket_name, Key=key)
                    items.append(json.loads(response["Body"].read().decode("utf-8")))
            return items

        base_path = self._full_local_path(relative_prefix) if relative_prefix else (self.root_path / self.base_prefix)
        if not base_path.exists():
            return items

        for path in sorted(base_path.rglob("*.json")):
            items.append(json.loads(path.read_text(encoding="utf-8")))
        return items

    def save_investor(self, investor_record):
        investor_id = str(investor_record["investor_id"]).strip()
        self.write_json(f"investors/{investor_id}.json", investor_record)

    def load_investor(self, investor_id, default=None):
        return self.read_json(f"investors/{str(investor_id).strip()}.json", default=default)

    def list_investors(self):
        investors = self.list_json("investors")
        return sorted(investors, key=lambda record: str(record.get("investor_id", "")))

    def save_request(self, request_record):
        request_id = str(request_record["request_id"]).strip()
        self.write_json(f"requests/{request_id}.json", request_record)

    def load_request(self, request_id, default=None):
        return self.read_json(f"requests/{str(request_id).strip()}.json", default=default)

    def list_requests(self):
        records = self.list_json("requests")
        return sorted(
            records,
            key=lambda record: (
                record.get("effective_at") or "",
                record.get("requested_at") or "",
                record.get("request_id") or "",
            ),
        )

    def save_execution(self, effective_at, request_id, execution_record):
        effective_at = _coerce_datetime(effective_at) or datetime.now(timezone.utc)
        request_id = str(request_id).strip()
        relative_key = (
            f"executions/{effective_at:%Y-%m-%d}/{request_id}.json"
        )
        self.write_json(relative_key, execution_record)

    def list_executions(self):
        records = self.list_json("executions")
        return sorted(records, key=lambda record: (record.get("executed_at") or "", record.get("request_id") or ""))

    def save_latest(self, name, payload):
        self.write_json(f"latest/{str(name).strip()}.json", payload)

    def load_latest(self, name, default=None):
        return self.read_json(f"latest/{str(name).strip()}.json", default=default)
