import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"]
ERRORS_KEY = os.environ.get("ERRORS_KEY", "errors/history.json")
MAX_ERRORS = int(os.environ.get("MAX_ERRORS", "40"))


def _read_history():
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=ERRORS_KEY)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code not in {"NoSuchKey", "404"}:
            raise
        return {
            "schema_version": 1,
            "updated_at": None,
            "latest_error_id": None,
            "errors": [],
        }

    return json.loads(response["Body"].read().decode("utf-8"))


def _is_failure(detail):
    containers = detail.get("containers", [])
    exit_codes = [
        container.get("exitCode")
        for container in containers
        if container.get("exitCode") is not None
    ]
    stopped_reason = detail.get("stoppedReason", "") or ""
    stop_code = detail.get("stopCode")

    return (
        any(exit_code != 0 for exit_code in exit_codes)
        or stop_code == "TaskFailedToStart"
        or "Error" in stopped_reason
        or "CannotPullContainer" in stopped_reason
        or "ResourceInitializationError" in stopped_reason
    )


def lambda_handler(event, context):
    detail = event.get("detail", {})
    if detail.get("lastStatus") != "STOPPED":
        return {"ignored": True, "reason": "Task is not stopped."}

    if not _is_failure(detail):
        return {"ignored": True, "reason": "Task stopped cleanly."}

    history = _read_history()
    generated_at = event.get("time", datetime.now(timezone.utc).isoformat())
    error_event = {
        "id": event.get("id"),
        "generated_at": generated_at,
        "source": "ecs_eventbridge",
        "severity": "error",
        "category": "ecs_task_stopped",
        "title": "ECS task failed",
        "message": detail.get("stoppedReason") or "Task stopped unexpectedly.",
        "run_id": detail.get("taskArn"),
        "context": {
            "clusterArn": detail.get("clusterArn"),
            "taskArn": detail.get("taskArn"),
            "taskDefinitionArn": detail.get("taskDefinitionArn"),
            "stopCode": detail.get("stopCode"),
            "containers": [
                {
                    "name": container.get("name"),
                    "lastStatus": container.get("lastStatus"),
                    "exitCode": container.get("exitCode"),
                    "reason": container.get("reason"),
                }
                for container in detail.get("containers", [])
            ],
        },
    }

    existing_errors = [
        error
        for error in history.get("errors", [])
        if error.get("id") != error_event["id"]
    ]
    existing_errors.insert(0, error_event)

    updated_history = {
        "schema_version": 1,
        "updated_at": generated_at,
        "latest_error_id": error_event["id"],
        "errors": existing_errors[:MAX_ERRORS],
    }

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=ERRORS_KEY,
        Body=json.dumps(updated_history, indent=2).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
    )

    return {"written": True, "error_id": error_event["id"]}
