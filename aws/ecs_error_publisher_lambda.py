import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"]
ERRORS_KEY = os.environ.get("ERRORS_KEY", "errors/history.json")
MAX_ERRORS = int(os.environ.get("MAX_ERRORS", "40"))


def _empty_history():
    return {
        "schema_version": 1,
        "updated_at": None,
        "latest_error_id": None,
        "errors": [],
    }


def _read_history():
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=ERRORS_KEY)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code not in {"NoSuchKey", "404"}:
            raise
        return _empty_history()

    return json.loads(response["Body"].read().decode("utf-8"))


def _is_task_failure(detail):
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


def _is_ecs_task_state_change_failure(event):
    detail = event.get("detail", {})
    return detail.get("lastStatus") == "STOPPED" and _is_task_failure(detail)


def _is_cloudtrail_runtask_failure(event):
    detail = event.get("detail", {})
    return (
        event.get("detail-type") == "AWS API Call via CloudTrail"
        and detail.get("eventSource") == "ecs.amazonaws.com"
        and detail.get("eventName") == "RunTask"
        and bool(detail.get("errorCode"))
    )


def _build_ecs_task_failure_event(event):
    detail = event.get("detail", {})
    generated_at = event.get("time", datetime.now(timezone.utc).isoformat())

    return {
        "id": event.get("id"),
        "generated_at": generated_at,
        "status": "active",
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


def _build_cloudtrail_runtask_failure_event(event):
    detail = event.get("detail", {})
    generated_at = event.get("time", datetime.now(timezone.utc).isoformat())
    request_parameters = detail.get("requestParameters") or {}
    network_configuration = request_parameters.get("networkConfiguration") or {}
    awsvpc_configuration = network_configuration.get("awsvpcConfiguration") or {}
    user_identity = detail.get("userIdentity") or {}

    return {
        "id": event.get("id"),
        "generated_at": generated_at,
        "status": "active",
        "source": "ecs_cloudtrail",
        "severity": "error",
        "category": "ecs_runtask_failed",
        "title": "ECS RunTask failed",
        "message": detail.get("errorMessage") or "ECS RunTask request failed.",
        "run_id": event.get("id"),
        "context": {
            "eventSource": detail.get("eventSource"),
            "eventName": detail.get("eventName"),
            "errorCode": detail.get("errorCode"),
            "errorMessage": detail.get("errorMessage"),
            "cluster": request_parameters.get("cluster"),
            "taskDefinition": request_parameters.get("taskDefinition"),
            "launchType": request_parameters.get("launchType"),
            "platformVersion": request_parameters.get("platformVersion"),
            "subnets": awsvpc_configuration.get("subnets", []),
            "securityGroups": awsvpc_configuration.get("securityGroups", []),
            "assignPublicIp": awsvpc_configuration.get("assignPublicIp"),
            "userIdentity": {
                "type": user_identity.get("type"),
                "arn": user_identity.get("arn"),
                "principalId": user_identity.get("principalId"),
                "accountId": user_identity.get("accountId"),
            },
        },
    }


def _select_error_event(event):
    if _is_ecs_task_state_change_failure(event):
        return _build_ecs_task_failure_event(event)

    if _is_cloudtrail_runtask_failure(event):
        return _build_cloudtrail_runtask_failure_event(event)

    return None


def _write_error_event(error_event):
    history = _read_history()
    generated_at = error_event["generated_at"]

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


def lambda_handler(event, context):
    error_event = _select_error_event(event)
    if error_event is None:
        return {"ignored": True, "reason": "Event did not match a supported ECS failure shape."}

    return _write_error_event(error_event)
