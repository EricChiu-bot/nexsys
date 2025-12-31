from __future__ import annotations

import datetime
from typing import Any


def utc_iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def build_telemetry_v1(
    *,
    ts: str,
    site_uid: str,
    device_uid: str,
    worker_name: str,
    worker_version: str,
    payload: dict[str, Any],
    status: str = "OK",
    schema_version: str = "telemetry.v1",
    extra_envelope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    負責把 metrics/payload 包成統一格式：
    {
      "envelope": {...},
      "payload": {...}
    }
    """
    env = {
        "schema_version": schema_version,
        "ts": ts,
        "site_uid": site_uid,
        "device_uid": device_uid,
        "worker_name": worker_name,
        "worker_version": worker_version,
        "status": status,
    }
    if extra_envelope:
        env.update(extra_envelope)

    return {
        "envelope": env,
        "payload": payload,
    }
