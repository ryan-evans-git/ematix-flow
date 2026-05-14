"""S3RunLog — AWS S3 backend.

Use this for cross-host coordination when you have S3 already in your
stack (or any S3-compatible store: MinIO, R2, Wasabi, ceph). It's
not as fast as Postgres for high-frequency writes (each record is a
PUT round-trip), but for hourly/daily orchestrator workloads it's
fine and removes a moving piece (no DB to provision).

Storage layout under `s3://{bucket}/{prefix}`:

    run_log/{name}.json          {"last_run_at": "...", "success": true}
    attempt_state/{name}.json    {"attempt_count": 2, ...}

One object per (table, pipeline). `restore_into_process` is a LIST
followed by parallel GETs (sequential GETs are simpler and adequate
for typical pipeline counts; can be made async later).

Optional dep: `boto3`.
"""

from __future__ import annotations

import json
from datetime import datetime

from ._iso import iso_utc, parse_iso


class S3RunLog:
    """AWS S3 (and S3-compatible) backend.

    Args:
        bucket: S3 bucket name.
        prefix: key prefix; trailing slash is optional. Default empty.
        client: pre-built boto3 S3 client. If None, a default one is
            constructed via `boto3.client("s3")`. Pass a custom client
            for non-AWS S3-compatible stores (MinIO, R2, etc.) or to
            inject credentials/endpoint/region explicitly.
    """

    def __init__(self, bucket: str, *, prefix: str = "", client=None):
        if client is None:
            try:
                import boto3
            except ImportError as e:
                raise ImportError(
                    "S3RunLog requires boto3. Install with `pip install boto3`."
                ) from e
            client = boto3.client("s3")
        self._s3 = client
        self._bucket = bucket
        # Normalise: strip leading slash, ensure trailing slash for joins.
        prefix = prefix.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self._prefix = prefix

    def close(self) -> None:  # boto3 clients are auto-closed by the SDK
        pass

    # ---- key helpers ---------------------------------------------------

    def _run_key(self, name: str) -> str:
        return f"{self._prefix}run_log/{name}.json"

    def _attempt_key(self, name: str) -> str:
        return f"{self._prefix}attempt_state/{name}.json"

    # ---- writes --------------------------------------------------------

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        body = json.dumps({"last_run_at": iso_utc(ts), "success": bool(success)})
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._run_key(name),
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

    def record_attempt(self, name: str, state) -> None:
        body = json.dumps({
            "attempt_count": state.attempt_count,
            "last_attempt_at": iso_utc(state.last_attempt_at),
            "gave_up": bool(state.gave_up),
        })
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._attempt_key(name),
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

    def clear_attempt_state(self, name: str) -> None:
        # DeleteObject is idempotent — succeeds even if the key is absent.
        self._s3.delete_object(Bucket=self._bucket, Key=self._attempt_key(name))

    # ---- restore -------------------------------------------------------

    def restore_into_process(self) -> None:
        from ematix_flow import pipeline as _p

        for key, payload in self._list_under(f"{self._prefix}run_log/"):
            name = key.rsplit("/", 1)[-1].removesuffix(".json")
            d = json.loads(payload)
            _p._LAST_RUN[name] = (parse_iso(d["last_run_at"]), bool(d["success"]))

        for key, payload in self._list_under(f"{self._prefix}attempt_state/"):
            name = key.rsplit("/", 1)[-1].removesuffix(".json")
            d = json.loads(payload)
            _p._ATTEMPT_STATE[name] = _p.AttemptState(
                attempt_count=d["attempt_count"],
                last_attempt_at=parse_iso(d["last_attempt_at"]),
                gave_up=bool(d["gave_up"]),
            )

    def _list_under(self, prefix: str):
        """Yield (key, body_bytes) for every object under `prefix`.

        Uses the v2 paginator so very large run-logs don't blow past
        the 1000-key page limit.
        """
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                resp = self._s3.get_object(Bucket=self._bucket, Key=key)
                body = resp["Body"].read()
                yield key, body
