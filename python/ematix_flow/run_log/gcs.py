"""GcsRunLog — Google Cloud Storage backend.

Symmetrical to S3RunLog / AzureBlobRunLog: one JSON object per
(table, pipeline) under a configurable prefix. Use when your stack
lives in GCP and you want a serverless durable home for orchestrator
state.

Storage layout under `gs://{bucket}/{prefix}`:

    run_log/{name}.json          {"last_run_at": "...", "success": true}
    attempt_state/{name}.json    {"attempt_count": 2, ...}

Optional dep: `google-cloud-storage`.
"""

from __future__ import annotations

import json
from datetime import datetime

from ._iso import iso_utc, parse_iso
from ._no_lease import NoLeaseBlobBackend


class GcsRunLog(NoLeaseBlobBackend):
    """GCS backend.

    Args:
        bucket: GCS bucket name.
        prefix: object-name prefix; trailing slash optional. Default empty.
        project: GCP project ID. Optional; defaults to whatever
            `google.auth.default()` resolves.
        credentials: an explicit google-auth credentials object.
            Default is application-default credentials.
        bucket_client: a pre-built `google.cloud.storage.Bucket` (or
            a duck-typed test double). Bypasses the SDK construction
            entirely — used by tests and by callers who want to inject
            a non-standard client (custom transport, emulator, etc.).
    """

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        project: str | None = None,
        credentials=None,
        bucket_client=None,
    ):
        if bucket_client is None:
            try:
                from google.cloud import storage
            except ImportError as e:
                raise ImportError(
                    "GcsRunLog requires google-cloud-storage. "
                    "Install with `pip install google-cloud-storage`."
                ) from e
            client = storage.Client(project=project, credentials=credentials)
            bucket_client = client.bucket(bucket)
        self._bucket = bucket_client
        # Normalise prefix.
        prefix = prefix.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self._prefix = prefix

    def close(self) -> None:  # google-cloud-storage clients are stateless-ish
        pass

    # ---- key helpers ---------------------------------------------------

    def _run_key(self, name: str) -> str:
        return f"{self._prefix}run_log/{name}.json"

    def _attempt_key(self, name: str) -> str:
        return f"{self._prefix}attempt_state/{name}.json"

    # ---- writes --------------------------------------------------------

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        body = json.dumps({"last_run_at": iso_utc(ts), "success": bool(success)})
        blob = self._bucket.blob(self._run_key(name))
        blob.upload_from_string(body, content_type="application/json")

    def record_attempt(self, name: str, state) -> None:
        body = json.dumps({
            "attempt_count": state.attempt_count,
            "last_attempt_at": iso_utc(state.last_attempt_at),
            "gave_up": bool(state.gave_up),
        })
        blob = self._bucket.blob(self._attempt_key(name))
        blob.upload_from_string(body, content_type="application/json")

    def clear_attempt_state(self, name: str) -> None:
        blob = self._bucket.blob(self._attempt_key(name))
        try:
            blob.delete()
        except Exception as e:
            # google-cloud-storage raises NotFound for absent blobs.
            # Idempotent delete is the right semantic; swallow that one.
            if type(e).__name__ != "NotFound":
                raise

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
        """Yield (name, body_bytes) for every blob under `prefix`."""
        for blob in self._bucket.list_blobs(prefix=prefix):
            yield blob.name, blob.download_as_bytes()
