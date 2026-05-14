"""AzureBlobRunLog — Azure Blob Storage backend.

Symmetrical to S3RunLog: one blob per (table, pipeline) under a
configurable prefix. Use this when your stack already lives in
Azure and you want a serverless durable home for orchestrator state.

Storage layout under `{container}/{prefix}`:

    run_log/{name}.json          {"last_run_at": "...", "success": true}
    attempt_state/{name}.json    {"attempt_count": 2, ...}

Optional dep: `azure-storage-blob`.
"""

from __future__ import annotations

import json
from datetime import datetime

from ._iso import iso_utc, parse_iso
from ._no_lease import NoLeaseBlobBackend


class AzureBlobRunLog(NoLeaseBlobBackend):
    """Azure Blob Storage backend.

    Args:
        account_url: e.g. "https://<account>.blob.core.windows.net".
            Can be omitted if `container_client` is provided.
        container: the blob container holding orchestrator state.
        prefix: key prefix; trailing slash optional. Default empty.
        credential: passed to BlobServiceClient (DefaultAzureCredential,
            a SAS, an account key, etc.). Ignored when `container_client`
            is provided.
        container_client: a pre-built ContainerClient. Bypasses the
            account_url + credential plumbing — useful for tests and
            for callers using non-default endpoints (Azurite emulator,
            Azure Stack, etc.).
    """

    def __init__(
        self,
        container: str,
        *,
        account_url: str | None = None,
        prefix: str = "",
        credential=None,
        container_client=None,
    ):
        if container_client is None:
            try:
                from azure.storage.blob import BlobServiceClient
            except ImportError as e:
                raise ImportError(
                    "AzureBlobRunLog requires azure-storage-blob. "
                    "Install with `pip install azure-storage-blob`."
                ) from e
            if account_url is None:
                raise ValueError(
                    "AzureBlobRunLog needs either container_client= or "
                    "account_url=. Pass the blob endpoint, e.g. "
                    "'https://<account>.blob.core.windows.net'."
                )
            svc = BlobServiceClient(account_url=account_url, credential=credential)
            container_client = svc.get_container_client(container)
        self._cc = container_client
        # Normalise prefix.
        prefix = prefix.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self._prefix = prefix

    def close(self) -> None:
        try:
            self._cc.close()
        except Exception:
            # Best-effort close; the SDK occasionally raises on
            # already-closed clients. Not worth surfacing.
            pass

    # ---- key helpers ---------------------------------------------------

    def _run_key(self, name: str) -> str:
        return f"{self._prefix}run_log/{name}.json"

    def _attempt_key(self, name: str) -> str:
        return f"{self._prefix}attempt_state/{name}.json"

    # ---- writes --------------------------------------------------------

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        body = json.dumps({"last_run_at": iso_utc(ts), "success": bool(success)})
        blob = self._cc.get_blob_client(self._run_key(name))
        blob.upload_blob(body.encode("utf-8"), overwrite=True)

    def record_attempt(self, name: str, state) -> None:
        body = json.dumps({
            "attempt_count": state.attempt_count,
            "last_attempt_at": iso_utc(state.last_attempt_at),
            "gave_up": bool(state.gave_up),
        })
        blob = self._cc.get_blob_client(self._attempt_key(name))
        blob.upload_blob(body.encode("utf-8"), overwrite=True)

    def clear_attempt_state(self, name: str) -> None:
        blob = self._cc.get_blob_client(self._attempt_key(name))
        try:
            blob.delete_blob()
        except Exception as e:
            # The SDK raises ResourceNotFoundError when the blob is
            # already absent. Idempotent delete is the right semantic
            # here, so swallow that one.
            if type(e).__name__ != "ResourceNotFoundError":
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
        for blob in self._cc.list_blobs(name_starts_with=prefix):
            data = self._cc.download_blob(blob.name).readall()
            yield blob.name, data
