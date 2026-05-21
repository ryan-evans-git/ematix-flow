"""Tests for the pluggable secrets resolver (`ematix_flow.secrets`).

Covers the surface that backs `${...}` interpolation:

- `EnvResolver` — the default; bare `${VAR}` continues to work as
  before so existing connection definitions don't break.
- `VaultResolver`, `AWSSecretsManagerResolver`, `GCPSecretManagerResolver`
  — opt-in resolvers selected by a `provider:` prefix inside the
  `${...}` reference. Their underlying SDK clients are mocked here;
  real-credential integration tests live in
  `tests/python/integration/` and are skipped when creds aren't set.
- `register_resolver` / `unregister_resolver` — the public registry
  API users invoke to plug in alternate secret stores.
- `expand` — the entry point both `config._interpolate` and
  `connections.resolve` delegate to.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from ematix_flow.secrets import (
    AWSSecretsManagerResolver,
    EnvResolver,
    GCPSecretManagerResolver,
    MissingSecretError,
    SecretResolver,
    VaultResolver,
    expand,
    register_resolver,
    unregister_resolver,
)


class TestExpand:
    """The user-facing entry point. Backwards-compatible with the old
    env-only behavior — bare `${VAR}` continues to work."""

    def test_passes_plain_string_through(self):
        assert expand("plain-string") == "plain-string"

    def test_passes_none_through(self):
        assert expand(None) is None

    def test_empty_string_is_unchanged(self):
        assert expand("") == ""

    def test_bare_var_resolves_via_env(self, monkeypatch):
        monkeypatch.setenv("MY_HOST", "broker.example.com")
        assert expand("${MY_HOST}:9092") == "broker.example.com:9092"

    def test_multiple_bare_vars_in_one_string(self, monkeypatch):
        monkeypatch.setenv("HOST", "h.example")
        monkeypatch.setenv("PORT", "1234")
        assert expand("${HOST}:${PORT}/db") == "h.example:1234/db"

    def test_missing_bare_var_raises_missing_secret(self, monkeypatch):
        monkeypatch.delenv("DEFINITELY_NOT_SET", raising=False)
        with pytest.raises(MissingSecretError, match="DEFINITELY_NOT_SET"):
            expand("${DEFINITELY_NOT_SET}")

    def test_unknown_provider_prefix_raises(self):
        with pytest.raises(MissingSecretError, match="no resolver"):
            expand("${nope:foo/bar}")

    def test_provider_prefix_dispatches_to_registered_resolver(self):
        class _Fake(SecretResolver):
            def resolve(self, reference: str) -> str:
                return f"<fake:{reference}>"

        register_resolver("fake", _Fake())
        try:
            assert expand("${fake:path/x}") == "<fake:path/x>"
        finally:
            unregister_resolver("fake")

    def test_provider_with_hash_subkey_passes_through_to_resolver(self):
        captured: dict[str, str] = {}

        class _Fake(SecretResolver):
            def resolve(self, reference: str) -> str:
                captured["ref"] = reference
                return "result"

        register_resolver("fake", _Fake())
        try:
            expand("${fake:secret-name#field}")
            assert captured["ref"] == "secret-name#field"
        finally:
            unregister_resolver("fake")

    def test_mixed_bare_and_provider_refs_in_one_string(self, monkeypatch):
        monkeypatch.setenv("USER", "alice")

        class _Fake(SecretResolver):
            def resolve(self, reference: str) -> str:
                return "shhh"

        register_resolver("fake", _Fake())
        try:
            assert expand("postgres://${USER}:${fake:pw}@host/db") == \
                "postgres://alice:shhh@host/db"
        finally:
            unregister_resolver("fake")


class TestEnvResolver:
    def test_resolves_set_var(self, monkeypatch):
        monkeypatch.setenv("EM_TEST_VAR", "hi")
        assert EnvResolver().resolve("EM_TEST_VAR") == "hi"

    def test_missing_var_raises_missing_secret(self, monkeypatch):
        monkeypatch.delenv("EM_NO_SUCH_VAR", raising=False)
        with pytest.raises(MissingSecretError):
            EnvResolver().resolve("EM_NO_SUCH_VAR")


class TestVaultResolver:
    """VaultResolver wraps hvac. We mock the client surface."""

    def _make_resolver_with_mock(self, secrets_data: dict[str, Any]) -> VaultResolver:
        mock_client = MagicMock()
        # hvac KV v2 read returns {"data": {"data": {...}, "metadata": ...}}
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": secrets_data, "metadata": {"version": 1}}
        }
        return VaultResolver(client=mock_client, mount_point="secret")

    def test_path_only_returns_full_dict_serialised(self):
        r = self._make_resolver_with_mock({"user": "alice", "password": "hunter2"})
        # Without `#key`, we serialise the dict as JSON. Predictable contract.
        out = r.resolve("app/db-creds")
        assert json.loads(out) == {"user": "alice", "password": "hunter2"}

    def test_path_with_hash_key_returns_value(self):
        r = self._make_resolver_with_mock({"user": "alice", "password": "hunter2"})
        assert r.resolve("app/db-creds#password") == "hunter2"

    def test_missing_key_raises(self):
        r = self._make_resolver_with_mock({"user": "alice"})
        with pytest.raises(MissingSecretError, match="password"):
            r.resolve("app/db-creds#password")

    def test_calls_client_with_correct_path_and_mount(self):
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"k": "v"}}
        }
        r = VaultResolver(client=mock_client, mount_point="kv2")
        r.resolve("app/db-creds#k")
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="app/db-creds", mount_point="kv2"
        )


class TestAWSSecretsManagerResolver:
    """AWSSecretsManagerResolver wraps boto3's secretsmanager client."""

    def _make_resolver_with_mock(self, secret_string: str) -> AWSSecretsManagerResolver:
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": secret_string}
        return AWSSecretsManagerResolver(client=mock_client)

    def test_plain_string_secret(self):
        r = self._make_resolver_with_mock("hunter2")
        assert r.resolve("prod/db/password") == "hunter2"

    def test_json_secret_with_hash_key(self):
        r = self._make_resolver_with_mock(json.dumps({"user": "alice", "password": "hunter2"}))
        assert r.resolve("prod/db#password") == "hunter2"

    def test_json_secret_without_hash_returns_raw_string(self):
        # Without #key, we don't try to parse. Callers can use the full
        # JSON downstream if they want.
        payload = json.dumps({"user": "alice"})
        r = self._make_resolver_with_mock(payload)
        assert r.resolve("prod/db") == payload

    def test_missing_json_key_raises(self):
        r = self._make_resolver_with_mock(json.dumps({"user": "alice"}))
        with pytest.raises(MissingSecretError, match="password"):
            r.resolve("prod/db#password")

    def test_calls_client_with_correct_secret_id(self):
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": "x"}
        AWSSecretsManagerResolver(client=mock_client).resolve("prod/foo")
        mock_client.get_secret_value.assert_called_once_with(SecretId="prod/foo")


class TestGCPSecretManagerResolver:
    """GCPSecretManagerResolver wraps google-cloud-secret-manager."""

    def _make_resolver_with_mock(self, payload: bytes) -> GCPSecretManagerResolver:
        mock_client = MagicMock()
        response = MagicMock()
        response.payload.data = payload
        mock_client.access_secret_version.return_value = response
        return GCPSecretManagerResolver(client=mock_client, project="my-proj")

    def test_full_resource_path(self):
        r = self._make_resolver_with_mock(b"hunter2")
        # A reference that already includes projects/.../versions/X is passed through.
        assert r.resolve("projects/my-proj/secrets/db-pw/versions/latest") == "hunter2"

    def test_short_form_expanded_against_project(self):
        r = self._make_resolver_with_mock(b"hunter2")
        # Just the secret name → expanded to projects/<project>/secrets/<name>/versions/latest.
        assert r.resolve("db-pw") == "hunter2"

    def test_short_form_with_version(self):
        r = self._make_resolver_with_mock(b"hunter2")
        assert r.resolve("db-pw#3") == "hunter2"

    def test_calls_client_with_correct_resource_name(self):
        mock_client = MagicMock()
        response = MagicMock()
        response.payload.data = b"x"
        mock_client.access_secret_version.return_value = response
        GCPSecretManagerResolver(client=mock_client, project="proj-1").resolve("foo")
        mock_client.access_secret_version.assert_called_once_with(
            name="projects/proj-1/secrets/foo/versions/latest"
        )

    def test_short_form_requires_project(self):
        # If the resolver is constructed without a project and the
        # reference is short-form, we can't form the resource name.
        mock_client = MagicMock()
        r = GCPSecretManagerResolver(client=mock_client, project=None)
        with pytest.raises(MissingSecretError, match="project"):
            r.resolve("foo")


class TestRegistry:
    def test_register_then_unregister(self):
        class _R(SecretResolver):
            def resolve(self, reference: str) -> str:
                return "x"

        register_resolver("ephemeral", _R())
        assert expand("${ephemeral:anything}") == "x"
        unregister_resolver("ephemeral")
        with pytest.raises(MissingSecretError):
            expand("${ephemeral:anything}")

    def test_register_overwrites(self):
        class _A(SecretResolver):
            def resolve(self, reference: str) -> str:
                return "A"

        class _B(SecretResolver):
            def resolve(self, reference: str) -> str:
                return "B"

        register_resolver("dup", _A())
        register_resolver("dup", _B())
        try:
            assert expand("${dup:ref}") == "B"
        finally:
            unregister_resolver("dup")

    def test_unregister_unknown_is_noop(self):
        # Should not raise.
        unregister_resolver("never-registered")

    def test_env_resolver_cannot_be_unregistered(self):
        """`env` is the default; unregistering would break the
        backwards-compat bare-`${VAR}` syntax."""
        # Try to unregister env; bare vars must still resolve.
        unregister_resolver("env")
        import os
        os.environ["EM_STILL_WORKS"] = "yes"
        try:
            assert expand("${EM_STILL_WORKS}") == "yes"
        finally:
            del os.environ["EM_STILL_WORKS"]
