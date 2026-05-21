"""GlueSchemaRegistryConnection — AWS Glue Schema Registry connection
typing surface.

Today's ``kind = "schema_registry"`` covers the Confluent wire
format (Confluent SR, Apicurio's Confluent-compat endpoint). AWS
Glue uses a different framing — ``0x03`` header byte + 16-byte
schema UUID + 1-byte compression byte + payload — and AWS IAM auth
instead of HTTP basic. This connection class is the typed surface
operators bind in their TOML / decorator; the actual wire-format
encode/decode lands in Rust under ``crates/ematix-flow-core/src/``.
"""
from __future__ import annotations

import pytest

from ematix_flow.connections import (
    _KIND_FACTORIES,
    GlueSchemaRegistryConnection,
)


class TestGlueSchemaRegistryConstruction:
    def test_minimal_construction(self):
        conn = GlueSchemaRegistryConnection(
            name="glue_sr",
            registry_name="my-registry",
            region="us-east-1",
        )
        assert conn.name == "glue_sr"
        assert conn.kind == "glue_schema_registry"
        assert conn.registry_name == "my-registry"
        assert conn.region == "us-east-1"

    def test_optional_aws_profile(self):
        conn = GlueSchemaRegistryConnection(
            name="glue_sr",
            registry_name="my-registry",
            region="us-east-1",
            aws_profile="prod",
        )
        assert conn.aws_profile == "prod"

    def test_optional_explicit_creds(self):
        """Explicit access key + secret is supported but discouraged —
        IAM role / SSO / env vars are the recommended path."""
        conn = GlueSchemaRegistryConnection(
            name="glue_sr",
            registry_name="my-registry",
            region="us-east-1",
            aws_access_key_id="AKIA...",
            aws_secret_access_key="secret-value",
        )
        assert conn.aws_access_key_id == "AKIA..."
        assert conn.aws_secret_access_key == "secret-value"

    def test_repr_redacts_secret_key(self):
        conn = GlueSchemaRegistryConnection(
            name="glue_sr",
            registry_name="my-registry",
            region="us-east-1",
            aws_access_key_id="AKIA...",
            aws_secret_access_key="super-secret-value",
        )
        r = repr(conn)
        assert "super-secret-value" not in r
        # Access key id contains "key" — it'll also redact under the
        # token-split rule. Both are credentials; redacting both is the
        # safe default.
        assert "<redacted>" in r


class TestGlueSchemaRegistryValidation:
    def test_rejects_missing_registry_name(self):
        with pytest.raises(ValueError, match="registry_name"):
            GlueSchemaRegistryConnection(
                name="glue_sr",
                registry_name="",
                region="us-east-1",
            )

    def test_rejects_missing_region(self):
        with pytest.raises(ValueError, match="region"):
            GlueSchemaRegistryConnection(
                name="glue_sr",
                registry_name="my-registry",
                region="",
            )

    def test_rejects_partial_explicit_creds(self):
        """If one of access_key_id / secret_access_key is set, the other
        must be too — partial cred config is an error."""
        with pytest.raises(ValueError, match="both"):
            GlueSchemaRegistryConnection(
                name="glue_sr",
                registry_name="my-registry",
                region="us-east-1",
                aws_access_key_id="AKIA...",
                # no secret
            )

    def test_aws_profile_and_explicit_creds_mutually_exclusive(self):
        with pytest.raises(ValueError, match="aws_profile.*aws_access_key_id"):
            GlueSchemaRegistryConnection(
                name="glue_sr",
                registry_name="my-registry",
                region="us-east-1",
                aws_profile="prod",
                aws_access_key_id="AKIA...",
                aws_secret_access_key="x",
            )


class TestGlueRegistryKindRegistration:
    def test_kind_resolves_to_glue_factory(self):
        cls = _KIND_FACTORIES.get("glue_schema_registry")
        assert cls is GlueSchemaRegistryConnection

    def test_kind_distinct_from_confluent_kind(self):
        """The Confluent-compat ``schema_registry`` kind and the Glue
        kind are separate factories — choosing the wrong one for your
        registry would corrupt the wire format on the first message."""
        from ematix_flow.connections import SchemaRegistryConnection

        confluent_cls = _KIND_FACTORIES.get("schema_registry")
        glue_cls = _KIND_FACTORIES.get("glue_schema_registry")
        assert confluent_cls is SchemaRegistryConnection
        assert glue_cls is GlueSchemaRegistryConnection
        assert confluent_cls is not glue_cls
