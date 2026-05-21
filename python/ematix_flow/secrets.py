"""Pluggable secret resolution for ``${...}`` interpolation.

ematix-flow connections, DSNs, and configuration strings support
``${...}`` references. Before this module, the only supported
reference was ``${VAR_NAME}`` resolved against ``os.environ``. With
the resolver registry below, references can also target external
secret stores:

::

    ${VAR}                              # env (backwards-compatible)
    ${vault:secret/db/creds#password}   # HashiCorp Vault KV v2
    ${aws:prod/db#password}             # AWS Secrets Manager
    ${gcp:db-password}                  # GCP Secret Manager

The provider prefix (``vault:``, ``aws:``, ``gcp:``, or any string
registered via :func:`register_resolver`) selects which resolver
handles the reference; everything after the colon is passed verbatim
to that resolver's :meth:`SecretResolver.resolve` method.

Resolvers are deliberately constructed by the user (or via
``register_resolver``) so the per-resolver credentials, regions, and
endpoints don't bleed into ematix-flow's own config surface. A typical
application bootstrap looks like:

.. code-block:: python

    import boto3
    from ematix_flow.secrets import (
        AWSSecretsManagerResolver, register_resolver,
    )

    register_resolver("aws", AWSSecretsManagerResolver(
        client=boto3.client("secretsmanager", region_name="us-east-1"),
    ))

After that, ``${aws:...}`` references in any registered connection
will resolve against that AWS account / region.

This module's regex (``\\$\\{([^}]+)\\}``) is intentionally permissive
about what goes between the braces — provider prefixes need ``:`` and
secret-store backends often use ``/``, ``-``, and ``#`` separators.
The previous restrictive regex (uppercase env-var names only) is
preserved on the env path: :class:`EnvResolver` validates that bare
references match ``[A-Z_][A-Z0-9_]*``.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "AWSSecretsManagerResolver",
    "EnvResolver",
    "GCPSecretManagerResolver",
    "MissingSecretError",
    "SecretResolver",
    "VaultResolver",
    "expand",
    "register_resolver",
    "unregister_resolver",
]


_INTERP_RE = re.compile(r"\$\{([^}]+)\}")
_ENV_VAR_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


class MissingSecretError(KeyError):
    """A referenced secret could not be resolved.

    Subclass of ``KeyError`` so callers that already handle ``KeyError``
    (e.g. existing code paths that used ``os.environ[VAR]``) continue
    to work. New code should catch :class:`MissingSecretError`
    explicitly for clearer error messages.
    """


@runtime_checkable
class SecretResolver(Protocol):
    """Resolve a reference string to a secret value.

    Implementations are free to interpret the reference however suits
    the underlying store. Convention: an optional ``#subkey`` suffix
    selects a field inside a structured secret (e.g. a JSON blob in
    AWS Secrets Manager, a KV map in Vault). Without ``#``, the
    resolver returns the raw secret value.

    Should raise :class:`MissingSecretError` (or a subclass) when the
    reference cannot be resolved.
    """

    def resolve(self, reference: str) -> str: ...


# ----- Concrete resolvers ----------------------------------------


class EnvResolver:
    """Resolves bare ``${VAR}`` references against ``os.environ``.

    Validates that the reference is a conventional env-var name
    (``[A-Z_][A-Z0-9_]*``); other shapes raise so users get a clear
    error instead of a confusing ``KeyError`` from the wrong store.
    """

    def resolve(self, reference: str) -> str:
        if not _ENV_VAR_RE.match(reference):
            raise MissingSecretError(
                f"env reference {reference!r} is not a valid env-var name "
                "(expected pattern [A-Z_][A-Z0-9_]*); did you mean to use a "
                "provider prefix like 'vault:' or 'aws:'?"
            )
        try:
            return os.environ[reference]
        except KeyError as exc:
            raise MissingSecretError(
                f"environment variable {reference!r} is referenced by an "
                "ematix-flow config or connection but is not set"
            ) from exc


class VaultResolver:
    """HashiCorp Vault KV v2 resolver.

    Reference syntax: ``path/to/secret`` (returns the full key-map
    serialised as JSON) or ``path/to/secret#key`` (returns just the
    named field). Path is relative to ``mount_point`` (default
    ``secret``).

    The ``client`` argument is an ``hvac.Client`` (or compatible
    mock). Construct it however suits your environment — token auth,
    AppRole, K8s service-account; ematix-flow doesn't impose a
    policy.
    """

    def __init__(self, client: Any, mount_point: str = "secret") -> None:
        self._client = client
        self._mount_point = mount_point

    def resolve(self, reference: str) -> str:
        path, _, subkey = reference.partition("#")
        # KV v2 returns {"data": {"data": {...}, "metadata": ...}}.
        response = self._client.secrets.kv.v2.read_secret_version(
            path=path, mount_point=self._mount_point
        )
        data = response["data"]["data"]
        if not subkey:
            return json.dumps(data, sort_keys=True)
        if subkey not in data:
            raise MissingSecretError(
                f"Vault secret at {path!r} has no key {subkey!r} "
                f"(available: {sorted(data.keys())})"
            )
        value = data[subkey]
        return value if isinstance(value, str) else json.dumps(value)


class AWSSecretsManagerResolver:
    """AWS Secrets Manager resolver.

    Reference syntax: ``secret-name`` (returns the raw ``SecretString``)
    or ``secret-name#field`` (parses the SecretString as JSON and
    returns the named field).

    The ``client`` argument is a boto3 secretsmanager client. Construct
    it with whatever region / credentials / IAM role apply — ematix-flow
    doesn't impose a policy.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def resolve(self, reference: str) -> str:
        secret_id, _, subkey = reference.partition("#")
        response = self._client.get_secret_value(SecretId=secret_id)
        secret_string: str = response["SecretString"]
        if not subkey:
            return secret_string
        try:
            payload = json.loads(secret_string)
        except json.JSONDecodeError as exc:
            raise MissingSecretError(
                f"AWS secret {secret_id!r} requested with key {subkey!r} but "
                "the SecretString is not valid JSON"
            ) from exc
        if subkey not in payload:
            raise MissingSecretError(
                f"AWS secret {secret_id!r} JSON has no key {subkey!r} "
                f"(available: {sorted(payload.keys())})"
            )
        value = payload[subkey]
        return value if isinstance(value, str) else json.dumps(value)


class GCPSecretManagerResolver:
    """GCP Secret Manager resolver.

    Reference syntax (any of):

    - ``projects/PROJECT/secrets/NAME/versions/VERSION`` (full resource name)
    - ``NAME`` (short form, expanded to ``projects/<project>/secrets/NAME/versions/latest``)
    - ``NAME#VERSION`` (short form, specific version)

    Short forms require ``project`` to be set on the resolver.

    The ``client`` argument is a
    ``google.cloud.secretmanager.SecretManagerServiceClient`` (or
    compatible). Construct it with whatever credentials apply.
    """

    def __init__(self, client: Any, project: str | None = None) -> None:
        self._client = client
        self._project = project

    def resolve(self, reference: str) -> str:
        if reference.startswith("projects/"):
            name = reference
        else:
            if self._project is None:
                raise MissingSecretError(
                    f"GCP short-form reference {reference!r} requires a "
                    "project to be set on GCPSecretManagerResolver"
                )
            short, _, version = reference.partition("#")
            version = version or "latest"
            name = f"projects/{self._project}/secrets/{short}/versions/{version}"
        response = self._client.access_secret_version(name=name)
        return response.payload.data.decode("utf-8")


# ----- Registry --------------------------------------------------

# The env resolver is registered by default so bare ``${VAR}``
# references continue to work without any user setup.
_RESOLVERS: dict[str, SecretResolver] = {"env": EnvResolver()}


def register_resolver(prefix: str, resolver: SecretResolver) -> None:
    """Register ``resolver`` under ``prefix``.

    After registration, ``${prefix:reference}`` in any ematix-flow
    connection or config string will dispatch to ``resolver.resolve``.

    Registering the same ``prefix`` twice overwrites the prior entry.
    """
    _RESOLVERS[prefix] = resolver


def unregister_resolver(prefix: str) -> None:
    """Remove ``prefix`` from the registry.

    Idempotent — unregistering an unknown prefix is a no-op. The
    built-in ``env`` resolver is special: bare ``${VAR}`` references
    always fall back to a fresh :class:`EnvResolver` even if the
    registered entry is removed, so users can't accidentally break
    backwards compatibility.
    """
    _RESOLVERS.pop(prefix, None)


# ----- Public entry point ----------------------------------------


def expand(value: str | None) -> str | None:
    """Resolve every ``${...}`` reference in ``value``.

    Bare ``${VAR}`` (no ``:``) routes to the env resolver — even if
    ``env`` has been unregistered, a fresh :class:`EnvResolver` is
    used as the fallback. References with a ``provider:`` prefix
    route to the matching registered resolver; an unknown prefix
    raises :class:`MissingSecretError`.

    ``None`` is passed through unchanged so callers can treat
    optional config fields without an extra ``if value is None`` guard.
    """
    if value is None:
        return None

    def _replace(match: re.Match[str]) -> str:
        ref = match.group(1)
        if ":" not in ref:
            # Bare env-var name. Use the registered env resolver if
            # present; otherwise fall back to a fresh one so users
            # who unregister "env" don't accidentally break the
            # backwards-compatible bare-`${VAR}` syntax.
            resolver = _RESOLVERS.get("env") or EnvResolver()
            return resolver.resolve(ref)
        prefix, _, rest = ref.partition(":")
        resolver = _RESOLVERS.get(prefix)
        if resolver is None:
            raise MissingSecretError(
                f"reference {ref!r} uses provider {prefix!r} but no resolver "
                f"is registered for that prefix (call "
                f"`ematix_flow.secrets.register_resolver({prefix!r}, ...)` "
                "during application setup)"
            )
        return resolver.resolve(rest)

    return _INTERP_RE.sub(_replace, value)
