# Security

## Reporting a vulnerability

**Please don't open a public issue for a vulnerability.** Use one of
these private channels instead:

- **GitHub** — the repository's **Security → Report a vulnerability**
  tab ([private advisory](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)).
- **Email** — **support@ematix.dev**.

Include a description of the issue and how to reproduce it. We aim to
acknowledge within 72 hours and ship a fix or coordinated disclosure
plan within 14 days for high-severity findings.

## Automated scanning in CI

Every push and pull request runs the following gates; the release
workflow re-runs them inline so a tag push can't bypass them on the
way to PyPI:

| Tool | What it scans | Fails CI on |
|---|---|---|
| `cargo fmt --check` | Rust formatting | Any drift |
| `cargo clippy --all-targets -- -D warnings` | Rust lints | Any warning |
| `cargo audit` | Cargo.lock vs RustSec DB | Any non-ignored advisory |
| `cargo test --workspace` | Rust unit + lib tests | Any failure |
| `ruff check` | Python lints (E, F, I, UP, B, SIM, RUF) | Any finding |
| `bandit -r python -ll -c pyproject.toml` | Python security lints (medium+) | Any medium / high finding |
| `pip-audit --skip-editable` | Python deps vs PyPI advisory DB | Any advisory |
| `pytest` | Python tests | Any failure |

The release workflow's `verify` job runs all of the above on
Linux. `build-linux`, `build-macos`, and `build-sdist` jobs each
declare `needs: verify`; the `publish` job declares `needs:
[build-linux, build-macos, build-sdist]`. So no wheel reaches PyPI
without all checks passing.

## Known accepted advisories

Each entry below is suppressed in CI either via `.cargo/audit.toml`
(Rust) or `pyproject.toml` `[tool.bandit]` (Python). Re-evaluate
quarterly or on any upstream upgrade that might unblock a fix.

### Rust — `.cargo/audit.toml`

| Advisory | Crate | Source | Why ignored |
|---|---|---|---|
| RUSTSEC-2026-0104 | `rustls-webpki 0.101.7` | AWS SDK Rust's `legacy-rustls = "^0.21"` (transitive via `aws-msk-iam-sasl-signer` for Kafka MSK IAM) | Panic during CRL parsing. Kafka brokers rarely serve CRL chains; OCSP-stapling and short-lived certs are the norm. Re-audit on AWS SDK Rust dropping `legacy-rustls`. |
| RUSTSEC-2026-0098 | `rustls-webpki 0.101.7` | Same chain | Name-constraint matching edge case for URI names. Constraint extensions are uncommon in commercial CA chains for managed Kafka (Confluent Cloud, AWS MSK). |
| RUSTSEC-2026-0099 | `rustls-webpki 0.101.7` | Same chain | Symmetric: name-constraint matching for wildcard names. Same risk profile. |

Unmaintained-crate advisories (RUSTSEC-2023-0089 / atomic-polyfill,
RUSTSEC-2024-0384 / instant, RUSTSEC-2024-0436 / paste,
RUSTSEC-2025-0134 / rustls-pemfile) are listed by `cargo audit` as
warnings, not errors. Default `cargo audit` exit code does not fail
CI on them; we don't ignore them explicitly because the upstream
crates may move at any time and we want the warning visible.

### Python — `pyproject.toml [tool.bandit]`

| Bandit ID | What | Why suppressed |
|---|---|---|
| `B608` | Hardcoded SQL expressions | Every hit is a framework SQL builder interpolating *identifiers* (schema / table / column from `@ematix.table` declarations) and *pre-escaped literals* (watermark literal goes through `_build_last_value_literal` which `''`-escapes the value and casts to a typed SQL type). Identifiers can't be parameterized in SQL — same architectural pattern as dbt / sqlmesh / every other ETL framework. |
| `B307` | `eval()` in `decorators.py::ematix.table` | Evaluates PEP 593 annotation strings (e.g. `"BigInt"`) in the user's own module globals. Same risk surface as `typing.get_type_hints` (which also evals string annotations). Trust boundary: the developer writing their own class — not external input. |

`tests/python` is excluded from bandit scanning entirely — it
contains fixture passwords (`"s3cret"`, `"hunter2"`) and exercises
SQL-builder paths bandit can't disambiguate from production code.

## Threat model

ematix-flow is an ETL framework — the operator (a developer at
your company) defines pipelines that read from configured sources
(Kafka, Postgres, S3, …) and write to configured sinks. The trust
boundaries are:

1. **The operator's pipeline code is trusted.** A malicious
   `@ematix.pipeline` function or `@ematix.table` annotation could
   trivially execute arbitrary code (return a `DROP TABLE` SQL
   string, evaluate to `__import__('os').system('…')`, etc.).
   Same model as dbt or Airflow operators.
2. **Source records are *partially* trusted.** They get parsed,
   filtered, projected through DataFusion SQL, and written to the
   target. Records can't influence the SQL plan; they're treated
   as Arrow data throughout.
3. **Configuration TOML / connection registry are trusted.**
   They're written by the operator. Inline credentials in TOML
   are flagged by Π.5 deprecation warnings; the registry path
   (`~/.ematix-flow/connections.toml` + env vars) is the
   recommended pattern.
4. **State store contents (Postgres / in-memory) are trusted.**
   Postcard deserialization from `state_store` can fail-loud on
   tampered bytes, but tamper requires already-compromised DB
   access — out of scope.
5. **Logged secrets are not trusted to anyone.** Every typed
   connection redacts password / secret / API-key fields in
   `repr()`; URL passwords get redacted via `_redact_url_password`;
   the `StateStore` / `tracing` paths never log credentials.

The framework is **not designed** for:

- Multi-tenant pipeline hosting (one operator → many untrusted
  users defining pipelines).
- Direct exposure of pipeline-definition endpoints to the
  internet.

If you need either, run ematix-flow inside an isolation boundary
(separate process, container, k8s namespace) per tenant.
