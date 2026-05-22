# Phase plan — closing "What's *not* shipped" items

**Goal**: Ship all four items currently listed in [ematix.dev /specs/03-whats-shipped#whats-not-in-v0.3.0](https://ematix.dev/specs/03-whats-shipped) and remove the section from the website (or repurpose it). As each phase lands, the site's `03-whats-shipped.mdx` is updated and republished to Cloudflare Pages.

Phases ordered by **scope ascending** so each lands a complete deliverable before the next begins.

## Phase 1 — Pluggable secrets stores (smallest)

**Current state**: `python/ematix_flow/config.py:44` and `connections.py:145` both hardcode `${VAR}` → `os.environ[VAR]`.

**Deliverable**:
- New `SecretResolver` protocol — `resolve(reference: str) -> str | None`.
- Extended `${...}` syntax with optional `provider:` prefix: `${VAR}` (default, env), `${vault:path/to/secret#key}`, `${aws:secret-name#key}`, `${gcp:projects/X/secrets/Y/versions/latest}`. Bare `${VAR}` stays exactly as today — backwards compatible.
- Three concrete resolvers:
  - `VaultResolver` (hvac client, KV v2)
  - `AWSSecretsManagerResolver` (boto3, region from env/config)
  - `GCPSecretManagerResolver` (google-cloud-secret-manager)
- Resolver registry + default resolver chain (env first, then any user-registered providers).
- Doc: extend `docs/USER_GUIDE.md` connection section.
- Tests: unit tests with mocked clients; integration tests gated on credentials env vars (skipped in CI without creds).
- pyproject extras: `ematix-flow[vault]`, `ematix-flow[aws-secrets]`, `ematix-flow[gcp-secrets]`.

**Cost**: 1–2 days. **Touches**: 5 Python files + tests. **Risk**: low — additive, behind explicit `provider:` prefix.

## Phase 2 — Snowflake / BigQuery / Redshift backends

**Current state**: no stubs. Existing backend pattern: `crates/ematix-flow-core/src/{pg,mysql,kafka,...}.rs` + `python/ematix_flow/connections.py` dataclasses.

**Deliverable**:
- **SnowflakeBackend** — Python via `snowflake-connector-python` for reads, `snowflake-snowpark-python` or `ADBC` for batch writes. Connection dataclass: `account`, `user`, `password` (or `private_key`), `warehouse`, `database`, `schema`.
- **BigQueryBackend** — `google-cloud-bigquery` + `bigquery-storage-api` for batch reads, streaming or batch inserts for writes. Connection dataclass: `project`, `dataset`, `credentials_path` (or default app creds).
- **RedshiftBackend** — psycopg2 reuses Postgres protocol, but uses `COPY FROM s3://...` for fast batch writes. Connection dataclass: `host`, `port`, `database`, `user`, `password`, `iam_role`, `s3_staging_dir`.
- DDL/schema-introspection for each.
- Integration tests: Snowflake / BigQuery / Redshift don't have testcontainers. Use mock servers (`fakesnow`, `bigquery-emulator`) where available; otherwise gate tests on real credentials env vars and skip in CI.
- pyproject extras: `[snowflake]`, `[bigquery]`, `[redshift]`.
- Doc: `docs/USER_GUIDE.md` + `docs/DEPLOYMENT.md` recipes for each.

**Cost**: 3–5 days. **Touches**: 6 new files (3 Python backends + 3 connection dataclasses), 3 sets of tests. **Risk**: medium — Snowflake especially has gnarly auth (key-pair, OAuth, MFA) and the BigQuery streaming-vs-batch API choice is consequential.

## Phase 3 — Distributed peer auto-detection

**Current state**: `crates/ematix-flow-distributed/src/lib.rs:72` notes the rule is "fixed-membership clusters where peers are known at config-time." Zero discovery code.

**Deliverable**:
- `PeerDiscovery` trait in `ematix-flow-distributed`: `async fn current_peers() -> Vec<PeerAddr>`.
- Three concrete impls:
  - **Static** (current behavior, kept for fixed-membership) — reads `peers = [...]` from config.
  - **K8s headless service** — resolves a DNS SRV record (`flow-workers.namespace.svc.cluster.local`), refreshes every N seconds.
  - **Multicast/mDNS** — broadcasts presence on the local segment, discovers peers without DNS. Optional, single-LAN dev convenience.
- Config: `peers = "auto"` triggers discovery; `peers = "k8s://flow-workers.ns"` selects a specific discovery URL; `peers = ["host1:port", ...]` stays static.
- Reconnection on peer set change (debounced).
- Doc: `docs/DEPLOYMENT.md` recipe for K8s.
- Tests: unit tests + a `testcontainers`-orchestrated 3-node mesh that adds/removes a node and verifies the others re-form the mesh.

**Cost**: 2–3 days. **Touches**: 1 new module + lib.rs + config schema + tests. **Risk**: medium — async DNS refresh + mesh re-form has its own concurrency subtleties.

## Phase 4 — Web UI (largest)

**Current state**: `flow runs ...` CLI only. RunLog backends (SQLite/Postgres/MySQL/S3/Azure/GCS/DuckDB) all share a Protocol surface (`crates/.../run_log.rs` or `python/ematix_flow/run_log/`).

**Deliverable**:
- **Backend** — new crate `ematix-flow-web` that exposes a small read-only HTTP API:
  - `GET /api/runs` — paginated list of runs (filter by pipeline, status, time range).
  - `GET /api/runs/:id` — single run detail (logs, attempts, retries).
  - `GET /api/pipelines` — pipelines + their latest run / failure-rate.
  - `GET /api/metrics` — proxies the Prometheus exposition endpoint.
  - Auth: bearer token from a file path; deployment recipe in DEPLOYMENT.md.
- **Frontend** — Vite + Svelte (or Astro islands) matching ematix.dev's Pip-Boy/Fallout aesthetic ([[project_ematix_dev_site]]). Three pages:
  - Runs list (sortable, filterable, paginated).
  - Run detail (timeline + logs + retries).
  - Pipelines overview (per-pipeline failure rate, recent runs).
- Single binary: the frontend bundle is embedded into the Rust binary via `include_bytes!` so `flow web` launches both.
- CLI: `flow web --port 8080` starts the server pointing at the configured RunLog.
- Doc: `docs/USER_GUIDE.md` "Web UI" section + `docs/DEPLOYMENT.md` recipe.
- Tests: Rust unit tests on the HTTP layer; Playwright smoke tests on the frontend.

**Cost**: 5–7 days. **Touches**: new crate, new frontend project, new CLI subcommand, docs. **Risk**: medium-high — bundling frontend, matching ematix.dev styling exactly, and getting the read API to feel snappy over Postgres at scale.

## Website updates

Each phase, on land:
1. Edit `~/RustroverProjects/ematix.dev/src/content/specs/03-whats-shipped.mdx`:
   - Remove the phase's bullet from "What's *not* shipped"
   - Add the corresponding capability to the appropriate shipped section
2. `cd ~/RustroverProjects/ematix.dev && npm run build` to verify
3. Push to the private repo → Cloudflare Pages auto-deploys

When all four phases land, the "What's *not* in v0.3.0" section is replaced with a forward-looking "What's next" or removed entirely.

## Sequencing

Recommended order is **Phase 1 → Phase 2 → Phase 3 → Phase 4** because (a) each lands faster than the next and (b) Phase 4's Web UI is more valuable once Phases 2 & 3 have added new infrastructure to show in it. Starting now with Phase 1.
