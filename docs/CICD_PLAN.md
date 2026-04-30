# ematix-flow — CI/CD Plan

GitHub Actions is currently **disabled** at the repo level until this plan is
implemented and the existing `.github/workflows/ci.yml` is rewritten.

Modeled on `ryan-evans-git/ca-oltp` (format → lint → security → test, with a
separate deploy workflow), adapted for a Rust + Python hybrid wheel project.

---

## Workflow shape

```
.github/workflows/
├── ci.yml          PR + push to main; also `workflow_call` for release
└── release.yml     tag push (v*) → build wheels → publish to PyPI
```

`ci.yml` is the gate. `release.yml` calls `ci.yml` first and only publishes
on green.

---

## ci.yml — staged jobs

Each stage is a `needs:` dependency on the previous one, matching ca-oltp's
fail-fast ordering (cheap checks first, expensive integration last).

### 1. `format` (parallel, fast)
- `cargo fmt --all -- --check`
- `ruff format --check python tests`

### 2. `lint` — `needs: format`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `ruff check python tests`
- `mypy python` (strict mode on the `ematix_flow` package)

### 3. `security` — `needs: format` (parallel with `lint`)
- **`cargo-audit`** — Rust dependency CVEs (`rustsec/audit-check@v2`).
- **`cargo-deny`** — license allowlist + advisory + duplicate-version check.
- **`pip-audit`** — Python dependency CVEs (`continue-on-error: true` with
  warning, matching ca-oltp's pattern so noisy upstream advisories don't
  block merges).
- **`semgrep`** with `p/rust` + `p/python` rulesets.
- **`gitleaks/gitleaks-action@v2`** — secret scan, with the same
  `permissions: { contents: read, pull-requests: read }` block used in
  ca-oltp's CI (the action 403s without `pull-requests: read`).

### 4. `test` — `needs: [lint, security]`
Matrix over `(os, python)`:
- OS: `ubuntu-latest`, `macos-latest`
- Python: `3.10`, `3.11`, `3.12`, `3.13`

Each matrix cell:
- `actions/checkout@v6`, `actions/setup-python@v5`, `dtolnay/rust-toolchain@stable`
- `Swatinem/rust-cache@v2`
- `cargo test --workspace --all-targets` (Rust unit + integration)
- `maturin develop --release`
- `pytest -m 'not integration'` (unit fast tests — the default `addopts`
  already excludes `integration`)

### 5. `integration` — `needs: test`
Single Linux runner with a Postgres service (matches ca-oltp's
`services: postgres:` block):
```yaml
services:
  postgres:
    image: postgres:16
    env: { POSTGRES_USER: test, POSTGRES_PASSWORD: test, POSTGRES_DB: test }
    ports: ['5432:5432']
    options: >-
      --health-cmd pg_isready --health-interval 10s
      --health-timeout 5s --health-retries 5
```
- `pytest -m integration` driving `testcontainers`-backed cases (Phase 13
  test infrastructure work).
- `cargo test --workspace -- --ignored` for any `#[ignore]`'d Rust tests
  that need a live DB.

---

## release.yml — wheel matrix + PyPI

Triggered on `push: tags: ['v*']`. Calls `ci.yml` via `workflow_call`, then
builds wheels:

- **`PyO3/maturin-action@v1`** with `command: build --release --strip` per
  `(platform, target, python)` cell:
  - Linux: `x86_64`, `aarch64` — manylinux2014 + musllinux
  - macOS: `x86_64`, `aarch64`
  - (Windows deferred — not in PRD scope for v0.1.)
- Build sdist once on Linux x86_64.
- Upload all artifacts to a single `dist/` collected job.
- Publish via **`pypa/gh-action-pypi-publish@release/v1`** with **trusted
  publishing** (OIDC, no API token in repo secrets). Requires one-time
  PyPI project setup linking the GitHub repo + `release.yml` workflow.

---

## Decisions & differences from ca-oltp

- **Rust-specific security tooling.** ca-oltp is pure Python; we add
  `cargo-audit` and `cargo-deny` for crate-side vulns and license drift.
- **Wheel matrix replaces ECS deploy.** ca-oltp's `deploy.yml` runs Alembic
  migrations on Fargate. For ematix-flow, "deploy" = publish a wheel.
- **Trusted publishing over PAT.** Avoids long-lived PyPI tokens; sets up
  OIDC the same way `aws-actions/configure-aws-credentials` does in ca-oltp.
- **Concurrency groups** per `github.ref` on `ci.yml` (cancel-in-progress)
  and named `release-${{ github.ref }}` on `release.yml` (no cancel — never
  abort an in-flight publish).
- **Branch protection on `main`** (set via `gh api -X PUT
  /repos/.../branches/main/protection`) requiring `format`, `lint`,
  `security`, `test (3.12, ubuntu-latest)` to pass before merge.
- **`workflow_call` reuse.** `ci.yml` is callable from `release.yml`,
  matching ca-oltp's pattern.

---

## Re-enable steps (when ready)

1. Implement the workflows in `.github/workflows/ci.yml` and
   `.github/workflows/release.yml`.
2. Add `cargo-deny.toml`, `.gitleaks.toml` (allowlist if needed), and a
   `requirements-dev.txt` or pinned `[project.optional-dependencies].dev`
   constraints file for reproducible CI installs.
3. Configure PyPI trusted publishing for `ematix-flow` project →
   `ryan-evans-git/ematix-flow` → `release.yml`.
4. Re-enable Actions:
   `gh api -X PUT /repos/ryan-evans-git/ematix-flow/actions/permissions -F enabled=true`.
5. Apply branch protection on `main`.
6. Open a no-op PR to confirm green CI before tagging `v0.1.0`.

---

## Status

- [x] Actions disabled at the repo level (2026-04-30).
- [ ] Workflows implemented per this plan.
- [ ] PyPI trusted publishing configured.
- [ ] Branch protection on `main`.
