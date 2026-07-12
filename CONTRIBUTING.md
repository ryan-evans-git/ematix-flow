# Contributing to ematix-flow

Thanks for your interest in ematix-flow. Issues, discussions, and pull
requests are welcome — this is an actively developed, open project and
outside input is genuinely useful while the API stabilizes toward 1.0.

ematix-flow is **beta** (current release `0.14.x`). The public API may
still change between minor versions; see
[`docs/DELIVERY_SEMANTICS.md`](docs/DELIVERY_SEMANTICS.md) for the
guarantees that are stable today.

## Ways to help

- **Report a bug** — open an issue with the *Bug report* template. A
  minimal reproduction (a small pipeline definition + the connection
  kinds involved) is worth more than anything else.
- **Request a feature or a new connector** — open an issue with the
  *Feature request* template. Adoption blockers (a target the data-quality
  probe doesn't yet support, a delivery mode you need) are especially
  useful signal.
- **Send a pull request** — see the workflow below.

## Project layout

| Path | What lives there |
| --- | --- |
| `crates/` | Rust workspace — the execution engine, Kafka backend, hash-join, distributed mesh. |
| `python/ematix_flow/` | The Python surface — decorators, streaming, scheduler, data quality, web UI server. |
| `web-ui/` | Svelte web UI (workflows, run history, SQL Lab, quality, dashboards). |
| `tests/python/` | Python test suite. |
| `docs/` | Design notes, plans, and reference docs. |
| `examples/` | Runnable example pipelines. |

## Development workflow

The `Makefile` is the source of truth for common tasks — run `make help`
to list every target. The essentials:

```bash
# Fast suites, no Docker required
make test            # Python + Rust lib tests
make test-python     # Python only
make test-rust       # Rust workspace lib tests

# Formatting and linting (clippy is the strict CI gate)
make fmt
make lint

# Security scanners (bandit + cargo-audit)
make security

# Integration / e2e (needs the demo stack: postgres + kafka + minio)
make up              # bring the stack up
make test-integration
make down            # tear it down
```

Building the Python extension for local work uses
[maturin](https://github.com/PyO3/maturin): `maturin develop` inside an
active virtualenv. Some benchmarks require extra Cargo features (e.g.
`--features triangulation` for the TPC-H harness) — see the relevant
`docs/` and `bench-results/` notes.

## Pull request expectations

1. **Branch** off the latest `main`.
2. **Tests first where practical.** New behavior should ship with tests;
   bug fixes should ship with a test that fails before the fix. CI runs
   `cargo nextest run --workspace` and `pytest`, plus `ruff`, `clippy`,
   `bandit`, and `cargo-audit` — run `make test lint` locally before
   pushing.
3. **Keep the diff focused.** One logical change per PR. Unrelated
   cleanups belong in their own PR.
4. **Match the surrounding style.** The codebase leans on thorough
   docstrings and comments that explain *why*, not *what* — follow that.
5. **Fill in the PR template**, including how you verified the change.

Green CI is required to merge. A maintainer will review; for anything
that touches delivery semantics, the distributed mesh, or the Kafka
runtime, expect questions about failure and recovery behavior.

## Reporting security issues

Please **do not** open a public issue for a vulnerability. See
[`SECURITY.md`](SECURITY.md) for private disclosure.

## License

By contributing, you agree that your contributions are licensed under the
project's [Apache-2.0](LICENSE) license.
