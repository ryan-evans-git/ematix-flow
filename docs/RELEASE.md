# Cutting a release

Step-by-step for tagging, building, and publishing a new version of
`ematix-flow` to PyPI. The CI pipeline does the heavy lifting; this
checklist captures the human prerequisites + the order to run things
in.

## One-time setup

These prerequisites must be configured before the first release.
Once done they persist.

### 1. PyPI trusted publisher

[Add the trusted publisher][pypi-tp] on
<https://pypi.org/manage/project/ematix-flow/settings/publishing/>:

- **Owner**: `ryan-evans-git`
- **Repository name**: `ematix-flow`
- **Workflow filename**: `release.yml`
- **Environment name**: `pypi`

For the very first release the project doesn't exist yet on PyPI —
add the publisher as a "pending publisher" instead, on
<https://pypi.org/manage/account/publishing/>. After the first
successful publish the pending record auto-promotes to a real one.

### 2. GitHub environment

Create a `pypi` environment under
`Settings → Environments` and add the `id-token: write` permission.
Optionally restrict the environment to tag pushes matching `v*.*.*`
so a stray manual workflow run can't publish.

### 3. GitHub Pages (for the docs site)

`Settings → Pages → Source = GitHub Actions`. The `docs.yml`
workflow handles deployment.

[pypi-tp]: https://docs.pypi.org/trusted-publishers/

## Per-release checklist

### Pre-tag

- [ ] Pull the latest `main`.
- [ ] Confirm `cargo test --workspace --lib` passes locally.
- [ ] Confirm `pytest tests/python` passes locally.
- [ ] Confirm `cargo fmt --all -- --check` and
      `cargo clippy --workspace --all-targets -- -D warnings` are clean.
- [ ] Confirm `cargo audit` and `pip-audit --skip-editable`
      pass locally (the CI's `verify` job runs both — see
      [`SECURITY.md`](https://github.com/ryan-evans-git/ematix-flow/blob/main/SECURITY.md) for what the gates check and
      the list of accepted advisories).
- [ ] Bump `version` in `pyproject.toml`. Match the workspace
      version in the root `Cargo.toml`'s `[workspace.package]` block.
- [ ] Add a new `## [X.Y.Z] — YYYY-MM-DD` section at the top of
      [`CHANGELOG.md`](https://github.com/ryan-evans-git/ematix-flow/blob/main/CHANGELOG.md),
      summarizing the changes since the previous release. Move
      "Unreleased" entries down.
- [ ] Update `[Unreleased]` and the new tag's compare-link footnote
      at the bottom of `CHANGELOG.md`.
- [ ] Skim [`docs/ROADMAP.md`](ROADMAP.md): if any P0/P1 items
      shipped in this release, mark them done (or remove them).
- [ ] If new features shipped, ensure
      [`docs/USER_GUIDE.md`](USER_GUIDE.md) covers them and the
      `examples/` directory has at least one demonstrating each.

### Tag + push

```sh
# From a clean working tree on main:
git tag -a v0.1.0 -m "v0.1.0 — first public release"
git push origin v0.1.0
```

The `release.yml` workflow fires on tag push:

1. Runs the `verify` job: rustfmt + clippy + tests + cargo audit +
   ruff + bandit + pip-audit + pytest. Wheels and the sdist only
   build if this passes — see [`SECURITY.md`](https://github.com/ryan-evans-git/ematix-flow/blob/main/SECURITY.md) for
   the per-tool gate spec.
2. Builds wheels for Linux x86_64 (manylinux2014) and macOS
   aarch64, across Python 3.11 / 3.12 / 3.13 — 6 wheels
   total. (Intel Mac + Python 3.10 wheels were dropped — see the
   top-of-file comment in `release.yml`. Those users install from
   sdist, which still works since `requires-python = ">=3.10"`.)
2. Builds the source distribution.
3. Uploads everything to <https://pypi.org/p/ematix-flow> via
   trusted publishing (no API token required).

The `docs.yml` workflow on push to main deploys the mkdocs site to
<https://ryan-evans-git.github.io/ematix-flow/>.

### Post-publish

- [ ] Verify the wheels installed end-to-end:
      `pip install --no-cache ematix-flow==X.Y.Z`,
      then `python -c "import ematix_flow; print(ematix_flow.__version__)"`.
- [ ] Verify the docs site updated.
- [ ] Create a GitHub Release from the tag, copying the relevant
      `CHANGELOG.md` section into the release body.
- [ ] Announce in the relevant places (Slack, mailing list,
      blog post).

## Rollback

If a release goes out and a critical bug surfaces:

1. **Don't yank** unless the published wheel is dangerous (auth bug
   leaking secrets, etc.) — see PEP 592 yanking semantics.
2. Cut `vX.Y.(Z+1)` with the fix; the `skip-existing` flag in the
   publish workflow plus PyPI's immutability guarantee make this
   safe.
3. If yanking is required: PyPI web UI → project page → release
   detail → "Yank release". Yanked versions stay installable for
   pinned consumers but aren't selected by `pip install ematix-flow`.

## Test releases (TestPyPI)

Not currently wired. To add:

```yaml
# .github/workflows/release.yml — additional job
publish-test:
  if: github.event_name == 'workflow_dispatch'
  needs: [build-linux, build-macos, build-sdist]
  runs-on: ubuntu-latest
  environment:
    name: testpypi
    url: https://test.pypi.org/p/ematix-flow
  permissions:
    id-token: write
  steps:
    - uses: actions/download-artifact@v4
      with:
        path: dist
        merge-multiple: true
    - uses: pypa/gh-action-pypi-publish@release/v1
      with:
        repository-url: https://test.pypi.org/legacy/
        skip-existing: true
```

Add a matching trusted publisher on TestPyPI + a `testpypi`
GitHub environment. Then `workflow_dispatch` runs publish to
TestPyPI; tag pushes still go to production PyPI.
