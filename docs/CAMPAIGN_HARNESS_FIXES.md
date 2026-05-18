# AWS campaign harness — fixes to land before the next run

Issues caught during the **2026-05-17 SF=1 + SF=10 validation
campaign** (instance i-0cc2f02c07d56a083). Each item below either
silently lost data, wasted compute, or both. Fix them in a single
infra PR before re-running the campaign.

## Build-side wastes

### F.1 — Pre-build `target.tar.gz` via GitHub Actions, ship via S3

**Today:** every fresh EC2 boot runs `cargo build --release --workspace`
from scratch — ~15 min, identical work each time.

**Fix:** GHA workflow on push-to-main builds the workspace on native
x86 Linux runners, tars `target/release/`, uploads to a dedicated
S3 bucket keyed by commit SHA. Userdata downloads + extracts before
running bench.sh.

**Savings:** ~10 min per campaign (the cargo *test* compile in stage
01 still has to compile test binaries from `#[cfg(test)]` code).

**Setup cost:** ~2 hr one-time GHA workflow + IAM grant.

**Alternatives rejected:**
- Docker buildx on Mac: Apple Silicon → qemu emulation, ~3-5× slower
  than native EC2.
- Cross-compile with zig: works for the Lambda wheel (single binary)
  but the workspace's C deps (deltalake, datafusion, ematix-parquet,
  libpq, sasl2, libsqlite3-sys, rdkafka-sys) are too fragile.
- "Bake docker image at end of first successful boot + reuse via
  ECR": clean but the image is 3-5 GB vs GHA tarball ~500 MB.

### F.2 — Drop stage 01 `cargo test --release --workspace` OR use a faster profile

**Today:** stage 01 runs the full workspace test suite with the LTO
release profile. ~30 min wall-clock (most of it LTO-link of test
binaries). CI already covers this on every PR.

**Fix:** either drop the stage (CI is authoritative for unit-test
correctness on x86) or use `--profile=release-no-lto` purely for
testing.

**Savings:** ~20 min per campaign.

### F.2a — Principle: nothing the campaign runs should overlap with CI

**Today:** stages 01 (`cargo test --release --workspace`) and 08
(`cargo test ... dict_preservation_end_to_end`) both ran tests that
CI already exercises on every PR. Stage 01 ate ~30 min; stage 08
ate another ~25 min before being SIGTERM'd. **~1 hour of paid EC2
time on work CI already does for free.**

**Principle for future campaign design:** the campaign budget is for
things CI **can't** do — x86 AVX-512 hardware, real-S3 / real-Lambda
/ real-EKS, SF=10+ data scales. Unit-test correctness on x86 is CI's
job. If a stage's exit criterion is "tests pass," it doesn't belong
in the campaign.

**Concrete fix for the next campaign:**
- Drop stage 01 (`cargo test --release --workspace`) entirely.
- Drop stage 08 (`cargo test ... dict_preservation_end_to_end`)
  entirely.
- For the dict-preservation validation we *do* care about, run the
  TPC-H triangulation with `with_dict_preservation(true)` and assert
  `EnableDictGroupCountRule` fires — that's a *behavioural* check,
  not a unit-test check, and CI can't run it (CI doesn't have
  SF=10 lineitem on disk).
- Audit each remaining stage: does its failure mode reproduce in CI?
  If yes, drop it. If no, keep it.

**Savings:** ~50 min per campaign, plus removes ambiguity about
"what does this stage even prove." Compounds with F.1 (prebuilt
target) — if we don't run `cargo test`, we don't need the test
binaries pre-built either.

### F.3 — Consolidate feature sets to avoid 3 separate compiles

**Today:**
- userdata builds with default features
- stages 03/03a/03b invoke `cargo run` against `ematix-parquet`
  examples with various feature combos
- stages 06/07 invoke `cargo run --features triangulation` on
  `ematix-flow-core`
- Each new combination triggers a full re-codegen of dependent
  crates.

**Fix:** at userdata level, do one `cargo build --release --workspace
--all-features` (or the union of needed features). Then each bench
stage invokes `cargo run --no-default-features --features <stage>`
against the pre-built target — should be a near-instant link.

**Savings:** ~10 min per campaign (compounds with F.1).

### F.4 — Stages share compile artefacts via `cargo test --no-run`

**Today:** stage 08 (`dict-preservation` test) recompiles
`ematix-flow-core`'s test binary from scratch, even though stage 01
already had it in flight. Cargo's build cache reuses, but the
`cargo test` driver re-evaluates everything.

**Fix:** add a "warmup" step at top of bench.sh:

```bash
cargo test --release --workspace --no-run --features triangulation
```

Compiles every test binary once. Downstream `cargo test --release -p X
my_test` runs are near-instant.

**Savings:** ~15 min per campaign (compounds with F.1, F.2).

## Stage-ordering + data-flow bugs

### F.5 — Move stages 03 + 03a after stage 05 (TPC-H data must exist first)

**Today:** stages `03-emat-parquet-avx-benches` +
`03a-emat-adaptive-dispatch-bench` require `TPCH_DATA_DIR` to exist,
but stages `04-tpch-generate-sf1` + `05-tpch-generate-sf10` come
AFTER. Both 03/03a ran and exited rc=0 with "TPC-H data not found"
— **silently lost the AVX bench data** for the 2026-05-17 campaign.

**Fix:** either renumber 03/03a to land after 05, or add at top of
each:

```bash
[ -d "$TPCH_DATA_DIR" ] || { echo "SKIP: TPC-H data not generated yet"; exit 0; }
```

The exit-0 path makes the dependency declarative; renumbering makes
it implicit. Renumbering preferred for clarity.

### F.6 — Per-scale-factor `BENCHMARKS-SF{N}.md` + explicit S3 upload

**Today:** stage 06 (SF=1 triangulation) writes
`/opt/ematix/ematix-flow/BENCHMARKS.md`. Stage 07 (SF=10) writes to
the **same path** — 07 overwrites 06. Worse, `bench.sh` only uploads
stdout (which is `tail -400`-truncated, losing per-query results
beyond Q05).

The 2026-05-17 campaign saved SF=1 via emergency SSM snapshot but
**lost the SF=10 BENCHMARKS.md** because stage 07 apparently didn't
reach end-of-main.

**Fix:**
1. Add `--output BENCHMARKS-SF{N}.md` flag to `tpch_triangulation_bench`
   example, accept the path from the stage harness.
2. Bench.sh stage 06: `--output BENCHMARKS-SF1.md`; stage 07:
   `--output BENCHMARKS-SF10.md`.
3. After each stage, explicitly `upload BENCHMARKS-SF1.md` /
   `upload BENCHMARKS-SF10.md` to S3.

### F.7 — Investigate why stage 07 stopped at Q05

**Today:** stage 07 SF=10 triangulation ran ~90 sec, exited rc=0,
but stdout shows only Q01-Q05 of the 22 queries. Two possibilities:
(a) the bench actually completed 22 queries and stdout was tail-truncated
(but then BENCHMARKS.md should be SF=10 content, which it wasn't);
(b) something crashed mid-bench at Q06+ and the BENCHMARKS.md write
was never reached.

**Fix path:** before re-running the campaign, instrument
`tpch_triangulation_bench` to write per-query incremental progress
to a file (not just at end), so a crash at Q12 still leaves Q01-Q11
on disk.

## Upload-path bugs

### F.8 — Don't swallow upload errors (already in place locally, not on main)

**Already fixed in the patched bench.sh I uploaded to S3 as
`fixes/bench.sh` during the campaign**, but the fix lives on the
local Mac, not on main. The fix:

```bash
upload() {
  local src="$1" dst="$2"
  if ! "$AWS_BIN" s3 cp "$src" "s3://$BUCKET/$PREFIX/$dst" --region "$REGION" \
      >>"/tmp/upload-errors.log" 2>&1; then
    echo "UPLOAD FAIL rc=$? src=$src dst=$dst" >> "/tmp/upload-errors.log"
  fi
}
```

Plus an `aws --version` sanity-check at the top of bench.sh that
`exit 3`s if the CLI isn't on PATH. Bring this onto main as part of
the infra PR.

### F.9 — `stage()` should not toggle `set -e`

**Already fixed locally.** Original stage() did `set +e; bash -c …;
set -e` — but the outer script never had `-e` enabled, so the trailing
`set -e` *armed* a foot-gun that would fire on the next non-zero
return from any command. Cause of mysterious "campaign ran for a
while then died" failures on prior boots. Fix is to remove the
`set +e/-e` toggle entirely.

### F.10 — Glob-resilient chmod in userdata heredoc

**Already fixed locally.** `chmod +x .../lambda/*.sh` under `set -e`
aborted the whole bench if the dir was empty (literal `*.sh` failure).
Fix: `2>/dev/null || true` on each chmod.

### F.11 — Chown `$LOG` to ec2-user before the sudo-to-ec2-user heredoc

**Already fixed locally.** `tee -a "$LOG"` inside the sudo-to-ec2-user
RUNBENCH heredoc would EPERM against root-owned `/var/log/ematix-bench.log`.
Pipefail propagated the failure, aborting the bench before any S3
upload happened.

## Toolchain / dependency traps

### F.12 — `perl-FindBin` + `OPENSSL_NO_VENDOR=1` for AL2023 openssl-sys

**Already in patched userdata.sh.** AL2023's perl ships without the
`FindBin` core module. openssl-sys's vendored build needs it. Both
`perl-FindBin perl-IPC-Cmd perl-File-Compare perl-File-Copy` added
to dnf, plus `OPENSSL_NO_VENDOR=1` exported before cargo build.

### F.13 — `cmake` for rdkafka-sys

**Already in patched userdata.sh.** `rdkafka-sys` uses CMake to
build its bundled librdkafka. AL2023 doesn't ship cmake by default.
Added to the dnf install list.

### F.14 — `${KUBECTL_VERSION}` shell-var escape in terraform-templated userdata

**Already fixed.** Terraform's `templatefile()` walks the entire
template — including comments — interpreting `${X}` as a variable
substitution. For shell variables that need to survive, escape `$`
as `$$`. This was the cause of the first boot's kubectl install
failure.

### F.15 — Docker socket perms for sudo'd ec2-user mid-userdata

**Already in patched userdata.sh.** `usermod -aG docker ec2-user`
only takes effect at next login. Every `sudo -u ec2-user bash` later
in userdata inherits the supplementary-group set from the existing
session — no docker group, so `docker buildx build` fails. Fix:
wait for dockerd ready then `chmod 666 /var/run/docker.sock`. Risk
acceptable on single-tenant short-lived EC2.

### F.16 — Commit `infra/test-validation/` to main BEFORE running

**Today (the worst finding from 2026-05-17):** all Phase B/C/D
stages (10 s3-runlog, 20-22 Lambda, 30-31 K8s, 40 distributed-e2e)
failed with `No such file or directory` because the entire
`infra/test-validation/` directory is **untracked in git**. The EC2's
`git clone origin/main` returned a tree without the test scripts, the
Lambda packager, the K8s submit script, or the Python integration
tests. Only `bench.sh` worked because I uploaded it to S3 and
SSM-copied it during the in-place fix.

**Effective campaign value lost:** 5 of 7 phases (Phase B, C, D x2,
distributed-e2e) — the *entire* "real AWS service" half of the
campaign. Only Phase A actually exercised real infrastructure.

**Fix:** PR the `infra/test-validation/` directory to main as part
of the campaign-cleanup PR. Each script lives where bench.sh expects
it:
- `infra/test-validation/scripts/bench.sh`
- `infra/test-validation/lambda/{build-package.sh,deploy.sh,handler.py}`
- `infra/test-validation/k8s/build-and-push.sh`
- `infra/test-validation/tests/{test_s3_runlog_real,test_lambda_real,test_k8s_real,test_distributed_e2e}.py`
- `infra/test-validation/Dockerfile.flow-worker`
- `infra/test-validation/main.tf` + variables + outputs + IAM policies

**Smoke gate:** before the next paid EC2 run, `git ls-files infra/test-validation/`
should return all the script paths listed above.

## Priority ranking

Land **all of these** in a single infra PR before the next campaign:

1. **F.16 (commit the whole infra/ directory)** — without this, **5 of 7 phases produce no real data**
2. F.5 (stage ordering) — most important, lost the AVX bench data
3. F.6 (per-SF BENCHMARKS) — lost the SF=10 results
4. F.2a (drop CI-overlapping stages 01 + 08) — saves ~50 min, removes ambiguity
5. F.1 (prebuilt target via GHA) — biggest time/money saver
6. F.4 (shared test-binary compile) — only matters if we keep cargo test stages
7. F.2 (drop or de-LTO stage 01) — subsumed by F.2a if we just drop it
8. F.3 (feature-set consolidation) — saves 10 min, harder to verify
9. F.7 (incremental Q write) — defensive; safety net for crashes
10. F.8-F.15 (the already-applied-locally fixes) — bring to main

PR title: `infra(aws-campaign): bench.sh + userdata.sh fixes from 2026-05-17 run`
