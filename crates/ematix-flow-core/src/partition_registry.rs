//! Concurrency-aware `target_partitions` — the cross-process partition
//! registry behind the `EMAT_TARGET_PARTITIONS` tri-state lever.
//!
//! ## Why
//!
//! ematix defaults `target_partitions = available_parallelism()` PER
//! PROCESS. Under concurrent query streams (TPC-H throughput style),
//! 10 streams × 14 partitions = ~140 runnable threads on 14 cores and
//! throughput collapses ~3.5× vs DuckDB at SF10 s10 (8,645 vs 30,135
//! QPH — see `bench-results/campaign-2026-07-01/REPORT.md` §3). A
//! diagnostic re-run with `PARTITIONS=2` per stream recovered makespan
//! 91.6 s → 29.1 s with zero code changes. DuckDB degrades gracefully
//! because of in-process morsel work-stealing; our equivalent scheduler
//! rewrite is out of scope — the product fix is choosing a sane
//! partition count when other ematix processes are active.
//!
//! ## The lever (house tri-state pattern, cf. [`crate::flags::tri_state`])
//!
//! `EMAT_TARGET_PARTITIONS`:
//! - `=N` (N≥1) — force `target_partitions = N`.
//! - `=0` — legacy: always `available_parallelism()`.
//! - unset / unparseable — **AUTO**: cooperative process-count sensing
//!   (below). A single process resolves to `available_parallelism()`,
//!   so solo behavior is bit-identical to the pre-lever default.
//!
//! ## AUTO — cooperative process-count sensing
//!
//! Each ematix process (first preset session build) registers itself
//! in a small cross-process registry: a directory (default
//! `$TMPDIR/ematix-partition-registry/`, overridable via
//! `EMAT_PARTITION_REGISTRY_DIR` for test isolation) holding one empty
//! file per live process, named by PID. At session-config build time
//! the process reads the count `N` of live registered processes
//! (liveness = `kill(pid, 0)` sweep; dead entries are removed on read)
//! and uses
//!
//! ```text
//! target_partitions = clamp(available_parallelism() / N,
//!                           MIN_AUTO_PARTITIONS, available_parallelism())
//! ```
//!
//! Deregistration is best-effort (`atexit`); the stale-PID sweep makes
//! leaked entries harmless. The live count is cached for
//! [`LIVE_COUNT_TTL`] (250 ms) per process so hot session-rebuild loops
//! don't hammer `readdir`.
//!
//! **Failure policy: silent degradation.** Any registry failure
//! (unwritable dir, weird TMPDIR, readdir error) falls back to legacy
//! `available_parallelism()` — never panic, never block.
//!
//! Set `EMAT_DEBUG=<anything>` to get a one-line stderr trace of the
//! resolution (mode, cores, live count, chosen value).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// AUTO-mode floor: never go below 2 partitions (a query still gets
/// intra-query parallelism even under heavy process oversubscription;
/// 2 was the diagnostic re-run value that recovered SF10 s10). Clamped
/// to `cores` on single-core boxes.
pub const MIN_AUTO_PARTITIONS: usize = 2;

/// TTL for the per-process cached live-process count (AUTO mode).
pub const LIVE_COUNT_TTL: Duration = Duration::from_millis(250);

/// The resolved meaning of `EMAT_TARGET_PARTITIONS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetPartitionsMode {
    /// `=N` (N≥1): force this exact value.
    Forced(usize),
    /// `=0`: legacy — always `available_parallelism()`.
    Legacy,
    /// unset / unparseable: cooperative process-count sensing.
    Auto,
}

/// Read `EMAT_TARGET_PARTITIONS` from the environment.
pub fn mode() -> TargetPartitionsMode {
    mode_of(std::env::var("EMAT_TARGET_PARTITIONS").ok().as_deref())
}

/// Pure core of [`mode`] so tests can pin the parse table without
/// racing on process-global env vars (the `tri_state_of` convention).
fn mode_of(val: Option<&str>) -> TargetPartitionsMode {
    match val.map(str::trim) {
        Some(v) if !v.is_empty() => match v.parse::<usize>() {
            Ok(0) => TargetPartitionsMode::Legacy,
            Ok(n) => TargetPartitionsMode::Forced(n),
            // Unrecognized value = AUTO, matching flags::tri_state_of.
            Err(_) => TargetPartitionsMode::Auto,
        },
        _ => TargetPartitionsMode::Auto,
    }
}

/// `available_parallelism()`, floored at 1.
pub fn available_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// The registry directory: `EMAT_PARTITION_REGISTRY_DIR` if set (test
/// isolation), else `<temp_dir>/ematix-partition-registry`.
pub fn registry_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("EMAT_PARTITION_REGISTRY_DIR") {
        if !d.is_empty() {
            return PathBuf::from(d);
        }
    }
    std::env::temp_dir().join("ematix-partition-registry")
}

/// The AUTO formula: `clamp(cores / live, MIN_AUTO_PARTITIONS, cores)`.
/// `live <= 1` returns `cores` unchanged (solo = bit-identical legacy).
pub fn auto_partitions(cores: usize, live: usize) -> usize {
    let cores = cores.max(1);
    let live = live.max(1);
    if live == 1 {
        return cores;
    }
    // On a 1-core box the floor must not exceed the ceiling.
    let floor = MIN_AUTO_PARTITIONS.min(cores);
    (cores / live).clamp(floor, cores)
}

/// Write this process's PID file into `dir`. Returns `false` on any
/// failure (silent-degradation contract — callers fall back to legacy).
pub fn register_process_in(dir: &Path) -> bool {
    let write = || -> std::io::Result<()> {
        fs::create_dir_all(dir)?;
        // The file name is the PID — unique among live processes, so a
        // plain create-or-truncate write is atomic enough (no partial
        // content matters: liveness comes from the name, not the body).
        fs::write(dir.join(std::process::id().to_string()), b"")?;
        Ok(())
    };
    write().is_ok()
}

/// `kill(pid, 0)` liveness: alive on success OR `EPERM` (exists but
/// owned by someone else); dead on `ESRCH`/other.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // pid 0 / overflow would signal a process group — never probe it.
    let Ok(pid_t) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid_t <= 0 {
        return false;
    }
    if unsafe { libc::kill(pid_t, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    // No cheap portable liveness probe — treat entries as live; the
    // count over-estimates, which only lowers partition counts (safe).
    true
}

/// Count live PID entries in `dir`, sweeping stale (dead-PID) files.
/// Returns `None` if the directory can't be read (degrade to legacy).
pub fn live_process_count_in(dir: &Path) -> Option<usize> {
    let entries = fs::read_dir(dir).ok()?;
    let mut live = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue; // not a PID file — ignore, don't touch
        };
        if pid_alive(pid) {
            live += 1;
        } else {
            // Stale entry from a crashed/killed process — sweep it.
            let _ = fs::remove_file(entry.path());
        }
    }
    Some(live)
}

/// Per-test/pure variant of the AUTO path: register in `dir`, count
/// live processes, apply the clamp formula. Any failure → `cores`.
pub fn auto_resolve_in(dir: &Path, cores: usize) -> usize {
    if !register_process_in(dir) {
        return cores;
    }
    let Some(live) = live_process_count_in(dir) else {
        return cores;
    };
    auto_partitions(cores, live)
}

/// This process's registration: `Some(pid-file path)` once registered,
/// `None` if the first attempt failed (never retried — a process that
/// can't register also can't be seen by peers, so it must not consume
/// a peer-derived smaller partition count asymmetrically).
static REGISTRATION: OnceLock<Option<PathBuf>> = OnceLock::new();

/// Register this process in the global registry (once per process;
/// subsequent calls are no-ops). Installs a best-effort `atexit`
/// cleanup. Returns the PID-file path, or `None` on failure.
pub fn register_process() -> Option<&'static PathBuf> {
    REGISTRATION
        .get_or_init(|| {
            let dir = registry_dir();
            if !register_process_in(&dir) {
                return None;
            }
            // Best-effort deregistration; the stale sweep covers the
            // SIGKILL / crash cases where atexit never runs.
            unsafe {
                libc::atexit(remove_registration_file);
            }
            Some(dir.join(std::process::id().to_string()))
        })
        .as_ref()
}

extern "C" fn remove_registration_file() {
    if let Some(Some(path)) = REGISTRATION.get() {
        let _ = fs::remove_file(path);
    }
}

/// Live-count cache (AUTO mode): the count is read at each
/// session-config construction, which the bench harness does per
/// query — TTL-cache it so readdir+kill sweeps stay off the hot path.
static LIVE_COUNT_CACHE: Mutex<Option<(Instant, usize)>> = Mutex::new(None);

fn cached_live_count(dir: &Path) -> Option<usize> {
    let mut guard = LIVE_COUNT_CACHE.lock().ok()?;
    if let Some((at, n)) = *guard {
        if at.elapsed() < LIVE_COUNT_TTL {
            return Some(n);
        }
    }
    let n = live_process_count_in(dir)?;
    *guard = Some((Instant::now(), n));
    Some(n)
}

/// Resolve the session's `target_partitions` from the
/// `EMAT_TARGET_PARTITIONS` tri-state (see module docs). AUTO
/// registers this process on first call and senses the live ematix
/// process count; every failure degrades silently to legacy
/// `available_parallelism()`.
pub fn resolve_target_partitions() -> usize {
    let cores = available_cores();
    let (resolved, detail) = match mode() {
        TargetPartitionsMode::Forced(n) => (n, "forced".to_string()),
        TargetPartitionsMode::Legacy => (cores, "legacy".to_string()),
        TargetPartitionsMode::Auto => match register_process() {
            None => (cores, "auto: registry unavailable -> legacy".to_string()),
            Some(pid_file) => {
                let live = pid_file
                    .parent()
                    .and_then(cached_live_count)
                    .unwrap_or(1)
                    .max(1);
                (auto_partitions(cores, live), format!("auto live={live}"))
            }
        },
    };
    if crate::flags::present("EMAT_DEBUG") {
        eprintln!(
            "[partition_registry] pid={} target_partitions={resolved} ({detail}, cores={cores})",
            std::process::id()
        );
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    /// Unique temp dir per test (no tempfile dep needed; PID + a
    /// monotonic counter disambiguates parallel tests in one process).
    fn test_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "emat-partreg-test-{}-{}-{}",
            tag,
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&d);
        d
    }

    // ---- tri-state parse table (pure — no env mutation) ----

    #[test]
    fn mode_parse_table() {
        use TargetPartitionsMode::*;
        assert_eq!(mode_of(None), Auto, "unset => AUTO");
        assert_eq!(mode_of(Some("")), Auto, "set-but-empty => AUTO");
        assert_eq!(mode_of(Some("0")), Legacy, "0 => legacy");
        assert_eq!(mode_of(Some("1")), Forced(1));
        assert_eq!(mode_of(Some("6")), Forced(6));
        assert_eq!(mode_of(Some(" 8 ")), Forced(8), "whitespace trimmed");
        assert_eq!(mode_of(Some("banana")), Auto, "unparseable => AUTO");
        assert_eq!(mode_of(Some("-3")), Auto, "negative => AUTO");
        assert_eq!(mode_of(Some("2.5")), Auto, "non-integer => AUTO");
    }

    // ---- clamp formula ----

    #[test]
    fn auto_partitions_clamp_bounds() {
        // Solo => full cores, bit-identical to legacy.
        assert_eq!(auto_partitions(14, 1), 14);
        assert_eq!(auto_partitions(14, 0), 14, "live floored at 1");
        // The measured M4 Max cases.
        assert_eq!(auto_partitions(14, 2), 7);
        assert_eq!(auto_partitions(14, 3), 4);
        assert_eq!(auto_partitions(14, 7), 2);
        // Heavy oversubscription clamps at the floor.
        assert_eq!(auto_partitions(14, 14), 2);
        assert_eq!(auto_partitions(14, 100), 2);
        // Bigger boxes.
        assert_eq!(auto_partitions(56, 10), 5);
        // Degenerate cores: floor never exceeds cores.
        assert_eq!(auto_partitions(1, 5), 1);
        assert_eq!(auto_partitions(2, 5), 2);
        assert_eq!(auto_partitions(0, 3), 1, "cores floored at 1");
    }

    // ---- registry mechanics ----

    #[test]
    fn register_and_count_self() {
        let dir = test_dir("self");
        assert!(register_process_in(&dir));
        assert_eq!(live_process_count_in(&dir), Some(1));
        // Idempotent re-register.
        assert!(register_process_in(&dir));
        assert_eq!(live_process_count_in(&dir), Some(1));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn stale_pid_swept_on_read() {
        let dir = test_dir("stale");
        assert!(register_process_in(&dir));
        // A reaped child's PID is (almost certainly) not alive.
        let mut child = Command::new("true").spawn().expect("spawn true");
        let dead_pid = child.id();
        child.wait().expect("reap child");
        let stale = dir.join(dead_pid.to_string());
        fs::write(&stale, b"").unwrap();
        // Non-PID junk is ignored, not deleted.
        let junk = dir.join("not-a-pid");
        fs::write(&junk, b"").unwrap();

        assert_eq!(
            live_process_count_in(&dir),
            Some(1),
            "dead PID must not be counted"
        );
        assert!(!stale.exists(), "stale entry swept");
        assert!(junk.exists(), "non-PID files untouched");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn counts_live_foreign_processes() {
        let dir = test_dir("live");
        assert!(register_process_in(&dir));
        // Two real live children.
        let mut kids: Vec<std::process::Child> = (0..2)
            .map(|_| {
                Command::new("sleep")
                    .arg("20")
                    .spawn()
                    .expect("spawn sleep")
            })
            .collect();
        for k in &kids {
            fs::write(dir.join(k.id().to_string()), b"").unwrap();
        }
        assert_eq!(live_process_count_in(&dir), Some(3), "self + 2 children");
        assert_eq!(auto_resolve_in(&dir, 14), 4, "clamp(14/3) = 4");
        for k in &mut kids {
            let _ = k.kill();
            let _ = k.wait();
        }
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- silent degradation ----

    #[test]
    fn unwritable_registry_degrades_to_legacy() {
        // Registry "dir" nested under a regular FILE — mkdir fails on
        // every platform, root included.
        let base = test_dir("degrade");
        fs::create_dir_all(&base).unwrap();
        let blocker = base.join("blocker");
        fs::write(&blocker, b"").unwrap();
        let dir = blocker.join("registry");

        assert!(!register_process_in(&dir));
        assert_eq!(live_process_count_in(&dir), None);
        assert_eq!(
            auto_resolve_in(&dir, 14),
            14,
            "registry failure => legacy full-core behavior"
        );
        let _ = fs::remove_dir_all(&base);
    }

    // ---- cross-process integration ----

    /// Child-process helper for `concurrent_processes_each_see_full_count`.
    /// Does nothing unless the parent set `EMAT_TEST_PROBE_OUT` (and it
    /// is `#[ignore]`d, so normal runs skip it).
    ///
    /// Protocol: register (via the real global path, whose dir the
    /// parent redirected with `EMAT_PARTITION_REGISTRY_DIR`), signal
    /// ready, wait until all siblings are ready, resolve, write the
    /// result, then linger until all siblings have written results so
    /// nobody's atexit deregistration deflates a sibling's count.
    #[test]
    #[ignore = "spawned as a child process by concurrent_processes_each_see_full_count"]
    fn registry_probe_child() {
        let Ok(out_dir) = std::env::var("EMAT_TEST_PROBE_OUT") else {
            return;
        };
        let expect: usize = std::env::var("EMAT_TEST_PROBE_EXPECT")
            .expect("probe expect")
            .parse()
            .expect("probe expect parse");
        let out = PathBuf::from(out_dir);
        let pid = std::process::id();

        register_process().expect("child registration must succeed");
        fs::write(out.join(format!("ready-{pid}")), b"").unwrap();
        wait_for_files(&out, "ready-", expect);

        let tp = resolve_target_partitions();
        let live = REGISTRATION
            .get()
            .and_then(|r| r.as_ref())
            .and_then(|p| p.parent())
            .and_then(live_process_count_in)
            .expect("live count");
        fs::write(out.join(format!("result-{pid}")), format!("{live} {tp}")).unwrap();
        wait_for_files(&out, "result-", expect);
    }

    fn wait_for_files(dir: &Path, prefix: &str, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let count = fs::read_dir(dir)
                .map(|rd| {
                    rd.flatten()
                        .filter(|e| e.file_name().to_string_lossy().starts_with(prefix))
                        .count()
                })
                .unwrap_or(0);
            if count >= n {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "barrier timeout waiting for {n} '{prefix}*' files in {dir:?}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    /// Spawn 3 real child processes that each register in a shared
    /// (isolated) registry and resolve AUTO concurrently: each must see
    /// live=3 and target_partitions = clamp(cores/3, 2, cores).
    #[test]
    fn concurrent_processes_each_see_full_count() {
        const N: usize = 3;
        let base = test_dir("xproc");
        let registry = base.join("registry");
        let out = base.join("out");
        fs::create_dir_all(&registry).unwrap();
        fs::create_dir_all(&out).unwrap();
        let exe = std::env::current_exe().expect("current_exe");

        let children: Vec<_> = (0..N)
            .map(|_| {
                Command::new(&exe)
                    .args(["registry_probe_child", "--ignored", "--nocapture"])
                    .env("EMAT_PARTITION_REGISTRY_DIR", &registry)
                    .env("EMAT_TEST_PROBE_OUT", &out)
                    .env("EMAT_TEST_PROBE_EXPECT", N.to_string())
                    .env_remove("EMAT_TARGET_PARTITIONS")
                    .env_remove("EMAT_DEBUG")
                    .spawn()
                    .expect("spawn child probe")
            })
            .collect();
        for mut c in children {
            let status = c.wait().expect("child wait");
            assert!(status.success(), "child probe failed: {status:?}");
        }

        let cores = available_cores();
        let expected_tp = auto_partitions(cores, N);
        let mut results = 0;
        for entry in fs::read_dir(&out).unwrap().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(_) = name.strip_prefix("result-") else {
                continue;
            };
            results += 1;
            let body = fs::read_to_string(entry.path()).unwrap();
            let mut it = body.split_whitespace();
            let live: usize = it.next().unwrap().parse().unwrap();
            let tp: usize = it.next().unwrap().parse().unwrap();
            assert_eq!(live, N, "{name}: each child must see all {N} processes");
            assert_eq!(
                tp, expected_tp,
                "{name}: AUTO must resolve clamp({cores}/{N}, 2, {cores})"
            );
        }
        assert_eq!(results, N, "every child must report a result");
        let _ = fs::remove_dir_all(&base);
    }
}
