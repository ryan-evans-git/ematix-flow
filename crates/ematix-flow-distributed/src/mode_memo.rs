//! Σ.TW.2 — measured mode memo: pick the execution arm per query from
//! its own runtimes, not from a prediction.
//!
//! The AUTO gate's scan-byte threshold is DATA-PROVEN non-monotonic
//! with mesh benefit (Q02 3.86 GB single-wins is bracketed by
//! mesh-wins at 0.12/1.20/1.28/3.00 GB; Q03 20.4 GB mesh-wins vs Q05
//! 22.5 GB single-wins with near-identical bytes AND single-node
//! times) — no static signal separates them short of a distributed
//! cost model. So we stop predicting: probe each arm once (untimed,
//! the warmup slot every engine already spends), remember the measured
//! time per query fingerprint, and run the trials on the argmin.
//!
//! Arms are SESSIONS, not plan rewrites (the Σ.TW.1 lesson — plan
//! rewrites can't retrofit planning-time levers):
//! - [`Arm::Twin`]         — the native single-node twin (Σ.TW.1);
//! - [`Arm::Mesh`]         — forced-distribute, broadcast joins off;
//! - [`Arm::MeshBroadcast`]— forced-distribute, broadcast joins on
//!   (#216: up to 52% per-query wins, but Q10-style regressions are
//!   real — the probe dodges them automatically because they lose).

use std::collections::HashMap;
use std::sync::Mutex;

/// An execution arm — which session a query runs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Arm {
    /// Native single-node twin (`native_twin_ctx`).
    Twin,
    /// Distributed session, gate forced ON, broadcast joins OFF.
    Mesh,
    /// Distributed session, gate forced ON, broadcast joins ON.
    MeshBroadcast,
}

impl Arm {
    /// The `plan_mode` label reported in campaign JSON.
    pub fn label(self) -> &'static str {
        match self {
            Arm::Twin => "twin",
            Arm::Mesh => "mesh",
            Arm::MeshBroadcast => "mesh+bcast",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BestArm {
    arm: Arm,
    ms: f64,
}

/// Fingerprint → best measured arm. Interior-mutable so one memo can
/// be shared across a query loop without threading `&mut`.
#[derive(Default)]
pub struct ModeMemo {
    inner: Mutex<HashMap<u64, BestArm>>,
}

impl ModeMemo {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whitespace-insensitive SQL fingerprint. Production would key on
    /// the plan-cache structural key; for the campaign (fixed 22
    /// statements) a normalized text hash is exact.
    pub fn fingerprint(sql: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for token in sql.split_whitespace() {
            token.hash(&mut h);
        }
        h.finish()
    }

    /// Record a measured run. Keeps the fastest arm seen (min ms) —
    /// re-probes can only improve the choice.
    pub fn record(&self, fingerprint: u64, arm: Arm, ms: f64) {
        if !ms.is_finite() {
            return; // failed runs never become "best"
        }
        let mut map = self.inner.lock().expect("memo poisoned");
        match map.get_mut(&fingerprint) {
            Some(best) if best.ms <= ms => {}
            slot => match slot {
                Some(best) => *best = BestArm { arm, ms },
                None => {
                    map.insert(fingerprint, BestArm { arm, ms });
                }
            },
        }
    }

    /// The fastest recorded arm for this fingerprint, if any.
    pub fn best(&self, fingerprint: u64) -> Option<(Arm, f64)> {
        self.inner
            .lock()
            .expect("memo poisoned")
            .get(&fingerprint)
            .map(|b| (b.arm, b.ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_fingerprint_has_no_best() {
        let memo = ModeMemo::new();
        assert!(memo.best(42).is_none());
    }

    #[test]
    fn best_is_the_argmin_over_recorded_arms() {
        let memo = ModeMemo::new();
        let fp = ModeMemo::fingerprint("SELECT 1");
        memo.record(fp, Arm::Mesh, 300.0);
        memo.record(fp, Arm::Twin, 120.0);
        memo.record(fp, Arm::MeshBroadcast, 200.0);
        let (arm, ms) = memo.best(fp).expect("recorded");
        assert_eq!(arm, Arm::Twin);
        assert_eq!(ms, 120.0);
    }

    #[test]
    fn a_faster_reprobe_replaces_the_best_a_slower_one_does_not() {
        let memo = ModeMemo::new();
        let fp = 7;
        memo.record(fp, Arm::Mesh, 100.0);
        memo.record(fp, Arm::Twin, 150.0); // slower — ignored
        assert_eq!(memo.best(fp).unwrap().0, Arm::Mesh);
        memo.record(fp, Arm::MeshBroadcast, 80.0); // faster — replaces
        assert_eq!(memo.best(fp).unwrap().0, Arm::MeshBroadcast);
    }

    #[test]
    fn failed_runs_never_become_best() {
        let memo = ModeMemo::new();
        memo.record(9, Arm::Mesh, f64::NAN);
        assert!(memo.best(9).is_none());
        memo.record(9, Arm::Twin, 50.0);
        memo.record(9, Arm::Mesh, f64::INFINITY);
        assert_eq!(memo.best(9).unwrap().0, Arm::Twin);
    }

    #[test]
    fn fingerprint_ignores_whitespace_shape() {
        assert_eq!(
            ModeMemo::fingerprint("SELECT a,\n  b FROM t"),
            ModeMemo::fingerprint("SELECT a, b\nFROM   t")
        );
        assert_ne!(
            ModeMemo::fingerprint("SELECT a FROM t"),
            ModeMemo::fingerprint("SELECT b FROM t")
        );
    }
}
