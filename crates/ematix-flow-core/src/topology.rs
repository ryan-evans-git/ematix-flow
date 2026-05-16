//! Σ.E4a: hardware topology discovery.
//!
//! Single source of truth for NUMA node count, cores per node, and
//! the calling thread's home node. Consumers (Σ.E4b NUMA-local
//! allocator, Σ.E4c node-partitioned hash execs) ask the topology
//! once at startup and shape their data structures + parallelism
//! accordingly.
//!
//! ## Today's implementation (Σ.E4a.1)
//!
//! Returns a single-node topology on every platform — `nodes() == 1`,
//! `cores() == num_cpus::get()`. The framework wiring is in place so
//! later commits can swap in a real backend (hwloc2 on Linux,
//! sysctl-based discovery on macOS) without touching call sites.
//!
//! ## Why ship a stub first?
//!
//! 1. The downstream operators (Σ.E4c) need to *consume* a Topology
//!    handle to design their partitioning correctly. Landing the API
//!    surface lets that work proceed; the backend swap doesn't change
//!    the contract.
//! 2. Both bench-validation hosts (Mac M3 Pro, Beelink mini-PC) are
//!    single-socket / single-NUMA-node — the stub returns the right
//!    answer for those hosts today.
//! 3. Multi-socket hardware isn't online yet; the hwloc backend would
//!    ship with no environment to validate against and no measurable
//!    win to claim.
//!
//! ## What lands next
//!
//! - **Σ.E4a.2** — `hwloc2`-backed Linux discovery, gated behind a
//!   `numa-hwloc` cargo feature so the default build stays portable.
//! - **Σ.E4a.3** — macOS sysctl probe (sysctl `hw.packages` /
//!   `hw.physicalcpu_max`), still always reporting 1 NUMA node on
//!   M-series and Intel single-socket Macs.
//!
//! Spec: `docs/PHASE_SIGMA_E4_NUMA.md`.

use std::sync::OnceLock;

/// Single-process hardware-topology view. Cheap to clone (just two
/// `usize`s today). Always returns at least one node and one core.
#[derive(Debug, Clone, Copy)]
pub struct Topology {
    nodes: usize,
    cores_total: usize,
}

impl Topology {
    /// Returns the process-wide topology, computed once on first call.
    pub fn current() -> Self {
        static CELL: OnceLock<Topology> = OnceLock::new();
        *CELL.get_or_init(Self::probe)
    }

    /// Number of NUMA nodes on this host. Always ≥ 1.
    pub fn nodes(&self) -> usize {
        self.nodes
    }

    /// Total physical-or-logical core count across all NUMA nodes.
    /// Always ≥ 1.
    pub fn cores(&self) -> usize {
        self.cores_total
    }

    /// Cores per node (assumes uniform distribution; the future
    /// hwloc backend will report exact per-node counts).
    pub fn cores_per_node(&self) -> usize {
        self.cores_total.div_ceil(self.nodes).max(1)
    }

    /// True iff this host has exactly one NUMA node — i.e. NUMA-aware
    /// partitioning would be a no-op and downstream Σ.E4c operators
    /// can pick the simpler single-pool path.
    pub fn is_single_node(&self) -> bool {
        self.nodes <= 1
    }

    fn probe() -> Self {
        // Σ.E4a.1 stub: always one node. Replaced by hwloc2 / sysctl
        // backends in Σ.E4a.2+.
        let cores_total = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1);
        Self {
            nodes: 1,
            cores_total,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_reports_at_least_one_node_and_core() {
        let t = Topology::current();
        assert!(t.nodes() >= 1);
        assert!(t.cores() >= 1);
        assert!(t.cores_per_node() >= 1);
    }

    #[test]
    fn current_is_memoized() {
        let a = Topology::current();
        let b = Topology::current();
        // Same value-shape; the OnceLock means both came from one
        // probe call.
        assert_eq!(a.nodes(), b.nodes());
        assert_eq!(a.cores(), b.cores());
    }

    #[test]
    fn single_node_predicate_is_true_on_stub() {
        // Σ.E4a.1: every host appears as one node. Downstream
        // operators rely on this to take the simpler path.
        let t = Topology::current();
        assert!(t.is_single_node());
    }
}
