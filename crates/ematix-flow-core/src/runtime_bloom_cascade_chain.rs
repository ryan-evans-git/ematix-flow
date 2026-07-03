//! Σ.Q05.CHAIN — L9 runtime-bloom CASCADE CHAINS (second phase of
//! [`crate::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule`]).
//!
//! The base (pass-1) rule emits a bloom per join in isolation, so its
//! admission gates are per-join: a bloom from an UNFILTERED build is a
//! no-op by FK containment, and a bloom onto a tiny scan can't pay for
//! itself. Both verdicts are wrong for a *chain*: Q05's
//!
//! ```text
//! region(ASIA filter) ⋈ (nation ⋈ (supplier ⋈ (cust-orders ⋈ lineitem)))
//! ```
//!
//! has a region-filtered bloom that filters only the 25-row nation scan
//! (worthless alone) — but the nation scan is the BUILD of the next
//! join, whose bloom then narrows the supplier scan (100K → ~20K), and
//! the narrowed supplier build's bloom on `s_suppkey` finally
//! pre-filters the 60M/600M-row lineitem probe. Each link is only
//! valuable because the chain terminates in a large fact scan.
//!
//! ## Runtime sequencing (why the cascade works without new machinery)
//!
//! Every emitting join is CollectLeft. DataFusion's HashJoinStream
//! drains the build future before polling its probe, and the probe
//! subtree's scans are first-polled only then — so each link's build
//! scan is polled strictly AFTER the parent link's emitter published.
//! The emitter samples the build stream POST-scan, i.e. after the
//! parent bloom already narrowed it. No stalls, no waits: publication
//! order is guaranteed by the poll order.
//!
//! ## Multi-key joins (`EMAT_MULTIKEY_BLOOM`)
//!
//! Q05's supplier join is a 2-key equi-join
//! (`s_suppkey = l_suppkey AND s_nationkey = c_nationkey`). For an
//! Inner/semi equi-join with AND-ed key conditions, a bloom on ONE key
//! pair is a superset pre-filter of the probe (blooms have false
//! positives, never false negatives) — it can never drop a row that
//! would have joined, and the join still enforces all keys. The chain
//! walker therefore considers EVERY i64-domain key pair of a candidate
//! join and picks the one that reaches the chain's terminal fact scan.
//! `EMAT_MULTIKEY_BLOOM=0` refuses links on joins with >1 equi-key.
//!
//! ## Gating (house tri-state pattern, Σ.AI.5)
//!
//! * `EMAT_L9_CASCADE`   — `=1` force on (thresholds relaxed so SF=1
//!   validation exercises the chain), `=0` force off, unset = AUTO.
//! * `EMAT_MULTIKEY_BLOOM` — `=1`/unset allow multi-key links inside an
//!   admitted chain, `=0` refuse them.
//!
//! AUTO is conservative and purely structural (row-count stats, not
//! dataset naming):
//!   (a) every emitting join is CollectLeft with a bounded build
//!       (estimate ≤ [`AUTO_MAX_BUILD_ROWS`], default 4M — admits the
//!       SF=100 supplier at 1M, refuses fact-sized builds);
//!   (b) the chain must TERMINATE in a large fact scan
//!       (≥ [`AUTO_MIN_TERMINAL_ROWS`], default 20M — admits lineitem
//!       at SF≥10 and orders at SF=100, refuses partsupp at SF=10 (8M)
//!       and every SF=1 scan, so SF=1 plans are untouched under AUTO);
//!   (c) the chain START's build subtree is statically filtered
//!       (FilterExec or a scan-pushed predicate) — an unfiltered start
//!       emits a full-key bloom that filters nothing;
//!   (d) chains are ≥ 2 links (single filtered-dim→fact links are the
//!       base rule / L9.DIMSEL's territory, not re-adjudicated here).
//!
//! ## Interaction with pass-1 wraps
//!
//! A scan holds ONE primary sideband; displacing it orphans the
//! pass-1 emitter (the Q8 DIMSEL displacement bug). The chain pass
//! never displaces: an occupied INTERMEDIATE target kills the chain
//! (those links would duplicate pass-1 blooms anyway — e.g. Q05's
//! spliced-semi chain re-deriving the customer/orders blooms), and an
//! occupied TERMINAL target is joined via
//! [`EmatixFastParquetExec::with_extra_runtime_sideband`] — but only
//! when the existing wrap targets a DIFFERENT column (Q05 SF=10:
//! pass-1's `l_orderkey` bloom + the chain's `l_suppkey` bloom
//! compose; a same-column re-emit would be a pure duplicate).
//!
//! ## Runtime safety nets on the terminal link
//!
//! The terminal emitter carries the L9.DIMSEL.RT build-selectivity
//! disarm when its build is a single raw scan: if at publish time the
//! chain didn't actually narrow the build (`actual_keys / scan_total >
//! EMAT_L9_CASCADE_RT_SEL`, default 0.5), it publishes EMPTY and the
//! probe runs unfiltered — the failure mode is "no win", never a
//! regression-by-useless-probing. Its sideband is also marked
//! tight-admitted + dimsel-gated: never stall, never late-arm, and
//! (when attached as the primary) thread the Guard-2 probe disarm.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use crate::bridge_filter_sideband::BridgeFilterSideband;
use crate::build_side_bloom_emitter_exec::{BuildSideBloomEmitterExec, widens_to_i64};
use crate::ematix_fast_parquet::EmatixFastParquetExec;
use crate::runtime_bloom_sideband_rule::{
    build_subtree_has_filter, estimate_build_rows, estimate_probe_scan_rows,
    find_probe_scan_for_column, single_dim_scan_total,
};

/// AUTO terminal-scan floor: the chain must end at a scan at least this
/// large. 20M sits above every SF=1 table and SF=10 partsupp (8M — the
/// Q02-adjacent shape we have no measurement for) while admitting the
/// scans the lever is designed for (lineitem 60M/600M, orders 150M at
/// SF=100). Override via `EMAT_L9_CASCADE_MIN_TERMINAL_ROWS`.
pub const AUTO_MIN_TERMINAL_ROWS: usize = 20_000_000;

/// AUTO per-link build ceiling (stats estimate of the emitting join's
/// build subtree). 4M admits every TPC-H dimension through SF=100
/// (supplier 1M, part 20M is refused — correctly, a 20M-key bloom
/// can't pay) while refusing fact-sized builds. Override via
/// `EMAT_L9_CASCADE_MAX_BUILD_ROWS`.
pub const AUTO_MAX_BUILD_ROWS: usize = 4_000_000;

/// Default terminal runtime build-selectivity disarm threshold
/// (`EMAT_L9_CASCADE_RT_SEL`): publish only when the chain kept ≤ this
/// fraction of the terminal build's raw scan. 0.5 = the bloom must drop
/// at least half the FK-matched probe rows. Q05 ASIA keeps ~20% of
/// suppliers, comfortably inside. 0 disables the runtime gate.
pub const DEFAULT_CASCADE_RT_MAX_SEL: f64 = 0.5;

/// Default cap on links per chain (`EMAT_L9_CASCADE_MAX_LINKS`).
pub const DEFAULT_CASCADE_MAX_LINKS: usize = 4;

/// Σ.Q05.CHAIN — config for the cascade-chain pass, carried as a field
/// of `EnableRuntimeBloomSidebandRule` so tests construct it explicitly
/// (no process-global env races). `Default::default()` resolves from
/// the environment once at rule construction (house pattern).
#[derive(Debug, Clone, Copy)]
pub struct CascadeChainConfig {
    /// `EMAT_L9_CASCADE` tri-state: `Some(true)` force on (row
    /// thresholds relaxed), `Some(false)` force off, `None` = AUTO
    /// (conservative structural thresholds).
    pub cascade: Option<bool>,
    /// `EMAT_MULTIKEY_BLOOM` tri-state: `Some(false)` refuses chain
    /// links on multi-key joins; `Some(true)`/`None` allow them inside
    /// an admitted chain (the chain gates carry the conservatism).
    pub multikey: Option<bool>,
    /// Explicit `EMAT_L9_CASCADE_MIN_TERMINAL_ROWS` override; `None`
    /// resolves to [`AUTO_MIN_TERMINAL_ROWS`] (AUTO) or 0 (forced).
    pub min_terminal_rows: Option<usize>,
    /// Explicit `EMAT_L9_CASCADE_MAX_BUILD_ROWS` override; `None`
    /// resolves to [`AUTO_MAX_BUILD_ROWS`] (AUTO) or `usize::MAX`
    /// (forced).
    pub max_build_rows: Option<usize>,
    /// Terminal-link runtime build-selectivity disarm threshold; see
    /// [`DEFAULT_CASCADE_RT_MAX_SEL`]. 0 disables.
    pub rt_max_sel: f64,
    /// Per-chain link cap; see [`DEFAULT_CASCADE_MAX_LINKS`].
    pub max_links: usize,
    /// `EMAT_L9_CASCADE_TERMINAL_APPLY` tri-state — admit a BARE
    /// terminal (fact scan with no existing wrap) and force-apply its
    /// bitmap even when the pass-rate routing picks dense. `None`
    /// resolves to true under forced cascade (`EMAT_L9_CASCADE=1`,
    /// so SF=1 validation exercises the full chain end-to-end) and
    /// false under AUTO. Measured at SF=10 (2026-07-02, single-trial):
    /// a bare ~20%-pass `l_suppkey` terminal on the 60M lineitem scan
    /// costs the scan its no-filter fast path (+440 ms CPU / +35 ms
    /// wall) for a bitmap the REV.23 routing would discard — so AUTO
    /// only installs a terminal that COMPOSES with an existing wrap
    /// (extra sideband; the AND-ed bitmap lands under the stash
    /// threshold, e.g. pass-1's `l_orderkey` bloom × chain `l_suppkey`
    /// ≈ 0.6%). This knob is the A/B arm for the bare-terminal case.
    pub terminal_apply: Option<bool>,
}

impl Default for CascadeChainConfig {
    fn default() -> Self {
        Self {
            cascade: crate::flags::tri_state("EMAT_L9_CASCADE"),
            multikey: crate::flags::tri_state("EMAT_MULTIKEY_BLOOM"),
            min_terminal_rows: std::env::var("EMAT_L9_CASCADE_MIN_TERMINAL_ROWS")
                .ok()
                .and_then(|s| s.parse().ok()),
            max_build_rows: std::env::var("EMAT_L9_CASCADE_MAX_BUILD_ROWS")
                .ok()
                .and_then(|s| s.parse().ok()),
            rt_max_sel: crate::flags::f64_or("EMAT_L9_CASCADE_RT_SEL", DEFAULT_CASCADE_RT_MAX_SEL),
            max_links: crate::flags::usize_or(
                "EMAT_L9_CASCADE_MAX_LINKS",
                DEFAULT_CASCADE_MAX_LINKS,
            ),
            terminal_apply: crate::flags::tri_state("EMAT_L9_CASCADE_TERMINAL_APPLY"),
        }
    }
}

impl CascadeChainConfig {
    /// Fully-off config for tests/benches that pin pass-1 behavior.
    pub fn off() -> Self {
        Self {
            cascade: Some(false),
            multikey: None,
            min_terminal_rows: None,
            max_build_rows: None,
            rt_max_sel: DEFAULT_CASCADE_RT_MAX_SEL,
            max_links: DEFAULT_CASCADE_MAX_LINKS,
            terminal_apply: None,
        }
    }

    /// Force-on config (thresholds relaxed) — what `EMAT_L9_CASCADE=1`
    /// resolves to; used by miniature-data tests.
    pub fn forced() -> Self {
        Self {
            cascade: Some(true),
            multikey: None,
            min_terminal_rows: None,
            max_build_rows: None,
            rt_max_sel: DEFAULT_CASCADE_RT_MAX_SEL,
            max_links: DEFAULT_CASCADE_MAX_LINKS,
            terminal_apply: None,
        }
    }

    /// Pass enabled at all? (`=0` is the only way to skip it entirely;
    /// AUTO keeps the pass on with conservative structural thresholds.)
    pub fn enabled(&self) -> bool {
        self.cascade != Some(false)
    }

    fn forced_on(&self) -> bool {
        self.cascade == Some(true)
    }

    /// Multi-key links allowed? (`EMAT_MULTIKEY_BLOOM=0` refuses.)
    pub fn multikey_allowed(&self) -> bool {
        self.multikey != Some(false)
    }

    /// Resolved terminal-scan floor: explicit override wins; else 0
    /// when forced (SF=1 validation must fire) or the AUTO constant.
    pub fn resolved_min_terminal_rows(&self) -> usize {
        self.min_terminal_rows.unwrap_or(if self.forced_on() {
            0
        } else {
            AUTO_MIN_TERMINAL_ROWS
        })
    }

    /// Resolved per-link build ceiling: explicit override wins; else
    /// unbounded when forced or the AUTO constant.
    pub fn resolved_max_build_rows(&self) -> usize {
        self.max_build_rows.unwrap_or(if self.forced_on() {
            usize::MAX
        } else {
            AUTO_MAX_BUILD_ROWS
        })
    }

    /// Resolved bare-terminal admission (see the field docs): explicit
    /// tri-state wins; AUTO admits only COMPOSED terminals, forced
    /// cascade admits bare ones too.
    pub fn resolved_terminal_apply(&self) -> bool {
        self.terminal_apply.unwrap_or(self.forced_on())
    }
}

/// One candidate chain link: "join J (identified by its build child)
/// can emit a bloom on `build_key_idx` that lands on
/// `target_scan_node[target_col_idx]`".
struct Link {
    /// The emitting join's BUILD child (Arc identity anchor — join
    /// nodes themselves are rebuilt as inner links install, but their
    /// build children survive by pointer).
    build_child: Arc<dyn ExecutionPlan>,
    /// Key column index in the build child's output schema.
    build_key_idx: usize,
    /// Number of equi-key pairs on the emitting join (multi-key gate).
    n_keys: usize,
    /// Stats estimate of the build subtree's output rows.
    build_rows: Option<usize>,
    /// Raw total of the build subtree's single scan (None when the
    /// build is not anchored on exactly one scan) — feeds the terminal
    /// runtime disarm.
    build_scan_total: Option<usize>,
    /// Does the build subtree carry a static filter (FilterExec or
    /// scan-pushed predicate)? Chain-start requirement.
    build_filtered: bool,
    /// FILE-schema column index in the target scan (the domain
    /// `ColumnPredicate` col_idx lives in).
    target_col_idx: usize,
    /// The target scan node (Arc identity anchor).
    target_scan_node: Arc<dyn ExecutionPlan>,
    /// Target scan's total row count.
    target_rows: Option<usize>,
    /// Target scan already holds a primary sideband (pass-1 wrap)?
    target_occupied: bool,
}

/// Σ.Q05.CHAIN — the pass entry point. Runs AFTER the base rule's
/// per-join transform; `plan` may already contain pass-1 emitter wraps
/// and sideband-carrying scans (both are respected, never displaced).
pub(crate) fn install_cascade_chains(
    plan: Arc<dyn ExecutionPlan>,
    cfg: &CascadeChainConfig,
    trace: bool,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    if !cfg.enabled() {
        return Ok(plan);
    }
    let min_terminal = cfg.resolved_min_terminal_rows();
    let max_build = cfg.resolved_max_build_rows();

    // ---- discovery ------------------------------------------------
    let mut links: Vec<Link> = Vec::new();
    // (scan-allocation ptr, col) pairs already targeted by SOME emitter
    // in the plan — pass-1 wraps and, as chains install, ours too.
    let mut targeted_cols: HashSet<(usize, usize)> = HashSet::new();
    collect_links_and_targets(&plan, cfg, &mut links, &mut targeted_cols);

    if links.is_empty() {
        return Ok(plan);
    }

    // ---- chain assembly -------------------------------------------
    // Claim sets so accepted chains don't collide: a join build is
    // wrapped at most once; a scan gains at most one PRIMARY sideband
    // from this pass.
    let mut claimed_builds: HashSet<usize> = HashSet::new();
    let mut claimed_primary_scans: HashSet<usize> = HashSet::new();
    let mut chains: Vec<Vec<usize>> = Vec::new(); // indices into `links`

    let link_admissible = |l: &Link,
                           claimed_builds: &HashSet<usize>,
                           cfg: &CascadeChainConfig,
                           max_build: usize|
     -> bool {
        if claimed_builds.contains(&arc_ptr(&l.build_child)) {
            return false;
        }
        if l.n_keys > 1 && !cfg.multikey_allowed() {
            return false;
        }
        matches!(l.build_rows, Some(b) if b <= max_build)
    };

    for start_idx in 0..links.len() {
        let start = &links[start_idx];
        // Chain start: statically-filtered bounded build, free
        // intermediate target (the start's target is by construction
        // intermediate — chains are ≥ 2 links).
        if !start.build_filtered
            || !link_admissible(start, &claimed_builds, cfg, max_build)
            || start.target_occupied
            || claimed_primary_scans.contains(&arc_ptr(&start.target_scan_node))
        {
            continue;
        }
        let mut chain: Vec<usize> = vec![start_idx];
        let mut complete = false;
        while chain.len() < cfg.max_links {
            let cur_target = &links[*chain.last().expect("chain non-empty")].target_scan_node;
            // Candidate next links: emitted from a join whose BUILD
            // subtree contains the scan the previous link just bloomed.
            let cand: Vec<usize> = (0..links.len())
                .filter(|&i| {
                    !chain.contains(&i)
                        && subtree_contains(&links[i].build_child, cur_target)
                        && link_admissible(&links[i], &claimed_builds, cfg, max_build)
                })
                .collect();
            // Terminal candidates: target is a large fact scan with NO
            // admissible onward hop (the chain always prefers to keep
            // descending — under forced mode the terminal floor is 0,
            // and without this preference the chain would stop at the
            // first intermediate dim), and — when its sideband slot is
            // taken — the existing wrap targets a DIFFERENT column
            // (extras compose, duplicates don't).
            let term = cand
                .iter()
                .copied()
                .filter(|&i| {
                    let l = &links[i];
                    let rows_ok = l.target_rows.is_some_and(|r| r >= min_terminal);
                    let dup =
                        targeted_cols.contains(&(arc_ptr(&l.target_scan_node), l.target_col_idx));
                    let has_onward = (0..links.len()).any(|j| {
                        j != i
                            && !chain.contains(&j)
                            && subtree_contains(&links[j].build_child, &l.target_scan_node)
                            && link_admissible(&links[j], &claimed_builds, cfg, max_build)
                    });
                    // Bare-terminal admission: a terminal bloom on an
                    // UN-wrapped fact scan only pays under the
                    // force-apply arm (see `terminal_apply` docs — the
                    // dense-route otherwise discards its bitmap while
                    // the scan still pays the filtered-path detour).
                    // A COMPOSED terminal (existing wrap on another
                    // column) is admitted under AUTO: the AND-ed
                    // bitmap prunes below the stash threshold.
                    let attach_ok = l.target_occupied || cfg.resolved_terminal_apply();
                    rows_ok && !dup && !has_onward && attach_ok
                })
                .max_by_key(|&i| links[i].target_rows.unwrap_or(0));
            if let Some(t) = term {
                chain.push(t);
                complete = true;
                break;
            }
            // Otherwise: continue through a free intermediate target.
            let next = cand.iter().copied().find(|&i| {
                let l = &links[i];
                !l.target_occupied && !claimed_primary_scans.contains(&arc_ptr(&l.target_scan_node))
            });
            match next {
                Some(n) => chain.push(n),
                None => break,
            }
        }
        if !complete || chain.len() < 2 {
            if trace && chain.len() > 1 {
                eprintln!(
                    "[L9.chain] chain from start build={:p} died at {} links (no terminal ≥ {min_terminal} rows)",
                    Arc::as_ptr(&links[chain[0]].build_child),
                    chain.len()
                );
            }
            continue;
        }
        // Claim.
        for &i in &chain {
            claimed_builds.insert(arc_ptr(&links[i].build_child));
        }
        for &i in &chain[..chain.len() - 1] {
            claimed_primary_scans.insert(arc_ptr(&links[i].target_scan_node));
        }
        let terminal = &links[*chain.last().expect("complete chain")];
        targeted_cols.insert((arc_ptr(&terminal.target_scan_node), terminal.target_col_idx));
        if !terminal.target_occupied {
            claimed_primary_scans.insert(arc_ptr(&terminal.target_scan_node));
        }
        if trace {
            eprintln!(
                "[L9.chain] ACCEPT {}-link chain → terminal scan rows={:?} col={} (occupied={} → {})",
                chain.len(),
                terminal.target_rows,
                terminal.target_col_idx,
                terminal.target_occupied,
                if terminal.target_occupied {
                    "extra sideband"
                } else {
                    "primary sideband"
                },
            );
        }
        chains.push(chain);
    }

    if chains.is_empty() {
        return Ok(plan);
    }

    // ---- install ----------------------------------------------------
    // Innermost (terminal) link first: outer links' anchors (their
    // build children + target scans) survive inner rewrites by Arc
    // identity, while join nodes on the root path get rebuilt.
    let mut plan = plan;
    for chain in chains.iter() {
        for (pos, &li) in chain.iter().enumerate().rev() {
            let l = &links[li];
            let is_terminal = pos == chain.len() - 1;
            let sideband = if is_terminal {
                if !l.target_occupied && cfg.resolved_terminal_apply() {
                    // Bare terminal under the force-apply arm: treat it
                    // like an intermediate — apply the bitmap even above
                    // the dense-route threshold. No tight/dimsel marks:
                    // Guard-2's 10% disarm would kill exactly the
                    // ~20%-pass prune this arm exists to measure.
                    BridgeFilterSideband::new().mark_chain_intermediate()
                } else {
                    // Composed terminal (extra sideband on an already-
                    // wrapped scan): peek-only merge; never stall,
                    // never late-arm.
                    BridgeFilterSideband::new()
                        .mark_tight_admitted()
                        .mark_dimsel_gated()
                }
            } else {
                // Intermediate links must actually prune their (dim-
                // sized) target scan — the next link samples its
                // output. The marker makes the reader apply the bitmap
                // even above the masked→dense pass-rate threshold
                // (Q05's 20%-pass nation/supplier blooms would
                // otherwise be silently discarded at 10%).
                BridgeFilterSideband::new().mark_chain_intermediate()
            };
            let expected_keys = l.build_rows.unwrap_or(50_000).max(64);
            let mut emitter = BuildSideBloomEmitterExec::try_new(
                Arc::clone(&l.build_child),
                l.build_key_idx,
                l.target_col_idx,
                sideband.clone(),
                expected_keys,
            )?;
            if is_terminal && cfg.rt_max_sel > 0.0 {
                if let Some(total) = l.build_scan_total {
                    // Runtime disarm: if the chain didn't narrow the
                    // build, publish empty instead of a useless bloom.
                    emitter = emitter.with_rt_sel_gate(total, cfg.rt_max_sel);
                }
            }
            if trace {
                eprintln!(
                    "[L9.chain] install link {}/{} — build_key_idx={} expected_keys={expected_keys} → target col={} (terminal={is_terminal})",
                    pos + 1,
                    chain.len(),
                    l.build_key_idx,
                    l.target_col_idx,
                );
            }
            plan = install_link(plan, l, Arc::new(emitter), &sideband)?;
        }
    }
    Ok(plan)
}

/// Pointer identity for an `Arc<dyn ExecutionPlan>` (data address —
/// unique per plan-node allocation).
fn arc_ptr(a: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(a) as *const () as usize
}

/// Does `hay`'s subtree contain the exact node `needle` (Arc identity)?
fn subtree_contains(hay: &Arc<dyn ExecutionPlan>, needle: &Arc<dyn ExecutionPlan>) -> bool {
    if Arc::ptr_eq(hay, needle) {
        return true;
    }
    hay.children().iter().any(|c| subtree_contains(c, needle))
}

/// Chain-start litmus: a static filter anywhere in the build subtree —
/// either a `FilterExec` or an Emat scan with a pushed-down predicate
/// bundle (the planner often folds the filter into the scan).
fn build_subtree_statically_filtered(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if build_subtree_has_filter(plan) {
        return true;
    }
    fn scan_pushed(p: &Arc<dyn ExecutionPlan>) -> bool {
        if let Some(scan) = p.as_any().downcast_ref::<EmatixFastParquetExec>() {
            return scan.pushed_filter().is_some();
        }
        p.children().iter().any(|c| scan_pushed(c))
    }
    scan_pushed(plan)
}

/// Walk the whole plan collecting candidate links + the (scan, col)
/// pairs already targeted by existing emitters (pass-1 wraps).
fn collect_links_and_targets(
    plan: &Arc<dyn ExecutionPlan>,
    cfg: &CascadeChainConfig,
    links: &mut Vec<Link>,
    targeted_cols: &mut HashSet<(usize, usize)>,
) {
    // Pre-pass: map every emitter sideband to its target col, then
    // resolve which scan holds that sideband. Cheaper: emitters and
    // scans are both visited below; collect emitters first.
    let mut emitters: Vec<(BridgeFilterSideband, usize)> = Vec::new();
    fn walk_emitters(p: &Arc<dyn ExecutionPlan>, out: &mut Vec<(BridgeFilterSideband, usize)>) {
        if let Some(em) = p.as_any().downcast_ref::<BuildSideBloomEmitterExec>() {
            out.push((em.sideband().clone(), em.target_col_idx()));
            for (ci, sb) in em.extra_targets() {
                out.push((sb.clone(), *ci));
            }
        }
        for c in p.children() {
            walk_emitters(c, out);
        }
    }
    walk_emitters(plan, &mut emitters);

    fn walk(
        p: &Arc<dyn ExecutionPlan>,
        cfg: &CascadeChainConfig,
        emitters: &[(BridgeFilterSideband, usize)],
        links: &mut Vec<Link>,
        targeted_cols: &mut HashSet<(usize, usize)>,
    ) {
        if let Some(scan) = p.as_any().downcast_ref::<EmatixFastParquetExec>() {
            // Record which columns existing wraps already target here.
            let mut sbs: Vec<&BridgeFilterSideband> = Vec::new();
            if let Some(sb) = scan.runtime_sideband() {
                sbs.push(sb);
            }
            sbs.extend(scan.extra_runtime_sidebands().iter());
            for sb in sbs {
                for (esb, col) in emitters.iter() {
                    if esb.ptr_eq(sb) {
                        targeted_cols.insert((arc_ptr(p), *col));
                    }
                }
            }
        }
        if let Some(hj) = p.as_any().downcast_ref::<HashJoinExec>() {
            use datafusion::common::JoinType;
            let type_ok = matches!(
                hj.join_type(),
                JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
            );
            // Bounded-build requirement (a): CollectLeft only.
            let mode_ok = matches!(hj.partition_mode(), PartitionMode::CollectLeft);
            // A pass-1 wrap on this join's build means it already emits;
            // wrapping again would double-publish.
            let already_wrapped = hj
                .left()
                .as_any()
                .downcast_ref::<BuildSideBloomEmitterExec>()
                .is_some();
            if type_ok && mode_ok && !already_wrapped {
                let build = hj.left();
                let probe = hj.right();
                let n_keys = hj.on().len();
                let build_rows = estimate_build_rows(build.as_ref());
                let build_scan_total = single_dim_scan_total(build.as_ref());
                let build_filtered = build_subtree_statically_filtered(build);
                for (le, re) in hj.on().iter() {
                    let (Some(lcol), Some(rcol)) = (
                        le.as_any().downcast_ref::<Column>(),
                        re.as_any().downcast_ref::<Column>(),
                    ) else {
                        continue;
                    };
                    let l_dt = build.schema().field(lcol.index()).data_type().clone();
                    let r_dt = probe.schema().field(rcol.index()).data_type().clone();
                    // i64-domain keys only (string chains are a future
                    // refinement; Q05-class chains are all integer FKs).
                    if !widens_to_i64(&l_dt) || !widens_to_i64(&r_dt) {
                        continue;
                    }
                    if let Some((scan_node, scan_typed, col_idx)) =
                        find_probe_scan_for_column(probe, rcol.name())
                    {
                        links.push(Link {
                            build_child: Arc::clone(build),
                            build_key_idx: lcol.index(),
                            n_keys,
                            build_rows,
                            build_scan_total,
                            build_filtered,
                            target_col_idx: col_idx,
                            target_rows: estimate_probe_scan_rows(&scan_typed),
                            target_occupied: scan_node
                                .as_any()
                                .downcast_ref::<EmatixFastParquetExec>()
                                .map(|s| s.runtime_sideband().is_some())
                                .unwrap_or(false),
                            target_scan_node: scan_node,
                        });
                    }
                }
            }
            let _ = cfg;
        }
        for c in p.children() {
            walk(c, cfg, emitters, links, targeted_cols);
        }
    }
    walk(plan, cfg, &emitters, links, targeted_cols);
}

/// Rewrite ONE link into the plan: find the join whose build child is
/// `link.build_child` (Arc identity), wrap that build with `emitter`,
/// and thread `sideband` into `link.target_scan_node` on the probe side
/// (primary slot if free, extra slot otherwise).
fn install_link(
    plan: Arc<dyn ExecutionPlan>,
    link: &Link,
    emitter: Arc<BuildSideBloomEmitterExec>,
    sideband: &BridgeFilterSideband,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let build_anchor = Arc::clone(&link.build_child);
    let target_anchor = Arc::clone(&link.target_scan_node);
    let sb = sideband.clone();
    let emitter_dyn: Arc<dyn ExecutionPlan> = emitter;
    plan.transform_up(|node| {
        let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        if !Arc::ptr_eq(hj.left(), &build_anchor) {
            return Ok(Transformed::no(node));
        }
        // Thread the sideband into the target scan on the probe side.
        let new_probe = hj
            .right()
            .clone()
            .transform_up(|n| {
                if Arc::ptr_eq(&n, &target_anchor) {
                    if let Some(scan) = n.as_any().downcast_ref::<EmatixFastParquetExec>() {
                        let new: Arc<dyn ExecutionPlan> = if scan.runtime_sideband().is_some() {
                            scan.with_extra_runtime_sideband(sb.clone())
                        } else {
                            scan.with_runtime_sideband(sb.clone())
                        };
                        return Ok(Transformed::yes(new));
                    }
                }
                Ok(Transformed::no(n))
            })
            .data()?;
        let new_join =
            Arc::clone(&node).with_new_children(vec![Arc::clone(&emitter_dyn), new_probe])?;
        Ok(Transformed::yes(new_join))
    })
    .data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("l9_chain_test_{}_{}", std::process::id(), name));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    /// Pass-1-inert rule (no Inner fires, no semi shapes in fixtures)
    /// carrying the given chain config — so every wrap the tests
    /// observe comes from the cascade pass.
    fn rule_with(cascade: CascadeChainConfig) -> EnableRuntimeBloomSidebandRule {
        EnableRuntimeBloomSidebandRule {
            min_probe_to_build_ratio: 1024,
            allow_inner_join: false,
            require_filtered_build: true,
            max_expected_keys_per_partition: 0,
            min_probe_proj_cols: 0,
            ndv_max_rows: 10_000_000,
            cascade,
        }
    }

    /// Chain fixture mirroring Q05's shape at miniature scale:
    ///   filt (filtered start dim) → supp (2-KEY middle join) → fact.
    ///
    /// Data is constructed so KEY-2 (nation) REJECTS rows that KEY-1's
    /// (suppkey) bloom passes: every f_supp is FK-contained in
    /// supp.s_supp, but half the fact rows carry a mismatched
    /// f_nation. A bloom that mis-encoded the pair (or dropped rows it
    /// must not) changes the result; the multi-key bloom being a
    /// SUPERSET filter on one key leaves it exact.
    fn write_chain_fixture() -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let filt = tmp_parquet("filt");
        let supp = tmp_parquet("supp");
        let fact = tmp_parquet("fact");
        // filt: nations {0,1} flagged 1 (kept), {2} flagged 0 (dropped).
        write_table_to_path(
            &filt,
            &[
                ("k_nation", ColumnData::I64(&[0, 1, 2])),
                ("k_flag", ColumnData::I64(&[1, 1, 0])),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        // supp: 10 suppliers, s_nation = s_supp % 3.
        let s_supp: Vec<i64> = (0..10).collect();
        let s_nation: Vec<i64> = s_supp.iter().map(|s| s % 3).collect();
        write_table_to_path(
            &supp,
            &[
                ("s_supp", ColumnData::I64(&s_supp)),
                ("s_nation", ColumnData::I64(&s_nation)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        // fact: 200 rows. f_supp FK-contained in s_supp (bloom on
        // key-1 passes everything); f_nation matches the supplier's
        // nation only on EVEN rows — key-2 must reject the odd ones.
        let f_supp: Vec<i64> = (0..200i64).map(|i| i % 10).collect();
        let f_nation: Vec<i64> = (0..200i64)
            .map(|i| {
                let s = i % 10;
                if i % 2 == 0 { s % 3 } else { (s + 1) % 3 }
            })
            .collect();
        let f_val: Vec<i64> = (0..200).collect();
        write_table_to_path(
            &fact,
            &[
                ("f_supp", ColumnData::I64(&f_supp)),
                ("f_nation", ColumnData::I64(&f_nation)),
                ("f_val", ColumnData::I64(&f_val)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        (filt, supp, fact)
    }

    const CHAIN_SQL: &str = "SELECT sub.f_val FROM filt \
         JOIN (SELECT s_nation, f_val FROM supp \
               JOIN fact ON s_supp = f_supp AND s_nation = f_nation) sub \
         ON k_nation = sub.s_nation \
         WHERE k_flag = 1";

    async fn run_chain_query(
        cascade: CascadeChainConfig,
        paths: &(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf),
    ) -> (Arc<dyn ExecutionPlan>, i64, usize) {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_physical_optimizer_rule(Arc::new(rule_with(cascade)))
            .build();
        let ctx = SessionContext::new_with_state(state);
        for (name, p) in [("filt", &paths.0), ("supp", &paths.1), ("fact", &paths.2)] {
            ctx.register_table(
                name,
                Arc::new(EmatixFastParquetTableProvider::try_new(p.to_string_lossy()).unwrap()),
            )
            .unwrap();
        }
        let df = ctx.sql(CHAIN_SQL).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let batches = df.collect().await.unwrap();
        let mut sum = 0i64;
        let mut rows = 0usize;
        for b in &batches {
            rows += b.num_rows();
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                sum += col.value(i);
            }
        }
        (plan, sum, rows)
    }

    fn count_emitters(plan: &Arc<dyn ExecutionPlan>) -> usize {
        fn walk(p: &Arc<dyn ExecutionPlan>, n: &mut usize) {
            if p.as_any()
                .downcast_ref::<BuildSideBloomEmitterExec>()
                .is_some()
            {
                *n += 1;
            }
            for c in p.children() {
                walk(c, n);
            }
        }
        let mut n = 0;
        walk(plan, &mut n);
        n
    }

    /// Collect every scan's (path, primary sideband, extra sidebands).
    #[allow(clippy::type_complexity)]
    fn scan_sidebands(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Vec<(
        String,
        Option<BridgeFilterSideband>,
        Vec<BridgeFilterSideband>,
    )> {
        fn walk(
            p: &Arc<dyn ExecutionPlan>,
            out: &mut Vec<(
                String,
                Option<BridgeFilterSideband>,
                Vec<BridgeFilterSideband>,
            )>,
        ) {
            if let Some(scan) = p.as_any().downcast_ref::<EmatixFastParquetExec>() {
                out.push((
                    scan.path().to_string(),
                    scan.runtime_sideband().cloned(),
                    scan.extra_runtime_sidebands().to_vec(),
                ));
            }
            for c in p.children() {
                walk(c, out);
            }
        }
        let mut v = Vec::new();
        walk(plan, &mut v);
        v
    }

    /// Multi-key bloom correctness: KEY-2 rejects rows KEY-1's bloom
    /// passes; the chain-on results must equal the chain-off results
    /// exactly (the bloom is a superset filter; the join enforces all
    /// keys). Also pins that the chain actually installed (2 emitters:
    /// filt→supp and supp→fact) and that the FACT scan carries the
    /// 2-key join's sideband.
    #[tokio::test]
    async fn multikey_chain_is_exact_and_wraps_fact_scan() {
        let paths = write_chain_fixture();
        let (off_plan, off_sum, off_rows) =
            run_chain_query(CascadeChainConfig::off(), &paths).await;
        assert_eq!(count_emitters(&off_plan), 0, "cascade=off must not wrap");
        // Ground truth by construction: even rows of fact match their
        // supplier's nation; kept nations are {0,1} → suppliers with
        // s_nation ∈ {0,1} = s_supp ∉ {2,5,8}. Even i with (i%10)%3≠2:
        // f_val sum over i ∈ {0,2,..,198} where (i%10) ∉ {2,5,8}.
        let expect_sum: i64 = (0..200i64)
            .filter(|i| i % 2 == 0 && (i % 10) % 3 != 2)
            .sum();
        let expect_rows = (0..200i64)
            .filter(|i| i % 2 == 0 && (i % 10) % 3 != 2)
            .count();
        assert_eq!((off_sum, off_rows), (expect_sum, expect_rows));

        let (on_plan, on_sum, on_rows) =
            run_chain_query(CascadeChainConfig::forced(), &paths).await;
        assert_eq!(
            count_emitters(&on_plan),
            2,
            "expected the 2-link chain's emitters:\n{on_plan:?}"
        );
        assert_eq!(
            (on_sum, on_rows),
            (off_sum, off_rows),
            "multi-key chain bloom must not change results"
        );
        // The fact scan must carry a sideband (the 2-key join's bloom).
        let fact_path = paths.2.to_string_lossy().to_string();
        let sbs = scan_sidebands(&on_plan);
        let fact = sbs
            .iter()
            .find(|(p, _, _)| *p == fact_path)
            .expect("fact scan present");
        assert!(
            fact.1.is_some(),
            "fact scan must carry the terminal sideband:\n{on_plan:?}"
        );
    }

    /// EMAT_MULTIKEY_BLOOM=0 — multi-key links refused: the chain has
    /// no terminal, so NOTHING installs (the region/nation-style
    /// prefix without the fact payoff is pure cost). Results unchanged.
    #[tokio::test]
    async fn multikey_off_installs_nothing() {
        let paths = write_chain_fixture();
        let cfg = CascadeChainConfig {
            multikey: Some(false),
            ..CascadeChainConfig::forced()
        };
        let (plan, sum, rows) = run_chain_query(cfg, &paths).await;
        let expect_sum: i64 = (0..200i64)
            .filter(|i| i % 2 == 0 && (i % 10) % 3 != 2)
            .sum();
        // The 2-key middle join is the only route to the fact scan —
        // with multi-key links refused the chain must not half-install.
        assert_eq!(
            count_emitters(&plan),
            0,
            "no half-chains under EMAT_MULTIKEY_BLOOM=0:\n{plan:?}"
        );
        assert_eq!(sum, expect_sum);
        assert!(rows > 0);
    }

    /// Tri-state e2e: AUTO on miniature data must leave the plan
    /// untouched (terminal scan far below the 20M floor) — the SF=1
    /// "plans untouched by construction" property.
    #[tokio::test]
    async fn auto_leaves_small_scale_untouched() {
        let paths = write_chain_fixture();
        let auto = CascadeChainConfig {
            cascade: None,
            ..CascadeChainConfig::off()
        };
        let (plan, sum, rows) = run_chain_query(auto, &paths).await;
        assert_eq!(
            count_emitters(&plan),
            0,
            "AUTO must not fire below the terminal floor:\n{plan:?}"
        );
        let expect_sum: i64 = (0..200i64)
            .filter(|i| i % 2 == 0 && (i % 10) % 3 != 2)
            .sum();
        assert_eq!(sum, expect_sum);
        assert!(rows > 0);
    }

    /// Q05-shape pin: region(filtered) → nation → supplier(2-key) →
    /// lineitem. The chain must thread (a) a nation-join bloom into the
    /// SUPPLIER scan's s_nationkey and (b) a supplier-join bloom into
    /// the LINEITEM scan's l_suppkey — the two blooms this lever
    /// exists to install. Values must match the cascade-off run.
    #[tokio::test]
    async fn q05_shape_installs_nation_and_supplier_blooms() {
        // Miniature Q05 star: 2 regions, 6 nations, 12 suppliers,
        // 30 customers, 60 orders, 600 lineitems.
        let region = tmp_parquet("q5_region");
        let nation = tmp_parquet("q5_nation");
        let supplier = tmp_parquet("q5_supplier");
        let customer = tmp_parquet("q5_customer");
        let orders = tmp_parquet("q5_orders");
        let lineitem = tmp_parquet("q5_lineitem");
        write_table_to_path(
            &region,
            &[
                ("r_regionkey", ColumnData::I64(&[0, 1])),
                ("r_flag", ColumnData::I64(&[1, 0])), // r_flag=1 ~ 'ASIA'
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let n_nationkey: Vec<i64> = (0..6).collect();
        let n_regionkey: Vec<i64> = n_nationkey.iter().map(|n| n % 2).collect();
        write_table_to_path(
            &nation,
            &[
                ("n_nationkey", ColumnData::I64(&n_nationkey)),
                ("n_regionkey", ColumnData::I64(&n_regionkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let s_suppkey: Vec<i64> = (0..12).collect();
        let s_nationkey: Vec<i64> = s_suppkey.iter().map(|s| s % 6).collect();
        write_table_to_path(
            &supplier,
            &[
                ("s_suppkey", ColumnData::I64(&s_suppkey)),
                ("s_nationkey", ColumnData::I64(&s_nationkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let c_custkey: Vec<i64> = (0..30).collect();
        let c_nationkey: Vec<i64> = c_custkey.iter().map(|c| c % 6).collect();
        write_table_to_path(
            &customer,
            &[
                ("c_custkey", ColumnData::I64(&c_custkey)),
                ("c_nationkey", ColumnData::I64(&c_nationkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let o_orderkey: Vec<i64> = (0..60).collect();
        let o_custkey: Vec<i64> = o_orderkey.iter().map(|o| o % 30).collect();
        write_table_to_path(
            &orders,
            &[
                ("o_orderkey", ColumnData::I64(&o_orderkey)),
                ("o_custkey", ColumnData::I64(&o_custkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let l_orderkey: Vec<i64> = (0..600i64).map(|i| i % 60).collect();
        let l_suppkey: Vec<i64> = (0..600i64).map(|i| i % 12).collect();
        let l_val: Vec<i64> = (0..600).collect();
        write_table_to_path(
            &lineitem,
            &[
                ("l_orderkey", ColumnData::I64(&l_orderkey)),
                ("l_suppkey", ColumnData::I64(&l_suppkey)),
                ("l_val", ColumnData::I64(&l_val)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        // Nested to reproduce Q05's right-deep dim chain: each dim is
        // the CollectLeft build of its join, the fact chain the probe.
        let sql = "SELECT sum(sub3.l_val) FROM region \
             JOIN (SELECT n_regionkey, l_val FROM nation \
                   JOIN (SELECT s_nationkey, l_val FROM supplier \
                         JOIN (SELECT c_nationkey, l_suppkey, l_val FROM customer \
                               JOIN orders ON c_custkey = o_custkey \
                               JOIN lineitem ON o_orderkey = l_orderkey) sub \
                         ON s_suppkey = sub.l_suppkey AND s_nationkey = sub.c_nationkey) sub2 \
                   ON n_nationkey = sub2.s_nationkey) sub3 \
             ON r_regionkey = sub3.n_regionkey \
             WHERE r_flag = 1";

        async fn run(
            cascade: CascadeChainConfig,
            sql: &str,
            tables: &[(&str, &std::path::Path)],
        ) -> (Arc<dyn ExecutionPlan>, i64) {
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_physical_optimizer_rule(Arc::new(rule_with(cascade)))
                .build();
            let ctx = SessionContext::new_with_state(state);
            for (name, p) in tables {
                ctx.register_table(
                    *name,
                    Arc::new(EmatixFastParquetTableProvider::try_new(p.to_string_lossy()).unwrap()),
                )
                .unwrap();
            }
            let df = ctx.sql(sql).await.unwrap();
            let plan = df.clone().create_physical_plan().await.unwrap();
            let batches = df.collect().await.unwrap();
            let sum = batches
                .iter()
                .flat_map(|b| {
                    let c = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int64Array>()
                        .unwrap()
                        .clone();
                    (0..c.len()).map(move |i| c.value(i)).collect::<Vec<_>>()
                })
                .sum::<i64>();
            (plan, sum)
        }
        let tables: Vec<(&str, &std::path::Path)> = vec![
            ("region", region.as_path()),
            ("nation", nation.as_path()),
            ("supplier", supplier.as_path()),
            ("customer", customer.as_path()),
            ("orders", orders.as_path()),
            ("lineitem", lineitem.as_path()),
        ];

        let (off_plan, off_sum) = run(CascadeChainConfig::off(), sql, &tables).await;
        assert_eq!(count_emitters(&off_plan), 0);
        let (on_plan, on_sum) = run(CascadeChainConfig::forced(), sql, &tables).await;
        assert_eq!(on_sum, off_sum, "chain must not change Q05-shape results");

        // Collect emitters (sideband, key idx) and scan sidebands.
        let mut emitters: Vec<BridgeFilterSideband> = Vec::new();
        fn walk_em(p: &Arc<dyn ExecutionPlan>, out: &mut Vec<BridgeFilterSideband>) {
            if let Some(em) = p.as_any().downcast_ref::<BuildSideBloomEmitterExec>() {
                out.push(em.sideband().clone());
            }
            for c in p.children() {
                walk_em(c, out);
            }
        }
        walk_em(&on_plan, &mut emitters);
        assert!(
            emitters.len() >= 3,
            "expected the 3-link chain's emitters (region→nation, \
             nation→supplier, supplier→lineitem), got {}:\n{on_plan:?}",
            emitters.len()
        );
        let sbs = scan_sidebands(&on_plan);
        let has_emitter_fed_sideband = |path: &std::path::Path| -> bool {
            let p = path.to_string_lossy().to_string();
            sbs.iter().any(|(sp, prim, extras)| {
                *sp == p
                    && (prim
                        .as_ref()
                        .map(|sb| emitters.iter().any(|e| e.ptr_eq(sb)))
                        .unwrap_or(false)
                        || extras
                            .iter()
                            .any(|sb| emitters.iter().any(|e| e.ptr_eq(sb))))
            })
        };
        assert!(
            has_emitter_fed_sideband(&supplier),
            "supplier scan must be pre-filtered by the nation-join bloom \
             (s_nationkey):\n{on_plan:?}"
        );
        assert!(
            has_emitter_fed_sideband(&lineitem),
            "lineitem scan must be pre-filtered by the supplier-join bloom \
             (l_suppkey):\n{on_plan:?}"
        );
    }

    /// Tri-state resolution table for the two levers + threshold
    /// resolution — pure, no env mutation (the
    /// `resolve_l9_ndv_max_rows` testing convention).
    #[test]
    fn cascade_config_tri_state_resolution() {
        // =0 → pass disabled entirely.
        let off = CascadeChainConfig::off();
        assert!(!off.enabled());

        // =1 → enabled with relaxed thresholds (SF=1 validation fires).
        let forced = CascadeChainConfig::forced();
        assert!(forced.enabled());
        assert_eq!(forced.resolved_min_terminal_rows(), 0);
        assert_eq!(forced.resolved_max_build_rows(), usize::MAX);

        // AUTO (unset) → enabled with the conservative structural
        // thresholds: SF=1 (6M lineitem) and SF=10 partsupp (8M) are
        // below the terminal floor; SF=10/SF=100 lineitem qualify.
        let auto = CascadeChainConfig {
            cascade: None,
            ..CascadeChainConfig::off()
        };
        assert!(auto.enabled());
        assert_eq!(auto.resolved_min_terminal_rows(), AUTO_MIN_TERMINAL_ROWS);
        assert_eq!(auto.resolved_max_build_rows(), AUTO_MAX_BUILD_ROWS);
        assert!(
            6_001_215 < auto.resolved_min_terminal_rows(),
            "SF=1 lineitem stays untouched"
        );
        assert!(
            8_000_000 < auto.resolved_min_terminal_rows(),
            "SF=10 partsupp stays untouched"
        );
        assert!(
            59_986_052 >= auto.resolved_min_terminal_rows(),
            "SF=10 lineitem qualifies"
        );
        // Builds: SF=100 supplier (1M) admitted, SF=100 part (20M) refused.
        assert!(1_000_000 <= auto.resolved_max_build_rows());
        assert!(20_000_000 > auto.resolved_max_build_rows());

        // Explicit numeric overrides win in both modes.
        let over = CascadeChainConfig {
            cascade: None,
            min_terminal_rows: Some(123),
            max_build_rows: Some(456),
            ..CascadeChainConfig::off()
        };
        assert_eq!(over.resolved_min_terminal_rows(), 123);
        assert_eq!(over.resolved_max_build_rows(), 456);

        // Terminal-apply: AUTO admits only composed terminals; forced
        // cascade admits bare ones; explicit tri-state wins both ways.
        assert!(
            forced.resolved_terminal_apply(),
            "forced => bare terminals admitted"
        );
        assert!(!auto.resolved_terminal_apply(), "AUTO => composed-only");
        assert!(
            !CascadeChainConfig {
                terminal_apply: Some(false),
                ..CascadeChainConfig::forced()
            }
            .resolved_terminal_apply(),
            "=0 wins over forced"
        );
        assert!(
            CascadeChainConfig {
                terminal_apply: Some(true),
                ..CascadeChainConfig::off()
            }
            .resolved_terminal_apply(),
            "=1 wins under AUTO/off resolution"
        );

        // EMAT_MULTIKEY_BLOOM: only an explicit =0 refuses.
        assert!(
            CascadeChainConfig {
                multikey: None,
                ..CascadeChainConfig::forced()
            }
            .multikey_allowed()
        );
        assert!(
            CascadeChainConfig {
                multikey: Some(true),
                ..CascadeChainConfig::forced()
            }
            .multikey_allowed()
        );
        assert!(
            !CascadeChainConfig {
                multikey: Some(false),
                ..CascadeChainConfig::forced()
            }
            .multikey_allowed()
        );
    }
}
