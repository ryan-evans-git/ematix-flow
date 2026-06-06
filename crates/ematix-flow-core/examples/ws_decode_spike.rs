//! PV.M.5 Phase-0 §3.2 — work-stealing scan-decode SPIKE (the GO/NO-GO kill-gate).
//!
//! Decodes the Q15 SCALAR-stage columns of REAL SF=10 lineitem
//! (l_shipdate=10 filter, l_extendedprice=5, l_discount=6 payload) and SUMs
//! extendedprice*(1-discount) over shipdate∈[1996-01-01,1996-04-01) survivors —
//! three ways that differ ONLY in decode SCHEDULING (identical dense+masked
//! primitives, identical `nthreads`, identical per-RG masked-sum). This isolates
//! "is the 51→36ms slack recoverable by work-stealing?" from decode CPU/codec.
//!
//!   Arm A — STATIC round-robin (production model). Thread t owns RGs ≡ t (mod
//!           nthreads); decodes each owned RG's 3 columns sequentially (budget=1).
//!           A straggling RG strands its thread's core — the hypothesized waste.
//!   Arm C — RG WORK-STEAL. Shared atomic cursor over RGs; `nthreads` workers
//!           steal the next whole-RG (3-col) unit. Isolates "shared bounded pool"
//!           from "sub-RG granularity". C≈A ⇒ RG-level stealing is irrelevant.
//!   Arm B — COL-SPLIT WORK-STEAL (finer than RG). Shared cursor over (rg,col)
//!           units; the worker that completes an RG's 3rd column computes its
//!           partial. Tests whether splitting the column-sequential decode into
//!           independently-stealable units recovers more than whole-RG stealing.
//!           (This is the buildable proxy for true page-range morsels: the finest
//!           unit here is one column-chunk of one RG — extprice ≈ 5ms. Page-range
//!           would subdivide THAT further. If col-split ≈ C, page-range is very
//!           unlikely to clear the gate; if col-split clears it, Phase 1 builds
//!           the real page-range kernel and should do at least as well.)
//!
//! DETERMINISM (R-det): every arm fills `partials[rg]` (each RG's masked-sum in
//! row order); the final SUM reduces `partials` in RG order → byte-identical
//! across arms regardless of steal order (Q15 ships DedupeAggregateForFloat-
//! Determinism precisely because f64 SUM is order-sensitive). Asserted each trial.
//!
//! KILL-GATE (§3.3): GO iff best steal-arm ≤ ~42 ms median (recovers ≥ half the
//! 51→36 gap), paired sign test p<0.05 vs Arm A, AND the win shows up as higher
//! parallel-efficiency (CPU/wall), not a timing artifact.
//!
//! Usage:
//!   TPCH_DATA=examples/tpch/data/sf10/lineitem.parquet TRIALS=25 \
//!     cargo run --release -p ematix-flow-core --example ws_decode_spike

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Instant;

use ematix_flow_core::ematix_parquet_bridge::{masked_decode_f64, open_cached};
use ematix_parquet_codec::read::read_column_i32;
use ematix_parquet_io::ParquetFile;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// TPC-H lineitem column indices.
const SHIP: usize = 10; // l_shipdate (INT32 days-since-epoch)
const EXT: usize = 5; //  l_extendedprice (DOUBLE)
const DISC: usize = 6; // l_discount (DOUBLE)

/// Per-RG SCALAR-stage decode + masked-sum, exactly as production does it:
/// decode shipdate → build row bitmap → masked-decode the two f64 payloads
/// (survivors only, same row order) → Σ extprice*(1-discount). Row-order ⇒
/// the per-RG partial is deterministic. Returns (partial, decode_busy_ns).
#[inline]
fn decode_rg_partial(file: &ParquetFile, rg: usize, lo: i32, hi: i32) -> (f64, u64) {
    let t0 = Instant::now();
    let ship = read_column_i32(file, rg, SHIP).expect("shipdate decode");
    let n = ship.len();
    let mut bm = vec![0u8; n.div_ceil(8)];
    for (i, &d) in ship.iter().enumerate() {
        if d >= lo && d < hi {
            bm[i >> 3] |= 1 << (i & 7);
        }
    }
    let ext = masked_decode_f64(file, rg, EXT, &bm).expect("extprice masked decode");
    let disc = masked_decode_f64(file, rg, DISC, &bm).expect("discount masked decode");
    debug_assert_eq!(ext.len(), disc.len());
    let mut s = 0.0f64;
    for i in 0..ext.len() {
        s += ext[i] * (1.0 - disc[i]);
    }
    (s, t0.elapsed().as_nanos() as u64)
}

/// Arm B work unit. Shipdate is decoded first (builds the mask + enqueues the
/// two masked payload units for the same RG); the payloads masked-decode the
/// survivors. Same decode method (masked) as Arms A/C — only the granularity
/// differs (one column-chunk per unit vs one whole RG).
enum Unit {
    Ship(usize),
    Ext(usize),
    Disc(usize),
}

// ---- Arm A: static round-robin (production) -------------------------------
fn arm_static(file: &ParquetFile, n_rg: usize, nt: usize, lo: i32, hi: i32) -> (Vec<f64>, f64) {
    let partials: Vec<AtomicU64> = (0..n_rg).map(|_| AtomicU64::new(0)).collect();
    let busy = AtomicU64::new(0);
    thread::scope(|s| {
        for t in 0..nt {
            let partials = &partials;
            let busy = &busy;
            s.spawn(move || {
                let mut b = 0u64;
                let mut rg = t;
                while rg < n_rg {
                    let (p, ns) = decode_rg_partial(file, rg, lo, hi);
                    b += ns;
                    partials[rg].store(p.to_bits(), Ordering::Relaxed);
                    rg += nt;
                }
                busy.fetch_add(b, Ordering::Relaxed);
            });
        }
    });
    let out = partials
        .iter()
        .map(|a| f64::from_bits(a.load(Ordering::Relaxed)))
        .collect();
    (out, busy.load(Ordering::Relaxed) as f64 / 1e6)
}

// ---- Arm C: RG work-steal -------------------------------------------------
fn arm_rg_steal(file: &ParquetFile, n_rg: usize, nt: usize, lo: i32, hi: i32) -> (Vec<f64>, f64) {
    let partials: Vec<AtomicU64> = (0..n_rg).map(|_| AtomicU64::new(0)).collect();
    let busy = AtomicU64::new(0);
    let cursor = AtomicUsize::new(0);
    thread::scope(|s| {
        for _ in 0..nt {
            let partials = &partials;
            let busy = &busy;
            let cursor = &cursor;
            s.spawn(move || {
                let mut b = 0u64;
                loop {
                    let rg = cursor.fetch_add(1, Ordering::Relaxed);
                    if rg >= n_rg {
                        break;
                    }
                    let (p, ns) = decode_rg_partial(file, rg, lo, hi);
                    b += ns;
                    partials[rg].store(p.to_bits(), Ordering::Relaxed);
                }
                busy.fetch_add(b, Ordering::Relaxed);
            });
        }
    });
    let out = partials
        .iter()
        .map(|a| f64::from_bits(a.load(Ordering::Relaxed)))
        .collect();
    (out, busy.load(Ordering::Relaxed) as f64 / 1e6)
}

// ---- Arm B: col-split work-steal (finer than RG, SAME masked decode) ------
// Shared `Mutex<VecDeque<Unit>>` work queue seeded with all (rg,Ship) units.
// A worker that decodes Ship(r) builds the mask, stores it, and enqueues
// Ext(r)+Disc(r) (dynamic enqueue — no shipdate→payload barrier; an RG's
// payloads become stealable the instant its mask is ready). The worker that
// completes an RG's 2nd payload computes that RG's masked-sum in row order
// (⇒ deterministic, byte-identical to Arms A/C). The finest unit is one
// extprice column-chunk (~5.5ms) vs Arm C's whole-RG (~9.4ms) — this is the
// buildable proxy for true page-range morsels.
fn arm_col_steal(file: &ParquetFile, n_rg: usize, nt: usize, lo: i32, hi: i32) -> (Vec<f64>, f64) {
    let q: Mutex<VecDeque<Unit>> = Mutex::new((0..n_rg).map(Unit::Ship).collect());
    let masks: Vec<OnceLock<Vec<u8>>> = (0..n_rg).map(|_| OnceLock::new()).collect();
    let ext_s: Vec<OnceLock<Vec<f64>>> = (0..n_rg).map(|_| OnceLock::new()).collect();
    let disc_s: Vec<OnceLock<Vec<f64>>> = (0..n_rg).map(|_| OnceLock::new()).collect();
    let pay_done: Vec<AtomicUsize> = (0..n_rg).map(|_| AtomicUsize::new(0)).collect();
    let partials: Vec<AtomicU64> = (0..n_rg).map(|_| AtomicU64::new(0)).collect();
    let remaining = AtomicUsize::new(3 * n_rg);
    let busy = AtomicU64::new(0);
    thread::scope(|sc| {
        for _ in 0..nt {
            let q = &q;
            let masks = &masks;
            let ext_s = &ext_s;
            let disc_s = &disc_s;
            let pay_done = &pay_done;
            let partials = &partials;
            let remaining = &remaining;
            let busy = &busy;
            // Completion: when an RG's 2nd payload lands, masked-sum it.
            let finish = move |r: usize, b: &mut u64| {
                if pay_done[r].fetch_add(1, Ordering::AcqRel) + 1 == 2 {
                    let t1 = Instant::now();
                    let e = ext_s[r].get().unwrap();
                    let d = disc_s[r].get().unwrap();
                    let mut s = 0.0f64;
                    for i in 0..e.len() {
                        s += e[i] * (1.0 - d[i]);
                    }
                    *b += t1.elapsed().as_nanos() as u64;
                    partials[r].store(s.to_bits(), Ordering::Relaxed);
                }
            };
            sc.spawn(move || {
                let mut b = 0u64;
                loop {
                    let unit = q.lock().unwrap().pop_front();
                    let unit = match unit {
                        Some(u) => u,
                        None => {
                            if remaining.load(Ordering::Acquire) == 0 {
                                break;
                            }
                            std::hint::spin_loop();
                            continue;
                        }
                    };
                    match unit {
                        Unit::Ship(r) => {
                            let t0 = Instant::now();
                            let ship = read_column_i32(file, r, SHIP).expect("ship");
                            let n = ship.len();
                            let mut bm = vec![0u8; n.div_ceil(8)];
                            for (i, &d) in ship.iter().enumerate() {
                                if d >= lo && d < hi {
                                    bm[i >> 3] |= 1 << (i & 7);
                                }
                            }
                            b += t0.elapsed().as_nanos() as u64;
                            let _ = masks[r].set(bm);
                            {
                                let mut g = q.lock().unwrap();
                                g.push_back(Unit::Ext(r));
                                g.push_back(Unit::Disc(r));
                            }
                            remaining.fetch_sub(1, Ordering::AcqRel);
                        }
                        Unit::Ext(r) => {
                            let bm = masks[r].get().expect("mask before ext");
                            let t0 = Instant::now();
                            let e = masked_decode_f64(file, r, EXT, bm).expect("ext masked");
                            b += t0.elapsed().as_nanos() as u64;
                            let _ = ext_s[r].set(e);
                            finish(r, &mut b);
                            remaining.fetch_sub(1, Ordering::AcqRel);
                        }
                        Unit::Disc(r) => {
                            let bm = masks[r].get().expect("mask before disc");
                            let t0 = Instant::now();
                            let dv = masked_decode_f64(file, r, DISC, bm).expect("disc masked");
                            b += t0.elapsed().as_nanos() as u64;
                            let _ = disc_s[r].set(dv);
                            finish(r, &mut b);
                            remaining.fetch_sub(1, Ordering::AcqRel);
                        }
                    }
                }
                busy.fetch_add(b, Ordering::Relaxed);
            });
        }
    });
    let out = partials
        .iter()
        .map(|a| f64::from_bits(a.load(Ordering::Relaxed)))
        .collect();
    (out, busy.load(Ordering::Relaxed) as f64 / 1e6)
}

// ---- stats helpers --------------------------------------------------------
fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Two-sided sign-test p-value: prob of `wins`-or-more-extreme out of `n` under
/// p=0.5. Exact binomial tails (n is small).
fn sign_test_p(wins: usize, n: usize) -> f64 {
    if n == 0 {
        return 1.0;
    }
    let mut coeff = 1.0f64; // C(n,0)
    let total = 2f64.powi(n as i32);
    let k_lo = wins.min(n - wins);
    let k_hi = wins.max(n - wins);
    let mut tail = 0.0f64;
    for k in 0..=n {
        let term = coeff; // C(n,k)
        if k <= k_lo || k >= k_hi {
            tail += term;
        }
        // advance C(n,k) -> C(n,k+1)
        coeff = coeff * (n - k) as f64 / (k + 1) as f64;
    }
    (tail / total).min(1.0)
}

fn sum_rg(p: &[f64]) -> f64 {
    p.iter().sum()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("TPCH_DATA")
        .unwrap_or_else(|_| "examples/tpch/data/sf10/lineitem.parquet".to_string());
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);
    let nt: usize = std::env::var("NTHREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| thread::available_parallelism().map(|n| n.get()).ok())
        .unwrap_or(14);
    // shipdate ∈ [1996-01-01, 1996-04-01) as days-since-epoch.
    let lo: i32 = std::env::var("LO")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9496);
    let hi: i32 = std::env::var("HI")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9587);

    let file = open_cached(Path::new(&path))?;
    let md = file.cached_metadata().map_err(|e| format!("meta: {e}"))?;
    let n_rg = md.row_groups.len();

    println!(
        "PV.M.5 §3.2 work-steal decode spike — {path}\n  row_groups={n_rg} nthreads={nt} trials={trials}  shipdate∈[{lo},{hi})"
    );

    // Selectivity sanity check on RG0 (guards wrong physical type / thresholds).
    {
        let ship0 = read_column_i32(&file, 0, SHIP)?;
        let set = ship0.iter().filter(|&&d| d >= lo && d < hi).count();
        println!(
            "  RG0 sanity: {} / {} rows pass ({:.2}%) — expect ≈3.6%",
            set,
            ship0.len(),
            100.0 * set as f64 / ship0.len().max(1) as f64
        );
    }

    // Warmup: prime OS page cache + any lazy file state, all three paths.
    let (wa, _) = arm_static(&file, n_rg, nt, lo, hi);
    let (wc, _) = arm_rg_steal(&file, n_rg, nt, lo, hi);
    let (wb, _) = arm_col_steal(&file, n_rg, nt, lo, hi);
    let ref_sum = sum_rg(&wa);
    assert!(
        (sum_rg(&wc) - ref_sum).abs() <= 0.0 && (sum_rg(&wb) - ref_sum).abs() <= 0.0,
        "warmup SUM mismatch: A={ref_sum} C={} B={}",
        sum_rg(&wc),
        sum_rg(&wb)
    );
    println!("  warmup OK — all arms byte-identical SUM = {ref_sum:.4}\n");

    let (mut a_ms, mut c_ms, mut b_ms) = (Vec::new(), Vec::new(), Vec::new());
    let (mut a_eff, mut c_eff, mut b_eff) = (Vec::new(), Vec::new(), Vec::new());
    let (mut c_wins, mut b_wins, mut b_vs_c) = (0usize, 0usize, 0usize);

    for _ in 0..trials {
        // Interleave A,C,B each trial so machine drift hits all arms equally.
        let t = Instant::now();
        let (pa, abusy) = arm_static(&file, n_rg, nt, lo, hi);
        let a = t.elapsed().as_secs_f64() * 1e3;

        let t = Instant::now();
        let (pc, cbusy) = arm_rg_steal(&file, n_rg, nt, lo, hi);
        let c = t.elapsed().as_secs_f64() * 1e3;

        let t = Instant::now();
        let (pb, bbusy) = arm_col_steal(&file, n_rg, nt, lo, hi);
        let b = t.elapsed().as_secs_f64() * 1e3;

        // Correctness: byte-identical SUM (determinism guard).
        let (sa, sc, sb) = (sum_rg(&pa), sum_rg(&pc), sum_rg(&pb));
        assert!(
            sc.to_bits() == sa.to_bits() && sb.to_bits() == sa.to_bits(),
            "SUM mismatch A={sa} C={sc} B={sb}"
        );

        a_ms.push(a);
        c_ms.push(c);
        b_ms.push(b);
        a_eff.push(abusy / (a * nt as f64)); // parallel efficiency in [0,1]
        c_eff.push(cbusy / (c * nt as f64));
        b_eff.push(bbusy / (b * nt as f64));
        if c < a {
            c_wins += 1;
        }
        if b < a {
            b_wins += 1;
        }
        if b < c {
            b_vs_c += 1;
        }
    }

    let (am, cm, bm_) = (median(a_ms), median(c_ms), median(b_ms));
    let (ae, ce, be) = (median(a_eff), median(c_eff), median(b_eff));
    println!("=== medians over {trials} trials (nthreads={nt}) ===");
    println!("Arm A  static   : {am:6.1} ms   par-eff {:.0}%", ae * 100.0);
    println!(
        "Arm C  rg-steal : {cm:6.1} ms   par-eff {:.0}%   vs A {:+.1}%  ({c_wins}/{trials} wins, sign-p {:.4})",
        ce * 100.0,
        100.0 * (cm - am) / am,
        sign_test_p(c_wins, trials)
    );
    println!(
        "Arm B  col-steal: {bm_:6.1} ms   par-eff {:.0}%   vs A {:+.1}%  ({b_wins}/{trials} wins, sign-p {:.4})   vs C {:+.1}% ({b_vs_c}/{trials})",
        be * 100.0,
        100.0 * (bm_ - am) / am,
        sign_test_p(b_wins, trials),
        100.0 * (bm_ - cm) / cm,
    );

    // Kill-gate §3.3.
    let best = cm.min(bm_);
    let best_arm = if bm_ < cm { "B" } else { "C" };
    let best_wins = if bm_ < cm { b_wins } else { c_wins };
    let best_eff = if bm_ < cm { be } else { ce };
    let p = sign_test_p(best_wins, trials);
    println!("\n--- KILL-GATE (§3.3): best steal-arm = {best_arm} @ {best:.1} ms ---");
    let verdict = if best <= 42.0 && p < 0.05 && best_eff > ae + 0.05 {
        "GO — recovers ≥half the 51→36 gap, p<0.05, attributed to higher par-eff → proceed to §4 architecture"
    } else if best <= 42.0 && p < 0.05 {
        "GO? — clears 42ms & p<0.05 but par-eff gain is weak; inspect for a timing artifact before committing"
    } else if best <= 51.0 {
        "MARGINAL/NO-GO — a <18% win that needs a shared pool fighting DF's per-partition model isn't worth it; record the number"
    } else {
        "NO-GO — thesis FALSIFIED: sub-RG/RG stealing does not beat the coarse path; Polars edge is per-byte decode or a tokio-can't-express overlap (§7 redirect)"
    };
    println!("VERDICT: {verdict}");
    Ok(())
}
