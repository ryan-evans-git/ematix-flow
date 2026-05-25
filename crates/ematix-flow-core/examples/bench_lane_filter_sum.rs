//! Σ.U.A microbench — Apache-Impala-style lane-parallel filter-sum
//! kernel vs the existing Cranelift JIT.
//!
//! Runs on the canonical Q06 shape (`FusedFilterAggSpec::q6`) but
//! the kernel itself is shape-agnostic: any spec with M predicate
//! clauses + one SUM-of-product aggregate goes through the same path.
//!
//! Output reports ns/row on a 1 M row synthetic batch for:
//!   1. scalar reference   — naive per-row loop, used as oracle
//!   2. lane-parallel      — the new kernel (this module)
//!   3. Cranelift JIT      — current production path
//!
//! The lane-parallel path uses no `unsafe { vintrinsic }` blocks and
//! no `#[cfg(target_arch)]` arms. LLVM auto-vectorises the fixed-size
//! lane arrays into NEON on aarch64 and SSE2/AVX2 on x86_64.

use std::time::Instant;

use ematix_flow_core::fused_jit::{FusedFilterAggSpec, Q6JitFn};
use ematix_flow_core::lane_filter_sum_kernel::LaneFilterSumKernel;

const N: usize = 1_000_000;
const TRIALS: usize = 50;

fn make_q6_fixture() -> (Vec<i32>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut sd = Vec::with_capacity(N);
    let mut d = Vec::with_capacity(N);
    let mut q = Vec::with_capacity(N);
    let mut e = Vec::with_capacity(N);
    for i in 0..N {
        sd.push(8000 + (i as i32 % 1300)); // ~1/3 pass [8766, 9131)
        d.push(0.04 + (i as f64 % 5.0) * 0.01); // 3/5 pass [0.05, 0.07]
        q.push(10.0 + (i as f64 % 30.0)); // ~half under 24
        e.push(100.0 + (i as f64));
    }
    (sd, d, q, e)
}

fn scalar_reference(
    sd: &[i32],
    d: &[f64],
    q: &[f64],
    e: &[f64],
    date_lo: i32,
    date_hi: i32,
    disc_lo: f64,
    disc_hi: f64,
    qty_hi: f64,
) -> f64 {
    let mut acc = 0.0f64;
    for i in 0..N {
        let pass = (sd[i] >= date_lo)
            & (sd[i] < date_hi)
            & (d[i] >= disc_lo)
            & (d[i] <= disc_hi)
            & (q[i] < qty_hi);
        let mask = if pass { 1.0 } else { 0.0 };
        acc += e[i] * d[i] * mask;
    }
    acc
}

fn time_loop(label: &str, mut f: impl FnMut() -> f64) -> (f64, f64) {
    // Warm up
    for _ in 0..3 {
        std::hint::black_box(f());
    }
    let mut samples = Vec::with_capacity(TRIALS);
    let mut chk = 0.0;
    for _ in 0..TRIALS {
        let t0 = Instant::now();
        let v = f();
        samples.push(t0.elapsed().as_nanos() as f64);
        chk += v;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_ns = samples[samples.len() / 2];
    let ns_per_row = median_ns / N as f64;
    println!(
        "  {label:<28}  {:>7.3} ms median   {ns_per_row:>5.2} ns/row   (check={chk:.2})",
        median_ns / 1_000_000.0,
    );
    (median_ns / 1_000_000.0, ns_per_row)
}

fn main() {
    println!("Σ.U.A — lane-parallel filter-sum kernel microbench");
    println!(
        "N = {N}, {TRIALS} trials, target arch = {}",
        std::env::consts::ARCH
    );
    println!();

    let (sd, d, q, e) = make_q6_fixture();

    let (ref_ms, _) = time_loop("scalar reference", || {
        scalar_reference(&sd, &d, &q, &e, 8766, 9131, 0.05, 0.07, 24.0)
    });

    // Lane-parallel kernel built from a generic FusedFilterAggSpec —
    // not Q06-specific.
    let spec = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
    let kernel = LaneFilterSumKernel::from_spec(&spec).expect("q6-shape supported");
    let ptrs = [
        sd.as_ptr() as *const u8,
        d.as_ptr() as *const u8,
        q.as_ptr() as *const u8,
        e.as_ptr() as *const u8,
    ];
    let (lane_ms, _) = time_loop("lane-parallel kernel", || unsafe {
        kernel.process(&ptrs, N)
    });

    // Cranelift JIT — current production path.
    let jit = Q6JitFn::try_build(8766, 9131, 0.05, 0.07, 24.0).expect("jit build");
    let (jit_ms, _) = time_loop("Cranelift JIT", || {
        let mut out = 0.0f64;
        unsafe {
            jit.run(
                N as i64,
                sd.as_ptr(),
                d.as_ptr(),
                q.as_ptr(),
                e.as_ptr(),
                &mut out as *mut f64,
            );
        }
        out
    });

    println!();
    println!(
        "speedup vs scalar:   lane {:.2}×   JIT {:.2}×",
        ref_ms / lane_ms,
        ref_ms / jit_ms,
    );
    println!("speedup lane vs JIT: {:.2}×", jit_ms / lane_ms);
}
