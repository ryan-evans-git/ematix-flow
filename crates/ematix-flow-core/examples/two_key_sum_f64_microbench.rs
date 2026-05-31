//! Lever #4 gate (per [[sigma-r2-rejected]] prescription):
//! cheap kernel-level A/B for 2-i64-key SUM(f64) at Q20-shape before
//! committing to a 2-3 week operator build.
//!
//! Q20 SF=10 stage profile: 9.1M rows, 5.44M distinct (l_partkey,
//! l_suppkey) groups. AggregateExec Partial + FinalPartitioned together
//! consume ~446 ms parallel compute (~32 ms equivalent single-thread).
//!
//! Four variants:
//!   1. std HashMap<(i64,i64), f64> — generic baseline
//!   2. Packed-i64 RobinHood — pack (l_partkey, l_suppkey) into a single
//!      i64; use existing RobinHoodI64F64
//!   3. Custom TwoKeyRH — fused 2-key Robin Hood (the full Lever #4
//!      build's hot loop in microcosm)
//!   4. DataFusion AggregateExec — actual ground-truth comparator (per
//!      Σ.R.2 memo prescription)
//!
//! Gate decision per the Σ.R.2 memo:
//!   - If variant 2 OR 3 beats variant 1 by ≥20% AND finishes the
//!     ingest within ~32 ms single-thread budget, proceed to build.
//!   - Otherwise reject — confirms the Σ.R.2 pattern recurs at compound
//!     keys and the full build won't beat DataFusion's split pipeline.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core \
//!     --example two_key_sum_f64_microbench

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::robin_hood_agg::RobinHoodI64F64;
use futures_util::TryStreamExt;

const N_ROWS: usize = 9_000_000;
const N_GROUPS: usize = 5_440_000;
const REPS: usize = 3;

/// Generate Q20-shaped (l_partkey, l_suppkey) pairs spanning ~5.44M
/// distinct groups, with f64 values mimicking l_quantity (1..50).
fn gen_q20_shape() -> (Vec<(i64, i64)>, Vec<f64>) {
    // Tune product space so 9M samples land near 5.44M distinct groups.
    // For N = 9M draws over P cells: expected distinct = P*(1-(1-1/P)^N).
    // Solving for distinct = 5.44M gives P ≈ 8M. We pick part_max=8192,
    // supp_max=1024 → product 8_388_608, mostly uniform draws.
    let part_max: i64 = 8192;
    let supp_max: i64 = 1024;
    let mut keys: Vec<(i64, i64)> = Vec::with_capacity(N_ROWS);
    let mut vals: Vec<f64> = Vec::with_capacity(N_ROWS);
    let mut x: u64 = 0xdead_beef_cafe_f00d;
    for i in 0..N_ROWS {
        x = (x ^ (i as u64)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 30)).wrapping_mul(0x94d0_49bb_1331_11eb);
        let p = ((x as i64).rem_euclid(part_max)) + 1;
        let s = (((x >> 19) as i64).rem_euclid(supp_max)) + 1;
        keys.push((p, s));
        vals.push(1.0 + ((x >> 11) & 0x3f) as f64);
    }
    // (No verify pass — it'd add ~9M HashMap inserts to setup time.)
    (keys, vals)
}

// ---------- Variant 1: std HashMap ----------
fn bench_hashmap(keys: &[(i64, i64)], vals: &[f64]) -> (f64, f64) {
    let t = Instant::now();
    let mut map: HashMap<(i64, i64), f64> = HashMap::with_capacity(N_GROUPS);
    for (k, v) in keys.iter().zip(vals.iter()) {
        *map.entry(*k).or_insert(0.0) += *v;
    }
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let n = map.len() as f64;
    std::hint::black_box(&map);
    (ms, n)
}

// ---------- Variant 2: pack (i64, i64) → i64, reuse existing RH ----------
//
// l_partkey ≤ 21 bits, l_suppkey ≤ 17 bits → pack into 38 bits of i64.
// Use a wider 64-bit pack so any (i64, i64) below 32 bits each is safe:
//   packed = (part << 32) | (supp & 0xFFFFFFFF)
// Caller validates fit at runtime; the cell budget at SF=10 keeps us
// well under 32 bits per side.
#[inline(always)]
fn pack(p: i64, s: i64) -> i64 {
    (p << 32) | (s & 0xFFFFFFFF)
}

fn bench_packed_rh(keys: &[(i64, i64)], vals: &[f64]) -> (f64, f64) {
    let t = Instant::now();
    // Pre-pack into a fresh Vec<i64> so the existing batch API works.
    let mut packed: Vec<i64> = Vec::with_capacity(keys.len());
    for (p, s) in keys {
        packed.push(pack(*p, *s));
    }
    let mut map = RobinHoodI64F64::with_capacity(N_GROUPS);
    map.insert_or_sum_batch(&packed, vals);
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let n = map.len() as f64;
    std::hint::black_box(&map);
    (ms, n)
}

fn bench_packed_rh_vec(keys: &[(i64, i64)], vals: &[f64]) -> (f64, f64) {
    let t = Instant::now();
    let mut packed: Vec<i64> = Vec::with_capacity(keys.len());
    for (p, s) in keys {
        packed.push(pack(*p, *s));
    }
    let mut map = RobinHoodI64F64::with_capacity(N_GROUPS);
    map.insert_or_sum_batch_vectorised(&packed, vals);
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let n = map.len() as f64;
    std::hint::black_box(&map);
    (ms, n)
}

// ---------- Variant 3: custom 2-key Robin Hood (Σ.R.2-style fused) ----------
//
// Minimal slot layout: { k1: i64, k2: i64, sum: f64, psl: u8, used: bool }
// Linear probing with Robin Hood. No fancy SIMD — just the inner loop
// the full build would use.
const RH_LOAD_PCT: usize = 50;

#[derive(Clone, Copy, Default)]
struct Slot {
    k1: i64,
    k2: i64,
    sum: f64,
    psl: i16, // -1 = empty
}

struct TwoKeyRH {
    slots: Vec<Slot>,
    mask: usize,
    used: usize,
}

impl TwoKeyRH {
    fn new(min_cap: usize) -> Self {
        let mut cap = 16;
        while cap < min_cap * 100 / RH_LOAD_PCT {
            cap *= 2;
        }
        let slots = vec![
            Slot {
                k1: 0,
                k2: 0,
                sum: 0.0,
                psl: -1,
            };
            cap
        ];
        Self {
            slots,
            mask: cap - 1,
            used: 0,
        }
    }

    #[inline]
    fn hash(k1: i64, k2: i64) -> u64 {
        // FxHash-ish mix.
        let mut x = (k1 as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        x ^= k2 as u64;
        x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x ^= x >> 30;
        x
    }

    fn insert_or_sum(&mut self, k1: i64, k2: i64, v: f64) {
        let mask = self.mask;
        let mut idx = (Self::hash(k1, k2) as usize) & mask;
        let mut psl: i16 = 0;
        let mut nk1 = k1;
        let mut nk2 = k2;
        let mut nv = v;
        loop {
            let slot = &mut self.slots[idx];
            if slot.psl < 0 {
                slot.k1 = nk1;
                slot.k2 = nk2;
                slot.sum = nv;
                slot.psl = psl;
                self.used += 1;
                return;
            }
            if slot.k1 == nk1 && slot.k2 == nk2 {
                slot.sum += nv;
                return;
            }
            if slot.psl < psl {
                // Robin Hood swap.
                let s_k1 = slot.k1;
                let s_k2 = slot.k2;
                let s_sum = slot.sum;
                let s_psl = slot.psl;
                slot.k1 = nk1;
                slot.k2 = nk2;
                slot.sum = nv;
                slot.psl = psl;
                nk1 = s_k1;
                nk2 = s_k2;
                nv = s_sum;
                psl = s_psl;
            }
            psl += 1;
            idx = (idx + 1) & mask;
        }
    }

    fn len(&self) -> usize {
        self.used
    }
}

fn bench_custom_two_key(keys: &[(i64, i64)], vals: &[f64]) -> (f64, f64) {
    let t = Instant::now();
    let mut map = TwoKeyRH::new(N_GROUPS);
    for ((k1, k2), v) in keys.iter().zip(vals.iter()) {
        map.insert_or_sum(*k1, *k2, *v);
    }
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let n = map.len() as f64;
    std::hint::black_box(&map);
    (ms, n)
}

// ---------- Variant 4: DataFusion AggregateExec (ground truth) ----------
//
// Build a single-partition MemTable from the (i64, i64, f64) buffers,
// run `SELECT sum(v) FROM t GROUP BY a, b` through DataFusion's actual
// physical plan. This is the comparator the Σ.R.2 memo prescribed.
//
// Single-threaded (target_partitions=1) so the comparison is fair to
// the single-thread microbench variants above.
fn build_mem_table(keys: &[(i64, i64)], vals: &[f64]) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]));
    let a: Vec<i64> = keys.iter().map(|(p, _)| *p).collect();
    let b: Vec<i64> = keys.iter().map(|(_, s)| *s).collect();
    let v: Vec<f64> = vals.to_vec();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(a)) as ArrayRef,
            Arc::new(Int64Array::from(b)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

async fn bench_datafusion(table: Arc<MemTable>) -> (f64, f64) {
    // Single-partition ctx so we measure single-thread AggregateExec.
    let cfg = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_table("t", table).unwrap();

    let t = Instant::now();
    let df = ctx
        .sql("SELECT a, b, sum(v) FROM t GROUP BY a, b")
        .await
        .unwrap();
    let stream = df.execute_stream().await.unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    std::hint::black_box(&batches);
    (ms, n as f64)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("Lever #4 microbench gate — Q20-shape 2-key SUM(f64)");
    println!("  rows={N_ROWS}  target_groups={N_GROUPS}  reps={REPS}");
    let (keys, vals) = gen_q20_shape();
    // Pre-build the MemTable once — its arrays are reused per rep.
    let table = build_mem_table(&keys, &vals);

    let mut hm = vec![];
    let mut prh_scalar = vec![];
    let mut prh_vec = vec![];
    let mut custom = vec![];
    let mut df = vec![];
    let mut hm_groups = 0.0;
    let mut prh_groups = 0.0;
    let mut custom_groups = 0.0;
    let mut df_groups = 0.0;

    for r in 0..REPS {
        eprintln!("rep {}", r + 1);
        let (a, ag) = bench_hashmap(&keys, &vals);
        hm.push(a);
        hm_groups = ag;
        let (b, bg) = bench_packed_rh(&keys, &vals);
        prh_scalar.push(b);
        prh_groups = bg;
        let (c, _) = bench_packed_rh_vec(&keys, &vals);
        prh_vec.push(c);
        let (d, dg) = bench_custom_two_key(&keys, &vals);
        custom.push(d);
        custom_groups = dg;
        let (e, eg) = bench_datafusion(Arc::clone(&table)).await;
        df.push(e);
        df_groups = eg;
    }

    let m_hm = median(hm.clone());
    let m_prh = median(prh_scalar.clone());
    let m_prh_vec = median(prh_vec.clone());
    let m_custom = median(custom.clone());
    let m_df = median(df.clone());

    println!();
    println!("Median per variant (lower is better):");
    println!(
        "  1. std HashMap                  : {:7.2} ms   groups={}",
        m_hm, hm_groups as i64
    );
    println!(
        "  2. packed-i64 RH (scalar)       : {:7.2} ms   groups={}",
        m_prh, prh_groups as i64
    );
    println!("  2v.packed-i64 RH (vectorised)   : {:7.2} ms", m_prh_vec);
    println!(
        "  3. custom 2-key RobinHood       : {:7.2} ms   groups={}",
        m_custom, custom_groups as i64
    );
    println!(
        "  4. DataFusion AggregateExec     : {:7.2} ms   groups={}  ← actual ground truth",
        m_df, df_groups as i64
    );
    println!();
    let best_rh = m_custom.min(m_prh_vec);
    let win_vs_hm = (m_hm - best_rh) / m_hm * 100.0;
    let win_vs_df = (m_df - best_rh) / m_df * 100.0;
    println!(
        "Best RH variant vs std HashMap   : {:+.1}%  (≥20% original gate)",
        win_vs_hm
    );
    println!(
        "Best RH variant vs DataFusion    : {:+.1}%  (≥20% Σ.R.2-prescribed gate)",
        win_vs_df
    );
    println!();
    println!("Q20 SF=10 Partial+Final agg compute ≈ 446 ms parallel (32 ms per-thread).");
    println!(
        "  ⇒ Per-thread ingest budget for 640k rows is ~32 ms; our microbench is ingest of 9M rows single-thread."
    );
    println!(
        "  ⇒ Per-thread equivalent for the custom RH = {:.1} ms (9M/640k scaling).",
        m_custom * 640_000.0 / 9_000_000.0
    );
}
