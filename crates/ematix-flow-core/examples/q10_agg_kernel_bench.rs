//! Q10 aggregate-kernel Phase-0 microbench (KILL-GATE for the FD-aware agg kernel).
//!
//! DuckDB profiling (rule #1) overturned the FD-reduction idea: DuckDB groups Q10 by
//! the SAME 7 cols ematix does, but its HASH_GROUP_BY is ~3× cheaper (1.65 vs 4.94
//! CPU-s) — the 4.86s of ematix cost is DataFusion encoding 5 WIDE STRING cols into
//! the group key for every one of ~11.6M input rows. The 6 non-key cols are FD on
//! c_custkey, so a hand-rolled kernel can hash ONLY the i64 c_custkey, carry the 6
//! dependents by FIRST-ROW-INDEX, and gather them once at finalize — never touching
//! the wide strings in the hot loop. (The Q10.WS.0 / q10_fd_spike `first_value` arm
//! was slow because of DataFusion's GENERIC accumulator; index+gather is untested.)
//!
//! KILL-GATE: compare TOTAL CPU-seconds (getrusage, thread-count-independent work) of
//!   ArmA = DataFusion's real 7-col group-by  vs  ArmB = the hand-rolled kernel.
//! If ArmB is not materially cheaper, STOP (no operator). If it is, build the operator.
//!
//!   ROWS (default 11_640_000), GROUPS (3_880_000), TRIALS (5), WARMUPS (2)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fd_aggregate_exec::FdAggregateExec;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) } != 0 {
        return f64::NAN;
    }
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(u.ru_utime) + tv(u.ru_stime)
}
fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

const NATIONS: &[&str] = &[
    "ALGERIA",
    "ARGENTINA",
    "BRAZIL",
    "CANADA",
    "EGYPT",
    "ETHIOPIA",
    "FRANCE",
    "GERMANY",
    "INDIA",
    "INDONESIA",
    "IRAN",
    "IRAQ",
    "JAPAN",
    "JORDAN",
    "KENYA",
    "MOROCCO",
    "MOZAMBIQUE",
    "PERU",
    "CHINA",
    "ROMANIA",
    "SAUDI ARABIA",
    "VIETNAM",
    "RUSSIA",
    "UNITED KINGDOM",
    "UNITED STATES",
];

/// Build one big batch of `rows` rows over `groups` distinct customers (FD: every
/// dependent col is a deterministic function of c_custkey), mirroring Q10's
/// post-join group-by input (i64 key + 5 wide strings + n_name + f64 revenue).
fn gen_data(rows: usize, groups: usize) -> (RecordBatch, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("n_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
        Field::new("revenue", DataType::Float64, false),
    ]));
    // Per-group (FD) dependent values, built once.
    let mut g_name = Vec::with_capacity(groups);
    let mut g_acct = Vec::with_capacity(groups);
    let mut g_phone = Vec::with_capacity(groups);
    let mut g_nation = Vec::with_capacity(groups);
    let mut g_addr = Vec::with_capacity(groups);
    let mut g_comment = Vec::with_capacity(groups);
    for g in 0..groups {
        g_name.push(format!("Customer#{g:09}"));
        g_acct.push((g % 100000) as f64 / 100.0);
        g_phone.push(format!(
            "{:02}-{:03}-{:03}-{:04}",
            g % 30 + 10,
            g % 900,
            g % 900,
            g % 9000
        ));
        g_nation.push(NATIONS[g % NATIONS.len()]);
        // ~25-char address, ~73-char comment (TPC-H-ish widths).
        g_addr.push(format!("{:0<25}", format!("addr{g}")));
        g_comment.push(format!(
            "{:0<73}",
            format!("comment for customer {g} with some filler text about orders")
        ));
    }
    let mut ck = Vec::with_capacity(rows);
    let mut name = Vec::with_capacity(rows);
    let mut acct = Vec::with_capacity(rows);
    let mut phone = Vec::with_capacity(rows);
    let mut nation = Vec::with_capacity(rows);
    let mut addr = Vec::with_capacity(rows);
    let mut comment = Vec::with_capacity(rows);
    let mut rev = Vec::with_capacity(rows);
    for i in 0..rows {
        let g = i % groups; // each group ~rows/groups occurrences, FD-consistent
        ck.push(g as i64);
        name.push(g_name[g].as_str());
        acct.push(g_acct[g]);
        phone.push(g_phone[g].as_str());
        nation.push(g_nation[g]);
        addr.push(g_addr[g].as_str());
        comment.push(g_comment[g].as_str());
        rev.push(((i % 1000) as f64) * 1.5 + 1.0);
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ck)),
            Arc::new(StringArray::from(name)),
            Arc::new(Float64Array::from(acct)),
            Arc::new(StringArray::from(phone)),
            Arc::new(StringArray::from(nation)),
            Arc::new(StringArray::from(addr)),
            Arc::new(StringArray::from(comment)),
            Arc::new(Float64Array::from(rev)),
        ],
    )
    .unwrap();
    (batch, schema)
}

/// ArmB: hand-rolled FD-exploit kernel. Hash ONLY i64 c_custkey; carry the 6
/// dependents by first-row index; gather once at finalize. Returns (groups, sum).
fn arm_b(batch: &RecordBatch) -> (usize, f64) {
    let ck = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rev = batch
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let n = ck.len();
    let mut map: HashMap<i64, u32> = HashMap::with_capacity(4_000_000);
    let mut sums: Vec<f64> = Vec::with_capacity(4_000_000);
    let mut first: Vec<u32> = Vec::with_capacity(4_000_000);
    // Hot loop: i64 hash only — no string touch.
    for i in 0..n {
        let k = ck.value(i);
        let gid = *map.entry(k).or_insert_with(|| {
            sums.push(0.0);
            first.push(i as u32);
            (sums.len() - 1) as u32
        });
        sums[gid as usize] += rev.value(i);
    }
    // Finalize: gather the 6 dependent cols + custkey at the first-row indices.
    let idx = UInt32Array::from(first.clone());
    let _ck_out = take(batch.column(0), &idx, None).unwrap();
    for c in [1usize, 3, 4, 5, 6] {
        let _ = take(batch.column(c), &idx, None).unwrap();
    }
    let _acct_out = take(batch.column(2), &idx, None).unwrap();
    let total: f64 = sums.iter().sum();
    (sums.len(), total)
}

/// ArmB|| : parallel FD-exploit kernel. Hash-partition rows by c_custkey into
/// `nparts` (power of 2) so every custkey lands in exactly ONE partition (no
/// cross-partition merge), run an independent per-partition kernel on each thread,
/// then concat + finalize-gather. Mirrors RepartitionExec + parallel AggregateExec.
fn arm_b_parallel(batch: &RecordBatch, nparts: usize) -> (usize, f64) {
    let ck = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rev = batch
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let n = ck.len();
    let mask = nparts - 1;
    let hashc = 0x9E37_79B9_7F4A_7C15u64;
    // Phase 1 — PARALLEL scatter: T threads each route their row-range into T×nparts
    // thread-local index buffers (no shared writes, no merge-concat — partition p's
    // rows are just the T slices buffers[*][p]).
    let tscat = nparts;
    let chunk = n.div_ceil(tscat);
    let buffers: Vec<Vec<Vec<u32>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..tscat)
            .map(|ti| {
                s.spawn(move || {
                    let lo = ti * chunk;
                    let hi = ((ti + 1) * chunk).min(n);
                    let mut local: Vec<Vec<u32>> = (0..nparts)
                        .map(|_| Vec::with_capacity(chunk / nparts + 64))
                        .collect();
                    for i in lo..hi {
                        let h = (ck.value(i) as u64).wrapping_mul(hashc);
                        local[((h >> 32) as usize) & mask].push(i as u32);
                    }
                    local
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    // Phase 2 — PARALLEL per-partition: each partition (disjoint custkeys, no merge)
    // builds its i64 hash table + sum, then gathers its OWN output (take on the global
    // columns with its first-row indices). No final concat — a real partitioned
    // operator emits one stream per partition.
    let counts: Vec<usize> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nparts)
            .map(|p| {
                let slices: Vec<&Vec<u32>> = buffers.iter().map(|b| &b[p]).collect();
                s.spawn(move || {
                    let cap: usize = slices.iter().map(|v| v.len()).sum();
                    let mut map: HashMap<i64, u32> = HashMap::with_capacity(cap);
                    let mut sums: Vec<f64> = Vec::new();
                    let mut first: Vec<u32> = Vec::new();
                    for sl in &slices {
                        for &ri in sl.iter() {
                            let k = ck.value(ri as usize);
                            let gid = *map.entry(k).or_insert_with(|| {
                                sums.push(0.0);
                                first.push(ri);
                                (sums.len() - 1) as u32
                            });
                            sums[gid as usize] += rev.value(ri as usize);
                        }
                    }
                    let idx = UInt32Array::from(first);
                    for c in [0usize, 1, 2, 3, 4, 5, 6] {
                        let _ = take(batch.column(c), &idx, None).unwrap();
                    }
                    sums.len()
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let groups: usize = counts.iter().sum();
    (groups, 0.0)
}

/// ArmB||-composite : the Q10-REALISTIC FD key. q10_fd_probe verified that c_custkey
/// does NOT functionally determine n_name (nation col), so the FD-minimal group key is
/// {c_custkey, n_name}. This hashes the i64 c_custkey + the SHORT n_name (col 4, ~15
/// bytes, 25 NDV) per row and gathers the 5 wide customer payload cols at finalize —
/// measuring whether the win survives adding n_name to the key vs the i64-only arm.
fn arm_b_composite_parallel(batch: &RecordBatch, nparts: usize) -> (usize, f64) {
    let ck = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let nname = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rev = batch
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let n = ck.len();
    let mask = nparts - 1;
    let hashc = 0x9E37_79B9_7F4A_7C15u64;
    #[inline]
    fn fnv1a(b: &[u8]) -> u64 {
        let mut h = 0xcbf2_9ce4_8422_2325u64;
        for &x in b {
            h ^= x as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }
    let tscat = nparts;
    let chunk = n.div_ceil(tscat);
    // Scatter by hash(ck) — FD means n_name is constant per ck, so each composite
    // {ck,n_name} group still lands in exactly one partition.
    let buffers: Vec<Vec<Vec<u32>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..tscat)
            .map(|ti| {
                s.spawn(move || {
                    let lo = ti * chunk;
                    let hi = ((ti + 1) * chunk).min(n);
                    let mut local: Vec<Vec<u32>> = (0..nparts)
                        .map(|_| Vec::with_capacity(chunk / nparts + 64))
                        .collect();
                    for i in lo..hi {
                        let h = (ck.value(i) as u64).wrapping_mul(hashc);
                        local[((h >> 32) as usize) & mask].push(i as u32);
                    }
                    local
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let counts: Vec<usize> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nparts)
            .map(|p| {
                let slices: Vec<&Vec<u32>> = buffers.iter().map(|b| &b[p]).collect();
                s.spawn(move || {
                    let cap: usize = slices.iter().map(|v| v.len()).sum();
                    let mut map: HashMap<(i64, u64), u32> = HashMap::with_capacity(cap);
                    let mut sums: Vec<f64> = Vec::new();
                    let mut first: Vec<u32> = Vec::new();
                    for sl in &slices {
                        for &ri in sl.iter() {
                            let k = ck.value(ri as usize);
                            let nh = fnv1a(nname.value(ri as usize).as_bytes());
                            let gid = *map.entry((k, nh)).or_insert_with(|| {
                                sums.push(0.0);
                                first.push(ri);
                                (sums.len() - 1) as u32
                            });
                            sums[gid as usize] += rev.value(ri as usize);
                        }
                    }
                    let idx = UInt32Array::from(first);
                    for c in [0usize, 1, 2, 3, 4, 5, 6] {
                        let _ = take(batch.column(c), &idx, None).unwrap();
                    }
                    sums.len()
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    (counts.iter().sum(), 0.0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows: usize = std::env::var("ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(11_640_000);
    let groups: usize = std::env::var("GROUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3_880_000);
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    eprintln!("generating {rows} rows / {groups} groups ...");
    let (batch, schema) = gen_data(rows, groups);
    eprintln!("generated.");

    // Faithful parallel input: a real scan yields many partitions (row groups), so the
    // RepartitionExec/aggregate read in parallel. A 1-partition MemTable serializes the
    // RepartitionExec INPUT side single-threaded (eff≈1), starving the operator — an
    // artifact, not an operator flaw. Split the single batch into P partitions for BOTH
    // DataFusion-driven arms (A and K).
    let n_input_parts = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let parts: Vec<Vec<RecordBatch>> = {
        let n = batch.num_rows();
        let chunk = n.div_ceil(n_input_parts);
        (0..n_input_parts)
            .filter_map(|p| {
                let off = p * chunk;
                (off < n).then(|| vec![batch.slice(off, chunk.min(n - off))])
            })
            .collect()
    };
    eprintln!("split into {} input partitions", parts.len());

    let sql = "select c_custkey, c_name, sum(revenue) as revenue, c_acctbal, n_name, \
        c_address, c_phone, c_comment from t \
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment";

    // ArmA: DataFusion's real multi-column (7-col) group-by kernel.
    let run_a = || {
        let schema = schema.clone();
        let parts = parts.clone();
        let sql = sql.to_string();
        async move {
            let ctx = SessionContext::new_with_config(SessionConfig::new());
            let mem = MemTable::try_new(schema, parts).unwrap();
            ctx.register_table("t", Arc::new(mem)).unwrap();
            let df = ctx.sql(&sql).await.unwrap();
            let batches = df.collect().await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            rows
        }
    };

    // ArmK: the REAL FdAggregateExec operator end-to-end —
    //   scan -> RepartitionExec(Hash[c_custkey, n_name]) -> FdAggregateExec (single phase).
    // This is the FAITHFUL operator cost: it pays the real repartition (streaming, so
    // RSS-safe at SF=100) materializing the wide payload per batch, PLUS the production
    // RowConverter+ahash+collision-safe kernel — unlike the idealized index-scatter arms
    // above (which never materialize wide strings outside the final gather). If this beats
    // ArmA, the operator is the real win; the rule is justified.
    let run_k = || {
        let schema = schema.clone();
        let parts = parts.clone();
        async move {
            let ctx = SessionContext::new_with_config(SessionConfig::new());
            let nparts = ctx.state().config().target_partitions();
            let mem = MemTable::try_new(schema, parts).unwrap();
            ctx.register_table("t", Arc::new(mem)).unwrap();
            let child = ctx
                .sql(
                    "SELECT c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, \
                     c_comment, revenue FROM t",
                )
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            // Group exprs in Q10 GROUP BY order; FD-minimal key = {c_custkey(0), n_name(4)}.
            let group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![
                (
                    Arc::new(Column::new("c_custkey", 0)),
                    "c_custkey".to_string(),
                ),
                (Arc::new(Column::new("c_name", 1)), "c_name".to_string()),
                (
                    Arc::new(Column::new("c_acctbal", 2)),
                    "c_acctbal".to_string(),
                ),
                (Arc::new(Column::new("c_phone", 3)), "c_phone".to_string()),
                (Arc::new(Column::new("n_name", 4)), "n_name".to_string()),
                (
                    Arc::new(Column::new("c_address", 5)),
                    "c_address".to_string(),
                ),
                (
                    Arc::new(Column::new("c_comment", 6)),
                    "c_comment".to_string(),
                ),
            ];
            let key_positions = vec![0usize, 4usize];
            let key_exprs: Vec<Arc<dyn PhysicalExpr>> = key_positions
                .iter()
                .map(|&p| group_exprs[p].0.clone())
                .collect();
            let repart = Arc::new(
                RepartitionExec::try_new(child, Partitioning::Hash(key_exprs, nparts)).unwrap(),
            );
            let op = Arc::new(
                FdAggregateExec::try_new(
                    repart,
                    group_exprs,
                    key_positions,
                    Arc::new(Column::new("revenue", 7)),
                    "revenue".to_string(),
                )
                .unwrap(),
            );
            let batches = collect(op, ctx.task_ctx()).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            rows
        }
    };

    println!(
        "\nQ10 agg-kernel microbench  rows={rows} groups={groups} warmups={warmups} trials={trials}"
    );
    println!(
        "{:<22} {:>9} {:>9} {:>6} {:>12}",
        "arm", "wall_ms", "cpu_s", "eff", "groups/sum"
    );

    // ArmA timing.
    for _ in 0..warmups {
        let _ = run_a().await;
    }
    let (mut wa, mut ca) = (Vec::new(), Vec::new());
    let mut a_rows = 0usize;
    for _ in 0..trials {
        let c0 = cpu_secs();
        let t = Instant::now();
        a_rows = run_a().await;
        wa.push(t.elapsed().as_secs_f64() * 1000.0);
        ca.push(cpu_secs() - c0);
    }
    let (wam, cam) = (median(&mut wa), median(&mut ca));
    println!(
        "{:<22} {wam:>9.1} {cam:>9.2} {:>6.1} {:>12}",
        "A: DF 7-col groupby",
        cam / (wam / 1000.0),
        a_rows
    );

    // ArmB timing.
    for _ in 0..warmups {
        let _ = arm_b(&batch);
    }
    let (mut wb, mut cb) = (Vec::new(), Vec::new());
    let mut b_res = (0usize, 0.0f64);
    for _ in 0..trials {
        let c0 = cpu_secs();
        let t = Instant::now();
        b_res = arm_b(&batch);
        wb.push(t.elapsed().as_secs_f64() * 1000.0);
        cb.push(cpu_secs() - c0);
    }
    let (wbm, cbm) = (median(&mut wb), median(&mut cb));
    println!(
        "{:<22} {wbm:>9.1} {cbm:>9.2} {:>6.1} {:>12}",
        "B: i64-hash+gather",
        cbm / (wbm / 1000.0),
        b_res.0
    );

    // ArmB|| : parallel FD-exploit kernel (16-way hash-partition by c_custkey).
    let nparts = 16usize;
    for _ in 0..warmups {
        let _ = arm_b_parallel(&batch, nparts);
    }
    let (mut wp, mut cp) = (Vec::new(), Vec::new());
    let mut p_res = (0usize, 0.0f64);
    for _ in 0..trials {
        let c0 = cpu_secs();
        let t = Instant::now();
        p_res = arm_b_parallel(&batch, nparts);
        wp.push(t.elapsed().as_secs_f64() * 1000.0);
        cp.push(cpu_secs() - c0);
    }
    let (wpm, cpm) = (median(&mut wp), median(&mut cp));
    println!(
        "{:<22} {wpm:>9.1} {cpm:>9.2} {:>6.1} {:>12}",
        format!("B||x{nparts}: parallel"),
        cpm / (wpm / 1000.0),
        p_res.0
    );

    let correct = b_res.0 == a_rows && p_res.0 == a_rows;
    println!(
        "\ncorrect={correct}  CPU-work A/B = {:.2}x  |  WALL: A={wam:.0}ms  B||={wpm:.0}ms  →  A/B|| = {:.2}x",
        cam / cbm,
        wam / wpm
    );
    println!(
        "KILL-GATE: parallel B|| must be correct AND beat A's WALL to justify the operator.\n\
         (real-query anchor: ematix agg 4.94 CPU-s vs DuckDB HASH_GROUP_BY 1.65 CPU-s = 3.0x)"
    );
    let verdict = if correct && wpm < wam {
        format!(
            "GO ✓  (B|| {wpm:.0}ms < A {wam:.0}ms, {:.2}x wall)",
            wam / wpm
        )
    } else if correct {
        format!("NO-GO ✗  (B|| {wpm:.0}ms !< A {wam:.0}ms — parallel kernel does not beat DF wall)")
    } else {
        "NO-GO ✗  (incorrect results)".to_string()
    };
    println!("VERDICT (single-key, i64 only): {verdict}");

    // ArmB||-composite : the Q10-realistic {c_custkey, n_name} key (n_name not FD-determined).
    for _ in 0..warmups {
        let _ = arm_b_composite_parallel(&batch, nparts);
    }
    let (mut wcp, mut ccp) = (Vec::new(), Vec::new());
    let mut c_res = (0usize, 0.0f64);
    for _ in 0..trials {
        let c0 = cpu_secs();
        let t = Instant::now();
        c_res = arm_b_composite_parallel(&batch, nparts);
        wcp.push(t.elapsed().as_secs_f64() * 1000.0);
        ccp.push(cpu_secs() - c0);
    }
    let (wcm, ccm) = (median(&mut wcp), median(&mut ccp));
    println!(
        "{:<22} {wcm:>9.1} {ccm:>9.2} {:>6.1} {:>12}",
        format!("B||x{nparts}: composite"),
        ccm / (wcm / 1000.0),
        c_res.0
    );
    let comp_ok = c_res.0 == a_rows;
    let comp_verdict = if comp_ok && wcm < wam {
        format!(
            "GO ✓  (composite {wcm:.0}ms < A {wam:.0}ms, {:.2}x wall; CPU-work A/comp = {:.2}x)",
            wam / wcm,
            cam / ccm
        )
    } else if comp_ok {
        format!("NO-GO ✗  (composite {wcm:.0}ms !< A {wam:.0}ms)")
    } else {
        format!(
            "NO-GO ✗  (composite incorrect: {} groups vs {a_rows})",
            c_res.0
        )
    };
    println!("VERDICT (composite {{c_custkey, n_name}} — the Q10 key): {comp_verdict}");

    // ArmK : the REAL operator (repartition + production kernel) — the number that
    // actually decides whether to build the rule.
    for _ in 0..warmups {
        let _ = run_k().await;
    }
    let (mut wk, mut ck2) = (Vec::new(), Vec::new());
    let mut k_rows = 0usize;
    for _ in 0..trials {
        let c0 = cpu_secs();
        let t = Instant::now();
        k_rows = run_k().await;
        wk.push(t.elapsed().as_secs_f64() * 1000.0);
        ck2.push(cpu_secs() - c0);
    }
    let (wkm, ckm) = (median(&mut wk), median(&mut ck2));
    println!(
        "{:<22} {wkm:>9.1} {ckm:>9.2} {:>6.1} {:>12}",
        "K: real operator",
        ckm / (wkm / 1000.0),
        k_rows
    );
    let k_ok = k_rows == a_rows;
    let k_verdict = if k_ok && wkm < wam {
        format!(
            "GO ✓  (operator {wkm:.0}ms < A {wam:.0}ms, {:.2}x wall; CPU-work A/K = {:.2}x)",
            wam / wkm,
            cam / ckm
        )
    } else if k_ok {
        format!("NO-GO ✗  (operator {wkm:.0}ms !< A {wam:.0}ms — repartition-take erodes the win)")
    } else {
        format!("NO-GO ✗  (operator incorrect: {k_rows} groups vs {a_rows})")
    };
    println!("VERDICT (REAL FdAggregateExec operator vs DataFusion): {k_verdict}");
    Ok(())
}
