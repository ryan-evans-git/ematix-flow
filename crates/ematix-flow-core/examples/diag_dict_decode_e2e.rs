//! Σ.E5.2b — diagnostic 1: end-to-end repro of the dict-aware
//! decode gap.
//!
//! Question this answers: when both providers are configured with
//! `with_dict_preservation(true)` (FastParquet) / streaming dict
//! preservation (EmatixFastParquet's default), which one is faster
//! on the canonical `COUNT(*) GROUP BY <dict_col>` shape, and by how
//! much?
//!
//! Output:
//!   * Median ± σ wall-clock for the SQL.
//!   * EXPLAIN ANALYZE for both providers (printed once per provider).
//!
//! Caveat: behaviour is unchanged — this is observational.
//!
//! Run:
//!     cargo run --release -p ematix-flow-core --example diag_dict_decode_e2e

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Canonical dict-aware shape: GROUP BY a low-cardinality dict-encoded
// BYTE_ARRAY column. l_returnflag has 3 distinct values across 6M rows.
const Q_DICT_COUNT: &str = "
    SELECT l_returnflag, COUNT(*) AS n
    FROM lineitem
    GROUP BY l_returnflag
    ORDER BY l_returnflag
";

// Q1 shape — what the audit was originally framed against. Useful
// cross-check: confirms the gap pattern is consistent with the Σ.E5.4
// finding (Q01 +111%).
const Q1: &str = "
    SELECT
        l_returnflag, l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        count(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= DATE '1998-09-02'
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
";

const TRIALS: usize = 11;
const WARMUPS: usize = 3;

fn stats(xs: &[f64]) -> (f64, f64) {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = s[s.len() / 2];
    let mean = s.iter().sum::<f64>() / s.len() as f64;
    let var = s.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / s.len() as f64;
    (median, var.sqrt())
}

async fn time_sql(ctx: &SessionContext, sql: &str) -> f64 {
    let t0 = Instant::now();
    let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    t0.elapsed().as_secs_f64() * 1000.0
}

async fn run_explain_analyze(ctx: &SessionContext, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN ANALYZE {sql}");
    let df = ctx.sql(&explain_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    pretty.to_string()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".into());
    let path: PathBuf = PathBuf::from(&dir).join("lineitem.parquet");
    assert!(
        path.exists(),
        "lineitem.parquet not found at {}; set TPCH_DATA_DIR",
        path.display()
    );
    let path_s = path.to_string_lossy().to_string();

    println!("=== Σ.E5.2b diag 1: dict-aware decode e2e gap ===");
    println!("file: {}", path_s);
    println!("trials: {TRIALS} (warmups {WARMUPS})\n");

    // ---- FastParquet (parquet-rs) + dict preservation ----
    let mut fast_dict_count: Vec<f64> = Vec::new();
    let mut fast_q1: Vec<f64> = Vec::new();
    let mut fast_explain_dict = String::new();
    let mut fast_explain_q1 = String::new();
    for t in 0..(TRIALS + WARMUPS) {
        let ctx = SessionContext::new();
        let prov = FastParquetTableProvider::try_new(&path_s)
            .unwrap()
            .with_dict_preservation(true)
            .unwrap();
        if t == WARMUPS {
            let fields = prov.dict_preserved_fields();
            println!("FastParquet+dict promoted fields: {fields:?}");
        }
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        if t == WARMUPS {
            fast_explain_dict = run_explain_analyze(&ctx, Q_DICT_COUNT).await;
            fast_explain_q1 = run_explain_analyze(&ctx, Q1).await;
        }
        let ms_d = time_sql(&ctx, Q_DICT_COUNT).await;
        let ms_q = time_sql(&ctx, Q1).await;
        if t >= WARMUPS {
            fast_dict_count.push(ms_d);
            fast_q1.push(ms_q);
        }
    }

    // ---- EmatixFastParquet (ematix-parquet) + dict preservation ----
    // Note: EmatixFastParquet's streaming path ALREADY routes
    // through `read_column_byte_array_dict_preserved` for Utf8
    // columns even when dict_preservation = false (PR #115). When
    // dict_preservation=true is requested, streaming is forced off
    // and the bridge path emits DictionaryArray directly. We test
    // both so we know whether the gap is on the streaming or the
    // explicit-dict-preserved path.
    let mut emat_stream_dict_count: Vec<f64> = Vec::new();
    let mut emat_stream_q1: Vec<f64> = Vec::new();
    let mut emat_dict_count: Vec<f64> = Vec::new();
    let mut emat_q1: Vec<f64> = Vec::new();
    let mut emat_explain_stream_dict = String::new();
    let mut emat_explain_dict = String::new();
    let mut emat_explain_q1 = String::new();

    for t in 0..(TRIALS + WARMUPS) {
        // Streaming default (StringView, dict-preserved-on-decode).
        let ctx = SessionContext::new();
        let prov = EmatixFastParquetTableProvider::try_new(&path_s).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        if t == WARMUPS {
            emat_explain_stream_dict = run_explain_analyze(&ctx, Q_DICT_COUNT).await;
        }
        let ms = time_sql(&ctx, Q_DICT_COUNT).await;
        let ms_q = time_sql(&ctx, Q1).await;
        if t >= WARMUPS {
            emat_stream_dict_count.push(ms);
            emat_stream_q1.push(ms_q);
        }

        // Bridge path with explicit DictionaryArray output.
        let ctx2 = SessionContext::new();
        let prov2 = EmatixFastParquetTableProvider::try_new(&path_s)
            .unwrap()
            .with_dict_preservation(true);
        ctx2.register_table("lineitem", Arc::new(prov2)).unwrap();
        if t == WARMUPS {
            emat_explain_dict = run_explain_analyze(&ctx2, Q_DICT_COUNT).await;
            emat_explain_q1 = run_explain_analyze(&ctx2, Q1).await;
        }
        let ms2 = time_sql(&ctx2, Q_DICT_COUNT).await;
        let ms2_q = time_sql(&ctx2, Q1).await;
        if t >= WARMUPS {
            emat_dict_count.push(ms2);
            emat_q1.push(ms2_q);
        }
    }

    let (f_d_med, f_d_sd) = stats(&fast_dict_count);
    let (f_q_med, f_q_sd) = stats(&fast_q1);
    let (e_s_d_med, e_s_d_sd) = stats(&emat_stream_dict_count);
    let (e_s_q_med, e_s_q_sd) = stats(&emat_stream_q1);
    let (e_d_med, e_d_sd) = stats(&emat_dict_count);
    let (e_q_med, e_q_sd) = stats(&emat_q1);

    println!("\n=== Q_DICT_COUNT  (SELECT l_returnflag, COUNT(*) GROUP BY l_returnflag) ===");
    println!("FastParquet+dict       median ± σ: {f_d_med:7.2} ± {f_d_sd:5.2} ms");
    println!("Emat (stream default)  median ± σ: {e_s_d_med:7.2} ± {e_s_d_sd:5.2} ms");
    println!("Emat+with_dict_pres    median ± σ: {e_d_med:7.2} ± {e_d_sd:5.2} ms");
    println!(
        "Δ (emat-stream/fast):  {:+.1}%",
        100.0 * (e_s_d_med - f_d_med) / f_d_med
    );
    println!(
        "Δ (emat-dict  /fast):  {:+.1}%",
        100.0 * (e_d_med - f_d_med) / f_d_med
    );

    println!("\n=== Q1  (full TPC-H Q1, the audit's regression query) ===");
    println!("FastParquet+dict       median ± σ: {f_q_med:7.2} ± {f_q_sd:5.2} ms");
    println!("Emat (stream default)  median ± σ: {e_s_q_med:7.2} ± {e_s_q_sd:5.2} ms");
    println!("Emat+with_dict_pres    median ± σ: {e_q_med:7.2} ± {e_q_sd:5.2} ms");
    println!(
        "Δ (emat-stream/fast):  {:+.1}%",
        100.0 * (e_s_q_med - f_q_med) / f_q_med
    );
    println!(
        "Δ (emat-dict  /fast):  {:+.1}%",
        100.0 * (e_q_med - f_q_med) / f_q_med
    );

    println!("\n--- EXPLAIN ANALYZE: FastParquet+dict, Q_DICT_COUNT ---");
    println!("{fast_explain_dict}");
    println!("\n--- EXPLAIN ANALYZE: Emat (streaming default), Q_DICT_COUNT ---");
    println!("{emat_explain_stream_dict}");
    println!("\n--- EXPLAIN ANALYZE: Emat+with_dict_preservation, Q_DICT_COUNT ---");
    println!("{emat_explain_dict}");
    println!("\n--- EXPLAIN ANALYZE: FastParquet+dict, Q1 ---");
    println!("{fast_explain_q1}");
    println!("\n--- EXPLAIN ANALYZE: Emat+with_dict_preservation, Q1 ---");
    println!("{emat_explain_q1}");
}
