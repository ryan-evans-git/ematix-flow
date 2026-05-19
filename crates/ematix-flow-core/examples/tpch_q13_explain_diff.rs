//! Σ.G.2f.3 Q13 diagnostic: dump EXPLAIN ANALYZE for Q13 under
//! EmatixFastParquetTableProvider AND FastParquetTableProvider so we
//! can compare per-operator elapsed_compute / output_bytes / output_rows.
//! Focused on finding where the 14× Utf8View buffer inflation actually
//! costs CPU (vs being pure accounting overhead).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;

const Q13_SQL: &str = "
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc
";

const TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[derive(Clone, Copy)]
enum Provider {
    Fast,
    Emat,
}

async fn build_ctx(data_dir: &Path, provider: Provider) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let path = path.to_string_lossy().to_string();
        match provider {
            Provider::Fast => {
                let p = FastParquetTableProvider::try_new(path).unwrap();
                ctx.register_table(*t, Arc::new(p)).unwrap();
            }
            Provider::Emat => {
                let p = EmatixFastParquetTableProvider::try_new(path).unwrap();
                ctx.register_table(*t, Arc::new(p)).unwrap();
            }
        }
    }
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let data_dir: PathBuf = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

    for (label, provider) in [
        ("FastParquet", Provider::Fast),
        ("EmatixFastParquet", Provider::Emat),
    ] {
        println!("\n========== {label} ==========\n");
        let ctx = build_ctx(&data_dir, provider).await;
        // warmup
        let _ = ctx.sql(Q13_SQL).await.unwrap().collect().await.unwrap();

        let explain = format!("EXPLAIN ANALYZE {Q13_SQL}");
        let batches = ctx.sql(&explain).await.unwrap().collect().await.unwrap();
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap()
        );

        // 6 timed trials
        let mut times = Vec::with_capacity(6);
        for _ in 0..6 {
            let t = Instant::now();
            let _ = ctx.sql(Q13_SQL).await.unwrap().collect().await.unwrap();
            times.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = times[times.len() / 2];
        println!(
            "\n[{label}] Q13 wall-clock: median {med:.2} ms (min {:.2}, max {:.2})",
            times[0],
            times[times.len() - 1]
        );
    }
}
