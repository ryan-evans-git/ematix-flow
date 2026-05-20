//! Σ.E5 Q16 diagnostic: side-by-side EXPLAIN ANALYZE for the two
//! providers. Q16 has count(distinct) which our InjectFilterMultiAggRule
//! doesn't handle, so the rule doesn't fire; the regression is in the
//! scan path.

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

const Q16_SQL: &str = "
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED %'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select s_suppkey from supplier
        where s_comment like '%Customer%Complaints%'
    )
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size
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
        let _ = ctx.sql(Q16_SQL).await.unwrap().collect().await.unwrap();

        let explain = format!("EXPLAIN ANALYZE {Q16_SQL}");
        let batches = ctx.sql(&explain).await.unwrap().collect().await.unwrap();
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap()
        );

        let mut times = Vec::with_capacity(8);
        for _ in 0..8 {
            let t = Instant::now();
            let _ = ctx.sql(Q16_SQL).await.unwrap().collect().await.unwrap();
            times.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = times[times.len() / 2];
        println!(
            "\n[{label}] Q16 wall-clock: median {med:.2} ms (min {:.2}, max {:.2})",
            times[0],
            times[times.len() - 1]
        );
    }
}
