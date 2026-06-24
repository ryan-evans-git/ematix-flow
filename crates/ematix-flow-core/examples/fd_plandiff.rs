//! FD-on-catalog plan neutrality check: for each TPC-H query, does declaring the
//! primary keys change the OPTIMIZED plan vs not declaring them?
//!
//! The late-mat rule needs PKs declared; declaring them adds FunctionalDependencies
//! the optimizer MAY use (aggregate/distinct elimination, join-key uniqueness).
//! Plan structure is largely scale-independent, so this SF=1 diff identifies the
//! exact blast radius of PK declaration on the OTHER 21 queries (the rule itself
//! fires Q10-only). A query that prints `same` has a byte-identical optimized plan
//! with vs without PKs → trivially FD-neutral at any scale.
//!
//! Runs through the production `FlowQueryPlanner` (preset walkers), so it sees the
//! same plans the rebench A/B measures. EMAT_LATE_MAT_AGG is left OFF so the late-
//! mat rewrite does not run — this isolates the PK/FD effect alone.
//!
//!   TPCH_DATA_DIR (default examples/tpch/data/sf1), VERBOSE=1 to print the diffs.

use std::path::Path;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::flow_query_planner::FlowQueryPlanner;
use ematix_flow_core::preset;

const TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn tpch_pk(t: &str) -> Option<Vec<usize>> {
    Some(match t {
        "region" | "nation" | "supplier" | "customer" | "part" | "orders" => vec![0],
        "partsupp" => vec![0, 1],
        "lineitem" => vec![0, 3],
        _ => return None,
    })
}

fn build_ctx(dir: &Path, with_pk: bool) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(8))
            .with_default_features(),
    )
    .with_query_planner(Arc::new(FlowQueryPlanner))
    .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let p = dir.join(format!("{t}.parquet"));
        let mut prov = EmatixFastParquetTableProvider::try_new(p.to_string_lossy().into_owned())?;
        if with_pk {
            if let Some(pk) = tpch_pk(t) {
                prov = prov.with_primary_key(pk);
            }
        }
        ctx.register_table(*t, Arc::new(prov))?;
    }
    Ok(ctx)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf1".into());
    let dir = Path::new(&dir);
    let verbose = std::env::var_os("VERBOSE").is_some();
    // Make sure the late-mat rewrite stays OFF — isolate the PK/FD effect.
    unsafe {
        std::env::remove_var("EMAT_LATE_MAT_AGG");
        std::env::set_var("EMAT_PLAN_CACHE", "0");
    }

    let no_pk = build_ctx(dir, false)?;
    let pk = build_ctx(dir, true)?;

    let mut changed = Vec::new();
    for q in 1..=22u8 {
        let path = format!("examples/tpch/queries/q{q:02}.sql");
        let sql = std::fs::read_to_string(&path)
            .or_else(|_| std::fs::read_to_string(dir.join(format!("../../queries/q{q:02}.sql"))))
            .unwrap_or_default();
        if sql.trim().is_empty() {
            continue;
        }
        let sql = sql.trim().trim_end_matches(';');

        let plan_a = no_pk.sql(sql).await?.create_physical_plan().await?;
        let plan_b = pk.sql(sql).await?.create_physical_plan().await?;
        let a = format!("{}", displayable(plan_a.as_ref()).indent(true));
        let b = format!("{}", displayable(plan_b.as_ref()).indent(true));
        let same = a == b;
        println!("Q{q:02}: {}", if same { "same" } else { "CHANGED" });
        if !same {
            changed.push(q);
            if verbose {
                println!("--- no-PK ---\n{a}\n--- PK ---\n{b}\n");
            }
        }
    }
    println!(
        "\nPK declaration changes the optimized plan for: {:?} (of 22).",
        changed
    );
    println!("(rule OFF → this is the FD-on-catalog blast radius; the rule itself fires Q10-only.)");
    Ok(())
}
