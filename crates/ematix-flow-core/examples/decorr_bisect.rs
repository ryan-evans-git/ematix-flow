//! v2 S3.1 — bisect the ematix physical rule that breaks correlated
//! subqueries (q10/q16/q69/q94).
//!
//! `decorr_probe` proved these queries fail on the ematix preset at
//! PHYSICAL planning ("No field named c.c_current_addr_sk") while vanilla
//! DataFusion runs them, and the logical plan is clean — so a physical
//! rule in the production chain drops an outer column. This harness
//! isolates *which* rule: it builds the session via
//! `preset::with_optimizer_rules_overridden` with `HarnessOverrides`
//! (default == the full production chain), reproduces the failure, then
//! flips each production-on rule OFF one at a time and reports which
//! single disable makes the query plan again.
//!
//! Run over SF=1: `cargo run --release -p ematix-flow-core --example decorr_bisect`
//! Optional: pass a query name as arg (default q10).

use std::path::{Path, PathBuf};

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dialect::{Dialect, translate};
use ematix_flow_core::preset::{self, HarnessOverrides};

/// Only the tables the correlated-subquery queries touch — keeps the
/// per-variant session build cheap.
const TABLES: &[&str] = &[
    "customer",
    "customer_address",
    "customer_demographics",
    "store_sales",
    "web_sales",
    "catalog_sales",
    "catalog_returns",
    "web_returns",
    "store_returns",
    "date_dim",
    "item",
    "warehouse",
];

/// Build a `SessionContext` with the given overrides + register the tables.
async fn build_ctx(
    overrides: &HarnessOverrides,
    data_dir: &Path,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let base = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_default_features();
    let (builder, _handles) = preset::with_optimizer_rules_overridden(base, overrides);
    let ctx = SessionContext::new_with_state(builder.build());
    for t in TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        if path.exists() {
            ctx.register_parquet(*t, path.to_str().unwrap(), Default::default())
                .await?;
        }
    }
    Ok(ctx)
}

/// Try to physically plan `df_sql` on `ctx`; return the error string if
/// physical planning fails (the failure mode we are bisecting).
async fn plan_result(ctx: &SessionContext, df_sql: &str) -> Result<(), String> {
    let df = ctx.sql(df_sql).await.map_err(|e| format!("SQL: {e}"))?;
    df.create_physical_plan()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q = std::env::args().nth(1).unwrap_or_else(|| "q10".to_string());
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or("workspace root not found")?
        .to_path_buf();
    let data_dir = std::env::var("TPCDS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace.join("examples/tpcds/data/sf1"));
    if !data_dir.join("customer.parquet").exists() {
        println!("skip: TPC-DS data not found at {}", data_dir.display());
        return Ok(());
    }

    let raw =
        std::fs::read_to_string(workspace.join(format!("examples/tpcds/queries/spark/{q}.sql")))?;
    let df_sql = translate(raw.trim().trim_end_matches(';').trim(), Dialect::Spark)?;

    println!("=== bisecting {q} — which production rule breaks physical planning? ===\n");

    // Baseline: full production chain — expected to FAIL (reproduce).
    let base_ctx = build_ctx(&HarnessOverrides::default(), &data_dir).await?;
    match plan_result(&base_ctx, &df_sql).await {
        Ok(()) => {
            println!("baseline (all rules on): PLANS OK — cannot reproduce the failure, aborting.");
            return Ok(());
        }
        Err(e) => println!("baseline (all rules on): FAILS ✓ — {}\n", short(&e)),
    }

    // The production-on rules (HarnessOverrides::default() == all true).
    // Each is (label, setter that flips it off on a fresh default).
    type Setter = fn(&mut HarnessOverrides);
    let rules: &[(&str, Setter)] = &[
        ("auto_target_partitions", |o| {
            o.auto_target_partitions = false
        }),
        ("dedupe_aggregate", |o| o.dedupe_aggregate = false),
        ("inject_fused_rules", |o| o.inject_fused_rules = false),
        ("swap_semi_join_build", |o| o.swap_semi_join_build = false),
        ("force_collect_left", |o| o.force_collect_left = false),
        ("sampled_join_side", |o| o.sampled_join_side = false),
        ("grace_join", |o| o.grace_join = false),
        ("clustered_single_phase_agg", |o| {
            o.clustered_single_phase_agg = false
        }),
        ("push_down_left_semi", |o| o.push_down_left_semi = false),
        ("robin_hood_sum_f64", |o| o.robin_hood_sum_f64 = false),
        ("runtime_bloom_sideband", |o| {
            o.runtime_bloom_sideband = false
        }),
        ("flow_query_planner", |o| o.flow_query_planner = false),
    ];

    let mut culprits = Vec::new();
    for (label, set) in rules {
        let mut o = HarnessOverrides::default();
        set(&mut o);
        let ctx = build_ctx(&o, &data_dir).await?;
        match plan_result(&ctx, &df_sql).await {
            Ok(()) => {
                println!("  disable {label:<28} → PLANS OK  ⇐ CULPRIT");
                culprits.push(*label);
            }
            Err(e) => println!("  disable {label:<28} → still fails ({})", short(&e)),
        }
    }

    println!("\n=== RESULT ===");
    match culprits.as_slice() {
        [] => println!(
            "No single rule disable fixed it — the failure needs >1 rule off, or the\n\
             trigger is rule interaction / ordering. Try disabling suspect pairs."
        ),
        [one] => println!(
            "Culprit isolated: `{one}`. Fix or guard THAT rule so it preserves the\n\
             referenced outer column (S3.2). Confirm no other correlated query regresses."
        ),
        many => println!(
            "Multiple single-disables fix it: {many:?}. Likely a shared mechanism or a\n\
             chain where any one break stops the bad rewrite — inspect these together."
        ),
    }
    Ok(())
}

fn short(s: &str) -> String {
    let one = s.replace('\n', " ");
    if one.len() > 80 {
        format!("{}…", &one[..80])
    } else {
        one
    }
}
