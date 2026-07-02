//! Σ.AH.5 — functional-dependency GROUP BY simplifier (opt-in `EMAT_FD_GROUPBY=1`).
//!
//! When a GROUP BY key contains a single declared-unique anchor column (a
//! declared PK) that functionally determines EVERY other group column, hashing
//! the other columns into the aggregate's group key is pure waste: they can
//! never split a group. This rule rewrites
//!
//! ```text
//! Aggregate: groupBy=[[k, d1, …, dn]], aggr=[[A…]]
//! ```
//!
//! into
//!
//! ```text
//! Projection: k, min(d1) AS d1, …, min(dn) AS dn, A…   (original order/names)
//!   Aggregate: groupBy=[[k]], aggr=[[A…, min(d1), …, min(dn)]]
//! ```
//!
//! Grouping by `k` alone yields exactly the same groups (soundness argument
//! below), and each determined column is single-valued within its group, so
//! `min` re-attaches its (unique) value — "project the determined columns
//! after aggregation" per the Σ.AH.5 spec. Q10 is the primary target: its
//! 7-column group key (5 wide strings + `n_name` + `c_custkey`) reduces to
//! the single i64 `c_custkey`, taking every wide string out of the per-row
//! hash/compare path of AggregateExec Partial+FinalPartitioned.
//!
//! ## Soundness (the rule fires ONLY when `k → {d1…dn}` is PROVEN)
//!
//! Two proof paths, both rooted in DECLARED constraints (never inferred from
//! data — see [`crate::ematix_fast_parquet::EmatixFastParquetTableProvider::with_primary_key`]):
//!
//! 1. **Schema FDs** ([`crate::late_mat_agg::fd_closure`] over the aggregate
//!    input's [`FunctionalDependencies`]): DataFusion derives `{pk} → {cols}`
//!    from a declared PK and propagates it through joins/projections. This
//!    covers same-table determined columns (Q10: `c_custkey → 5 customer cols`).
//! 2. **PK-fold extension** (the [`crate::late_mat_agg`] prod-B argument): a
//!    group column NOT covered by the schema FDs is still determined by the
//!    anchor when its leaf joins into the anchor's leaf via its OWN declared
//!    single-column PK (a many-to-one edge — each anchor-leaf row matches at
//!    most one dim row, so the joined region stays 1:1 with the anchor row,
//!    which the anchor PK determines). Q10: `nation` joins on `n_nationkey`
//!    (its PK) = `c_nationkey`, so `c_custkey → n_name` even though the
//!    optimizer projected `n_nationkey` away (dropping the schema FD).
//!
//! If neither path proves determination for every non-anchor group column,
//! the rule does NOT fire — a partial reduction is deliberately out of scope
//! for v1 (documented in `docs/PERF_REVIEW_2026_05.md` Σ.AH.5).
//!
//! ## Relation to `EMAT_LATE_MAT_AGG` (prod-B/C late materialization)
//!
//! Late-mat also reduces the grouping key (to a build rowid) but restructures
//! the whole subtree (EmatixHashJoinExec[BuildRowId] → AggregateExec(rowid) →
//! LateGatherExec + a query-scoped 1M batch). This rule is the PLAN-SHAPE-
//! PRESERVING sibling: stock joins, stock AggregateExec, only the group key
//! shrinks. In `FlowQueryPlanner` it runs BEFORE the late-mat recognizer;
//! when it fires the reduced aggregate no longer matches late-mat's ≥3-wide-
//! string shape gate, so the two never stack.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, FunctionalDependencies};
use datafusion::functions_aggregate::expr_fn::min;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan, Projection};

use crate::join_reorder::flatten_inner_join_chain;
use crate::late_mat_agg::{fd_closure, join_root, leaf_of_column, leaf_pk_names};

/// Opt-in gate: `EMAT_FD_GROUPBY=1` enables the rewrite (default OFF). Read
/// once at physical-planning time (in [`crate::flow_query_planner`]), never hot.
pub fn enabled() -> bool {
    crate::flags::opt_in("EMAT_FD_GROUPBY")
}

/// Group-column datatypes the `min` carrier is known-cheap and total-ordered
/// for. Conservative: anything else bails (no rewrite).
fn min_carriable(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// The proven analysis: `group_expr[anchor_pos]` functionally determines every
/// other group column (positions `determined`, in original group order).
struct FdGroupKey {
    anchor_pos: usize,
    determined: Vec<usize>,
}

/// Prove (or refuse) a single-column anchor for `agg`'s group key. See the
/// module docs for the two proof paths.
fn analyze_agg(agg: &Aggregate) -> Option<FdGroupKey> {
    let in_schema = agg.input.schema();
    let fds = in_schema.functional_dependencies();

    // Group columns → schema indices; bail on any non-column group expression
    // (grouping sets, computed keys) and on duplicate group columns.
    let mut group_idx = Vec::with_capacity(agg.group_expr.len());
    let mut group_col = Vec::with_capacity(agg.group_expr.len());
    for ge in &agg.group_expr {
        let Expr::Column(c) = ge else {
            return None;
        };
        group_idx.push(in_schema.index_of_column(c).ok()?);
        group_col.push(c.clone());
    }
    if group_idx.len() < 2 || group_idx.iter().collect::<HashSet<_>>().len() != group_idx.len() {
        return None;
    }

    // Every determined column (= every group column except the anchor) must be
    // min-carriable. With more than one non-carriable column no anchor choice
    // can work; with exactly one, that column must BE the anchor.
    let non_carriable: usize = group_idx
        .iter()
        .filter(|&&i| !min_carriable(in_schema.field(i).data_type()))
        .count();
    if non_carriable > 1 {
        return None;
    }

    // Lazily-flattened inner-join chain for the PK-fold proof path (path 2).
    // `None` when the aggregate input isn't a pure inner-join chain — the
    // schema-FD path (single table / already-covered) still applies.
    let mut chain_cache: Option<Option<ChainInfo>> = None;

    for (pos, anchor_idx) in group_idx.iter().enumerate() {
        if non_carriable == 1 && min_carriable(in_schema.field(*anchor_idx).data_type()) {
            continue; // the sole non-carriable column must be the anchor
        }

        // Path 1 — schema-FD closure of the anchor.
        let covered = fd_closure(&BTreeSet::from([*anchor_idx]), fds);
        let uncovered: Vec<usize> = (0..group_idx.len())
            .filter(|&q| q != pos && !covered.contains(&group_idx[q]))
            .collect();
        if uncovered.is_empty() {
            return Some(FdGroupKey {
                anchor_pos: pos,
                determined: (0..group_idx.len()).filter(|&q| q != pos).collect(),
            });
        }

        // Path 2 — PK-fold extension for the uncovered columns.
        let chain = chain_cache
            .get_or_insert_with(|| build_chain_info(&agg.input))
            .as_ref()?;
        let anchor_col = &group_col[pos];
        let anchor_leaf = leaf_of_column(&chain.leaves, anchor_col)?;
        // The anchor must BE its leaf's declared single-column PK: the fold
        // argument determines the anchor-leaf ROW, and only a unique anchor
        // ties that row to the anchor VALUE.
        let pks = leaf_pk_names(&chain.leaves[anchor_leaf]);
        if !(pks.len() == 1 && pks[0] == anchor_col.name) {
            continue;
        }
        // BFS fold: a leaf joins the region many-to-one iff its edge lands on
        // its OWN declared single-column PK (late-mat's prod-B argument).
        let mut folded = vec![false; chain.leaves.len()];
        folded[anchor_leaf] = true;
        let mut q = VecDeque::from([anchor_leaf]);
        while let Some(cur) = q.pop_front() {
            for (a, b, ca, cb) in &chain.edges {
                let (other, other_col) = if *a == cur {
                    (*b, cb)
                } else if *b == cur {
                    (*a, ca)
                } else {
                    continue;
                };
                if folded[other] {
                    continue;
                }
                let pks = leaf_pk_names(&chain.leaves[other]);
                if pks.len() == 1 && pks[0] == other_col.name {
                    folded[other] = true;
                    q.push_back(other);
                }
            }
        }
        if uncovered
            .iter()
            .all(|&q| leaf_of_column(&chain.leaves, &group_col[q]).is_some_and(|l| folded[l]))
        {
            return Some(FdGroupKey {
                anchor_pos: pos,
                determined: (0..group_idx.len()).filter(|&q| q != pos).collect(),
            });
        }
    }
    None
}

/// The flattened inner-join chain with leaf-resolved equi-edges.
struct ChainInfo {
    leaves: Vec<LogicalPlan>,
    edges: Vec<(usize, usize, Column, Column)>,
}

fn build_chain_info(input: &LogicalPlan) -> Option<ChainInfo> {
    let chain = flatten_inner_join_chain(join_root(input)?)?;
    if chain.extra_filter.is_some() {
        return None; // residual non-equi join filter → bail (conservative)
    }
    let leaves = chain.leaves;
    let mut edges = Vec::with_capacity(chain.equi_preds.len());
    for (a, b) in &chain.equi_preds {
        let (la, lb) = (leaf_of_column(&leaves, a)?, leaf_of_column(&leaves, b)?);
        if la == lb {
            return None;
        }
        edges.push((la, lb, a.clone(), b.clone()));
    }
    Some(ChainInfo { leaves, edges })
}

/// Rewrite the proven `agg` into `Projection(original order) → Aggregate(gby=
/// anchor, aggr = original ++ min-carriers)`. Returns `None` on any construction
/// failure or schema mismatch (caller keeps the stock plan).
fn rewrite_agg(agg: &Aggregate, key: &FdGroupKey) -> Option<LogicalPlan> {
    let n_group = agg.group_expr.len();
    let n_aggr = agg.aggr_expr.len();

    let anchor_expr = agg.group_expr[key.anchor_pos].clone();
    let carriers: Vec<Expr> = key
        .determined
        .iter()
        .map(|&qpos| min(agg.group_expr[qpos].clone()))
        .collect();

    // Refuse a carrier whose schema name collides with an existing aggregate
    // output (e.g. the query itself computes `min(d)`), rather than reasoning
    // about dedup semantics.
    let existing: HashSet<String> = agg
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if carriers
        .iter()
        .any(|c| existing.contains(&c.schema_name().to_string()))
    {
        return None;
    }

    let mut new_aggr = agg.aggr_expr.clone();
    new_aggr.extend(carriers);
    let new_agg = LogicalPlan::Aggregate(
        Aggregate::try_new(Arc::clone(&agg.input), vec![anchor_expr], new_aggr).ok()?,
    );
    let new_schema = new_agg.schema().clone();

    // Projection restoring the ORIGINAL output columns (qualified names, order).
    // New-aggregate layout: [anchor, original aggs…, carriers…].
    let mut proj_exprs = Vec::with_capacity(agg.schema.fields().len());
    for i in 0..agg.schema.fields().len() {
        let (oq, of) = agg.schema.qualified_field(i);
        let src_pos = if i < n_group {
            if i == key.anchor_pos {
                0
            } else {
                let j = key.determined.iter().position(|&q| q == i)?;
                1 + n_aggr + j
            }
        } else {
            1 + (i - n_group)
        };
        let (nq, nf) = new_schema.qualified_field(src_pos);
        let col = Expr::Column(Column::new(nq.cloned(), nf.name()));
        if nq == oq && nf.name() == of.name() {
            proj_exprs.push(col);
        } else {
            proj_exprs.push(col.alias_qualified(oq.cloned(), of.name()));
        }
    }
    let proj = LogicalPlan::Projection(Projection::try_new(proj_exprs, Arc::new(new_agg)).ok()?);

    // Defensive invariant: the replacement exposes the exact original columns
    // (qualifier + name + type), so every wrapper above splices in unchanged.
    let ps = proj.schema();
    if ps.fields().len() != agg.schema.fields().len() {
        return None;
    }
    for i in 0..ps.fields().len() {
        let (oq, of) = agg.schema.qualified_field(i);
        let (nq, nf) = ps.qualified_field(i);
        if oq != nq || of.name() != nf.name() || of.data_type() != nf.data_type() {
            return None;
        }
    }
    Some(proj)
}

/// Apply the simplifier to the first `Aggregate` under the post-aggregate
/// wrappers (Sort/Projection/SubqueryAlias/Filter/Limit), preserving every
/// wrapper. Pure; returns `None` when nothing rewrites (caller keeps the
/// stock plan). Mirrors `late_mat_agg::reconstruct`.
pub fn simplify(plan: &LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            let key = analyze_agg(agg)?;
            rewrite_agg(agg, &key)
        }
        LogicalPlan::Sort(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_) => {
            let input = plan.inputs().first().copied()?;
            let new_input = simplify(input)?;
            plan.with_new_exprs(plan.expressions(), vec![new_input])
                .ok()
        }
        _ => None,
    }
}

// =========================================================================
// Σ.AH.5 plan-diff tests (TDD; repo convention — real SF=1 plans, skip when
// the data isn't present). Positive: Q10 reduces to the single unique key
// with a post-agg projection of the determined columns; single-table PK
// group reduces. Negative: no declared PK, an FK-joined (undetermined)
// second-table group column, and a composite PK must NOT rewrite.
// =========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::{Path, PathBuf};

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.join("customer.parquet").exists() {
                return Some(p);
            }
        }
        let m = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = m.parent()?.parent()?.join("examples/tpch/data/sf1");
        p.join("customer.parquet").exists().then_some(p)
    }

    fn q10_sql(dir: &Path) -> String {
        let s = std::fs::read_to_string("examples/tpch/queries/q10.sql")
            .or_else(|_| std::fs::read_to_string(dir.join("../../queries/q10.sql")))
            .unwrap();
        s.trim().trim_end_matches(';').to_string()
    }

    fn prov(dir: &Path, t: &str, pk: Option<Vec<usize>>) -> EmatixFastParquetTableProvider {
        let p = dir.join(format!("{t}.parquet"));
        let mut prov = EmatixFastParquetTableProvider::try_new(p.to_string_lossy()).unwrap();
        if let Some(i) = pk {
            prov = prov.with_primary_key(i);
        }
        prov
    }

    /// Q10's four tables with declared PKs (customer/orders/nation @0;
    /// lineitem = the unconstrained fact).
    async fn q10_ctx(dir: &Path) -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        for (t, pk) in [
            ("customer", Some(vec![0])),
            ("orders", Some(vec![0])),
            ("lineitem", None),
            ("nation", Some(vec![0])),
        ] {
            ctx.register_table(t, Arc::new(prov(dir, t, pk))).unwrap();
        }
        ctx
    }

    async fn optimized(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap()
    }

    /// Descend to the first Aggregate of a (rewritten) plan.
    fn first_agg(plan: &LogicalPlan) -> Option<&Aggregate> {
        if let LogicalPlan::Aggregate(a) = plan {
            return Some(a);
        }
        plan.inputs().first().and_then(|p| first_agg(p))
    }

    /// PLAN-DIFF (positive, the Σ.AH.5 primary target): Q10's 7-column group
    /// key reduces to the single unique key `c_custkey`; the 6 determined
    /// columns (5 wide customer strings + the PK-folded `n_name`) move to
    /// min carriers; a projection above restores the exact original output
    /// schema so the wrappers (Sort/Projection) splice in unchanged.
    #[tokio::test]
    async fn q10_group_key_reduces_to_custkey_with_postagg_projection() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let plan = optimized(&ctx, &q10_sql(&dir)).await;
        let orig_schema = plan.schema().clone();

        let rewritten = simplify(&plan).expect("Q10 must rewrite");

        // The aggregate now groups on c_custkey ALONE.
        let agg = first_agg(&rewritten).expect("rewritten plan keeps an Aggregate");
        assert_eq!(agg.group_expr.len(), 1, "single-column group key");
        assert_eq!(
            format!("{}", agg.group_expr[0]),
            "customer.c_custkey",
            "the group key is the declared-unique anchor"
        );

        // The determined columns are carried as min(...) aggregates.
        let dump = format!("{}", rewritten.display_indent());
        for c in [
            "c_name",
            "c_acctbal",
            "c_phone",
            "n_name",
            "c_address",
            "c_comment",
        ] {
            assert!(
                dump.contains(&format!("min({}", column_owner(c))),
                "determined column `{c}` must be min-carried:\n{dump}"
            );
        }
        assert_eq!(
            agg.aggr_expr.len(),
            7,
            "1 original SUM + 6 min carriers:\n{dump}"
        );

        // Top-level schema is EXACTLY the original (qualifier + name + type).
        let new_schema = rewritten.schema();
        assert_eq!(new_schema.fields().len(), orig_schema.fields().len());
        for i in 0..orig_schema.fields().len() {
            let (oq, of) = orig_schema.qualified_field(i);
            let (nq, nf) = new_schema.qualified_field(i);
            assert_eq!((oq, of.name()), (nq, nf.name()), "output column preserved");
            assert_eq!(of.data_type(), nf.data_type(), "type preserved");
        }
    }

    fn column_owner(c: &str) -> String {
        let table = if c.starts_with("n_") {
            "nation"
        } else {
            "customer"
        };
        format!("{table}.{c}")
    }

    /// CORRECTNESS (SF=1): the simplified Q10 plan returns row-for-row
    /// identical values to the stock plan.
    #[tokio::test]
    async fn q10_simplified_results_match_stock() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let plan = optimized(&ctx, &q10_sql(&dir)).await;
        let rewritten = simplify(&plan).expect("Q10 must rewrite");

        let stock = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let simplified = ctx
            .execute_logical_plan(rewritten)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let render = |bs: &[datafusion::arrow::record_batch::RecordBatch]| {
            let mut rows: Vec<Vec<String>> = bs
                .iter()
                .flat_map(|b| {
                    let cols = b.columns();
                    (0..b.num_rows()).map(move |r| {
                        cols.iter()
                            .map(|c| {
                                datafusion::arrow::util::display::array_value_to_string(c, r)
                                    .unwrap()
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            rows.sort();
            rows
        };
        // Cell compare with FP-relative tolerance on the f64 revenue: the two
        // plans sum the SAME per-group values in a different order, so the
        // last-ulp of the f64 SUM legitimately jitters (same policy as
        // tpch_validate's FP_RTOL).
        let cell_eq = |a: &str, b: &str| {
            if a == b {
                return true;
            }
            match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => (x - y).abs() <= 1e-9 * x.abs().max(y.abs()),
                _ => false,
            }
        };
        let (a, b) = (render(&stock), render(&simplified));
        assert_eq!(a.len(), b.len(), "row count must match");
        for (i, (ra, rb)) in a.iter().zip(b.iter()).enumerate() {
            assert_eq!(ra.len(), rb.len(), "arity at row {i}");
            for (ca, cb) in ra.iter().zip(rb.iter()) {
                assert!(
                    cell_eq(ca, cb),
                    "row {i} differs beyond FP tolerance:\n stock: {ra:?}\n simpl: {rb:?}"
                );
            }
        }
    }

    /// NEGATIVE: no declared PK → no FD → no rewrite. An un-annotated catalog
    /// stays entirely on the stock path.
    #[tokio::test]
    async fn no_pk_declared_does_not_rewrite() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        for t in ["customer", "orders", "lineitem", "nation"] {
            ctx.register_table(t, Arc::new(prov(&dir, t, None)))
                .unwrap();
        }
        let plan = optimized(&ctx, &q10_sql(&dir)).await;
        assert!(
            simplify(&plan).is_none(),
            "no unique key declared → must NOT rewrite"
        );
    }

    /// NEGATIVE: group columns from two tables where the second table's column
    /// is NOT determined by the anchor (orders joins customer via the FK
    /// `o_custkey`, not via its own PK — a customer has MANY orderdates) →
    /// must NOT rewrite, even with every PK declared.
    #[tokio::test]
    async fn fk_joined_second_table_group_col_does_not_rewrite() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let sql = "select c_custkey, o_orderdate, sum(o_totalprice) as t \
                   from customer, orders where c_custkey = o_custkey \
                   group by c_custkey, o_orderdate";
        let plan = optimized(&ctx, sql).await;
        assert!(
            simplify(&plan).is_none(),
            "o_orderdate is NOT determined by c_custkey → must NOT rewrite:\n{}",
            plan.display_indent()
        );
    }

    /// POSITIVE (single-table slice): a PK + determined-columns group over ONE
    /// table reduces to the PK with min carriers — no join required.
    #[tokio::test]
    async fn single_table_pk_group_reduces() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let sql = "select c_custkey, c_name, c_phone, count(*) as n \
                   from customer group by c_custkey, c_name, c_phone";
        let plan = optimized(&ctx, sql).await;
        let rewritten = simplify(&plan).expect("single-table PK group must rewrite");
        let agg = first_agg(&rewritten).unwrap();
        assert_eq!(agg.group_expr.len(), 1);
        assert_eq!(format!("{}", agg.group_expr[0]), "customer.c_custkey");
        // Results identical.
        let stock = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let simp = ctx
            .execute_logical_plan(rewritten)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = |bs: &[datafusion::arrow::record_batch::RecordBatch]| {
            bs.iter().map(|b| b.num_rows()).sum::<usize>()
        };
        assert_eq!(n(&stock), n(&simp), "same group count");
    }

    /// NEGATIVE: a composite declared PK (partsupp = [ps_partkey, ps_suppkey])
    /// provides no single-column anchor → must NOT rewrite (v1 scope).
    #[tokio::test]
    async fn composite_pk_does_not_rewrite() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_table(
            "partsupp",
            Arc::new(prov(&dir, "partsupp", Some(vec![0, 1]))),
        )
        .unwrap();
        let sql = "select ps_partkey, ps_availqty, sum(ps_supplycost) as s \
                   from partsupp group by ps_partkey, ps_availqty";
        let plan = optimized(&ctx, sql).await;
        assert!(
            simplify(&plan).is_none(),
            "ps_partkey alone is not unique → must NOT rewrite"
        );
    }

    /// The gate is OPT-IN: default environment leaves the rule disabled.
    #[test]
    fn flag_is_opt_in_default_off() {
        if std::env::var_os("EMAT_FD_GROUPBY").is_none() {
            assert!(!enabled(), "EMAT_FD_GROUPBY must default OFF");
        }
    }
}
