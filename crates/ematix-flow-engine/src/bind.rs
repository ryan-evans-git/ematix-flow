//! P3 binder: SQL text → the flat, bound, typed [`BoundQuery`].
//!
//! `sqlparser` supplies tokenize→AST only (the sanctioned bootstrap — a
//! standalone lib, not DataFusion); everything from the AST inward is owned
//! engine code. The binder resolves names against the [`Catalog`] into a
//! **global slot space** (see `logical.rs`), desugars `BETWEEN`, resolves
//! `date '…'` literals to `Date32` days, and — the first real correctness
//! obligation — **constant-folds literal arithmetic in decimal**, casting to
//! the target type only at the leaf. Folding `0.06 + 0.01` in f64 yields
//! `0.069999999999999996`, one ULP below the stored `0.07`, silently
//! dropping the whole 0.07 bucket (~1/3 of Q6's matches) — the `lib.rs`
//! lesson, now owned by the binder.
//!
//! Multi-table: FROM lists tables comma-style, optionally aliased
//! (`nation n1, nation n2`); qualified names (`n1.n_name`) resolve through
//! the alias. WHERE splits into conjuncts — an equality between columns of
//! two different tables becomes a **join edge**; any other conjunct must
//! reference exactly one table and becomes that table's filter. SELECT
//! items are **output projections in row space**: aggregate calls are
//! extracted into the aggregate list and replaced by row references, group
//! keys match bound GROUP BY expressions — so `sum(a)/sum(b)` and
//! CASE-wrapped measures project naturally over computed aggregates.
//!
//! Each unsupported construct errors by name rather than mis-binding. Not
//! yet: `JOIN … ON` syntax, HAVING, ORDER BY / LIMIT (grouped output is
//! key-sorted), NULLs, subqueries.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::{Catalog, TableDef};
use crate::expr::{BinaryOp, DateField, DateTruncUnit, Expr, NumFn, ScalarValue, StrFn};
use crate::logical::{
    AggExpr, AggFunc, BoundQuery, GroupExpr, JoinEdge, OrderByKey, OutputExpr, ScanColumn, SetOp,
    Slot, TableInput, TableSource, WindowExpr, WindowFunc,
};
use crate::vector::LogicalType;

/// Parse `sql` and bind it against `catalog` into a typed query.
pub fn bind_sql(sql: &str, catalog: &Catalog) -> Result<BoundQuery, String> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| format!("parse: {e}"))?;
    let [stmt] = stmts.as_slice() else {
        return Err(format!("expected one statement, got {}", stmts.len()));
    };
    let ast::Statement::Query(query) = stmt else {
        return Err("only SELECT queries are supported".into());
    };
    bind_query(query, catalog, false, &HashMap::new())
}

/// Bind one query level (the top level, or a subquery). `set_semantics`
/// marks an IN-subquery: an aggregate-less, group-less inner SELECT is then
/// rewritten as GROUP BY its select items — membership only cares about the
/// value SET, so the dedup is semantics-preserving (and gives the executor
/// its grouped path).
/// A CTE registry: name → (bound definition, output columns as
/// (name, type), defining AST). CTE references materialize as derived
/// tables; the AST enables the scalar-aggregate cross-join view rewrite
/// (q77's `FROM cs, cr` with a 1-row cr).
type CteMap = HashMap<
    String,
    (
        std::sync::Arc<BoundQuery>,
        Vec<(String, LogicalType)>,
        ast::Query,
    ),
>;

fn bind_query(
    query: &ast::Query,
    catalog: &Catalog,
    set_semantics: bool,
    outer_ctes: &CteMap,
) -> Result<BoundQuery, String> {
    // Constant pushdown into single-referenced CTE group keys (q78's sf10
    // lever): an outer `WHERE cte_col = const` — directly, or transitively
    // through join equalities — injects the constant into the CTE's own
    // WHERE, so its aggregation never builds the filtered-away groups.
    if let Some(q2) = rewrite_cte_const_pushdown(query, catalog) {
        return bind_query(&q2, catalog, set_semantics, outer_ctes);
    }
    // CTE set-narrowing (q95's sf10 lever): a CTE referenced ONLY from
    // IN-subqueries narrows to SELECT DISTINCT of its used columns.
    if let Some(q2) = rewrite_cte_set_narrowing(query) {
        return bind_query(&q2, catalog, set_semantics, outer_ctes);
    }
    // Offset-equijoin promotion (q59/q2's sf10 lever): `WHERE a = b ± N`
    // between two derived FROM items becomes a real join edge via a hidden
    // computed column on b's side; both participants must materialize.
    // Idempotent — the rewritten conjunct has no compound side, so the
    // pattern no longer matches. Fires only on a plain-Select body with an
    // all-derived FROM, which the flatten/set-op/full-outer early paths
    // below never see.
    let mut forced_mat: BTreeSet<String> = BTreeSet::new();
    let owned_query: ast::Query;
    let query = match rewrite_offset_equijoin(query) {
        Some((q2, force)) => {
            forced_mat = force;
            owned_query = q2;
            &owned_query
        }
        None => query,
    };
    // WITH: bind each CTE (earlier CTEs visible to later ones), extending
    // the registry this query level sees.
    let mut ctes: CteMap = outer_ctes.clone();
    if let Some(with) = &query.with {
        if with.recursive {
            return Err("recursive CTEs are not yet supported".into());
        }
        for cte in &with.cte_tables {
            let bq = bind_query(&cte.query, catalog, false, &ctes)?;
            let tys = output_types(&bq);
            let names: Vec<String> = if cte.alias.columns.is_empty() {
                bq.output.iter().map(|o| o.name.clone()).collect()
            } else {
                cte.alias
                    .columns
                    .iter()
                    .map(|c| c.name.value.clone())
                    .collect()
            };
            if names.len() != tys.len() {
                return Err(format!(
                    "CTE '{}' column list arity mismatch",
                    cte.alias.name.value
                ));
            }
            ctes.insert(
                cte.alias.name.value.clone(),
                (
                    std::sync::Arc::new(bq),
                    names.into_iter().zip(tys).collect(),
                    cte.query.as_ref().clone(),
                ),
            );
        }
    }
    let ctes = &ctes;

    // A parenthesized set-operation side — `a UNION ALL (SELECT …)`, or a
    // whole `((…) EXCEPT (…))` body — parses as a `SetExpr::Query` wrapper.
    // Flatten wrappers carrying no WITH/ORDER/LIMIT of their own into their
    // bodies so every side is a plain Select/SetOperation (q2/q8/q23/q66/q87).
    if contains_flattenable_wrapper(query.body.as_ref()) {
        let mut q2 = query.clone();
        *q2.body = flatten_setexpr(*q2.body);
        return bind_query(&q2, catalog, set_semantics, ctes);
    }

    if matches!(query.body.as_ref(), ast::SetExpr::SetOperation { .. }) {
        return bind_set_query(query, catalog, ctes);
    }
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("only plain SELECT is supported here".into());
    };

    // FULL OUTER JOIN rewrites to a UNION ALL of a LEFT join and the
    // mirrored ANTI join (both machinery this binder already has), wrapped
    // in a derived table; side-qualified references in the enclosing select
    // rewrite to the wrapper's prefixed columns. See rewrite_full_outer.
    if let Some(q2) = rewrite_full_outer(query, select, catalog, ctes)? {
        return bind_query(&q2, catalog, set_semantics, ctes);
    }

    // FROM: comma-separated plain tables (the TPC-H canonical form), with
    // optional aliases for self-joins (`nation n1, nation n2`).
    if select.from.is_empty() {
        return Err("a FROM clause is required".into());
    }
    let mut b = Binder {
        catalog,
        ctes,
        tables: Vec::new(),
        slots: Vec::new(),
        touched: BTreeSet::new(),
        subs: Vec::new(),
        derived: Vec::new(),
        views: Vec::new(),
        windows: Vec::new(),
        extra_edges: Vec::new(),
        pending_conjuncts: Vec::new(),
        force_materialize: forced_mat,
        left_tables: BTreeSet::new(),
        plain_passthrough: false,
        uses_grouping: false,
        output_aliases: Vec::new(),
    };
    for twj in &select.from {
        b.add_from_item(&twj.relation)?;
        for join in &twj.joins {
            b.add_join(join)?;
        }
    }

    // GROUP BY first (slot space) — SELECT items match against these. Keys
    // may be integer, float, or string valued (the executor's typed group
    // keys); booleans group as 0/1.
    let mut rollup_terms: Vec<usize> = Vec::new();
    let group: Vec<GroupExpr> = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            // `GROUP BY ROLLUP(t₁, …)` parses as a single `Expr::Rollup`
            // wrapping the terms; flatten the term columns into the group
            // list and record each term's width for the executor.
            if let [ast::Expr::Rollup(terms)] = exprs.as_slice() {
                let mut out = Vec::new();
                for term in terms {
                    rollup_terms.push(term.len());
                    for e in term {
                        out.push(GroupExpr {
                            expr: b.bind_scalar(e)?,
                        });
                    }
                }
                out
            } else if exprs
                .iter()
                .any(|e| matches!(e, ast::Expr::Cube(_) | ast::Expr::GroupingSets(_)))
            {
                return Err("GROUP BY CUBE / GROUPING SETS are not yet supported".into());
            } else {
                let mut out = Vec::new();
                for e in exprs {
                    // Positional GROUP BY (`GROUP BY 1`): an integer literal
                    // refers to the n-th SELECT item, like ORDER BY ordinals.
                    let target = positional_ref(e, select.projection.len())
                        .map(|pos| select_item_expr(&select.projection[pos]))
                        .transpose()?
                        .unwrap_or(e);
                    let bound = b.bind_scalar(target)?;
                    out.push(GroupExpr { expr: bound });
                }
                out
            }
        }
        ast::GroupByExpr::Expressions(..) => {
            return Err("GROUP BY WITH ROLLUP/CUBE/… modifiers are not yet supported".into());
        }
        other => return Err(format!("unsupported GROUP BY form: {other:?}")),
    };

    // `SELECT DISTINCT` — plain DISTINCT dedups the projected rows; DISTINCT
    // ON is a different (Postgres) feature we don't model.
    let is_distinct = match &select.distinct {
        // `SELECT ALL` is the explicit no-dedup default.
        None | Some(ast::Distinct::All) => false,
        Some(ast::Distinct::Distinct) => true,
        Some(ast::Distinct::On(_)) => {
            return Err("DISTINCT ON is not supported".into());
        }
    };

    // An aggregate-less, group-less IN-subquery SELECT — or an equivalent
    // `SELECT DISTINCT` — becomes GROUP BY its items (set semantics; see fn
    // docs). Folding DISTINCT into the group-by dedups during aggregation
    // rather than materializing every row and deduping after.
    let fold_into_group = group.is_empty()
        && (set_semantics || is_distinct)
        && select.projection.iter().all(|it| match it {
            ast::SelectItem::UnnamedExpr(e) => !contains_function(e),
            ast::SelectItem::ExprWithAlias { expr, .. } => !contains_function(expr),
            _ => false,
        });
    let group: Vec<GroupExpr> = if fold_into_group {
        let mut g = Vec::new();
        for item in &select.projection {
            let (ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. }) =
                item
            else {
                unreachable!("checked above");
            };
            g.push(GroupExpr {
                expr: b.bind_scalar(e)?,
            });
        }
        g
    } else {
        group
    };
    // A DISTINCT that grouping did NOT fold away (layered on an explicit
    // GROUP BY, or over an aggregate/expression projection) is deduped from
    // the final rows by the executor. When it folded in, the group-by
    // already made the rows unique, so no post-dedup is needed.
    let distinct = is_distinct && !fold_into_group;

    // SELECT. Three shapes:
    // - aggregate/grouped: items are row-space projections, aggregate calls
    //   extracted into `aggs`;
    // - PLAIN ROW query (no aggregates, no GROUP BY): items bind in slot
    //   space and the executor emits one output row per joined row — no
    //   grouping, no dedup (Q2, Q15). Scalar functions (concat, substr, …)
    //   are fine here; only an AGGREGATE forces the grouped path.
    let plain_rows = !set_semantics
        && group.is_empty()
        && select.projection.iter().all(|it| match it {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                !contains_aggregate(e)
            }
            ast::SelectItem::Wildcard(_) => true,
            _ => false,
        });
    // `SELECT *` expands to every column of every FROM table, in scope
    // order (inlined view subqueries would need their own expansion —
    // not yet supported).
    let projection: Vec<ast::SelectItem> = if select
        .projection
        .iter()
        .any(|it| matches!(it, ast::SelectItem::Wildcard(_)))
    {
        let mut items = Vec::new();
        for item in &select.projection {
            if matches!(item, ast::SelectItem::Wildcard(_)) {
                for bt in &b.tables {
                    // Synthetic decorrelation tables (`__corr…`/`__ex…`) are
                    // internal — `SELECT *` never exposes them.
                    if bt.display.starts_with("__") {
                        continue;
                    }
                    for c in &bt.def.columns {
                        // QUALIFIED references: two FROM tables may share a
                        // column name (q14b's `channel` in both deriveds) —
                        // each expansion must resolve to its own table.
                        items.push(ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(
                            vec![
                                ast::Ident::new(bt.display.clone()),
                                ast::Ident::new(c.name.clone()),
                            ],
                        )));
                    }
                }
                // Inlined views (including the scalar-aggregate cross-join
                // views, q28) expand to their defining expressions.
                for v in &b.views {
                    for (name, e) in &v.cols {
                        items.push(ast::SelectItem::ExprWithAlias {
                            expr: e.clone(),
                            alias: ast::Ident::new(name.clone()),
                        });
                    }
                }
            } else {
                items.push(item.clone());
            }
        }
        items
    } else {
        select.projection.clone()
    };
    // A no-GROUP-BY window query (`rank() OVER … FROM derived`, q49): its
    // non-window projections are slot-space passthroughs. Distinct from a
    // grouped window (which computes over aggregate rows); a *true*
    // aggregate at this level would need the grouped path, so it disqualifies.
    fn item_expr(it: &ast::SelectItem) -> Option<&ast::Expr> {
        match it {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                Some(e)
            }
            _ => None,
        }
    }
    let windowed_plain = !set_semantics
        && group.is_empty()
        && projection
            .iter()
            .any(|it| item_expr(it).is_some_and(contains_window))
        && projection
            .iter()
            .all(|it| item_expr(it).is_none_or(|e| !contains_nonwindow_agg(e)));
    b.plain_passthrough = windowed_plain;

    let mut aggs: Vec<AggExpr> = Vec::new();
    let mut output: Vec<OutputExpr> = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(e) => (e, None),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            other => return Err(format!("unsupported select item: {other}")),
        };
        let row_expr = if plain_rows {
            b.bind_scalar(expr)?
        } else {
            b.bind_output(expr, &group, &mut aggs)?
        };
        let name = alias.unwrap_or_else(|| match expr {
            ast::Expr::Identifier(id) => id.value.clone(),
            ast::Expr::CompoundIdentifier(ids) => {
                ids.last().map(|i| i.value.clone()).unwrap_or_default()
            }
            _ => format!("col{idx}"),
        });
        output.push(OutputExpr {
            expr: row_expr,
            name,
        });
    }
    if aggs.is_empty() && group.is_empty() && !plain_rows && !windowed_plain {
        return Err("SELECT list must contain an aggregate, a GROUP BY, or plain columns".into());
    }

    // WHERE: split the conjunct tree, after **OR-factoring** — a top-level
    // `(J ∧ A) ∨ (J ∧ B) ∨ …` rewrites to `J ∧ (A ∨ B ∨ …)` by hoisting
    // conjuncts common to every branch (Q19's shape, where the join
    // condition and shared filters live inside each OR branch). Then each
    // conjunct routes: a `colA = colB` across two tables is a join edge; a
    // single-table conjunct is that table's filter; a multi-table conjunct
    // becomes the post-join filter, evaluated at the root after payload
    // attach.
    let mut filters: Vec<Vec<Expr>> = vec![Vec::new(); b.tables.len()];
    let mut edges: Vec<JoinEdge> = Vec::new();
    let mut post: Vec<Expr> = Vec::new();
    {
        // Conjuncts inherited from inlined derived tables come first, then
        // the outer WHERE.
        let mut raw_owned: Vec<(ast::Expr, bool)> = std::mem::take(&mut b.pending_conjuncts);
        if let Some(where_expr) = &select.selection {
            let mut raw = Vec::new();
            split_and(where_expr, &mut raw);
            raw_owned.extend(raw.into_iter().cloned().map(|c| (c, false)));
        }
        let mut conjuncts: Vec<(ast::Expr, bool)> = Vec::new();
        for (conj, from_on) in &raw_owned {
            let mut factored = Vec::new();
            factor_or(conj, &mut factored);
            conjuncts.extend(factored.into_iter().map(|c| (c, *from_on)));
        }
        for (conj, from_on) in &conjuncts {
            if let ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } = conj
            {
                if let (Some(lp), Some(rp)) = (ident_parts(left), ident_parts(right)) {
                    // A join edge needs BOTH sides to be real (non-view)
                    // columns; view references fall through to expression
                    // binding.
                    if b.is_real_column(&lp) && b.is_real_column(&rp) {
                        let a = b.resolve_parts(&lp)?;
                        let bb = b.resolve_parts(&rp)?;
                        let (ta, tb) = (b.slots[a].table, b.slots[bb].table);
                        if ta != tb {
                            let (ka, kb) = (b.slot_col(a), b.slot_col(bb));
                            let both_int = is_integer_family(ka.ty) && is_integer_family(kb.ty);
                            let both_str = ka.ty == LogicalType::Utf8 && kb.ty == LogicalType::Utf8;
                            if !both_int && !both_str {
                                return Err(format!(
                                    "join keys '{}' = '{}' must both be integers or both \
                                     strings",
                                    ka.name, kb.name
                                ));
                            }
                            // A WHERE equijoin touching a LEFT JOIN's
                            // nullable side demotes that join to INNER: a
                            // NULL-filled key can never satisfy the equality
                            // (q93's `sr_reason_sk = r_reason_sk`).
                            if !from_on {
                                for t in [ta, tb] {
                                    b.demote_left(t);
                                }
                            }
                            edges.push(JoinEdge {
                                a,
                                b: bb,
                                preserved: None,
                            });
                            continue;
                        }
                        // Same table ⇒ an ordinary filter; falls through.
                    }
                }
            }
            let (e, t) = b.bind_multi(conj)?;
            match t {
                Attribution::None => {
                    return Err(format!(
                        "constant WHERE predicate '{conj}' is not supported"
                    ));
                }
                Attribution::Single(t) if b.left_tables.contains(&t) && !from_on => {
                    // A WHERE predicate on a LEFT JOIN's nullable side:
                    // - `IS NULL` selects the UNMATCHED rows (an anti-join,
                    //   q78's `WHERE wr_order_number IS NULL`). It must see
                    //   the NULL-filled payload, so it routes as a POST-JOIN
                    //   filter — pushing it down to the table would instead
                    //   filter the table's real rows pre-join.
                    // - anything NULL-rejecting (comparison/arithmetic —
                    //   NULL makes it UNKNOWN, so unmatched rows drop) is
                    //   exactly an INNER join: demote and push down.
                    if matches!(conj, ast::Expr::IsNull(_)) {
                        post.push(e);
                    } else {
                        b.demote_left(t);
                        filters[t].push(e);
                    }
                }
                Attribution::Single(t) => {
                    // `rk <= K` on a derived's rank-family window output
                    // arms the executor's top-K prune (the filter itself
                    // still applies — it trims threshold ties).
                    b.try_window_topk(conj);
                    filters[t].push(e);
                }
                Attribution::Multi => post.push(e),
            }
        }
    }
    edges.append(&mut b.extra_edges);
    // Decorrelation may have appended tables after `filters` was sized.
    filters.resize(b.tables.len(), Vec::new());
    let post_filter = post.into_iter().reduce(and);

    // A table with no join edge to the rest is a CROSS join — legitimate
    // SQL (q2's `y, z` linked only by an arithmetic predicate; q8's substr
    // equijoin). The executor attaches disconnected components as keyless
    // fan-out children whose early residual prunes during expansion, so no
    // connectivity check is needed here.

    // HAVING: a row-space predicate over the per-group result rows (it may
    // reference aggregates, like the SELECT list).
    let having = select
        .having
        .as_ref()
        .map(|e| b.bind_output(e, &group, &mut aggs))
        .transpose()?;

    // ORDER BY: an output column (alias or name), an ordinal, or an
    // arbitrary expression — the latter appends a HIDDEN output column
    // used only for ordering (dropped from the final rows). SELECT aliases
    // become visible to expression binding for this pass only.
    b.output_aliases = output
        .iter()
        .map(|o| (o.name.clone(), o.expr.clone()))
        .collect();
    let mut order_by = Vec::new();
    let mut hidden_outputs = 0usize;
    if let Some(ob) = &query.order_by {
        let ast::OrderByKind::Expressions(exprs) = &ob.kind else {
            return Err("unsupported ORDER BY form".into());
        };
        for oe in exprs {
            let desc = oe.options.asc == Some(false);
            let named = ident_parts(&oe.expr).and_then(|parts| {
                let name = parts.last().expect("nonempty ident");
                output.iter().position(|o| o.name == *name)
            });
            let idx = if let Some(idx) = named {
                idx
            } else if let ast::Expr::Value(v) = &oe.expr
                && let ast::Value::Number(n, _) = &v.value
                && let Ok(pos) = n.parse::<usize>()
                && (1..=output.len()).contains(&pos)
            {
                pos - 1
            } else {
                let expr = if plain_rows {
                    b.bind_scalar(&oe.expr)?
                } else {
                    b.bind_output(&oe.expr, &group, &mut aggs)?
                };
                match output.iter().position(|o| o.expr == expr) {
                    Some(idx) => idx,
                    None => {
                        output.push(OutputExpr {
                            expr,
                            name: format!("__ord{}", output.len()),
                        });
                        hidden_outputs += 1;
                        output.len() - 1
                    }
                }
            };
            order_by.push(OrderByKey { output: idx, desc });
        }
    }
    b.output_aliases.clear();

    // LIMIT n.
    let limit = match &query.limit_clause {
        None => None,
        Some(ast::LimitClause::LimitOffset {
            limit,
            offset: None,
            limit_by,
        }) if limit_by.is_empty() => match limit {
            None => None,
            Some(ast::Expr::Value(v)) => match &v.value {
                ast::Value::Number(s, _) => {
                    Some(s.parse::<usize>().map_err(|_| format!("bad LIMIT '{s}'"))?)
                }
                other => return Err(format!("unsupported LIMIT: {other}")),
            },
            Some(other) => return Err(format!("unsupported LIMIT expression: {other}")),
        },
        Some(other) => return Err(format!("unsupported LIMIT clause: {other:?}")),
    };

    let tables = b
        .tables
        .into_iter()
        .zip(filters)
        .map(|(bt, fs)| {
            // A table referenced only by `count(*)` (no column touched)
            // still needs one projected column, or the scan can't report
            // a row count (an empty chunk reads as zero rows).
            let mut projection = bt.used;
            if projection.is_empty()
                && let Some(c0) = bt.def.columns.first()
            {
                projection.push(ScanColumn {
                    name: c0.name.clone(),
                    leaf: c0.leaf,
                    ty: c0.ty,
                    dec_scale: c0.dec_scale,
                    nullable: c0.nullable,
                });
            }
            TableInput {
                name: bt.display,
                source: bt.source,
                projection,
                filter: fs.into_iter().reduce(and),
            }
        })
        .collect();

    // Synthetic columns were bound against sentinels (agg count wasn't final
    // yet): remap them to true row-space positions. Layout is
    // `[keys…, aggs…, grouping flags…, windows…]`. GROUPING flags (one per
    // key) sit before windows so a window spec can read them; they exist
    // only when GROUPING is used.
    let mut output = output;
    let mut having = having;
    let has_grouping = b.uses_grouping;
    let mut windows = b.windows;
    if has_grouping || !windows.is_empty() {
        let group_base = group.len() + aggs.len();
        let flag_count = if has_grouping { group.len() } else { 0 };
        // A no-GROUP-BY window query (`windowed_plain`) has no grouping flags
        // and its base row space IS the slot space.
        let win_base = if group.is_empty() && aggs.is_empty() {
            b.slots.len()
        } else {
            group_base + flag_count
        };
        for o in &mut output {
            remap_window_cols(&mut o.expr, win_base, group_base);
        }
        if let Some(h) = &mut having {
            remap_window_cols(h, win_base, group_base);
        }
        // Window specs may reference GROUPING flags (q36/q70's `PARTITION BY
        // grouping(a) + grouping(b)`); remap those (they carry no window
        // sentinels of their own).
        for w in &mut windows {
            remap_window_cols(&mut w.arg, win_base, group_base);
            for p in &mut w.partition {
                remap_window_cols(p, win_base, group_base);
            }
            for (o, _) in &mut w.order {
                remap_window_cols(o, win_base, group_base);
            }
        }
    }

    Ok(BoundQuery {
        tables,
        edges,
        slots: b.slots,
        post_filter,
        group,
        rollup_terms,
        aggs,
        having,
        output,
        hidden_outputs,
        has_grouping,
        windows,
        distinct,
        order_by,
        limit,
        subqueries: b.subs,
        derived: b.derived,
        set_ops: Vec::new(),
    })
}

/// Rewrite the synthetic-column sentinels to their true row-space
/// positions: `Column(GROUPING_BASE + k)` → `Column(group_base + k)` and
/// `Column(WINDOW_BASE + i)` → `Column(win_base + i)`. Grouping is checked
/// first because `GROUPING_BASE > WINDOW_BASE`.
fn remap_window_cols(e: &mut Expr, win_base: usize, group_base: usize) {
    match e {
        Expr::Column(i) => {
            if *i >= GROUPING_BASE {
                *i = group_base + (*i - GROUPING_BASE);
            } else if *i >= WINDOW_BASE {
                *i = win_base + (*i - WINDOW_BASE);
            }
        }
        Expr::Literal(_) | Expr::ScalarSub(_) => {}
        Expr::Binary { lhs, rhs, .. } => {
            remap_window_cols(lhs, win_base, group_base);
            remap_window_cols(rhs, win_base, group_base);
        }
        Expr::Extract { arg: i, .. } | Expr::DateTrunc { arg: i, .. } | Expr::CastInt(i) | Expr::Round { expr: i, .. } | Expr::Upper(i) => {
            remap_window_cols(i, win_base, group_base)
        }
        Expr::Like { expr, .. }
        | Expr::InSub { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::InSetStr { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Substr { expr, .. } => remap_window_cols(expr, win_base, group_base),
        Expr::Concat(parts) | Expr::NumFn { args: parts, .. } | Expr::StrFn { args: parts, .. } => {
            for p in parts {
                remap_window_cols(p, win_base, group_base);
            }
        }
        Expr::Case { whens, else_ } => {
            for (c, v) in whens {
                remap_window_cols(c, win_base, group_base);
                remap_window_cols(v, win_base, group_base);
            }
            remap_window_cols(else_, win_base, group_base);
        }
    }
}

/// Bind a set-operation query (`a UNION ALL b INTERSECT c …`): each side
/// binds as a standalone block; the tree is left-deep, so the first side
/// becomes the base block carrying the combined ORDER BY / LIMIT and the
/// rest attach as [`BoundQuery::set_ops`].
fn bind_set_query(
    query: &ast::Query,
    catalog: &Catalog,
    ctes: &CteMap,
) -> Result<BoundQuery, String> {
    type Tagged<'a> = (
        Option<(ast::SetOperator, ast::SetQuantifier)>,
        &'a ast::SetExpr,
    );
    fn flatten<'a>(body: &'a ast::SetExpr, out: &mut Vec<Tagged<'a>>) -> Result<(), String> {
        match body {
            ast::SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                flatten(left, out)?;
                if matches!(right.as_ref(), ast::SetExpr::SetOperation { .. }) {
                    return Err("right-nested set operations are not yet supported".into());
                }
                out.push((Some((*op, *set_quantifier)), right));
                Ok(())
            }
            other => {
                out.push((None, other));
                Ok(())
            }
        }
    }
    let mut sides = Vec::new();
    flatten(query.body.as_ref(), &mut sides)?;

    // A side combined by a SET-flavored operator (UNION / INTERSECT /
    // EXCEPT — everything but UNION ALL) contributes only its DISTINCT
    // rows, so it binds with set semantics: a plain projection folds into
    // a GROUP BY and dedup runs in the PARALLEL aggregation, instead of
    // execute_set sorting the side's raw rows on one thread (q14's
    // INTERSECT sides arrived as 28.8M raw fact rows — 7s of sort). The
    // base pre-dedups when the FIRST operator is set-flavored (that op
    // dedups the combination anyway). The fold reroutes the side through
    // the grouped path, so it only fires for PLAIN-IDENTIFIER projections
    // (q75's `cs_quantity - COALESCE(…)` UNION sides don't re-bind as
    // group keys); everything else keeps row semantics — execute_set's
    // combine-time dedup still enforces the set semantics there.
    let flavored = |t: &Tagged| {
        !matches!(
            t.0,
            None | Some((ast::SetOperator::Union, ast::SetQuantifier::All))
        )
    };
    let foldable = |body: &ast::SetExpr| match body {
        ast::SetExpr::Select(s) => s.projection.iter().all(|it| match it {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                ident_parts(e).is_some()
            }
            _ => false,
        }),
        _ => false,
    };

    let mut base: Option<BoundQuery> = None;
    let mut ops: Vec<(SetOp, BoundQuery)> = Vec::new();
    for (i, (op, side)) in sides.iter().enumerate() {
        // Rebuild the side as a standalone query. The outer ORDER BY /
        // LIMIT stay with the FIRST side (the executor applies them to
        // the combined rows); CTEs were already folded into `ctes`.
        let mut qside = query.clone();
        qside.with = None;
        *qside.body = (*side).clone();
        if i != 0 {
            qside.order_by = None;
            qside.limit_clause = None;
        }
        let set_sem = if i == 0 {
            sides.get(1).is_some_and(&flavored)
        } else {
            flavored(&(*op, *side))
        } && foldable(side);
        let bq = bind_query(&qside, catalog, set_sem, ctes)?;
        match (i, op) {
            (0, _) => base = Some(bq),
            (_, Some((o, quant))) => {
                use ast::{SetOperator as O, SetQuantifier as Q};
                let sop = match (o, quant) {
                    (O::Union, Q::All) => SetOp::UnionAll,
                    (O::Union, Q::None | Q::Distinct) => SetOp::Union,
                    (O::Intersect, Q::None | Q::Distinct) => SetOp::Intersect,
                    (O::Except | O::Minus, Q::None | Q::Distinct) => SetOp::Except,
                    other => return Err(format!("unsupported set operation: {other:?}")),
                };
                ops.push((sop, bq));
            }
            _ => unreachable!("non-first side always carries an operator"),
        }
    }
    let mut base = base.expect("at least one side");
    if base.hidden_outputs > 0 {
        return Err(
            "ORDER BY expressions over a set operation are not yet supported — \
             name an output column"
                .into(),
        );
    }
    let n = base.output.len();
    for (_, side) in &ops {
        if side.output.len() != n {
            return Err("set-operation sides have different column counts".into());
        }
    }
    base.set_ops = ops;
    Ok(base)
}

/// How a bound WHERE conjunct attributes to tables.
enum Attribution {
    /// Pure literals only.
    None,
    /// References exactly one table.
    Single(usize),
    /// References several tables (→ post-join filter).
    Multi,
}

/// OR-factoring: if `conj` is an OR whose every branch shares common
/// conjuncts (`(J ∧ A) ∨ (J ∧ B)`), emit the common conjuncts `J` and the
/// residual `A ∨ B` separately; otherwise emit `conj` unchanged. The TPC-H
/// Q19 shape — the join condition and shared filters live inside each
/// branch — factors into a plain equi-join plus a post-join OR.
fn factor_or(conj: &ast::Expr, out: &mut Vec<ast::Expr>) {
    let mut branches_raw = Vec::new();
    split_or(conj, &mut branches_raw);
    if branches_raw.len() < 2 {
        out.push(conj.clone());
        return;
    }
    let branches: Vec<Vec<&ast::Expr>> = branches_raw
        .iter()
        .map(|b| {
            let mut v = Vec::new();
            split_and(b, &mut v);
            v
        })
        .collect();
    let common: Vec<&ast::Expr> = branches[0]
        .iter()
        .copied()
        .filter(|c| branches[1..].iter().all(|b| b.iter().any(|x| x == c)))
        .collect();
    if common.is_empty() {
        out.push(conj.clone());
        return;
    }
    for c in &common {
        out.push((*c).clone());
    }
    // Residue: each branch minus the common conjuncts, OR-ed back together.
    // A branch with no residue makes the whole OR vacuous — skip it then.
    let mut residues: Vec<ast::Expr> = Vec::new();
    for b in &branches {
        let rest: Vec<&ast::Expr> = b
            .iter()
            .copied()
            .filter(|c| !common.iter().any(|x| x == c))
            .collect();
        if rest.is_empty() {
            return; // one branch is fully implied ⇒ residual OR is true.
        }
        residues.push(
            rest.into_iter()
                .cloned()
                .reduce(|l, r| ast::Expr::BinaryOp {
                    left: Box::new(l),
                    op: ast::BinaryOperator::And,
                    right: Box::new(r),
                })
                .expect("nonempty residue"),
        );
    }
    out.push(
        residues
            .into_iter()
            .reduce(|l, r| ast::Expr::BinaryOp {
                left: Box::new(l),
                op: ast::BinaryOperator::Or,
                right: Box::new(r),
            })
            .expect("≥2 branches"),
    );
}

/// Flatten an `OR` tree into its branches.
fn split_or<'e>(e: &'e ast::Expr, out: &mut Vec<&'e ast::Expr>) {
    if let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOperator::Or,
        right,
    } = e
    {
        split_or(left, out);
        split_or(right, out);
    } else if let ast::Expr::Nested(inner) = e {
        split_or(inner, out);
    } else {
        out.push(e);
    }
}

/// One table in scope: its display name (alias-aware), definition (owned —
/// derived tables have synthetic defs), source, and the columns referenced
/// so far (its scan projection, in first-use order).
struct BoundTable {
    display: String,
    def: TableDef,
    source: TableSource,
    used: Vec<ScanColumn>,
}

/// An inlined derived table (a plain select-project FROM-subquery): its
/// alias and column-name → defining-AST-expression map. References to view
/// columns bind by recursively binding the defining expression in the
/// merged scope.
struct ViewMap {
    alias: String,
    cols: Vec<(String, ast::Expr)>,
}

impl ViewMap {
    fn get(&self, name: &str) -> Option<&ast::Expr> {
        // Unquoted SQL identifiers are case-insensitive: prefer an exact
        // hit, fall back to a case-insensitive one.
        self.cols
            .iter()
            .find(|(n, _)| n == name)
            .or_else(|| self.cols.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)))
            .map(|(_, e)| e)
    }
}

/// Per-query binding state.
/// Sentinel row-space base for window references while binding (the agg
/// count isn't final until the whole block binds); remapped to
/// `group.len() + aggs.len() + grouping_flags + i` at the end of
/// `bind_query`.
const WINDOW_BASE: usize = 1 << 20;

/// Sentinel row-space base for `GROUPING(col)` references — remapped to
/// `group.len() + aggs.len() + key_index`. Above [`WINDOW_BASE`] so the
/// remap can disambiguate the two by magnitude (grouping checked first).
const GROUPING_BASE: usize = 1 << 21;

struct Binder<'a> {
    catalog: &'a Catalog,
    ctes: &'a CteMap,
    tables: Vec<BoundTable>,
    /// The global slot space: slot `s` = `(table, col-in-projection)`.
    slots: Vec<Slot>,
    /// Tables touched by the expression currently being bound (single-table
    /// attribution for filters).
    touched: BTreeSet<usize>,
    /// Subqueries bound so far (referenced by `Expr::ScalarSub` / `InSub`).
    subs: Vec<BoundQuery>,
    /// Materialized derived queries (`TableSource::Derived` indices).
    derived: Vec<std::sync::Arc<BoundQuery>>,
    /// Inlined derived tables.
    views: Vec<ViewMap>,
    /// Window expressions bound so far — referenced as
    /// `Column(WINDOW_BASE + i)` until the final remap.
    windows: Vec<WindowExpr>,
    /// Join edges created outside the WHERE loop (correlated-scalar
    /// decorrelation), merged into the query's edges.
    extra_edges: Vec<JoinEdge>,
    /// WHERE conjuncts inherited from inlined derived tables and JOIN…ON
    /// clauses; the bool marks ON-origin (exempt from the LEFT-side WHERE
    /// guard — ON conditions are pre-join filters by definition).
    pending_conjuncts: Vec<(ast::Expr, bool)>,
    /// Derived-table aliases (lowercased) that must MATERIALIZE even when
    /// inline-eligible — set by the offset-equijoin promotion: inlining a
    /// participant would re-open a join cycle whose broken edge falls back
    /// to the fan-out residual path this rewrite exists to avoid.
    force_materialize: BTreeSet<String>,
    /// Tables joined via LEFT OUTER (their rows may be unmatched).
    left_tables: BTreeSet<usize>,
    /// A window query with no GROUP BY (`rank() OVER … FROM derived`): its
    /// non-window projections are slot-space passthroughs, so `bind_output`
    /// returns a plain column reference instead of demanding a GROUP BY key.
    plain_passthrough: bool,
    /// `GROUPING(col)` appeared — the executor must append the per-key
    /// grouping-flag columns (see [`BoundQuery::has_grouping`]).
    uses_grouping: bool,
    /// Output (SELECT) aliases in row space, consulted only while binding
    /// ORDER BY: an alias may appear *inside* an ORDER BY expression
    /// (`CASE WHEN lochierarchy = 0 THEN … END`, q36/q70), not just as a
    /// bare key. Empty except during that pass.
    output_aliases: Vec<(String, Expr)>,
}

/// A partially-bound expression: either a real bound expression, or a
/// still-foldable decimal literal. Literal arithmetic folds in `Dec`
/// (exact); a `Dec` materializes to a typed [`ScalarValue`] only when it
/// meets a non-literal context (or the tree root).
enum Bound {
    Expr(Expr),
    Dec(Dec),
}

impl Binder<'_> {
    /// Register one FROM item: a catalog table, a CTE reference
    /// (materialized), or a derived subquery (inlined when it is a plain
    /// select-project; materialized when it aggregates).
    fn add_from_item(&mut self, relation: &ast::TableFactor) -> Result<(), String> {
        match relation {
            ast::TableFactor::Table { name, alias, .. } => {
                let tname = name.to_string();
                let display = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| tname.clone());
                if let Some((cte_bq, cols, cte_ast)) = self.ctes.get(&tname) {
                    // A SCALAR-aggregate CTE (1 row) past the first FROM
                    // item: cross-join-as-constants via scalar-subquery
                    // views (q77's `FROM cs, cr`), same as the derived form.
                    if !self.tables.is_empty() && scalar_agg_body(cte_ast) {
                        let cte_ast = cte_ast.clone();
                        return self.push_scalar_view(display, &cte_ast);
                    }
                    // CTE reference → materialized derived table.
                    let idx = self.derived.len();
                    self.derived.push(cte_bq.clone());
                    let def = TableDef {
                        path: PathBuf::new(),
                        columns: cols
                            .iter()
                            .enumerate()
                            .map(|(i, (n, ty))| crate::catalog::ColumnDef {
                                name: n.clone(),
                                leaf: i,
                                ty: *ty,
                                dec_scale: None,
                                nullable: false,
                            })
                            .collect(),
                    };
                    self.push_table(display, def, TableSource::Derived(idx))
                } else {
                    let def = self
                        .catalog
                        .table(&tname)
                        .ok_or_else(|| format!("unknown table '{tname}'"))?
                        .clone();
                    let source = TableSource::Parquet(def.path.clone());
                    self.push_table(display, def, source)
                }
            }
            ast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let Some(alias) = alias else {
                    return Err("FROM subqueries need an alias".into());
                };
                // A plain select-project body INLINES into this scope;
                // anything else (aggregates, set operations, DISTINCT,
                // ORDER/LIMIT) MATERIALIZES as a derived table.
                let inner_select = match subquery.body.as_ref() {
                    ast::SetExpr::Select(inner) => Some(inner),
                    _ => None,
                };
                let plain = inner_select.is_some_and(|inner| {
                    !self
                        .force_materialize
                        .contains(&alias.name.value.to_ascii_lowercase())
                        && matches!(
                            &inner.group_by,
                            ast::GroupByExpr::Expressions(g, m) if g.is_empty() && m.is_empty()
                        )
                        && inner.having.is_none()
                        && subquery.order_by.is_none()
                        && subquery.limit_clause.is_none()
                        && inner.distinct.is_none()
                        // A FULL OUTER join must MATERIALIZE so bind_query's
                        // UNION-ALL rewrite fires (q51's derived wrapper).
                        && inner.from.iter().all(|twj| {
                            twj.joins.iter().all(|j| {
                                !matches!(j.join_operator, ast::JoinOperator::FullOuter(_))
                            })
                        })
                        // Inlining merges the inner tables into THIS scope —
                        // a name already registered (q2/q59's twin deriveds
                        // over the same CTE + date_dim) must MATERIALIZE
                        // instead, or the displays collide.
                        && inner.from.iter().all(|twj| match &twj.relation {
                            ast::TableFactor::Table { name, alias, .. } => {
                                let d = alias
                                    .as_ref()
                                    .map(|a| a.name.value.clone())
                                    .unwrap_or_else(|| name.to_string());
                                !self.tables.iter().any(|t| t.display.eq_ignore_ascii_case(&d))
                                    && !self.views.iter().any(|v| v.alias.eq_ignore_ascii_case(&d))
                            }
                            _ => true,
                        })
                        && inner.projection.iter().all(|it| match it {
                            ast::SelectItem::UnnamedExpr(e)
                            | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                                !contains_function(e)
                            }
                            _ => false,
                        })
                });
                if plain {
                    let inner = inner_select.expect("plain implies a Select body");
                    // INLINE: merge its tables + WHERE into this scope, and
                    // expose its select items as view columns. JOIN … ON
                    // clauses route exactly like the top-level FROM loop
                    // (q93's `store_sales LEFT OUTER JOIN store_returns ON …`
                    // inside a derived table).
                    for twj in &inner.from {
                        self.add_from_item(&twj.relation)?;
                        for join in &twj.joins {
                            self.add_join(join)?;
                        }
                    }
                    if let Some(w) = &inner.selection {
                        let mut cs = Vec::new();
                        split_and(w, &mut cs);
                        self.pending_conjuncts
                            .extend(cs.into_iter().cloned().map(|c| (c, false)));
                    }
                    let mut cols = Vec::new();
                    for (i, item) in inner.projection.iter().enumerate() {
                        let (e, name) = match item {
                            ast::SelectItem::ExprWithAlias { expr, alias } => {
                                (expr, alias.value.clone())
                            }
                            ast::SelectItem::UnnamedExpr(e) => (
                                e,
                                match e {
                                    ast::Expr::Identifier(id) => id.value.clone(),
                                    ast::Expr::CompoundIdentifier(ids) => ids
                                        .last()
                                        .map(|x| x.value.clone())
                                        .unwrap_or_else(|| format!("col{i}")),
                                    _ => format!("col{i}"),
                                },
                            ),
                            _ => unreachable!("checked plain above"),
                        };
                        cols.push((name, e.clone()));
                    }
                    self.views.push(ViewMap {
                        alias: alias.name.value.clone(),
                        cols,
                    });
                    Ok(())
                } else if !self.tables.is_empty() && scalar_agg_body(subquery) {
                    // A SCALAR-AGGREGATE derived (no GROUP BY → exactly one
                    // row) past the first FROM item is a CROSS join of a
                    // 1-row table (q28/q61/q88/q90's bucket shape).
                    self.push_scalar_view(alias.name.value.clone(), subquery)
                } else {
                    // MATERIALIZE: bind the aggregate inner as a derived
                    // query with an inferred output schema.
                    let bq = bind_query(subquery, self.catalog, false, self.ctes)?;
                    let tys = output_types(&bq);
                    let names: Vec<String> = if alias.columns.is_empty() {
                        bq.output.iter().map(|o| o.name.clone()).collect()
                    } else {
                        alias.columns.iter().map(|c| c.name.value.clone()).collect()
                    };
                    if names.len() != tys.len() {
                        return Err(format!(
                            "derived table '{}' column list arity mismatch",
                            alias.name.value
                        ));
                    }
                    let idx = self.derived.len();
                    self.derived.push(std::sync::Arc::new(bq));
                    let def = TableDef {
                        path: PathBuf::new(),
                        columns: names
                            .into_iter()
                            .zip(tys)
                            .enumerate()
                            .map(|(i, (n, ty))| crate::catalog::ColumnDef {
                                name: n,
                                leaf: i,
                                ty,
                                dec_scale: None,
                                nullable: false,
                            })
                            .collect(),
                    };
                    self.push_table(alias.name.value.clone(), def, TableSource::Derived(idx))
                }
            }
            other => Err(format!("unsupported FROM item: {other}")),
        }
    }

    /// If `f` is `round(x [, digits])` or `upper(x)`, bind it (slot space).
    /// `Ok(None)` = neither.
    fn bind_round_or_upper(&mut self, f: &ast::Function) -> Result<Option<Expr>, String> {
        let fname = f.name.to_string().to_lowercase();
        const KNOWN: &[&str] = &[
            "round", "upper", "lower", "lcase", "replace", "length", "char_length",
            "character_length", "len", "mod", "date_trunc", "datetrunc",
        ];
        if !KNOWN.contains(&fname.as_str()) {
            return Ok(None);
        }
        let ast::FunctionArguments::List(list) = &f.args else {
            return Err(format!("'{fname}' needs an argument list"));
        };
        let mut args: Vec<&ast::Expr> = Vec::new();
        for a in &list.args {
            match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => args.push(e),
                other => return Err(format!("unsupported {fname} argument: {other}")),
            }
        }
        match (fname.as_str(), args.as_slice()) {
            ("upper", [x]) => Ok(Some(Expr::Upper(Box::new(self.bind_scalar(x)?)))),
            ("lower" | "lcase", [x]) => Ok(Some(Expr::StrFn {
                func: StrFn::Lower,
                args: vec![self.bind_scalar(x)?],
            })),
            ("replace", [s, a, b]) => Ok(Some(Expr::StrFn {
                func: StrFn::Replace,
                args: vec![
                    self.bind_scalar(s)?,
                    self.bind_scalar(a)?,
                    self.bind_scalar(b)?,
                ],
            })),
            ("length" | "char_length" | "character_length" | "len", [x]) => {
                Ok(Some(Expr::NumFn {
                    func: NumFn::Length,
                    args: vec![self.bind_scalar(x)?],
                }))
            }
            ("mod", [a, b]) => Ok(Some(Expr::NumFn {
                func: NumFn::Mod,
                args: vec![self.bind_scalar(a)?, self.bind_scalar(b)?],
            })),
            ("date_trunc" | "datetrunc", [u, d]) => {
                let unit = match self.clone_free_literal(u)? {
                    ScalarValue::Utf8(s) => bind_trunc_unit(&s)?,
                    other => return Err(format!("date_trunc unit must be a string: {other:?}")),
                };
                Ok(Some(Expr::DateTrunc {
                    unit,
                    arg: Box::new(self.bind_scalar(d)?),
                }))
            }
            ("round", [x]) => Ok(Some(Expr::Round {
                expr: Box::new(self.bind_scalar(x)?),
                digits: 0,
            })),
            ("round", [x, d]) => {
                let digits = match self.clone_free_literal(d)? {
                    ScalarValue::Int64(v) => v as i32,
                    other => return Err(format!("round digits must be an integer: {other:?}")),
                };
                Ok(Some(Expr::Round {
                    expr: Box::new(self.bind_scalar(x)?),
                    digits,
                }))
            }
            _ => Err(format!("'{fname}' takes the wrong number of arguments")),
        }
    }

    /// Register a SCALAR-aggregate query (guaranteed one row) as a view of
    /// single-column scalar subqueries: each column reference substitutes
    /// as a constant through the uncorrelated-subquery machinery, making a
    /// cross join of the 1-row table edge-free (q28/q61/q77/q88/q90).
    fn push_scalar_view(&mut self, alias: String, subquery: &ast::Query) -> Result<(), String> {
        let ast::SetExpr::Select(inner) = subquery.body.as_ref() else {
            unreachable!("scalar_agg_body checked Select");
        };
        let mut cols = Vec::new();
        for (i, item) in inner.projection.iter().enumerate() {
            let (e_item, name) = match item {
                ast::SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                ast::SelectItem::UnnamedExpr(e) => (e, format!("col{i}")),
                other => {
                    return Err(format!("unsupported select item: {other}"));
                }
            };
            let mut q1 = subquery.clone();
            let ast::SetExpr::Select(s1) = q1.body.as_mut() else {
                unreachable!("checked Select");
            };
            s1.projection = vec![ast::SelectItem::UnnamedExpr(e_item.clone())];
            q1.order_by = None;
            q1.limit_clause = None;
            cols.push((name, ast::Expr::Subquery(Box::new(q1))));
        }
        self.views.push(ViewMap { alias, cols });
        Ok(())
    }

    /// Register an explicit `JOIN … ON` clause. INNER joins route their ON
    /// conjuncts exactly like WHERE conjuncts; LEFT OUTER joins mark the
    /// joined table left-preserved: its edge remembers the preserved side,
    /// single-table ON conditions on the joined table become its pre-join
    /// filter (correct outer-join semantics), and conditions on the
    /// preserved side error (they are not WHERE filters).
    fn add_join(&mut self, join: &ast::Join) -> Result<(), String> {
        let (on, left) = match &join.join_operator {
            ast::JoinOperator::Inner(ast::JoinConstraint::On(e))
            | ast::JoinOperator::Join(ast::JoinConstraint::On(e)) => (e, false),
            // `LEFT JOIN` and `LEFT OUTER JOIN` parse as distinct variants
            // but mean the same thing.
            ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(e))
            | ast::JoinOperator::Left(ast::JoinConstraint::On(e)) => (e, true),
            other => return Err(format!("unsupported join: {other:?}")),
        };
        let ntables_before = self.tables.len();
        self.add_from_item(&join.relation)?;
        if self.tables.len() != ntables_before + 1 {
            return Err("a joined relation must be a single table".into());
        }
        let new_t = ntables_before;
        if left {
            self.left_tables.insert(new_t);
        }
        let mut conjuncts = Vec::new();
        split_and(on, &mut conjuncts);
        for conj in conjuncts {
            if let ast::Expr::BinaryOp {
                left: l,
                op: ast::BinaryOperator::Eq,
                right: r,
            } = conj
            {
                if let (Some(lp), Some(rp)) = (ident_parts(l), ident_parts(r)) {
                    if self.is_real_column(&lp) && self.is_real_column(&rp) {
                        let a = self.resolve_parts(&lp)?;
                        let bb = self.resolve_parts(&rp)?;
                        let (ta, tb) = (self.slots[a].table, self.slots[bb].table);
                        if ta != tb {
                            let preserved = if left {
                                Some(if ta == new_t { tb } else { ta })
                            } else {
                                None
                            };
                            self.extra_edges.push(JoinEdge {
                                a,
                                b: bb,
                                preserved,
                            });
                            continue;
                        }
                    }
                }
            }
            // Non-equi ON conjunct: must belong to the joined table (its
            // pre-join filter under LEFT semantics).
            let (e, attr) = self.bind_multi(conj)?;
            match attr {
                Attribution::Single(t) if t == new_t => {
                    self.pending_conjuncts.push((conj.clone(), true));
                    let _ = e; // re-bound in the WHERE pass
                }
                _ if !left => {
                    self.pending_conjuncts.push((conj.clone(), true));
                    let _ = e;
                }
                _ => {
                    return Err(format!(
                        "ON condition '{conj}' on a LEFT JOIN's preserved side is not yet \
                         supported"
                    ));
                }
            }
        }
        Ok(())
    }

    fn push_table(
        &mut self,
        display: String,
        def: TableDef,
        source: TableSource,
    ) -> Result<(), String> {
        if self
            .tables
            .iter()
            .any(|t| t.display.eq_ignore_ascii_case(&display))
            || self
                .views
                .iter()
                .any(|v| v.alias.eq_ignore_ascii_case(&display))
        {
            return Err(format!(
                "duplicate table name/alias '{display}' — alias one of them"
            ));
        }
        self.tables.push(BoundTable {
            display,
            def,
            source,
            used: Vec::new(),
        });
        Ok(())
    }

    fn slot_col(&self, s: usize) -> &ScanColumn {
        let Slot { table, col } = self.slots[s];
        &self.tables[table].used[col]
    }

    /// Is `parts` a plain column of a real (non-view) table in scope?
    fn is_real_column(&self, parts: &[&str]) -> bool {
        match parts {
            [c] => {
                self.views.iter().all(|v| v.get(c).is_none())
                    && self.tables.iter().any(|t| t.def.column(c).is_some())
            }
            [tbl, c] => self
                .tables
                .iter()
                .any(|t| t.display.eq_ignore_ascii_case(tbl) && t.def.column(c).is_some()),
            _ => false,
        }
    }

    /// Resolve a (possibly qualified) column name to its global slot,
    /// extending the owning table's scan projection on first use.
    fn resolve_parts(&mut self, parts: &[&str]) -> Result<usize, String> {
        let (t, cname) = match parts {
            [c] => {
                // Real (user-visible) tables take precedence for unqualified
                // names; synthetic decorrelation tables (`__corr…`/`__ex…`)
                // only resolve when no real table has the column — an
                // internal materialization must not make a query's own
                // column reference ambiguous (q16/q94).
                let mut hit = None;
                for synthetic_pass in [false, true] {
                    for (t, bt) in self.tables.iter().enumerate() {
                        if bt.display.starts_with("__") != synthetic_pass {
                            continue;
                        }
                        if bt.def.column(c).is_some() {
                            if let Some(prev) = hit {
                                return Err(format!(
                                    "column '{c}' is ambiguous (in '{}' and '{}') — qualify it",
                                    self.tables[prev as usize].display, bt.display
                                ));
                            }
                            hit = Some(t as u32);
                        }
                    }
                    if hit.is_some() {
                        break;
                    }
                }
                (
                    hit.ok_or_else(|| format!("unknown column '{c}'"))? as usize,
                    *c,
                )
            }
            [tbl, c] => {
                let t = self
                    .tables
                    .iter()
                    .position(|bt| bt.display.eq_ignore_ascii_case(tbl))
                    .ok_or_else(|| format!("unknown table or alias '{tbl}'"))?;
                if self.tables[t].def.column(c).is_none() {
                    return Err(format!("unknown column '{tbl}.{c}'"));
                }
                (t, *c)
            }
            _ => return Err(format!("unsupported name: {}", parts.join("."))),
        };
        self.touched.insert(t);
        let bt = &mut self.tables[t];
        let col = match bt
            .used
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(cname))
        {
            Some(i) => i,
            None => {
                let def = bt.def.column(cname).expect("column just checked");
                bt.used.push(ScanColumn {
                    name: def.name.clone(),
                    leaf: def.leaf,
                    ty: def.ty,
                    dec_scale: def.dec_scale,
                    nullable: def.nullable,
                });
                bt.used.len() - 1
            }
        };
        if let Some(s) = self.slots.iter().position(|s| s.table == t && s.col == col) {
            return Ok(s);
        }
        self.slots.push(Slot { table: t, col });
        Ok(self.slots.len() - 1)
    }

    /// Bind a scalar expression (slot space), fully materialized.
    fn bind_scalar(&mut self, e: &ast::Expr) -> Result<Expr, String> {
        Ok(materialize(self.bind(e)?))
    }

    /// Look a name up in the inlined-view maps (qualified by alias when
    /// given). Returns a clone of the defining AST expression.
    fn view_expr(&self, alias: Option<&str>, col: &str) -> Option<ast::Expr> {
        match alias {
            Some(a) => self
                .views
                .iter()
                .find(|v| v.alias.eq_ignore_ascii_case(a))
                .and_then(|v| v.get(col).cloned()),
            None => self.views.iter().find_map(|v| v.get(col).cloned()),
        }
    }

    /// Bind a WHERE conjunct and report which tables it touches — the
    /// routing signal: single-table conjuncts become that table's filter,
    /// multi-table conjuncts the post-join filter.
    fn bind_multi(&mut self, e: &ast::Expr) -> Result<(Expr, Attribution), String> {
        self.touched.clear();
        let bound = materialize(self.bind(e)?);
        let attr = match self.touched.len() {
            0 => Attribution::None,
            1 => Attribution::Single(*self.touched.first().expect("len 1")),
            _ => Attribution::Multi,
        };
        Ok((bound, attr))
    }

    /// Bind one SELECT item into **row space**: aggregate calls are pushed
    /// into `aggs` and replaced by row references; subtrees matching a
    /// GROUP BY expression become group-key references; literals pass
    /// through. Any other column reference is a non-aggregated column —
    /// an error.
    fn bind_output(
        &mut self,
        e: &ast::Expr,
        group: &[GroupExpr],
        aggs: &mut Vec<AggExpr>,
    ) -> Result<Expr, String> {
        // While binding ORDER BY, a bare identifier that names a SELECT
        // alias resolves to that output's (row-space) expression — the alias
        // may sit inside a larger ORDER BY expression, not just be the whole
        // key. A real GROUP BY column takes precedence (checked below).
        if let Some(name) = ident_parts(e).and_then(|p| p.last().map(|s| s.to_string()))
            && !self.is_real_column(&[name.as_str()])
            && let Some((_, ex)) = self
                .output_aliases
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(&name))
        {
            return Ok(ex.clone());
        }
        if !contains_function(e) {
            // Fast path: the whole expression is a single GROUP BY key or a
            // constant. `bind` may fail here (an ORDER BY expression can
            // embed a SELECT alias that is not a real column) — on failure,
            // fall through to structural recursion below.
            if let Ok(bnd) = self.bind(e) {
                let bound = materialize(bnd);
                if let Some(g) = group.iter().position(|ge| ge.expr == bound) {
                    return Ok(Expr::Column(g));
                }
                // In a no-GROUP-BY window query the row space IS the slot
                // space, so a plain column reference is a valid passthrough
                // output (it becomes a scan column the window stage reads
                // alongside the window results).
                if self.plain_passthrough || !references_columns(&bound) {
                    return Ok(bound);
                }
                // A BARE non-group column is an error; a COMPOUND expression
                // over group keys / aliases (`CASE WHEN alias=0 THEN key END`
                // in ORDER BY) recurses structurally below.
                if matches!(
                    e,
                    ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)
                ) {
                    return Err(format!("'{e}' is neither an aggregate nor a GROUP BY key"));
                }
            }
        }
        // A function-VALUED GROUP BY key used in the projection (`lower(x)`,
        // `mod(x, k)`, a scalar builtin) binds as a group reference — the
        // `!contains_function` fast path above only catches non-function keys
        // (a bare column or `EXTRACT`, which isn't an AST `Function`).
        if contains_function(e)
            && let Ok(bnd) = self.bind(e)
        {
            let bound = materialize(bnd);
            if let Some(g) = group.iter().position(|ge| ge.expr == bound) {
                return Ok(Expr::Column(g));
            }
        }
        match e {
            ast::Expr::Function(f) => {
                if f.over.is_some() {
                    return self.bind_window(f, group, aggs);
                }
                if let Some(k) = self.bind_grouping(f, group)? {
                    // GROUPING(col) → the grouping-flag column for that key;
                    // remapped to its true row-space position at block end.
                    self.uses_grouping = true;
                    return Ok(Expr::Column(GROUPING_BASE + k));
                }
                // concat over group keys / row-space parts (q66's
                // `concat(w_warehouse_name, …)` in a grouped projection).
                if f.name.to_string().eq_ignore_ascii_case("concat")
                    && let ast::FunctionArguments::List(list) = &f.args
                {
                    let parts = list
                        .args
                        .iter()
                        .map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(x)) => {
                                self.bind_output(x, group, aggs)
                            }
                            other => Err(format!("unsupported concat argument: {other}")),
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    return Ok(Expr::Concat(parts));
                }
                // round/upper over row-space arguments (round(sum(x),2)).
                let fname = f.name.to_string().to_lowercase();
                if (fname == "round" || fname == "upper")
                    && let ast::FunctionArguments::List(list) = &f.args
                {
                    let mut args: Vec<&ast::Expr> = Vec::new();
                    for a in &list.args {
                        match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(x)) => {
                                args.push(x)
                            }
                            other => {
                                return Err(format!("unsupported {fname} argument: {other}"));
                            }
                        }
                    }
                    return match (fname.as_str(), args.as_slice()) {
                        ("upper", [x]) => {
                            Ok(Expr::Upper(Box::new(self.bind_output(x, group, aggs)?)))
                        }
                        ("round", [x]) => Ok(Expr::Round {
                            expr: Box::new(self.bind_output(x, group, aggs)?),
                            digits: 0,
                        }),
                        ("round", [x, d]) => {
                            let digits = match self.clone_free_literal(d)? {
                                ScalarValue::Int64(v) => v as i32,
                                other => {
                                    return Err(format!(
                                        "round digits must be an integer: {other:?}"
                                    ));
                                }
                            };
                            Ok(Expr::Round {
                                expr: Box::new(self.bind_output(x, group, aggs)?),
                                digits,
                            })
                        }
                        _ => Err(format!("'{fname}' takes the wrong number of arguments")),
                    };
                }
                if let Some(rewritten) = rewrite_scalar_fn(f)? {
                    return self.bind_output(&rewritten, group, aggs);
                }
                let agg = self.bind_aggregate(e)?;
                aggs.push(agg);
                Ok(Expr::Column(group.len() + aggs.len() - 1))
            }
            ast::Expr::Case {
                operand: None,
                conditions,
                else_result,
                ..
            } => {
                let whens = conditions
                    .iter()
                    .map(|cw| {
                        Ok((
                            self.bind_output(&cw.condition, group, aggs)?,
                            self.bind_output(&cw.result, group, aggs)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let else_ = match else_result {
                    Some(e2) => self.bind_output(e2, group, aggs)?,
                    None => Expr::Literal(ScalarValue::Null),
                };
                Ok(Expr::Case {
                    whens,
                    else_: Box::new(else_),
                })
            }
            ast::Expr::Nested(inner) => self.bind_output(inner, group, aggs),
            ast::Expr::Subquery(_) => Ok(materialize(self.bind(e)?)),
            ast::Expr::BinaryOp { left, op, right } => {
                let op = bind_op(op)?;
                let l = self.bind_output(left, group, aggs)?;
                let r = self.bind_output(right, group, aggs)?;
                Ok(binary(op, l, r))
            }
            // A cast wrapping an aggregate (q49's `cast(sum(x) AS
            // DECIMAL(15,4))`): bind the inner aggregate in row space, then
            // apply the same numeric-cast typing as the scalar path.
            ast::Expr::Cast {
                expr, data_type, ..
            } => {
                use ast::DataType as DT;
                let inner = self.bind_output(expr, group, aggs)?;
                match data_type {
                    DT::Decimal(_)
                    | DT::Numeric(_)
                    | DT::Float(_)
                    | DT::Double(_)
                    | DT::DoublePrecision => Ok(float_cast_expr(inner)),
                    DT::Int(_)
                    | DT::Integer(_)
                    | DT::BigInt(_)
                    | DT::SmallInt(_)
                    | DT::Char(_)
                    | DT::Varchar(_)
                    | DT::Text
                    | DT::Date => Ok(inner),
                    other => Err(format!("unsupported CAST target over aggregate: {other}")),
                }
            }
            // SUBSTR over a row-space string (q8/q85's grouped
            // `substr(ca_zip, 1, 5)`): the inner binds in row space,
            // bounds are literals.
            ast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let inner = self.bind_output(expr, group, aggs)?;
                let mut lit_i64 = |e: &Option<Box<ast::Expr>>| -> Result<Option<i64>, String> {
                    match e {
                        None => Ok(None),
                        Some(x) => match self.clone_free_literal(x)? {
                            ScalarValue::Int64(v) => Ok(Some(v)),
                            other => Err(format!("SUBSTRING bounds must be integers: {other:?}")),
                        },
                    }
                };
                let from = lit_i64(substring_from)?
                    .ok_or("SUBSTRING requires a FROM position (so far)")?;
                let len = lit_i64(substring_for)?;
                Ok(Expr::Substr {
                    expr: Box::new(inner),
                    from,
                    len,
                })
            }
            other => Err(format!("unsupported expression over aggregates: {other}")),
        }
    }

    /// If `e` is a 'YYYY-MM-DD' string literal compared against a
    /// date-typed expression, fold it to a Date32 literal (the Spark
    /// query texts elide the `date` keyword).
    fn coerce_date_literal(&self, e: &mut Expr, other: &Expr) {
        let Expr::Literal(ScalarValue::Utf8(sv)) = &*e else {
            return;
        };
        if !self.expr_is_date(other) {
            return;
        }
        if let Ok(d) = parse_date32(sv) {
            *e = Expr::Literal(ScalarValue::Date32(d));
        }
    }

    fn expr_is_date(&self, e: &Expr) -> bool {
        match e {
            Expr::Column(sl) => self.slot_col(*sl).ty == LogicalType::Date32,
            Expr::Literal(ScalarValue::Date32(_)) => true,
            _ => false,
        }
    }

    /// Bind a table-free AST expression to a literal (substring bounds).
    fn clone_free_literal(&mut self, e: &ast::Expr) -> Result<ScalarValue, String> {
        match materialize(self.bind(e)?) {
            Expr::Literal(v) => Ok(v),
            other => Err(format!("expected a literal, got {other:?}")),
        }
    }

    /// Try to decorrelate `(SELECT <agg expr> FROM … WHERE inner_col =
    /// outer_col AND rest)` into a derived table `(SELECT inner_col,
    /// <agg expr> FROM … WHERE rest GROUP BY inner_col)` joined to the
    /// outer query on the correlation key; the subquery expression becomes
    /// a reference to the derived value column. Returns `None` when the
    /// subquery has no correlation (the uncorrelated path handles it).
    fn try_decorrelate_scalar(&mut self, sq: &ast::Query) -> Result<Option<Bound>, String> {
        let ast::SetExpr::Select(sel) = sq.body.as_ref() else {
            return Ok(None);
        };
        // The inner tables' defs — catalog tables or CTEs, optionally
        // aliased (q1's `customer_total_return ctr2`). Each entry pairs the
        // def with the name a qualified inner reference may use.
        let mut inner_defs: Vec<(TableDef, String)> = Vec::new();
        for twj in &sel.from {
            if !twj.joins.is_empty() {
                return Ok(None);
            }
            let ast::TableFactor::Table { name, alias, .. } = &twj.relation else {
                return Ok(None);
            };
            let tname = name.to_string();
            let display = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| tname.clone());
            let def = if let Some((_, cols, _)) = self.ctes.get(&tname) {
                TableDef {
                    path: PathBuf::new(),
                    columns: cols
                        .iter()
                        .enumerate()
                        .map(|(i, (n, ty))| crate::catalog::ColumnDef {
                            name: n.clone(),
                            leaf: i,
                            ty: *ty,
                            dec_scale: None,
                            nullable: false,
                        })
                        .collect(),
                }
            } else {
                match self.catalog.table(&tname) {
                    Some(d) => d.clone(),
                    None => return Ok(None),
                }
            };
            inner_defs.push((def, display));
        }
        let inner_has = |c: &str| inner_defs.iter().any(|(d, _)| d.column(c).is_some());
        // A qualified reference belongs to the inner scope when its
        // qualifier names an inner table/alias.
        let inner_qualified = |tbl: &str, c: &str| {
            inner_defs
                .iter()
                .any(|(d, disp)| disp.eq_ignore_ascii_case(tbl) && d.column(c).is_some())
        };
        let outer_has = |parts: &[&str]| match parts {
            [c] => !inner_has(c) && self.tables.iter().any(|t| t.def.column(c).is_some()),
            [tbl, c] => {
                !inner_qualified(tbl, c)
                    && self
                        .tables
                        .iter()
                        .any(|t| t.display.eq_ignore_ascii_case(tbl) && t.def.column(c).is_some())
            }
            _ => false,
        };

        // Find THE correlation conjunct(s) — after OR-factoring, which
        // hoists a correlation duplicated in every OR branch
        // (q41's `(corr AND A) OR (corr AND B)` → `corr AND (A OR B)`).
        let mut conjuncts_owned: Vec<ast::Expr> = Vec::new();
        if let Some(w) = &sel.selection {
            let mut raw = Vec::new();
            split_and(w, &mut raw);
            for c in raw {
                factor_or(c, &mut conjuncts_owned);
            }
        }
        let conjuncts: Vec<&ast::Expr> = conjuncts_owned.iter().collect();
        // One or more correlation equalities (Q20 correlates on partkey AND
        // suppkey — the derived table groups by the composite key).
        let mut corr: Vec<(String, Vec<String>)> = Vec::new(); // (inner col, outer parts)
        let mut rest: Vec<ast::Expr> = Vec::new();
        for conj in conjuncts {
            if let ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } = conj
            {
                if let (Some(lp), Some(rp)) = (ident_parts(left), ident_parts(right)) {
                    let pick = |ip: &[&str], op: &[&str]| -> Option<(String, Vec<String>)> {
                        match ip {
                            [c] if inner_has(c) && outer_has(op) => {
                                Some((c.to_string(), op.iter().map(|x| x.to_string()).collect()))
                            }
                            // Inner side qualified by an inner alias
                            // (`ctr2.ctr_store_sk = ctr1.ctr_store_sk`).
                            [tbl, c] if inner_qualified(tbl, c) && outer_has(op) => {
                                Some((c.to_string(), op.iter().map(|x| x.to_string()).collect()))
                            }
                            _ => None,
                        }
                    };
                    if let Some(found) = pick(&lp, &rp).or_else(|| pick(&rp, &lp)) {
                        corr.push(found);
                        continue;
                    }
                }
            }
            rest.push(conj.clone());
        }
        if corr.is_empty() {
            return Ok(None);
        }
        let [item] = sel.projection.as_slice() else {
            return Err("a scalar subquery must select exactly one column".into());
        };

        // Rebuild the subquery decorrelated: SELECT inner_col, <item> FROM …
        // WHERE rest GROUP BY inner_col.
        let mut q2 = sq.clone();
        let ast::SetExpr::Select(sel2) = q2.body.as_mut() else {
            unreachable!("checked Select above");
        };
        let key_idents: Vec<ast::Expr> = corr
            .iter()
            .map(|(c, _)| ast::Expr::Identifier(ast::Ident::new(c.clone())))
            .collect();
        sel2.projection = key_idents
            .iter()
            .map(|k| ast::SelectItem::UnnamedExpr(k.clone()))
            .chain([item.clone()])
            .collect();
        sel2.group_by = ast::GroupByExpr::Expressions(key_idents, Vec::new());
        sel2.selection = rest.into_iter().reduce(|l, r| ast::Expr::BinaryOp {
            left: Box::new(l),
            op: ast::BinaryOperator::And,
            right: Box::new(r),
        });

        let bq = bind_query(&q2, self.catalog, false, self.ctes)?;
        let tys = output_types(&bq);
        let idx = self.derived.len();
        self.derived.push(std::sync::Arc::new(bq));
        let display = format!("__corr{idx}");
        let mut columns: Vec<crate::catalog::ColumnDef> = corr
            .iter()
            .enumerate()
            .map(|(i, (c, _))| crate::catalog::ColumnDef {
                name: c.clone(),
                leaf: i,
                ty: tys[i],
                dec_scale: None,
                nullable: false,
            })
            .collect();
        columns.push(crate::catalog::ColumnDef {
            name: "__val".into(),
            leaf: corr.len(),
            ty: tys[corr.len()],
            dec_scale: None,
            nullable: false,
        });
        let def = TableDef {
            path: PathBuf::new(),
            columns,
        };
        self.push_table(display.clone(), def, TableSource::Derived(idx))?;

        // Join outer keys = derived keys (a composite link when several);
        // the subquery's value is the derived value column.
        for (inner_col, outer_parts) in &corr {
            let outer_ref: Vec<&str> = outer_parts.iter().map(|x| x.as_str()).collect();
            let outer_slot = self.resolve_parts(&outer_ref)?;
            let key_slot = self.resolve_parts(&[&display, inner_col])?;
            self.extra_edges.push(JoinEdge {
                a: outer_slot,
                b: key_slot,
                preserved: None,
            });
        }
        let val_slot = self.resolve_parts(&[&display, "__val"])?;
        Ok(Some(Bound::Expr(Expr::Column(val_slot))))
    }

    /// Decorrelate a `[NOT] EXISTS (subquery)` of the simple TPC-H shape —
    /// one inner table whose WHERE holds exactly one correlation
    /// `inner_col = outer_col`, the rest inner-only filters — into the
    /// semijoin `outer_col [NOT] IN (SELECT inner_col FROM inner WHERE
    /// rest)`, riding the IN-subquery machinery (set semantics = EXISTS's
    /// at-least-one). Richer correlation shapes error by name.
    fn bind_exists(&mut self, subquery: &ast::Query, negated: bool) -> Result<Bound, String> {
        let ast::SetExpr::Select(select) = subquery.body.as_ref() else {
            return Err("EXISTS subquery must be a plain SELECT".into());
        };
        // MULTI-table EXISTS (q10/q35/q69's `EXISTS (SELECT * FROM fact,
        // date_dim WHERE c_customer_sk = fact_customer_sk AND …)`): find
        // the single correlation equality, strip it, and bind the rest as
        // a set-semantics IN-subquery — `outer IN (SELECT inner FROM …)`.
        if select.from.len() > 1 {
            return self.bind_exists_multi(subquery, select, negated);
        }
        let [from] = select.from.as_slice() else {
            return Err("EXISTS subquery must have exactly one FROM table (so far)".into());
        };
        if !from.joins.is_empty() {
            return Err("JOIN inside EXISTS is not yet supported".into());
        }
        let ast::TableFactor::Table { name, alias, .. } = &from.relation else {
            return Err("EXISTS FROM must be a plain table".into());
        };
        let tname = name.to_string();
        let def = self
            .catalog
            .table(&tname)
            .ok_or_else(|| format!("unknown table '{tname}'"))?
            .clone();
        let display = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());

        // Split the inner WHERE: find the correlation conjuncts — one
        // equality (the join key) and optionally one INEQUALITY (`<>` on
        // another outer column, Q21's shape).
        let mut conjuncts = Vec::new();
        if let Some(w) = &select.selection {
            split_and(w, &mut conjuncts);
        }
        let mut eq_corr: Option<(String, usize)> = None; // (inner col, outer slot)
        let mut neq_corr: Option<(String, usize)> = None;
        let mut inner_conjuncts: Vec<&ast::Expr> = Vec::new();
        for conj in conjuncts {
            if let ast::Expr::BinaryOp { left, op, right } = conj {
                if matches!(op, ast::BinaryOperator::Eq | ast::BinaryOperator::NotEq) {
                    if let (Some(lp), Some(rp)) = (ident_parts(left), ident_parts(right)) {
                        // Inner columns may be qualified by the EXISTS
                        // table's alias (`l2.l_suppkey`).
                        let inner_of = |parts: &[&str]| match parts {
                            [c] if def.column(c).is_some() => Some(c.to_string()),
                            [t, c]
                                if t.eq_ignore_ascii_case(&display) && def.column(c).is_some() =>
                            {
                                Some(c.to_string())
                            }
                            _ => None,
                        };
                        match (inner_of(&lp), inner_of(&rp)) {
                            (Some(ic), None) | (None, Some(ic)) => {
                                let outer = if inner_of(&lp).is_some() { &rp } else { &lp };
                                let slot = self.resolve_parts(outer)?;
                                let target = if matches!(op, ast::BinaryOperator::Eq) {
                                    &mut eq_corr
                                } else {
                                    &mut neq_corr
                                };
                                if target.is_some() {
                                    return Err(
                                        "multiple correlated conditions of the same kind in \
                                         EXISTS are not yet supported"
                                            .into(),
                                    );
                                }
                                *target = Some((ic, slot));
                                continue;
                            }
                            _ => {}
                        }
                    }
                }
            }
            inner_conjuncts.push(conj);
        }
        let Some((inner_col, outer_slot)) = eq_corr else {
            return Err("uncorrelated EXISTS is not yet supported".into());
        };
        if let Some((ineq_col, outer_s_slot)) = neq_corr {
            return self.bind_exists_counted(
                def,
                display,
                inner_col,
                outer_slot,
                ineq_col,
                outer_s_slot,
                inner_conjuncts,
                negated,
            );
        }

        // Bind the inner query directly: SELECT inner_col FROM t WHERE rest
        // GROUP BY inner_col (set semantics).
        let source = TableSource::Parquet(def.path.clone());
        let mut inner = Binder {
            catalog: self.catalog,
            ctes: self.ctes,
            tables: vec![BoundTable {
                display,
                def,
                source,
                used: Vec::new(),
            }],
            slots: Vec::new(),
            touched: BTreeSet::new(),
            subs: Vec::new(),
            derived: Vec::new(),
            views: Vec::new(),
            windows: Vec::new(),
            extra_edges: Vec::new(),
            pending_conjuncts: Vec::new(),
            force_materialize: BTreeSet::new(),
            left_tables: BTreeSet::new(),
            plain_passthrough: false,
            uses_grouping: false,
            output_aliases: Vec::new(),
        };
        let key_slot = inner.resolve_parts(&[&inner_col])?;
        let mut inner_filters: Vec<Expr> = Vec::new();
        for conj in inner_conjuncts {
            let (e, _) = inner.bind_multi(conj)?;
            inner_filters.push(e);
        }
        let bq = BoundQuery {
            tables: vec![TableInput {
                name: inner.tables[0].display.clone(),
                source: inner.tables[0].source.clone(),
                projection: inner.tables[0].used.clone(),
                filter: inner_filters.into_iter().reduce(and),
            }],
            edges: Vec::new(),
            slots: inner.slots,
            post_filter: None,
            group: vec![GroupExpr {
                expr: Expr::Column(key_slot),
            }],
            rollup_terms: Vec::new(),
            aggs: Vec::new(),
            having: None,
            output: vec![OutputExpr {
                expr: Expr::Column(0),
                name: inner_col,
            }],
            hidden_outputs: 0,
            distinct: false,
            order_by: Vec::new(),
            limit: None,
            subqueries: inner.subs,
            has_grouping: false,
            windows: Vec::new(),
            set_ops: Vec::new(),
            derived: inner.derived,
        };
        self.subs.push(bq);
        Ok(Bound::Expr(Expr::InSub {
            expr: Box::new(Expr::Column(outer_slot)),
            sub: self.subs.len() - 1,
            negated,
        }))
    }

    /// A multi-table `[NOT] EXISTS`: exactly one correlation equality
    /// (inner col = outer col) among the WHERE conjuncts; everything else
    /// (including the inner tables' own join edges) stays in the rebuilt
    /// subquery, which binds through the ordinary multi-table machinery
    /// with set semantics.
    fn bind_exists_multi(
        &mut self,
        subquery: &ast::Query,
        select: &ast::Select,
        negated: bool,
    ) -> Result<Bound, String> {
        // Inner defs (catalog tables or CTEs, optionally aliased).
        let mut inner_defs: Vec<(TableDef, String)> = Vec::new();
        for twj in &select.from {
            if !twj.joins.is_empty() {
                return Err("JOIN inside EXISTS is not yet supported".into());
            }
            let ast::TableFactor::Table { name, alias, .. } = &twj.relation else {
                return Err("EXISTS FROM must be plain tables".into());
            };
            let tname = name.to_string();
            let display = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| tname.clone());
            let def = if let Some((_, cs, _)) = self.ctes.get(&tname) {
                TableDef {
                    path: PathBuf::new(),
                    columns: cs
                        .iter()
                        .enumerate()
                        .map(|(i, (n, ty))| crate::catalog::ColumnDef {
                            name: n.clone(),
                            leaf: i,
                            ty: *ty,
                            dec_scale: None,
                            nullable: false,
                        })
                        .collect(),
                }
            } else {
                self.catalog
                    .table(&tname)
                    .ok_or_else(|| format!("unknown table '{tname}'"))?
                    .clone()
            };
            inner_defs.push((def, display));
        }
        let inner_of = |parts: &[&str]| -> Option<()> {
            match parts {
                [c] => inner_defs
                    .iter()
                    .any(|(d, _)| d.column(c).is_some())
                    .then_some(()),
                [t, c] => inner_defs
                    .iter()
                    .any(|(d, disp)| disp.eq_ignore_ascii_case(t) && d.column(c).is_some())
                    .then_some(()),
                _ => None,
            }
        };

        let mut conjuncts = Vec::new();
        if let Some(w) = &select.selection {
            split_and(w, &mut conjuncts);
        }
        // (inner-side AST expr, outer slot)
        let mut corr: Option<(ast::Expr, usize)> = None;
        let mut rest: Vec<ast::Expr> = Vec::new();
        for conj in conjuncts {
            if let ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } = conj
                && let (Some(lp), Some(rp)) = (ident_parts(left), ident_parts(right))
            {
                let l_in = inner_of(&lp).is_some();
                let r_in = inner_of(&rp).is_some();
                // Exactly one side inner, the other resolvable outside.
                if l_in ^ r_in {
                    let (ie, op_) = if l_in {
                        (left.as_ref(), &rp)
                    } else {
                        (right.as_ref(), &lp)
                    };
                    let outer_parts: Vec<&str> = op_.to_vec();
                    if let Ok(slot) = self.resolve_parts(&outer_parts) {
                        if corr.is_some() {
                            return Err(
                                "multiple correlated conditions in EXISTS are not yet supported"
                                    .into(),
                            );
                        }
                        corr = Some((ie.clone(), slot));
                        continue;
                    }
                }
            }
            rest.push(conj.clone());
        }
        let Some((inner_expr, outer_slot)) = corr else {
            return Err("uncorrelated EXISTS is not yet supported".into());
        };

        // Rebuild: SELECT <inner corr col> FROM … WHERE rest — bound with
        // set semantics (membership only cares about the value set).
        let mut q2 = subquery.clone();
        let ast::SetExpr::Select(s2) = q2.body.as_mut() else {
            unreachable!("checked Select");
        };
        s2.projection = vec![ast::SelectItem::UnnamedExpr(inner_expr)];
        s2.selection = rest.into_iter().reduce(|l, r| ast::Expr::BinaryOp {
            left: Box::new(l),
            op: ast::BinaryOperator::And,
            right: Box::new(r),
        });
        let bq = bind_query(&q2, self.catalog, true, self.ctes)?;
        if bq.output.len() != 1 {
            return Err("an EXISTS rewrite must select exactly one column".into());
        }
        self.subs.push(bq);
        Ok(Bound::Expr(Expr::InSub {
            expr: Box::new(Expr::Column(outer_slot)),
            sub: self.subs.len() - 1,
            negated,
        }))
    }

    /// The count-based rewrite for EXISTS with an inequality correlation
    /// (Q21's `l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <>
    /// l1.l_suppkey`): a derived table computes, per join key,
    /// `count(distinct s)` and `min(s)` plus a constant matched marker
    /// `__m = 1`, and LEFT-joins to the outer query so misses read `__m=0`.
    /// Then
    ///   EXISTS      ⟺ `__m = 1 AND (cd ≥ 2 OR ms <> outer_s)`
    ///   NOT EXISTS  ⟺ `__m = 0 OR (cd = 1 AND ms = outer_s)`
    /// — "some row with this key has a different s" reduced to counting
    /// distinct s values (cd ≥ 1 whenever matched, so cd = 1 means the only
    /// s is `ms`).
    #[allow(clippy::too_many_arguments)]
    fn bind_exists_counted(
        &mut self,
        def: TableDef,
        display: String,
        inner_col: String,
        outer_slot: usize,
        ineq_col: String,
        outer_s_slot: usize,
        inner_conjuncts: Vec<&ast::Expr>,
        negated: bool,
    ) -> Result<Bound, String> {
        let source = TableSource::Parquet(def.path.clone());
        let mut inner = Binder {
            catalog: self.catalog,
            ctes: self.ctes,
            tables: vec![BoundTable {
                display,
                def,
                source,
                used: Vec::new(),
            }],
            slots: Vec::new(),
            touched: BTreeSet::new(),
            subs: Vec::new(),
            derived: Vec::new(),
            views: Vec::new(),
            windows: Vec::new(),
            extra_edges: Vec::new(),
            pending_conjuncts: Vec::new(),
            force_materialize: BTreeSet::new(),
            left_tables: BTreeSet::new(),
            plain_passthrough: false,
            uses_grouping: false,
            output_aliases: Vec::new(),
        };
        let key_slot = inner.resolve_parts(&[&inner_col])?;
        let s_slot = inner.resolve_parts(&[&ineq_col])?;
        let key_ty = inner.slot_col(key_slot).ty;
        let mut inner_filters: Vec<Expr> = Vec::new();
        for conj in inner_conjuncts {
            let (e, _) = inner.bind_multi(conj)?;
            inner_filters.push(e);
        }
        let bq = BoundQuery {
            tables: vec![TableInput {
                name: inner.tables[0].display.clone(),
                source: inner.tables[0].source.clone(),
                projection: inner.tables[0].used.clone(),
                filter: inner_filters.into_iter().reduce(and),
            }],
            edges: Vec::new(),
            slots: inner.slots,
            post_filter: None,
            group: vec![GroupExpr {
                expr: Expr::Column(key_slot),
            }],
            rollup_terms: Vec::new(),
            aggs: vec![
                AggExpr {
                    func: AggFunc::CountDistinct,
                    arg: Expr::Column(s_slot),
                },
                AggExpr {
                    func: AggFunc::Min,
                    arg: Expr::Column(s_slot),
                },
            ],
            having: None,
            output: vec![
                OutputExpr {
                    expr: Expr::Column(0),
                    name: inner_col.clone(),
                },
                OutputExpr {
                    expr: Expr::Literal(ScalarValue::Int64(1)),
                    name: "__m".into(),
                },
                OutputExpr {
                    expr: Expr::Column(1),
                    name: "__cd".into(),
                },
                OutputExpr {
                    expr: Expr::Column(2),
                    name: "__ms".into(),
                },
            ],
            hidden_outputs: 0,
            distinct: false,
            order_by: Vec::new(),
            limit: None,
            subqueries: inner.subs,
            has_grouping: false,
            windows: Vec::new(),
            set_ops: Vec::new(),
            derived: inner.derived,
        };
        let idx = self.derived.len();
        self.derived.push(std::sync::Arc::new(bq));
        let dname = format!("__ex{idx}");
        let ddef = TableDef {
            path: PathBuf::new(),
            columns: vec![
                crate::catalog::ColumnDef {
                    name: inner_col.clone(),
                    leaf: 0,
                    ty: key_ty,
                    dec_scale: None,
                    nullable: false,
                },
                crate::catalog::ColumnDef {
                    name: "__m".into(),
                    leaf: 1,
                    ty: LogicalType::Int64,
                    dec_scale: None,
                    nullable: false,
                },
                crate::catalog::ColumnDef {
                    name: "__cd".into(),
                    leaf: 2,
                    ty: LogicalType::Int64,
                    dec_scale: None,
                    nullable: false,
                },
                crate::catalog::ColumnDef {
                    name: "__ms".into(),
                    leaf: 3,
                    ty: LogicalType::Float64,
                    dec_scale: None,
                    nullable: false,
                },
            ],
        };
        self.push_table(dname.clone(), ddef, TableSource::Derived(idx))?;

        // LEFT edge outer.k = derived.k, preserved = the outer side.
        let preserved = self.slots[outer_slot].table;
        let dkey = self.resolve_parts(&[&dname, &inner_col])?;
        self.extra_edges.push(JoinEdge {
            a: outer_slot,
            b: dkey,
            preserved: Some(preserved),
        });
        let m = Expr::Column(self.resolve_parts(&[&dname, "__m"])?);
        let cd = Expr::Column(self.resolve_parts(&[&dname, "__cd"])?);
        let ms = Expr::Column(self.resolve_parts(&[&dname, "__ms"])?);
        let outer_s = Expr::Column(outer_s_slot);
        let lit = |v: i64| Expr::Literal(ScalarValue::Int64(v));

        // NULL semantics (q16's nullable cs_warehouse_sk, surfaced at
        // sf10): a NULL outer_s makes `s <> outer_s` UNKNOWN for every
        // inner row — EXISTS is FALSE regardless of cd. And a key whose
        // inner rows all have NULL s (cd = 0, ms NULL) satisfies nothing —
        // EXISTS false, NOT EXISTS true.
        let s_null = |neg: bool| Expr::IsNull {
            expr: Box::new(Expr::Column(outer_s_slot)),
            negated: neg,
        };
        let ms_null = |ms: Expr| Expr::IsNull {
            expr: Box::new(ms),
            negated: false,
        };
        let pred = if !negated {
            // outer_s IS NOT NULL AND __m = 1 AND (cd >= 2 OR ms <> outer_s)
            // (an all-NULL key has ms NULL → the <> is UNKNOWN → false ✓)
            and(
                s_null(true),
                and(
                    binary(BinaryOp::Eq, m, lit(1)),
                    or(
                        binary(BinaryOp::GtEq, cd, lit(2)),
                        binary(BinaryOp::NotEq, ms, outer_s),
                    ),
                ),
            )
        } else {
            // outer_s IS NULL OR __m = 0
            //   OR (cd <= 1 AND (ms IS NULL OR ms = outer_s))
            or(
                s_null(false),
                or(
                    binary(BinaryOp::Eq, m, lit(0)),
                    and(
                        binary(BinaryOp::LtEq, cd, lit(1)),
                        or(ms_null(ms.clone()), binary(BinaryOp::Eq, ms, outer_s)),
                    ),
                ),
            )
        };
        Ok(Bound::Expr(pred))
    }

    /// Demote table `t`'s LEFT OUTER join to INNER (no-op if `t` is not a
    /// nullable side): clear the preserved marking on its edges and drop it
    /// from `left_tables`. Called when a WHERE conjunct on `t` rejects NULLs
    /// — a filter, or an equijoin whose NULL-filled key can never match.
    fn demote_left(&mut self, t: usize) {
        if !self.left_tables.remove(&t) {
            return;
        }
        for ei in 0..self.extra_edges.len() {
            let (a, bb, pres) = {
                let ed = &self.extra_edges[ei];
                (ed.a, ed.b, ed.preserved.is_some())
            };
            if pres && (self.slots[a].table == t || self.slots[bb].table == t) {
                self.extra_edges[ei].preserved = None;
            }
        }
    }

    /// `rk <= K` (or `rk < K`, or the mirrored literal-first forms) where
    /// `rk` is the LONE rank()/row_number() window output of a
    /// single-referenced materialized derived: record `top_k = K` on that
    /// window so the executor prunes each partition to its K best rows
    /// before sorting/projecting (q67's dw2: a 5.8M-row window input for a
    /// `rk <= 100` consumer). dense_rank is excluded — its rank-≤-K
    /// frontier extends past the K-th best ROW. Never changes semantics:
    /// the conjunct still applies as a filter; this only prunes rows the
    /// filter was guaranteed to drop.
    fn try_window_topk(&mut self, conj: &ast::Expr) {
        use ast::BinaryOperator as B;
        let ast::Expr::BinaryOp { left, op, right } = conj else {
            return;
        };
        let (ident, num, less_eq) = match (ident_parts(left), ident_parts(right), op) {
            (Some(_), None, B::LtEq) => (left, right, true),
            (Some(_), None, B::Lt) => (left, right, false),
            (None, Some(_), B::GtEq) => (right, left, true),
            (None, Some(_), B::Gt) => (right, left, false),
            _ => return,
        };
        let ast::Expr::Value(v) = &**num else { return };
        let ast::Value::Number(s, _) = &v.value else {
            return;
        };
        let Ok(kraw) = s.parse::<i64>() else { return };
        let k = if less_eq { kraw } else { kraw - 1 };
        if k < 1 {
            return;
        }
        let parts = ident_parts(ident).expect("matched above");
        let Ok(slot) = self.resolve_parts(&parts) else {
            return;
        };
        let Slot { table, col } = self.slots[slot];
        let di = match &self.tables[table].source {
            TableSource::Derived(i) => *i,
            _ => return,
        };
        let leaf = self.tables[table].used[col].leaf;
        // A shared (CTE) derived can't take a reference-specific hint.
        let Some(dq) = std::sync::Arc::get_mut(&mut self.derived[di]) else {
            return;
        };
        // Lone window only: pruning rows must not disturb siblings.
        if dq.windows.len() != 1
            || !matches!(dq.windows[0].func, WindowFunc::Rank | WindowFunc::RowNumber)
        {
            return;
        }
        let win_base = if dq.group.is_empty() && dq.aggs.is_empty() {
            dq.slots.len()
        } else {
            dq.group.len() + dq.aggs.len() + if dq.has_grouping { dq.group.len() } else { 0 }
        };
        // The filtered column must BE the window value (not derived math).
        if !matches!(dq.output.get(leaf), Some(o) if o.expr == Expr::Column(win_base)) {
            return;
        }
        let k = k as usize;
        let w = &mut dq.windows[0];
        w.top_k = Some(w.top_k.map_or(k, |old| old.min(k)));
    }

    /// If `f` is `GROUPING(col)`, resolve `col` to its GROUP BY key index —
    /// `GROUPING` returns 1 when that key is a ROLLUP subtotal (aggregated
    /// away), 0 otherwise, so its argument must be a grouping column.
    /// `Ok(None)` = not a `GROUPING` call.
    fn bind_grouping(
        &mut self,
        f: &ast::Function,
        group: &[GroupExpr],
    ) -> Result<Option<usize>, String> {
        if f.name.to_string().to_lowercase() != "grouping" {
            return Ok(None);
        }
        let ast::FunctionArguments::List(list) = &f.args else {
            return Err("GROUPING needs an argument".into());
        };
        let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] = list.args.as_slice()
        else {
            return Err("GROUPING takes exactly one column".into());
        };
        let bound = self.bind_scalar(arg)?;
        let k = group
            .iter()
            .position(|g| g.expr == bound)
            .ok_or_else(|| format!("GROUPING argument '{arg}' must be a GROUP BY key"))?;
        Ok(Some(k))
    }

    /// Bind a window call `fn(...) OVER (PARTITION BY … [ORDER BY …]
    /// [frame])` — all components in row space; the value is referenced
    /// as `Column(WINDOW_BASE + i)` until the block-level remap.
    fn bind_window(
        &mut self,
        f: &ast::Function,
        group: &[GroupExpr],
        aggs: &mut Vec<AggExpr>,
    ) -> Result<Expr, String> {
        let Some(ast::WindowType::WindowSpec(spec)) = f.over.as_ref() else {
            return Err("named WINDOW references are not yet supported".into());
        };
        let fname = f.name.to_string().to_lowercase();
        let (func, arg) = match fname.as_str() {
            "rank" => (WindowFunc::Rank, Expr::Literal(ScalarValue::Int64(0))),
            "dense_rank" => (WindowFunc::DenseRank, Expr::Literal(ScalarValue::Int64(0))),
            "row_number" => (WindowFunc::RowNumber, Expr::Literal(ScalarValue::Int64(0))),
            "lag" | "lead" => {
                let ast::FunctionArguments::List(args) = &f.args else {
                    return Err(format!("window '{fname}' needs an argument list"));
                };
                let a = args.args.as_slice();
                let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(x)) =
                    a.first().ok_or(format!("{fname} needs a value argument"))?
                else {
                    return Err(format!("unsupported {fname} argument"));
                };
                let val = self.bind_output(x, group, aggs)?;
                let off = match a.get(1) {
                    None => 1,
                    Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(o))) => {
                        match self.clone_free_literal(o)? {
                            ScalarValue::Int64(v) if v >= 0 => v as u32,
                            other => {
                                return Err(format!("{fname} offset must be a non-negative integer: {other:?}"));
                            }
                        }
                    }
                    _ => return Err(format!("{fname} takes (value[, offset])")),
                };
                let wf = if fname == "lag" {
                    WindowFunc::Lag(off)
                } else {
                    WindowFunc::Lead(off)
                };
                (wf, val)
            }
            "first_value" => {
                let ast::FunctionArguments::List(args) = &f.args else {
                    return Err("first_value needs an argument list".into());
                };
                let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(x))] =
                    args.args.as_slice()
                else {
                    return Err("first_value takes exactly one argument".into());
                };
                (WindowFunc::FirstValue, self.bind_output(x, group, aggs)?)
            }
            "ntile" => {
                let ast::FunctionArguments::List(args) = &f.args else {
                    return Err("ntile needs an argument list".into());
                };
                let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(nb))] =
                    args.args.as_slice()
                else {
                    return Err("ntile takes exactly one argument".into());
                };
                let nb = match self.clone_free_literal(nb)? {
                    ScalarValue::Int64(v) if v > 0 => v as u32,
                    other => return Err(format!("ntile bucket count must be a positive integer: {other:?}")),
                };
                (WindowFunc::Ntile(nb), Expr::Literal(ScalarValue::Int64(0)))
            }
            name => {
                let af = match name {
                    "sum" => AggFunc::Sum,
                    "avg" => AggFunc::Avg,
                    "min" => AggFunc::Min,
                    "max" => AggFunc::Max,
                    "count" => AggFunc::Count,
                    "stddev_samp" | "stddev" | "stddev_sample" => AggFunc::StddevSamp,
                    "stddev_pop" => AggFunc::StddevPop,
                    "var_samp" | "variance" | "var" => AggFunc::VarSamp,
                    "var_pop" => AggFunc::VarPop,
                    other => return Err(format!("unsupported window function '{other}'")),
                };
                let ast::FunctionArguments::List(args) = &f.args else {
                    return Err(format!("window '{name}' needs an argument list"));
                };
                let arg = match args.args.as_slice() {
                    [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)] => {
                        Expr::Literal(ScalarValue::Int64(1))
                    }
                    [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(a))] => {
                        // Row space — the argument may itself extract an
                        // aggregate (`sum(sum(x)) OVER …`).
                        self.bind_output(a, group, aggs)?
                    }
                    _ => return Err(format!("window '{name}' takes exactly one argument")),
                };
                (WindowFunc::Agg(af), arg)
            }
        };
        let partition = spec
            .partition_by
            .iter()
            .map(|e| self.bind_output(e, group, aggs))
            .collect::<Result<Vec<_>, _>>()?;
        let order = spec
            .order_by
            .iter()
            .map(|oe| {
                Ok((
                    self.bind_output(&oe.expr, group, aggs)?,
                    oe.options.asc == Some(false),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let rows_frame = match &spec.window_frame {
            None => false,
            Some(fr) => {
                use ast::WindowFrameBound as B;
                let start_ok = matches!(fr.start_bound, B::Preceding(None));
                let end_ok = matches!(fr.end_bound, None | Some(B::CurrentRow));
                if !start_ok || !end_ok {
                    return Err(format!("unsupported window frame: {fr:?}"));
                }
                matches!(fr.units, ast::WindowFrameUnits::Rows)
            }
        };
        self.windows.push(WindowExpr {
            func,
            arg,
            partition,
            order,
            rows_frame,
            top_k: None,
        });
        Ok(Expr::Column(WINDOW_BASE + self.windows.len() - 1))
    }

    /// If `f` is a `concat(...)` call, bind each argument (slot space) and
    /// return the parts; otherwise `None` so the caller tries other forms.
    /// concat produces an owned string, so it only ever lands in a
    /// projection / group-key slot (see [`Expr::Concat`]).
    fn bind_concat(&mut self, f: &ast::Function) -> Result<Option<Vec<Expr>>, String> {
        if f.name.to_string().to_lowercase() != "concat" {
            return Ok(None);
        }
        let ast::FunctionArguments::List(args) = &f.args else {
            return Err("concat needs an argument list".into());
        };
        let mut parts = Vec::with_capacity(args.args.len());
        for a in &args.args {
            let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = a else {
                return Err("concat arguments must be plain expressions".into());
            };
            parts.push(self.bind_scalar(e)?);
        }
        Ok(Some(parts))
    }

    /// Bind an aggregate call: `sum/count/min/max/avg(<expr>)` or
    /// `count(*)`.
    fn bind_aggregate(&mut self, e: &ast::Expr) -> Result<AggExpr, String> {
        let ast::Expr::Function(f) = e else {
            return Err(format!("expected an aggregate call, got: {e}"));
        };
        let fname = f.name.to_string().to_lowercase();
        let func = match fname.as_str() {
            "sum" => AggFunc::Sum,
            "count" => AggFunc::Count,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            "avg" => AggFunc::Avg,
            "stddev_samp" | "stddev" | "stddev_sample" => AggFunc::StddevSamp,
            "stddev_pop" => AggFunc::StddevPop,
            "var_samp" | "variance" | "var" => AggFunc::VarSamp,
            "var_pop" => AggFunc::VarPop,
            other => return Err(format!("unsupported aggregate function '{other}'")),
        };
        let ast::FunctionArguments::List(args) = &f.args else {
            return Err(format!("aggregate '{fname}' needs an argument list"));
        };
        let func = match (func, args.duplicate_treatment) {
            (f, None) => f,
            (AggFunc::Count, Some(ast::DuplicateTreatment::Distinct)) => AggFunc::CountDistinct,
            (_, Some(dt)) => return Err(format!("unsupported {dt} in aggregate '{fname}'")),
        };
        let arg = match args.args.as_slice() {
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)] => {
                if func != AggFunc::Count {
                    return Err(format!("'{fname}(*)' is not valid — only count(*)"));
                }
                Expr::Literal(ScalarValue::Int64(1))
            }
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] => {
                self.bind_scalar(arg)?
            }
            _ => return Err(format!("aggregate '{fname}' takes exactly one argument")),
        };
        // A COUNT over a LEFT-joined table's column counts only matched
        // occurrences (the engine has no NULLs; unmatched preserved rows
        // contribute 0). The column itself is not needed as a payload.
        if func == AggFunc::Count {
            if let Expr::Column(slot) = arg {
                let t = self.slots[slot].table;
                if self.left_tables.contains(&t) {
                    return Ok(AggExpr {
                        func: AggFunc::CountMatched(t),
                        arg: Expr::Literal(ScalarValue::Int64(1)),
                    });
                }
            }
        }
        Ok(AggExpr { func, arg })
    }

    /// Bind an AST expression bottom-up (slot space), folding literal
    /// arithmetic in decimal.
    fn bind(&mut self, e: &ast::Expr) -> Result<Bound, String> {
        match e {
            ast::Expr::Identifier(id) => {
                if let Some(e2) = self.view_expr(None, &id.value) {
                    // A pass-through view column (`c_acctbal` defined as
                    // itself) must resolve as the real column, not recurse.
                    if !matches!(&e2, ast::Expr::Identifier(x) if x.value == id.value) {
                        return self.bind(&e2);
                    }
                }
                let s = self.resolve_parts(&[&id.value])?;
                Ok(Bound::Expr(Expr::Column(s)))
            }
            ast::Expr::CompoundIdentifier(ids) => {
                let parts: Vec<&str> = ids.iter().map(|i| i.value.as_str()).collect();
                if let [tbl, c] = parts.as_slice() {
                    if let Some(e2) = self.view_expr(Some(tbl), c) {
                        if !matches!(&e2, ast::Expr::Identifier(x) if x.value == *c) {
                            return self.bind(&e2);
                        }
                        let s = self.resolve_parts(&[c])?;
                        return Ok(Bound::Expr(Expr::Column(s)));
                    }
                }
                let s = self.resolve_parts(&parts)?;
                Ok(Bound::Expr(Expr::Column(s)))
            }
            ast::Expr::Nested(inner) => self.bind(inner),
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(s, _) => Ok(Bound::Dec(Dec::parse(s)?)),
                ast::Value::SingleQuotedString(s) => Ok(Bound::Expr(Expr::Literal(
                    ScalarValue::Utf8(s.as_str().into()),
                ))),
                ast::Value::Null => Ok(Bound::Expr(Expr::Literal(ScalarValue::Null))),
                other => Err(format!("unsupported literal: {other}")),
            },
            ast::Expr::Cast {
                expr, data_type, ..
            } => {
                use ast::DataType as DT;
                match data_type {
                    // CAST(<string literal> AS DATE) folds; a date-typed
                    // expression passes through.
                    DT::Date => {
                        let inner = materialize(self.bind(expr)?);
                        if let Expr::Literal(ScalarValue::Utf8(s)) = &inner {
                            return Ok(Bound::Expr(Expr::Literal(ScalarValue::Date32(
                                parse_date32(s)?,
                            ))));
                        }
                        Ok(Bound::Expr(inner))
                    }
                    // CAST to a decimal/floating type yields Float64 — the
                    // engine's fractional numerics are all f64. A numeric
                    // *literal* is coerced so its static type is Float64
                    // (`CAST(0 AS DECIMAL(7,2))` is a float 0, not int 0):
                    // this is what lets a UNION ALL reconcile an int-looking
                    // padding literal in one branch with a real decimal
                    // column in another (q5's `salesreturns`).
                    DT::Decimal(_)
                    | DT::Numeric(_)
                    | DT::Float(_)
                    | DT::Double(_)
                    | DT::DoublePrecision => {
                        Ok(Bound::Expr(float_cast_expr(materialize(self.bind(expr)?))))
                    }
                    // CAST to an integer type rounds a fractional value to
                    // the nearest integer (DuckDB semantics); an integer
                    // operand is unchanged. A literal folds now.
                    DT::Int(_) | DT::Integer(_) | DT::BigInt(_) | DT::SmallInt(_) => {
                        let inner = materialize(self.bind(expr)?);
                        Ok(Bound::Expr(match inner {
                            Expr::Literal(ScalarValue::Float64(f)) => {
                                Expr::Literal(ScalarValue::Int64(f.round() as i64))
                            }
                            Expr::Literal(ScalarValue::Int32(_))
                            | Expr::Literal(ScalarValue::Int64(_))
                            | Expr::Literal(ScalarValue::Date32(_)) => inner,
                            other => Expr::CastInt(Box::new(other)),
                        }))
                    }
                    DT::Char(_) | DT::Varchar(_) | DT::Text => Ok(self.bind(expr)?),
                    other => Err(format!("unsupported CAST target: {other}")),
                }
            }
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => match self.bind(expr)? {
                Bound::Dec(d) => Ok(Bound::Dec(d.neg())),
                // `-e` on a runtime expression = `0 - e` (arith promotes the
                // literal to the operand's type; NULL propagates).
                Bound::Expr(e) => Ok(Bound::Expr(Expr::Binary {
                    op: BinaryOp::Sub,
                    lhs: Box::new(Expr::Literal(ScalarValue::Int64(0))),
                    rhs: Box::new(e),
                })),
            },
            ast::Expr::TypedString(ts) => bind_typed_string(ts).map(Bound::Expr),
            ast::Expr::Extract { field, expr, .. } => {
                let f = bind_date_field(field)?;
                let inner = materialize(self.bind(expr)?);
                Ok(Bound::Expr(Expr::Extract {
                    field: f,
                    arg: Box::new(inner),
                }))
            }
            // FLOOR/CEIL parse as their own AST nodes (not Function calls).
            // Only the numeric form is supported — reject `FLOOR(x TO field)`.
            ast::Expr::Floor { expr, field } | ast::Expr::Ceil { expr, field } => {
                // Plain `FLOOR(x)` carries a `NoDateTime` sentinel; only a
                // real `FLOOR(x TO <field>)` datetime-truncation is rejected.
                if matches!(field, ast::CeilFloorKind::DateTimeField(f)
                    if !matches!(f, ast::DateTimeField::NoDateTime))
                {
                    return Err(format!("unsupported {e} (only numeric floor/ceil)"));
                }
                let func = if matches!(e, ast::Expr::Floor { .. }) {
                    NumFn::Floor
                } else {
                    NumFn::Ceil
                };
                Ok(Bound::Expr(Expr::NumFn {
                    func,
                    args: vec![materialize(self.bind(expr)?)],
                }))
            }
            // TRIM likewise; only plain `TRIM(x)` (both-side whitespace).
            ast::Expr::Trim {
                expr,
                trim_where: None,
                trim_what: None,
                trim_characters: None,
            } => Ok(Bound::Expr(Expr::StrFn {
                func: StrFn::Trim,
                args: vec![materialize(self.bind(expr)?)],
            })),
            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                // `CASE <operand> WHEN v …` desugars to `WHEN operand = v`.
                let null_else = ast::Expr::Value(ast::Value::Null.into());
                let else_: &ast::Expr = match else_result {
                    Some(e2) => e2,
                    None => &null_else,
                };
                let whens = conditions
                    .iter()
                    .map(|cw| {
                        let cond = match operand {
                            None => cw.condition.clone(),
                            Some(op) => ast::Expr::BinaryOp {
                                left: op.clone(),
                                op: ast::BinaryOperator::Eq,
                                right: Box::new(cw.condition.clone()),
                            },
                        };
                        Ok((
                            materialize(self.bind(&cond)?),
                            materialize(self.bind(&cw.result)?),
                        ))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let else_ = materialize(self.bind(else_)?);
                Ok(Bound::Expr(Expr::Case {
                    whens,
                    else_: Box::new(else_),
                }))
            }
            ast::Expr::Between {
                expr,
                negated: false,
                low,
                high,
            } => {
                // Desugar: e BETWEEN lo AND hi  →  e >= lo AND e <= hi.
                // A 'YYYY-MM-DD' string bound against a date-typed `e`
                // folds to Date32 (as the comparison arm does — the Spark
                // texts write `d_date BETWEEN '…' AND …`).
                let bound = materialize(self.bind(expr)?);
                let mut lo = materialize(self.bind(low)?);
                let mut hi = materialize(self.bind(high)?);
                self.coerce_date_literal(&mut lo, &bound);
                self.coerce_date_literal(&mut hi, &bound);
                Ok(Bound::Expr(and(
                    binary(BinaryOp::GtEq, bound.clone(), lo),
                    binary(BinaryOp::LtEq, bound, hi),
                )))
            }
            ast::Expr::Between { negated: true, .. } => {
                Err("NOT BETWEEN is not yet supported".into())
            }
            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                // Desugar: e IN (a, b, c) → e=a OR e=b OR e=c (NOT IN → the
                // AND of ≠). Lists are small in practice; a set-probe
                // InList kernel is a labelled follow-on.
                let bound = materialize(self.bind(expr)?);
                let (cmp, fold): (BinaryOp, fn(Expr, Expr) -> Expr) = if *negated {
                    (BinaryOp::NotEq, and)
                } else {
                    (BinaryOp::Eq, or)
                };
                list.iter()
                    .map(|item| {
                        let mut it = materialize(self.bind(item)?);
                        // `date_col IN ('YYYY-MM-DD', …)` — fold each string
                        // element to Date32 against a date-typed column.
                        self.coerce_date_literal(&mut it, &bound);
                        Ok(binary(cmp, bound.clone(), it))
                    })
                    .collect::<Result<Vec<_>, String>>()?
                    .into_iter()
                    .reduce(fold)
                    .map(Bound::Expr)
                    .ok_or_else(|| "IN () requires at least one element".into())
            }
            ast::Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                let bound = materialize(self.bind(expr)?);
                let pat = materialize(self.bind(pattern)?);
                let Expr::Literal(ScalarValue::Utf8(p)) = pat else {
                    return Err("LIKE pattern must be a string literal".into());
                };
                Ok(Bound::Expr(Expr::Like {
                    expr: Box::new(bound),
                    pattern: p.to_string(),
                    negated: *negated,
                }))
            }
            ast::Expr::BinaryOp { left, op, right } => {
                // Date ± interval folds at bind time (literal dates only —
                // TPC-H's `date '…' - interval '90' day`).
                if let ast::Expr::Interval(iv) = right.as_ref() {
                    let base = materialize(self.bind(left)?);
                    let signed = match op {
                        ast::BinaryOperator::Plus => 1,
                        ast::BinaryOperator::Minus => -1,
                        other => {
                            return Err(format!("unsupported interval operator: {other}"));
                        }
                    };
                    // A literal date folds at bind time (any interval unit).
                    if let Expr::Literal(ScalarValue::Date32(d)) = base {
                        return Ok(Bound::Expr(Expr::Literal(ScalarValue::Date32(shift_date(
                            d, iv, signed,
                        )?))));
                    }
                    // A date COLUMN ± a day/week interval is a constant
                    // offset on the Date32 (days since epoch), so it lowers
                    // to integer add — the evaluator compares dates as their
                    // day counts (q72's `d3.d_date > d1.d_date + 5 days`).
                    // Months/years would need per-row civil arithmetic and
                    // stay literal-only.
                    let days = interval_days(iv, signed)?;
                    return Ok(Bound::Expr(binary(
                        BinaryOp::Add,
                        base,
                        Expr::Literal(ScalarValue::Int64(days as i64)),
                    )));
                }
                let op = bind_op(op)?;
                let l = self.bind(left)?;
                let r = self.bind(right)?;
                // ★ Literal ⊕ literal folds in decimal — exact, no f64 ULP
                // loss. `0.06 + 0.01` → Dec(7,2), later cast to f64 0.07.
                if let (Bound::Dec(a), Bound::Dec(b)) = (&l, &r) {
                    match op {
                        BinaryOp::Add => return Ok(Bound::Dec(a.add(b))),
                        BinaryOp::Sub => return Ok(Bound::Dec(a.sub(b))),
                        BinaryOp::Mul => return Ok(Bound::Dec(a.mul(b))),
                        _ => {}
                    }
                }
                let (mut le, mut re) = (materialize(l), materialize(r));
                // Comparing a date-typed column with a 'YYYY-MM-DD' string
                // literal (the Spark texts elide `date`): fold the literal
                // to Date32.
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) {
                    self.coerce_date_literal(&mut le, &re);
                    self.coerce_date_literal(&mut re, &le);
                }
                Ok(Bound::Expr(binary(op, le, re)))
            }
            ast::Expr::IsNull(inner) => Ok(Bound::Expr(Expr::IsNull {
                expr: Box::new(materialize(self.bind(inner)?)),
                negated: false,
            })),
            ast::Expr::IsNotNull(inner) => Ok(Bound::Expr(Expr::IsNull {
                expr: Box::new(materialize(self.bind(inner)?)),
                negated: true,
            })),
            ast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let inner = materialize(self.bind(expr)?);
                let mut lit_i64 = |e: &Option<Box<ast::Expr>>| -> Result<Option<i64>, String> {
                    match e {
                        None => Ok(None),
                        Some(x) => match self.clone_free_literal(x)? {
                            ScalarValue::Int64(v) => Ok(Some(v)),
                            other => Err(format!("SUBSTRING bounds must be integers: {other:?}")),
                        },
                    }
                };
                let from = lit_i64(substring_from)?
                    .ok_or("SUBSTRING requires a FROM position (so far)")?;
                let len = lit_i64(substring_for)?;
                Ok(Bound::Expr(Expr::Substr {
                    expr: Box::new(inner),
                    from,
                    len,
                }))
            }
            ast::Expr::Exists { subquery, negated } => self.bind_exists(subquery, *negated),
            ast::Expr::Subquery(sq) => {
                // Correlated scalar (single equality correlation, the TPC-H
                // shape) decorrelates into a grouped derived table joined on
                // the correlation key; uncorrelated executes standalone.
                if let Some(b) = self.try_decorrelate_scalar(sq)? {
                    return Ok(b);
                }
                let bq = bind_query(sq, self.catalog, false, self.ctes)?;
                if bq.output.len() != 1 {
                    return Err("a scalar subquery must select exactly one column".into());
                }
                self.subs.push(bq);
                Ok(Bound::Expr(Expr::ScalarSub(self.subs.len() - 1)))
            }
            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let bound = materialize(self.bind(expr)?);
                let bq = bind_query(subquery, self.catalog, true, self.ctes)?;
                if bq.output.len() != 1 {
                    return Err("an IN subquery must select exactly one column".into());
                }
                self.subs.push(bq);
                Ok(Bound::Expr(Expr::InSub {
                    expr: Box::new(bound),
                    sub: self.subs.len() - 1,
                    negated: *negated,
                }))
            }
            ast::Expr::Function(f) => {
                if let Some(parts) = self.bind_concat(f)? {
                    return Ok(Bound::Expr(Expr::Concat(parts)));
                }
                if let Some(e2) = self.bind_round_or_upper(f)? {
                    return Ok(Bound::Expr(e2));
                }
                if let Some(rewritten) = rewrite_scalar_fn(f)? {
                    return self.bind(&rewritten);
                }
                Err(format!(
                    "unsupported function '{}' outside the SELECT list",
                    f.name
                ))
            }
            other => Err(format!("unsupported expression: {other}")),
        }
    }
}

/// Infer the output column types of a bound query (row space =
/// [group keys…, agg values…]) — the schema a materialized derived table
/// exposes. Conservative: unknown shapes default to Float64.
pub(crate) fn output_types(q: &BoundQuery) -> Vec<LogicalType> {
    let key_tys: Vec<LogicalType> = q
        .group
        .iter()
        .map(|g| infer_slot_type(q, &g.expr))
        .collect();
    // Hidden ORDER-BY-only outputs are dropped from the result rows and
    // must not appear in a derived table's schema.
    let visible = &q.output[..q.output.len() - q.hidden_outputs];
    // A PLAIN-ROWS query's outputs live in SLOT space, not row space.
    if q.group.is_empty() && q.aggs.is_empty() {
        // A no-GROUP-BY *window* query's row space is [slots…, windows…]:
        // a column past the last slot is a window value.
        if !q.windows.is_empty() {
            let nslots = q.slots.len();
            return visible
                .iter()
                .map(|o| infer_windowed_slot_type(q, nslots, &o.expr))
                .collect();
        }
        return visible
            .iter()
            .map(|o| infer_slot_type(q, &o.expr))
            .collect();
    }
    visible
        .iter()
        .map(|o| infer_row_type(q, &key_tys, &o.expr))
        .collect()
}

/// Infer a type in a no-GROUP-BY window query's row space: columns
/// `[0, nslots)` are scan slots, columns at/after `nslots` are window values.
fn infer_windowed_slot_type(q: &BoundQuery, nslots: usize, e: &Expr) -> LogicalType {
    match e {
        Expr::Column(i) if *i >= nslots => match q.windows[*i - nslots].func {
            WindowFunc::Rank
            | WindowFunc::DenseRank
            | WindowFunc::RowNumber
            | WindowFunc::Ntile(_) => LogicalType::Int64,
            WindowFunc::Agg(_)
            | WindowFunc::Lag(_)
            | WindowFunc::Lead(_)
            | WindowFunc::FirstValue => LogicalType::Float64,
        },
        Expr::Binary { op, lhs, rhs } => binary_type(
            *op,
            infer_windowed_slot_type(q, nslots, lhs),
            infer_windowed_slot_type(q, nslots, rhs),
        ),
        Expr::Case { whens, .. } => whens
            .first()
            .map(|(_, v)| infer_windowed_slot_type(q, nslots, v))
            .unwrap_or(LogicalType::Float64),
        // Slot columns and everything else fall back to the slot inference.
        _ => infer_slot_type(q, e),
    }
}

fn infer_slot_type(q: &BoundQuery, e: &Expr) -> LogicalType {
    match e {
        Expr::Column(s) => {
            let Slot { table, col } = q.slots[*s];
            q.tables[table].projection[col].ty
        }
        Expr::Literal(v) => literal_type(v),
        Expr::Binary { op, lhs, rhs } => {
            binary_type(*op, infer_slot_type(q, lhs), infer_slot_type(q, rhs))
        }
        Expr::Extract { .. } | Expr::CastInt(_) => LogicalType::Int64,
        Expr::DateTrunc { .. } => LogicalType::Date32,
        Expr::Substr { .. } | Expr::Concat(_) | Expr::Upper(_) | Expr::StrFn { .. } => {
            LogicalType::Utf8
        }
        Expr::NumFn { func, .. } => match func {
            crate::expr::NumFn::Floor | crate::expr::NumFn::Ceil => LogicalType::Float64,
            crate::expr::NumFn::Mod | crate::expr::NumFn::Length => LogicalType::Int64,
        },
        Expr::Case { whens, .. } => whens
            .first()
            .map(|(_, v)| infer_slot_type(q, v))
            .unwrap_or(LogicalType::Float64),
        _ => LogicalType::Float64,
    }
}

fn infer_row_type(q: &BoundQuery, key_tys: &[LogicalType], e: &Expr) -> LogicalType {
    match e {
        Expr::Column(i) => {
            if *i < key_tys.len() {
                key_tys[*i]
            } else if *i < key_tys.len() + q.aggs.len() {
                match q.aggs[*i - key_tys.len()].func {
                    AggFunc::Count | AggFunc::CountDistinct | AggFunc::CountMatched(_) => {
                        LogicalType::Int64
                    }
                    _ => LogicalType::Float64,
                }
            } else {
                match q.windows[*i - key_tys.len() - q.aggs.len()].func {
                    WindowFunc::Rank
                    | WindowFunc::DenseRank
                    | WindowFunc::RowNumber
                    | WindowFunc::Ntile(_) => LogicalType::Int64,
                    WindowFunc::Agg(_)
                    | WindowFunc::Lag(_)
                    | WindowFunc::Lead(_)
                    | WindowFunc::FirstValue => LogicalType::Float64,
                }
            }
        }
        Expr::Literal(v) => literal_type(v),
        Expr::Binary { op, lhs, rhs } => binary_type(
            *op,
            infer_row_type(q, key_tys, lhs),
            infer_row_type(q, key_tys, rhs),
        ),
        Expr::Extract { .. } | Expr::CastInt(_) => LogicalType::Int64,
        Expr::DateTrunc { .. } => LogicalType::Date32,
        Expr::Substr { .. } | Expr::Concat(_) | Expr::Upper(_) | Expr::StrFn { .. } => {
            LogicalType::Utf8
        }
        Expr::NumFn { func, .. } => match func {
            crate::expr::NumFn::Floor | crate::expr::NumFn::Ceil => LogicalType::Float64,
            crate::expr::NumFn::Mod | crate::expr::NumFn::Length => LogicalType::Int64,
        },
        Expr::Case { whens, .. } => whens
            .first()
            .map(|(_, v)| infer_row_type(q, key_tys, v))
            .unwrap_or(LogicalType::Float64),
        _ => LogicalType::Float64,
    }
}

fn literal_type(v: &ScalarValue) -> LogicalType {
    match v {
        ScalarValue::Int32(_) => LogicalType::Int32,
        ScalarValue::Int64(_) => LogicalType::Int64,
        ScalarValue::Date32(_) => LogicalType::Date32,
        ScalarValue::Utf8(_) => LogicalType::Utf8,
        _ => LogicalType::Float64,
    }
}

fn binary_type(op: BinaryOp, l: LogicalType, r: LogicalType) -> LogicalType {
    use BinaryOp::*;
    match op {
        Add | Sub | Mul => {
            if is_integer_family(l) && is_integer_family(r) {
                LogicalType::Int64
            } else {
                LogicalType::Float64
            }
        }
        Div => LogicalType::Float64,
        _ => LogicalType::Int64, // comparisons/logic used as keys: 0/1
    }
}

/// Is this query a SCALAR aggregate — a plain SELECT whose every item is
/// an aggregate, with no GROUP BY — and therefore guaranteed to produce
/// exactly one row? (The cross-join-as-constant view rewrite depends on
/// the one-row guarantee.)
fn scalar_agg_body(q: &ast::Query) -> bool {
    let ast::SetExpr::Select(inner) = q.body.as_ref() else {
        return false;
    };
    matches!(
        &inner.group_by,
        ast::GroupByExpr::Expressions(g, m) if g.is_empty() && m.is_empty()
    ) && inner.having.is_none()
        && inner.distinct.is_none()
        && !inner.from.is_empty()
        && q.with.is_none()
        && inner.projection.iter().all(|it| match it {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_aggregate(e) && !contains_window(e)
            }
            _ => false,
        })
}

/// Is this a `SetExpr::Query` wrapper that [`flatten_setexpr`] can unwrap
/// (no WITH / ORDER BY / LIMIT of its own), anywhere in the set-op tree?
fn contains_flattenable_wrapper(e: &ast::SetExpr) -> bool {
    match e {
        ast::SetExpr::Query(q) => {
            q.with.is_none() && q.order_by.is_none() && q.limit_clause.is_none()
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            contains_flattenable_wrapper(left) || contains_flattenable_wrapper(right)
        }
        _ => false,
    }
}

/// Recursively unwrap parenthesized `SetExpr::Query` sides into their
/// bodies (see [`contains_flattenable_wrapper`]).
fn flatten_setexpr(e: ast::SetExpr) -> ast::SetExpr {
    match e {
        ast::SetExpr::Query(q)
            if q.with.is_none() && q.order_by.is_none() && q.limit_clause.is_none() =>
        {
            flatten_setexpr(*q.body)
        }
        ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left: Box::new(flatten_setexpr(*left)),
            right: Box::new(flatten_setexpr(*right)),
        },
        other => other,
    }
}

/// Rewrite `FROM A [a] FULL OUTER JOIN B [b] ON cond` (q51/q97) into
/// ```text
/// FROM (SELECT a.c AS __fo_a_c…, b.c AS __fo_b_c…
///         FROM A LEFT JOIN B ON cond
///       UNION ALL
///       SELECT a.c AS __fo_a_c…, b.c AS __fo_b_c…
///         FROM B LEFT JOIN A ON cond WHERE a.key IS NULL) __fo
/// ```
/// — a LEFT join plus its mirrored ANTI join, both machinery the binder
/// already has. Every `a.c` / `b.c` reference in the enclosing select
/// rewrites to the wrapper's prefixed column. Returns `None` when the
/// select has no FULL OUTER join.
fn rewrite_full_outer(
    query: &ast::Query,
    select: &ast::Select,
    catalog: &Catalog,
    ctes: &CteMap,
) -> Result<Option<ast::Query>, String> {
    let has_full = select.from.iter().any(|twj| {
        twj.joins
            .iter()
            .any(|j| matches!(j.join_operator, ast::JoinOperator::FullOuter(_)))
    });
    if !has_full {
        return Ok(None);
    }
    let [twj] = select.from.as_slice() else {
        return Err("FULL OUTER JOIN alongside other FROM tables is not supported".into());
    };
    let [join] = twj.joins.as_slice() else {
        return Err("FULL OUTER JOIN chained with other joins is not supported".into());
    };
    let ast::JoinOperator::FullOuter(ast::JoinConstraint::On(cond)) = &join.join_operator else {
        return Err("FULL OUTER JOIN requires an ON condition".into());
    };
    // Each side: (display name, column list) — a CTE or a catalog table.
    let side = |rel: &ast::TableFactor| -> Result<(String, Vec<String>), String> {
        let ast::TableFactor::Table { name, alias, .. } = rel else {
            return Err("FULL OUTER JOIN sides must be plain tables or CTEs".into());
        };
        let tname = name.to_string();
        let display = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        let cols: Vec<String> = if let Some((_, cs, _)) = ctes.get(&tname) {
            cs.iter().map(|(n, _)| n.clone()).collect()
        } else if let Some(d) = catalog.table(&tname) {
            d.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            return Err(format!("unknown table '{tname}' in FULL OUTER JOIN"));
        };
        Ok((display, cols))
    };
    let (da, cols_a) = side(&twj.relation)?;
    let (db, cols_b) = side(&join.relation)?;

    // The A-side key for the anti branch's probe: a matched B row carries a
    // non-NULL A key, so `a.key IS NULL` is exactly "no A match".
    let mut conjs = Vec::new();
    split_and(cond, &mut conjs);
    let mut akey: Option<String> = None;
    'outer: for cj in &conjs {
        if let ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } = cj
        {
            for e in [left.as_ref(), right.as_ref()] {
                if let Some([t, c]) = ident_parts(e).as_deref()
                    && t.eq_ignore_ascii_case(&da)
                {
                    akey = Some(c.to_string());
                    break 'outer;
                }
            }
        }
    }
    let Some(akey) = akey else {
        return Err("FULL OUTER JOIN needs an ON equality keyed on the left side".into());
    };

    // The prefixed projection both branches share (UNION ALL is positional).
    let qual = |t: &str, c: &str| {
        ast::Expr::CompoundIdentifier(vec![ast::Ident::new(t), ast::Ident::new(c)])
    };
    let proj: Vec<ast::SelectItem> = cols_a
        .iter()
        .map(|c| (da.as_str(), "a", c))
        .chain(cols_b.iter().map(|c| (db.as_str(), "b", c)))
        .map(|(t, s, c)| ast::SelectItem::ExprWithAlias {
            expr: qual(t, c),
            alias: ast::Ident::new(format!("__fo_{s}_{c}")),
        })
        .collect();
    let left_join = |lhs: &ast::TableFactor, rhs: &ast::TableFactor| {
        vec![ast::TableWithJoins {
            relation: lhs.clone(),
            joins: vec![ast::Join {
                relation: rhs.clone(),
                global: false,
                join_operator: ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(cond.clone())),
            }],
        }]
    };
    let mut b1 = select.clone();
    b1.projection = proj.clone();
    b1.from = left_join(&twj.relation, &join.relation);
    b1.selection = None;
    b1.group_by = ast::GroupByExpr::Expressions(Vec::new(), Vec::new());
    b1.having = None;
    b1.distinct = None;
    let mut b2 = b1.clone();
    b2.from = left_join(&join.relation, &twj.relation);
    b2.selection = Some(ast::Expr::IsNull(Box::new(qual(&da, &akey))));

    let mut union_q = query.clone();
    union_q.with = None;
    union_q.order_by = None;
    union_q.limit_clause = None;
    union_q.body = Box::new(ast::SetExpr::SetOperation {
        op: ast::SetOperator::Union,
        set_quantifier: ast::SetQuantifier::All,
        left: Box::new(ast::SetExpr::Select(Box::new(b1))),
        right: Box::new(ast::SetExpr::Select(Box::new(b2))),
    });

    // The enclosing select reads from the wrapper; its side-qualified
    // references rewrite to the prefixed columns.
    let mut outer = select.clone();
    outer.from = vec![ast::TableWithJoins {
        relation: ast::TableFactor::Derived {
            lateral: false,
            subquery: Box::new(union_q),
            alias: Some(ast::TableAlias {
                name: ast::Ident::new("__fo"),
                explicit: false,
                columns: Vec::new(),
            }),
            sample: None,
        },
        joins: Vec::new(),
    }];
    let rw = |e: &mut ast::Expr| rewrite_side_refs(e, &da, &cols_a, &db, &cols_b);
    for item in &mut outer.projection {
        match item {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                rw(e);
            }
            _ => {}
        }
    }
    if let Some(s) = &mut outer.selection {
        rw(s);
    }
    if let ast::GroupByExpr::Expressions(gs, _) = &mut outer.group_by {
        for g in gs {
            rw(g);
        }
    }
    if let Some(h) = &mut outer.having {
        rw(h);
    }

    let mut q2 = query.clone();
    q2.with = None;
    q2.body = Box::new(ast::SetExpr::Select(Box::new(outer)));
    if let Some(ob) = &mut q2.order_by
        && let ast::OrderByKind::Expressions(exprs) = &mut ob.kind
    {
        for oe in exprs {
            rw(&mut oe.expr);
        }
    }
    Ok(Some(q2))
}

/// Rewrite side-qualified (`a.c` / `b.c`) and unambiguous unqualified
/// references to the FULL-OUTER wrapper's prefixed columns (see
/// [`rewrite_full_outer`]).
fn rewrite_side_refs(e: &mut ast::Expr, da: &str, cols_a: &[String], db: &str, cols_b: &[String]) {
    let in_side = |cols: &[String], c: &str| cols.iter().any(|x| x.eq_ignore_ascii_case(c));
    match e {
        ast::Expr::CompoundIdentifier(ids) => {
            if let [t, c] = ids.as_slice() {
                let side = if t.value.eq_ignore_ascii_case(da) && in_side(cols_a, &c.value) {
                    Some("a")
                } else if t.value.eq_ignore_ascii_case(db) && in_side(cols_b, &c.value) {
                    Some("b")
                } else {
                    None
                };
                if let Some(s) = side {
                    *e = ast::Expr::Identifier(ast::Ident::new(format!("__fo_{s}_{}", c.value)));
                }
            }
        }
        ast::Expr::Identifier(id) => {
            let ina = in_side(cols_a, &id.value);
            let inb = in_side(cols_b, &id.value);
            if ina ^ inb {
                let s = if ina { "a" } else { "b" };
                *e = ast::Expr::Identifier(ast::Ident::new(format!("__fo_{s}_{}", id.value)));
            }
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            rewrite_side_refs(left, da, cols_a, db, cols_b);
            rewrite_side_refs(right, da, cols_a, db, cols_b);
        }
        ast::Expr::UnaryOp { expr, .. }
        | ast::Expr::Nested(expr)
        | ast::Expr::IsNull(expr)
        | ast::Expr::IsNotNull(expr)
        | ast::Expr::Cast { expr, .. } => rewrite_side_refs(expr, da, cols_a, db, cols_b),
        ast::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                rewrite_side_refs(op, da, cols_a, db, cols_b);
            }
            for cw in conditions {
                rewrite_side_refs(&mut cw.condition, da, cols_a, db, cols_b);
                rewrite_side_refs(&mut cw.result, da, cols_a, db, cols_b);
            }
            if let Some(el) = else_result {
                rewrite_side_refs(el, da, cols_a, db, cols_b);
            }
        }
        ast::Expr::Function(f) => {
            if let ast::FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(x)) = a {
                        rewrite_side_refs(x, da, cols_a, db, cols_b);
                    }
                }
            }
        }
        ast::Expr::InList { expr, list, .. } => {
            rewrite_side_refs(expr, da, cols_a, db, cols_b);
            for x in list {
                rewrite_side_refs(x, da, cols_a, db, cols_b);
            }
        }
        ast::Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_side_refs(expr, da, cols_a, db, cols_b);
            rewrite_side_refs(low, da, cols_a, db, cols_b);
            rewrite_side_refs(high, da, cols_a, db, cols_b);
        }
        _ => {}
    }
}

/// Flatten an `AND` tree into its conjuncts (leaves kept in source order).
fn split_and<'e>(e: &'e ast::Expr, out: &mut Vec<&'e ast::Expr>) {
    match e {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            split_and(left, out);
            split_and(right, out);
        }
        // A parenthesized conjunction — `ON (a = b AND c = d)`, the TPC-DS
        // two-key outer-join shape — is one `Nested` node; descend so the
        // equijoins split into separate edges instead of one multi-table
        // conjunct that a LEFT JOIN can't route.
        ast::Expr::Nested(inner) => split_and(inner, out),
        _ => out.push(e),
    }
}

/// The name parts of a plain (possibly qualified, possibly parenthesized)
/// identifier, if that's all `e` is.
fn ident_parts(e: &ast::Expr) -> Option<Vec<&str>> {
    match e {
        ast::Expr::Identifier(id) => Some(vec![&id.value]),
        ast::Expr::CompoundIdentifier(ids) => Some(ids.iter().map(|i| i.value.as_str()).collect()),
        ast::Expr::Nested(inner) => ident_parts(inner),
        _ => None,
    }
}

/// Count word-boundary occurrences of `name` in `text`, case-insensitively
/// (`_` and alphanumerics are word characters, so CTE `ws` does not match
/// column `ws_qty`). Drives the conservative single-reference check for
/// constant pushdown: any doubt counts as an extra reference.
fn count_word(text: &str, name: &str) -> usize {
    let text = text.to_ascii_lowercase();
    let name = name.to_ascii_lowercase();
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let (t, n) = (text.as_bytes(), name.as_bytes());
    let mut count = 0;
    let mut i = 0;
    while i + n.len() <= t.len() {
        if &t[i..i + n.len()] == n
            && (i == 0 || !is_word(t[i - 1]))
            && (i + n.len() == t.len() || !is_word(t[i + n.len()]))
        {
            count += 1;
            i += n.len();
        } else {
            i += 1;
        }
    }
    count
}

/// Constant pushdown into single-referenced CTE group keys — q78's sf10
/// lever. The outer query aggregates nothing away, but its `WHERE
/// cte_col = const` arrives only AFTER each CTE has materialized every
/// group (q78's `ss` CTE builds 24M year×item×customer groups at sf10,
/// then the outer filter keeps one year). This pre-bind AST rewrite:
///
/// 1. attributes every outer FROM item's columns (CTE outputs from the
///    WITH clause, base-table columns from the catalog);
/// 2. seeds `col = literal` conjuncts from the outer WHERE (and from
///    INNER-join ONs; from a LEFT ON only when the column sits on the
///    nullable side);
/// 3. propagates the constants across join equalities — both ways through
///    WHERE/INNER-ON equalities, and only INTO the nullable side through a
///    LEFT ON (`ws_sold_year = ss_sold_year` carries `= 2000` into `ws`:
///    matched rows must satisfy it, unmatched rows are NULL-filled either
///    way, so pruning the build side never changes a preserved row);
/// 4. injects `inner_expr = const` into a CTE's WHERE when the column maps
///    to one of its plain GROUP BY keys AND the CTE is referenced exactly
///    once in the whole query (a shared CTE must not inherit one
///    reference's filter).
///
/// Returns the rewritten query, or None when nothing (new) is pushable —
/// the recursion in [`bind_query`] terminates because a second pass finds
/// every conjunct already present.
fn rewrite_cte_const_pushdown(query: &ast::Query, catalog: &Catalog) -> Option<ast::Query> {
    let with = query.with.as_ref()?;
    if with.recursive {
        return None;
    }
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let selection = select.selection.as_ref()?;

    // The CTE output-column names, straight from the AST (explicit column
    // list, or each select item's alias / bare identifier; anything
    // unnameable attributes nothing).
    let cte_out_cols = |q: &ast::Query| -> Vec<Option<String>> {
        let ast::SetExpr::Select(s) = q.body.as_ref() else {
            return Vec::new();
        };
        s.projection
            .iter()
            .map(|item| match item {
                ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                ast::SelectItem::UnnamedExpr(e) => {
                    ident_parts(e).map(|p| p.last().expect("nonempty").to_string())
                }
                _ => None,
            })
            .collect()
    };
    let ctes: Vec<(&ast::Cte, Vec<Option<String>>)> = with
        .cte_tables
        .iter()
        .map(|c| {
            let cols = if c.alias.columns.is_empty() {
                cte_out_cols(&c.query)
            } else {
                c.alias
                    .columns
                    .iter()
                    .map(|col| Some(col.name.value.clone()))
                    .collect()
            };
            (c, cols)
        })
        .collect();

    // FROM items in join order: display name, backing CTE (if any), and
    // column set. Any FROM shape beyond plain named tables bails — the
    // attribution below must be exact.
    struct Item {
        display: String,
        cte: Option<usize>,
        cols: Vec<String>,
    }
    let mut items: Vec<Item> = Vec::new();
    // Directed constant-flow edges between (item, col) nodes, plus seeds.
    type Node = (usize, String);
    let mut edges: Vec<(Node, Node, bool)> = Vec::new();
    let mut seeds: Vec<(Node, ast::Expr)> = Vec::new();

    let add_item = |items: &mut Vec<Item>, tf: &ast::TableFactor| -> Option<()> {
        let ast::TableFactor::Table { name, alias, .. } = tf else {
            return None;
        };
        let tname = name.to_string();
        let display = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        let cte = ctes
            .iter()
            .position(|(c, _)| c.alias.name.value.eq_ignore_ascii_case(&tname));
        let cols: Vec<String> = match cte {
            Some(i) => ctes[i].1.iter().flatten().cloned().collect(),
            None => catalog
                .table(&tname)?
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
        };
        items.push(Item { display, cte, cols });
        Some(())
    };

    // (item, col) for an identifier, only when unambiguous.
    let resolve = |items: &[Item], e: &ast::Expr| -> Option<(usize, String)> {
        let parts = ident_parts(e)?;
        match parts.as_slice() {
            [col] => {
                let mut hit = None;
                for (i, it) in items.iter().enumerate() {
                    if it.cols.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                        if hit.is_some() {
                            return None; // ambiguous
                        }
                        hit = Some((i, col.to_ascii_lowercase()));
                    }
                }
                hit
            }
            [tab, col] => {
                let i = items
                    .iter()
                    .position(|it| it.display.eq_ignore_ascii_case(tab))?;
                items[i]
                    .cols
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(col))
                    .then(|| (i, col.to_ascii_lowercase()))
            }
            _ => None,
        }
    };

    // Walk FROM: collect items and, per join ON, the equality edges with
    // their legal flow direction.
    let mut pending_ons: Vec<(usize, bool, ast::Expr)> = Vec::new(); // (right item, is_left, on)
    for twj in &select.from {
        add_item(&mut items, &twj.relation)?;
        for join in &twj.joins {
            let (on, is_left) = match &join.join_operator {
                ast::JoinOperator::Inner(ast::JoinConstraint::On(e))
                | ast::JoinOperator::Join(ast::JoinConstraint::On(e)) => (e, false),
                ast::JoinOperator::LeftOuter(ast::JoinConstraint::On(e))
                | ast::JoinOperator::Left(ast::JoinConstraint::On(e)) => (e, true),
                _ => return None,
            };
            add_item(&mut items, &join.relation)?;
            pending_ons.push((items.len() - 1, is_left, on.clone()));
        }
    }

    let is_literal = |e: &ast::Expr| matches!(e, ast::Expr::Value(_));
    let mut eat_conjunct = |e: &ast::Expr, nullable_item: Option<usize>| {
        let ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } = e
        else {
            return;
        };
        match (
            resolve(&items, left),
            resolve(&items, right),
            is_literal(left),
            is_literal(right),
        ) {
            (Some(a), Some(b), _, _) => {
                // In a LEFT ON, constants may flow only INTO the nullable
                // side; everywhere else both ways.
                let (a_ok, b_ok) = match nullable_item {
                    Some(n) => (a.0 == n, b.0 == n),
                    None => (true, true),
                };
                edges.push((a.clone(), b.clone(), b_ok));
                edges.push((b, a, a_ok));
            }
            (Some(a), None, _, true) if nullable_item.is_none_or(|n| a.0 == n) => {
                seeds.push((a, (**right).clone()));
            }
            (None, Some(b), true, _) if nullable_item.is_none_or(|n| b.0 == n) => {
                seeds.push((b, (**left).clone()));
            }
            _ => {}
        }
    };
    let mut where_conjs = Vec::new();
    split_and(selection, &mut where_conjs);
    for c in &where_conjs {
        eat_conjunct(c, None);
    }
    for (right, is_left, on) in &pending_ons {
        let mut on_conjs = Vec::new();
        split_and(on, &mut on_conjs);
        for c in on_conjs {
            eat_conjunct(c, is_left.then_some(*right));
        }
    }

    // Propagate constants to fixpoint over the directed edges.
    let mut consts: HashMap<(usize, String), ast::Expr> = HashMap::new();
    for (node, lit) in seeds {
        consts.entry(node).or_insert(lit);
    }
    loop {
        let mut grew = false;
        for (from, to, ok) in &edges {
            if *ok && consts.contains_key(from) && !consts.contains_key(to) {
                let lit = consts[from].clone();
                consts.insert(to.clone(), lit);
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }

    // Inject into single-referenced CTEs whose column is a plain GROUP BY
    // key. The reference count is textual but word-boundary exact, over the
    // body plus every OTHER CTE definition — a false extra only skips the
    // optimization.
    let body_text = query.body.to_string();
    let mut new_query = query.clone();
    let mut changed = false;
    for ((item, col), lit) in &consts {
        let Some(ci) = items[*item].cte else { continue };
        let cte_name = &ctes[ci].0.alias.name.value;
        let refs = count_word(&body_text, cte_name)
            + with
                .cte_tables
                .iter()
                .filter(|c| !c.alias.name.value.eq_ignore_ascii_case(cte_name))
                .map(|c| count_word(&c.query.to_string(), cte_name))
                .sum::<usize>();
        if refs != 1 {
            continue;
        }
        // The inner expression behind this output column, which must be a
        // plain (modifier-free, non-ROLLUP) GROUP BY key.
        let ast::SetExpr::Select(inner) = ctes[ci].0.query.body.as_ref() else {
            continue;
        };
        let out_idx = ctes[ci]
            .1
            .iter()
            .position(|c| c.as_ref().is_some_and(|n| n.eq_ignore_ascii_case(col)));
        let Some(out_idx) = out_idx else { continue };
        let inner_expr = match inner.projection.get(out_idx) {
            Some(ast::SelectItem::ExprWithAlias { expr, .. })
            | Some(ast::SelectItem::UnnamedExpr(expr)) => expr,
            _ => continue,
        };
        let ast::GroupByExpr::Expressions(gexprs, mods) = &inner.group_by else {
            continue;
        };
        if !mods.is_empty()
            || gexprs
                .iter()
                .any(|g| matches!(g, ast::Expr::Rollup(_) | ast::Expr::Cube(_)))
            || !gexprs.contains(inner_expr)
        {
            continue;
        }
        let conj = ast::Expr::BinaryOp {
            left: Box::new(inner_expr.clone()),
            op: ast::BinaryOperator::Eq,
            right: Box::new(lit.clone()),
        };
        // Idempotence: skip if this exact conjunct is already there (the
        // re-bind after a successful rewrite lands back here).
        let target = new_query
            .with
            .as_mut()
            .expect("cloned WITH")
            .cte_tables
            .get_mut(ci)
            .expect("cte index");
        let ast::SetExpr::Select(tsel) = target.query.body.as_mut() else {
            continue;
        };
        if let Some(sel) = &tsel.selection {
            let mut existing = Vec::new();
            split_and(sel, &mut existing);
            if existing.contains(&&conj) {
                continue;
            }
        }
        tsel.selection = Some(match tsel.selection.take() {
            Some(old) => ast::Expr::BinaryOp {
                left: Box::new(old),
                op: ast::BinaryOperator::And,
                right: Box::new(conj),
            },
            None => conj,
        });
        changed = true;
    }
    changed.then_some(new_query)
}

/// Offset-equijoin promotion — q59/q2's sf10 lever. Their outer join is
/// `FROM (…) y, (…) x WHERE y.store = x.store AND y.week = x.week - 52`:
/// the arithmetic conjunct can't be a join edge, so the executor joined on
/// the store key alone (~180 duplicate rows per store at sf10) and filtered
/// ~17M fanned-out candidates row-by-row (15s; DuckDB 0.15s). The offset
/// IS an equijoin — on a computed key. This pre-bind rewrite:
///
/// - matches WHERE conjuncts `a = b ± N` (either orientation) where `a`
///   and `b` are output columns of two DIFFERENT derived FROM items;
/// - appends `<b's underlying expr> ± N AS __ejk<i>` to b's subquery
///   projection and rewrites the conjunct to the plain equality
///   `a = x.__ejk<i>` — a real edge the planner merges with the other
///   equalities into one composite-key join;
/// - forces BOTH participants to materialize: inlining one would splice
///   its inner tables into this scope and re-open a join cycle whose
///   broken edge falls right back into the fan-out residual path.
///
/// Fires only when every FROM item is an aliased, join-free derived
/// subquery — attribution must be exact. Returns the rewritten query plus
/// the aliases to force-materialize, or None.
fn rewrite_offset_equijoin(query: &ast::Query) -> Option<(ast::Query, BTreeSet<String>)> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let selection = select.selection.as_ref()?;

    // Every FROM item: an aliased derived with a plain Select body.
    let mut items: Vec<(String, Vec<Option<String>>)> = Vec::new();
    for twj in &select.from {
        if !twj.joins.is_empty() {
            return None;
        }
        let ast::TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = &twj.relation
        else {
            return None;
        };
        let ast::SetExpr::Select(inner) = subquery.body.as_ref() else {
            return None;
        };
        let cols = inner
            .projection
            .iter()
            .map(|item| match item {
                ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                ast::SelectItem::UnnamedExpr(e) => {
                    ident_parts(e).map(|p| p.last().expect("nonempty").to_string())
                }
                _ => None,
            })
            .collect();
        items.push((alias.name.value.clone(), cols));
    }
    if items.len() < 2 {
        return None;
    }

    // (item, output idx) for an identifier, only when unambiguous.
    let resolve = |e: &ast::Expr| -> Option<(usize, usize)> {
        let parts = ident_parts(e)?;
        let find = |i: usize, col: &str| {
            items[i]
                .1
                .iter()
                .position(|c| c.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(col)))
        };
        match parts.as_slice() {
            [col] => {
                let mut hit = None;
                for i in 0..items.len() {
                    if let Some(j) = find(i, col) {
                        if hit.is_some() {
                            return None; // ambiguous
                        }
                        hit = Some((i, j));
                    }
                }
                hit
            }
            [tab, col] => {
                let i = items
                    .iter()
                    .position(|(a, _)| a.eq_ignore_ascii_case(tab))?;
                find(i, col).map(|j| (i, j))
            }
            _ => None,
        }
    };
    let mut conjs: Vec<&ast::Expr> = Vec::new();
    split_and(selection, &mut conjs);
    let mut new_conjs: Vec<ast::Expr> = conjs.iter().map(|c| (*c).clone()).collect();
    let mut force: BTreeSet<String> = BTreeSet::new();
    // Per-item appended computed columns: (item, expr, hidden name).
    let mut appended: Vec<(usize, ast::Expr, String)> = Vec::new();
    for (ci, conj) in conjs.iter().enumerate() {
        let ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } = conj
        else {
            continue;
        };
        // One plain side, one `ident ± lit` side (either orientation).
        let (plain, compound) = if compound_side(right).is_some() {
            (left, right)
        } else if compound_side(left).is_some() {
            (right, left)
        } else {
            continue;
        };
        let (cident, cop, clit) = compound_side(compound).expect("checked");
        let Some((pa, _)) = resolve(plain) else {
            continue;
        };
        let Some((pb, jb)) = resolve(cident) else {
            continue;
        };
        if pa == pb {
            continue; // same table — an ordinary filter
        }
        // b's underlying expression (strip the output alias).
        let hidden = format!("__ejk{}", appended.len());
        let jexpr = ast::Expr::BinaryOp {
            left: Box::new(match query_derived_item(query, pb, jb) {
                Some(e) => e.clone(),
                None => continue,
            }),
            op: cop,
            right: Box::new(clit.clone()),
        };
        appended.push((pb, jexpr, hidden.clone()));
        new_conjs[ci] = ast::Expr::BinaryOp {
            left: Box::new((**plain).clone()),
            op: ast::BinaryOperator::Eq,
            right: Box::new(ast::Expr::CompoundIdentifier(vec![
                ast::Ident::new(items[pb].0.clone()),
                ast::Ident::new(hidden),
            ])),
        };
        force.insert(items[pa].0.to_ascii_lowercase());
        force.insert(items[pb].0.to_ascii_lowercase());
    }
    if appended.is_empty() {
        return None;
    }

    let mut q2 = query.clone();
    let ast::SetExpr::Select(sel2) = q2.body.as_mut() else {
        unreachable!("checked Select above");
    };
    for (i, expr, name) in appended {
        let ast::TableFactor::Derived { subquery, .. } = &mut sel2.from[i].relation else {
            unreachable!("checked Derived above");
        };
        let ast::SetExpr::Select(inner) = subquery.body.as_mut() else {
            unreachable!("checked Select above");
        };
        inner.projection.push(ast::SelectItem::ExprWithAlias {
            expr,
            alias: ast::Ident::new(name),
        });
    }
    sel2.selection = new_conjs.into_iter().reduce(|a, b| ast::Expr::BinaryOp {
        left: Box::new(a),
        op: ast::BinaryOperator::And,
        right: Box::new(b),
    });
    Some((q2, force))
}

/// CTE set-narrowing — q95's sf10 lever. Its `ws_wh` CTE (a web_sales
/// self-join) materialized 74.8M `(order, wh1, wh2)` rows, but both
/// consumers are IN-subqueries that use only `ws_order_number`: under set
/// semantics row multiplicity can never matter, so the CTE legally
/// narrows to `SELECT DISTINCT ws_order_number …` (~600k rows) — and the
/// downstream `web_returns ⋈ ws_wh` stops fanning out ~125 duplicate rows
/// per order. Conditions, all conservative:
///
/// - every textual occurrence of the CTE's name (word-boundary count over
///   the body and every other CTE definition) is accounted for by a
///   structural reference inside an `IN (SELECT …)` conjunct of the
///   top-level WHERE whose FROM is a plain comma list — any unaccounted
///   occurrence (outer FROM, EXISTS, join shapes, another CTE) blocks the
///   rewrite, so a row-context consumer keeps full width and duplicates;
/// - the used-column set comes from the subqueries' projections and WHERE
///   clauses via [`collect_idents`], which returns false on any expression
///   variant it doesn't model (blocking the rewrite rather than guessing);
/// - the rewrite must PRUNE something (`used ⊂ outputs`); the injected
///   `DISTINCT` then terminates the re-bind recursion via the
///   `distinct.is_some()` skip.
fn rewrite_cte_set_narrowing(query: &ast::Query) -> Option<ast::Query> {
    let with = query.with.as_ref()?;
    if with.recursive {
        return None;
    }
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let selection = select.selection.as_ref()?;
    let body_text = query.body.to_string();
    let mut conjs: Vec<&ast::Expr> = Vec::new();
    split_and(selection, &mut conjs);

    let mut q2: Option<ast::Query> = None;
    'ctes: for (ci, cte) in with.cte_tables.iter().enumerate() {
        let name = &cte.alias.name.value;
        if !cte.alias.columns.is_empty() {
            continue; // a declared column list fixes the arity
        }
        let ast::SetExpr::Select(inner) = cte.query.body.as_ref() else {
            continue;
        };
        if inner.distinct.is_some() {
            continue;
        }
        let outs: Vec<Option<String>> = inner
            .projection
            .iter()
            .map(|it| match it {
                ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                ast::SelectItem::UnnamedExpr(e) => {
                    ident_parts(e).map(|p| p.last().expect("nonempty").to_string())
                }
                _ => None,
            })
            .collect();
        if outs.iter().any(Option::is_none) {
            continue;
        }
        let outs: Vec<String> = outs.into_iter().flatten().collect();

        let mut accounted = 0usize; // word occurrences inside accounted refs
        let mut nrefs = 0usize;
        let mut used: BTreeSet<usize> = BTreeSet::new();
        for conj in &conjs {
            let ast::Expr::InSubquery { subquery, .. } = conj else {
                continue;
            };
            let ast::SetExpr::Select(sub) = subquery.body.as_ref() else {
                continue;
            };
            if subquery.with.is_some() || sub.from.iter().any(|t| !t.joins.is_empty()) {
                continue; // not accountable; the count guard blocks below
            }
            // The CTE's display name(s) in this FROM.
            let mut displays: Vec<(String, bool)> = Vec::new();
            let mut refs_here = 0usize;
            for twj in &sub.from {
                let ast::TableFactor::Table {
                    name: tn, alias, ..
                } = &twj.relation
                else {
                    continue;
                };
                let tname = tn.to_string();
                let display = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| tname.clone());
                let is_cte = tname.eq_ignore_ascii_case(name);
                refs_here += usize::from(is_cte);
                displays.push((display, is_cte));
            }
            if refs_here == 0 {
                continue;
            }
            nrefs += refs_here;
            accounted += count_word(&subquery.to_string(), name);
            let mut idents: Vec<Vec<&str>> = Vec::new();
            let mut ok = true;
            for it in &sub.projection {
                match it {
                    ast::SelectItem::UnnamedExpr(e)
                    | ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                        ok &= collect_idents(e, &mut idents);
                    }
                    _ => used.extend(0..outs.len()), // wildcard → all columns
                }
            }
            if let Some(w) = &sub.selection {
                ok &= collect_idents(w, &mut idents);
            }
            if !ok {
                continue 'ctes;
            }
            for parts in idents {
                match parts.as_slice() {
                    [col] => {
                        if let Some(j) = outs.iter().position(|o| o.eq_ignore_ascii_case(col)) {
                            used.insert(j);
                        }
                    }
                    [tab, col] => {
                        if displays
                            .iter()
                            .any(|(d, is)| *is && d.eq_ignore_ascii_case(tab))
                        {
                            match outs.iter().position(|o| o.eq_ignore_ascii_case(col)) {
                                Some(j) => {
                                    used.insert(j);
                                }
                                None => continue 'ctes,
                            }
                        }
                    }
                    _ => continue 'ctes,
                }
            }
        }
        if nrefs == 0 || used.is_empty() || used.len() == outs.len() {
            continue;
        }
        // Every occurrence accounted: none elsewhere in the body, none in
        // any other CTE definition.
        if count_word(&body_text, name) != accounted
            || with
                .cte_tables
                .iter()
                .enumerate()
                .any(|(j, c)| j != ci && count_word(&c.query.to_string(), name) > 0)
        {
            continue;
        }
        let q = q2.get_or_insert_with(|| query.clone());
        let target = &mut q.with.as_mut().expect("cloned WITH").cte_tables[ci];
        let ast::SetExpr::Select(tsel) = target.query.body.as_mut() else {
            unreachable!("checked Select above");
        };
        tsel.projection = used.iter().map(|&j| tsel.projection[j].clone()).collect();
        tsel.distinct = Some(ast::Distinct::Distinct);
    }
    q2
}

/// Collect identifier paths appearing in an expression. Returns `false`
/// on any variant it doesn't model — callers must treat that as "unknown
/// column usage" and skip their rewrite rather than guess.
fn collect_idents<'e>(e: &'e ast::Expr, out: &mut Vec<Vec<&'e str>>) -> bool {
    if let Some(p) = ident_parts(e) {
        out.push(p);
        return true;
    }
    match e {
        ast::Expr::Value(_) => true,
        ast::Expr::BinaryOp { left, right, .. } => {
            collect_idents(left, out) && collect_idents(right, out)
        }
        ast::Expr::UnaryOp { expr, .. }
        | ast::Expr::IsNull(expr)
        | ast::Expr::IsNotNull(expr)
        | ast::Expr::Cast { expr, .. } => collect_idents(expr, out),
        ast::Expr::Like { expr, .. } => collect_idents(expr, out),
        ast::Expr::Between {
            expr, low, high, ..
        } => collect_idents(expr, out) && collect_idents(low, out) && collect_idents(high, out),
        ast::Expr::InList { expr, list, .. } => {
            collect_idents(expr, out) && list.iter().all(|x| collect_idents(x, out))
        }
        ast::Expr::Function(f) => {
            let ast::FunctionArguments::List(list) = &f.args else {
                return matches!(f.args, ast::FunctionArguments::None);
            };
            list.args.iter().all(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(x)) => collect_idents(x, out),
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => true,
                _ => false,
            })
        }
        _ => false,
    }
}

/// `ident ± Number`, decomposed as (ident, op, literal) — the compound
/// side of an offset-equijoin conjunct.
fn compound_side(e: &ast::Expr) -> Option<(&ast::Expr, ast::BinaryOperator, &ast::Expr)> {
    let ast::Expr::BinaryOp { left, op, right } = e else {
        return None;
    };
    if !matches!(op, ast::BinaryOperator::Plus | ast::BinaryOperator::Minus) {
        return None;
    }
    let lit_ok = |x: &ast::Expr| matches!(x, ast::Expr::Value(v) if matches!(v.value, ast::Value::Number(..)));
    if ident_parts(left).is_some() && lit_ok(right) {
        Some((left, op.clone(), right))
    } else {
        None
    }
}

/// The underlying (alias-stripped) expression of derived FROM item `i`'s
/// `j`-th output column, if it is a plain expr item.
fn query_derived_item(query: &ast::Query, i: usize, j: usize) -> Option<&ast::Expr> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let ast::TableFactor::Derived { subquery, .. } = &select.from[i].relation else {
        return None;
    };
    let ast::SetExpr::Select(inner) = subquery.body.as_ref() else {
        return None;
    };
    match inner.projection.get(j)? {
        ast::SelectItem::ExprWithAlias { expr, .. } | ast::SelectItem::UnnamedExpr(expr) => {
            Some(expr)
        }
        _ => None,
    }
}

/// Does the AST expression contain an **aggregate or window** call? Unlike
/// [`contains_function`], a scalar function (`concat`, `substr`, `coalesce`,
/// …) is transparent — the walk descends into its arguments so a nested
/// aggregate (`substr(max(x), 1)`) is still found. Drives the plain-row vs
/// grouped decision, so a plain projection may carry scalar functions.
fn contains_aggregate(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Function(f) => {
            if f.over.is_some() {
                return true; // a window call needs the grouped/window path
            }
            if matches!(
                f.name.to_string().to_lowercase().as_str(),
                "sum" | "count" | "min" | "max" | "avg" | "stddev_samp" | "stddev" | "stddev_sample" | "stddev_pop" | "var_samp" | "variance" | "var" | "var_pop"
            ) {
                return true;
            }
            // A scalar function: look for aggregates in its arguments.
            match &f.args {
                ast::FunctionArguments::List(list) => list.args.iter().any(|a| {
                    matches!(a,
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                            if contains_aggregate(e))
                }),
                _ => false,
            }
        }
        ast::Expr::Nested(i) => contains_aggregate(i),
        ast::Expr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ast::Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        ast::Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        ast::Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        ast::Expr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        ast::Expr::Extract { expr, .. } => contains_aggregate(expr),
        ast::Expr::Substring { expr, .. } => contains_aggregate(expr),
        // A cast is transparent to the aggregate it wraps — q49's
        // `cast(sum(x) AS DECIMAL(15,4))` must still route to the grouped
        // path, not be mistaken for a plain scalar projection.
        ast::Expr::Cast { expr, .. } => contains_aggregate(expr),
        ast::Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            conditions
                .iter()
                .any(|cw| contains_aggregate(&cw.condition) || contains_aggregate(&cw.result))
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        _ => false,
    }
}

/// Does the AST expression contain a window call (`fn(...) OVER (...)`)?
/// A window forces its own execution stage even with no GROUP BY.
fn contains_window(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Function(f) => {
            f.over.is_some()
                || match &f.args {
                    ast::FunctionArguments::List(list) => list.args.iter().any(|a| {
                        matches!(a,
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                                if contains_window(e))
                    }),
                    _ => false,
                }
        }
        ast::Expr::Nested(i) => contains_window(i),
        ast::Expr::BinaryOp { left, right, .. } => contains_window(left) || contains_window(right),
        ast::Expr::UnaryOp { expr, .. } => contains_window(expr),
        ast::Expr::Cast { expr, .. } => contains_window(expr),
        ast::Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            conditions
                .iter()
                .any(|cw| contains_window(&cw.condition) || contains_window(&cw.result))
                || else_result.as_ref().is_some_and(|e| contains_window(e))
        }
        _ => false,
    }
}

/// Does the AST expression contain a **non-window** aggregate call (a bare
/// `sum`/`count`/… with no `OVER`)? A no-GROUP-BY window query is only a
/// slot-passthrough shape when it has none of these.
fn contains_nonwindow_agg(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Function(f) => {
            if f.over.is_some() {
                // A window call — descend into its arguments but the call
                // itself is not a bare aggregate.
                return match &f.args {
                    ast::FunctionArguments::List(list) => list.args.iter().any(|a| {
                        matches!(a,
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                                if contains_nonwindow_agg(e))
                    }),
                    _ => false,
                };
            }
            if matches!(
                f.name.to_string().to_lowercase().as_str(),
                "sum" | "count" | "min" | "max" | "avg" | "stddev_samp" | "stddev" | "stddev_sample" | "stddev_pop" | "var_samp" | "variance" | "var" | "var_pop"
            ) {
                return true;
            }
            match &f.args {
                ast::FunctionArguments::List(list) => list.args.iter().any(|a| {
                    matches!(a,
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                            if contains_nonwindow_agg(e))
                }),
                _ => false,
            }
        }
        ast::Expr::Nested(i) => contains_nonwindow_agg(i),
        ast::Expr::BinaryOp { left, right, .. } => {
            contains_nonwindow_agg(left) || contains_nonwindow_agg(right)
        }
        ast::Expr::UnaryOp { expr, .. } => contains_nonwindow_agg(expr),
        ast::Expr::Cast { expr, .. } => contains_nonwindow_agg(expr),
        ast::Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            conditions.iter().any(|cw| {
                contains_nonwindow_agg(&cw.condition) || contains_nonwindow_agg(&cw.result)
            }) || else_result
                .as_ref()
                .is_some_and(|e| contains_nonwindow_agg(e))
        }
        _ => false,
    }
}

/// Does the AST expression contain a function call (= an aggregate, since
/// scalar functions aren't supported yet)?
fn contains_function(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Function(_) => true,
        ast::Expr::Nested(i) => contains_function(i),
        ast::Expr::BinaryOp { left, right, .. } => {
            contains_function(left) || contains_function(right)
        }
        ast::Expr::UnaryOp { expr, .. } => contains_function(expr),
        ast::Expr::Between {
            expr, low, high, ..
        } => contains_function(expr) || contains_function(low) || contains_function(high),
        ast::Expr::InList { expr, list, .. } => {
            contains_function(expr) || list.iter().any(contains_function)
        }
        ast::Expr::Like { expr, pattern, .. } => {
            contains_function(expr) || contains_function(pattern)
        }
        ast::Expr::Extract { expr, .. } => contains_function(expr),
        ast::Expr::Cast { expr, .. } => contains_function(expr),
        ast::Expr::Subquery(_) | ast::Expr::InSubquery { .. } | ast::Expr::Exists { .. } => false,
        ast::Expr::Substring { expr, .. } => contains_function(expr),
        ast::Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            conditions
                .iter()
                .any(|cw| contains_function(&cw.condition) || contains_function(&cw.result))
                || else_result.as_ref().is_some_and(|e| contains_function(e))
        }
        _ => false,
    }
}

/// Does a bound expression reference any column?
fn references_columns(e: &Expr) -> bool {
    match e {
        Expr::Column(_) => true,
        Expr::Literal(_) => false,
        Expr::Binary { lhs, rhs, .. } => references_columns(lhs) || references_columns(rhs),
        Expr::Extract { arg: i, .. } | Expr::DateTrunc { arg: i, .. } | Expr::CastInt(i) | Expr::Round { expr: i, .. } | Expr::Upper(i) => {
            references_columns(i)
        }
        Expr::Like { expr, .. } => references_columns(expr),
        Expr::ScalarSub(_) => false,
        Expr::InSub { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::InSetStr { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Substr { expr, .. } => references_columns(expr),
        Expr::Concat(parts) | Expr::NumFn { args: parts, .. } | Expr::StrFn { args: parts, .. } => {
            parts.iter().any(references_columns)
        }
        Expr::Case { whens, else_ } => {
            whens
                .iter()
                .any(|(c, v)| references_columns(c) || references_columns(v))
                || references_columns(else_)
        }
    }
}

/// The signed integer count of an interval (`'90' day` → 90).
fn interval_count(iv: &ast::Interval, sign: i32) -> Result<i32, String> {
    let ast::Expr::Value(v) = iv.value.as_ref() else {
        return Err(format!("unsupported interval value: {}", iv.value));
    };
    let n: i32 = match &v.value {
        ast::Value::SingleQuotedString(s) => s.trim().parse(),
        ast::Value::Number(s, _) => s.parse(),
        other => return Err(format!("unsupported interval value: {other}")),
    }
    .map_err(|_| format!("bad interval count in {iv}"))?;
    Ok(n * sign)
}

/// Shift a Date32 day count by an interval (`'90' day`, `'3' month`,
/// `'1' year`), folding at bind time.
fn shift_date(days: i32, iv: &ast::Interval, sign: i32) -> Result<i32, String> {
    let n = interval_count(iv, sign)?;
    match iv.leading_field {
        Some(ast::DateTimeField::Day | ast::DateTimeField::Days) => Ok(days + n),
        Some(ast::DateTimeField::Week(_) | ast::DateTimeField::Weeks) => Ok(days + n * 7),
        Some(ast::DateTimeField::Month | ast::DateTimeField::Months) => Ok(shift_months(days, n)),
        Some(ast::DateTimeField::Year | ast::DateTimeField::Years) => {
            Ok(shift_months(days, n * 12))
        }
        ref other => Err(format!("unsupported interval field: {other:?}")),
    }
}

/// The signed day offset of a **day or week** interval — the constant a
/// date column shifts by. Month/year intervals vary in length, so they
/// are literal-only (they can't lower to a constant integer add).
fn interval_days(iv: &ast::Interval, sign: i32) -> Result<i32, String> {
    let n = interval_count(iv, sign)?;
    match iv.leading_field {
        Some(ast::DateTimeField::Day | ast::DateTimeField::Days) => Ok(n),
        Some(ast::DateTimeField::Week(_) | ast::DateTimeField::Weeks) => Ok(n * 7),
        Some(ast::DateTimeField::Month | ast::DateTimeField::Months)
        | Some(ast::DateTimeField::Year | ast::DateTimeField::Years) => Err(
            "month/year intervals on a date column are not yet supported (literal dates only)"
                .into(),
        ),
        ref other => Err(format!("unsupported interval field: {other:?}")),
    }
}

/// Add `n` calendar months to a day count, clamping the day-of-month.
fn shift_months(days: i32, n: i32) -> i32 {
    let (y, m, d) = civil_of_days(days);
    let months = y * 12 + (m as i32 - 1) + n;
    let (ny, nm) = (months.div_euclid(12), months.rem_euclid(12) as u32 + 1);
    let nd = d.min(days_in_month(ny, nm));
    days_from_civil(ny, nm, nd)
}

/// Days since the Unix epoch → (year, month, day) — Hinnant's
/// `civil_from_days`.
fn civil_of_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}

fn is_integer_family(ty: LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::Int32 | LogicalType::Int64 | LogicalType::Date32
    )
}

/// Cast a partially-bound value to a real [`Expr`] — the decimal→leaf-type
/// boundary: scale 0 stays integer; a fractional decimal becomes the
/// **nearest f64** of its exact value (`7/100 → 0.07`, the correctly-rounded
/// double — not the ULP-low result of folding in f64).
fn materialize(b: Bound) -> Expr {
    match b {
        Bound::Expr(e) => e,
        Bound::Dec(d) => Expr::Literal(d.to_scalar()),
    }
}

/// Coerce an expression under a `CAST(_ AS DECIMAL/NUMERIC/FLOAT/DOUBLE)` to
/// Float64 typing: a numeric *literal* becomes a Float64 literal (so a union
/// branch's `cast(0 AS DECIMAL)` types as float, not int); everything else
/// passes through (aggregate columns and arithmetic already evaluate in f64,
/// and `infer_*_type` already reports Float64 for them).
fn float_cast_expr(inner: Expr) -> Expr {
    match inner {
        Expr::Literal(ScalarValue::Int64(i)) => Expr::Literal(ScalarValue::Float64(i as f64)),
        Expr::Literal(ScalarValue::Int32(i)) => Expr::Literal(ScalarValue::Float64(i as f64)),
        other => other,
    }
}

fn binary(op: BinaryOp, lhs: Expr, rhs: Expr) -> Expr {
    Expr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

fn and(lhs: Expr, rhs: Expr) -> Expr {
    binary(BinaryOp::And, lhs, rhs)
}

fn or(lhs: Expr, rhs: Expr) -> Expr {
    binary(BinaryOp::Or, lhs, rhs)
}

fn bind_op(op: &ast::BinaryOperator) -> Result<BinaryOp, String> {
    use ast::BinaryOperator as A;
    Ok(match op {
        A::Plus => BinaryOp::Add,
        A::Minus => BinaryOp::Sub,
        A::Multiply => BinaryOp::Mul,
        A::Divide => BinaryOp::Div,
        A::Eq => BinaryOp::Eq,
        A::NotEq => BinaryOp::NotEq,
        A::Lt => BinaryOp::Lt,
        A::LtEq => BinaryOp::LtEq,
        A::Gt => BinaryOp::Gt,
        A::GtEq => BinaryOp::GtEq,
        A::And => BinaryOp::And,
        A::Or => BinaryOp::Or,
        other => return Err(format!("unsupported operator: {other}")),
    })
}

/// Bind a typed string literal — today only `date '<YYYY-MM-DD>'` → `Date32`.
/// Rewrite a scalar function call into core AST forms (`Ok(None)` = not
/// a known scalar function — the caller treats it as an aggregate):
/// `abs(x)` → CASE, `coalesce(a, b, …)` → nested CASE over IS NOT NULL,
/// `nullif(a, b)` → CASE.
fn rewrite_scalar_fn(f: &ast::Function) -> Result<Option<ast::Expr>, String> {
    let fname = f.name.to_string().to_lowercase();
    if !matches!(fname.as_str(), "abs" | "coalesce" | "nullif") {
        return Ok(None);
    }
    let ast::FunctionArguments::List(list) = &f.args else {
        return Err(format!("'{fname}' needs an argument list"));
    };
    let mut args: Vec<&ast::Expr> = Vec::new();
    for a in &list.args {
        match a {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => args.push(e),
            other => return Err(format!("unsupported argument in '{fname}': {other}")),
        }
    }
    let case =
        |conditions: Vec<ast::CaseWhen>, else_result: Option<Box<ast::Expr>>| ast::Expr::Case {
            case_token: ast::helpers::attached_token::AttachedToken::empty(),
            end_token: ast::helpers::attached_token::AttachedToken::empty(),
            operand: None,
            conditions,
            else_result,
        };
    let zero = || ast::Expr::value(ast::Value::Number("0".into(), false));
    Ok(Some(match (fname.as_str(), args.as_slice()) {
        ("abs", [x]) => case(
            vec![ast::CaseWhen {
                condition: ast::Expr::BinaryOp {
                    left: Box::new((*x).clone()),
                    op: ast::BinaryOperator::GtEq,
                    right: Box::new(zero()),
                },
                result: (*x).clone(),
            }],
            Some(Box::new(ast::Expr::BinaryOp {
                left: Box::new(zero()),
                op: ast::BinaryOperator::Minus,
                right: Box::new((*x).clone()),
            })),
        ),
        ("nullif", [a, b]) => case(
            vec![ast::CaseWhen {
                condition: ast::Expr::BinaryOp {
                    left: Box::new((*a).clone()),
                    op: ast::BinaryOperator::Eq,
                    right: Box::new((*b).clone()),
                },
                result: ast::Expr::Value(ast::Value::Null.into()),
            }],
            Some(Box::new((*a).clone())),
        ),
        ("coalesce", args) if !args.is_empty() => {
            let last = (*args.last().expect("nonempty")).clone();
            let conditions = args[..args.len() - 1]
                .iter()
                .map(|a| ast::CaseWhen {
                    condition: ast::Expr::IsNotNull(Box::new((*a).clone())),
                    result: (*a).clone(),
                })
                .collect::<Vec<_>>();
            if conditions.is_empty() {
                return Ok(Some(last));
            }
            case(conditions, Some(Box::new(last)))
        }
        _ => return Err(format!("'{fname}' called with {} arguments", args.len())),
    }))
}

/// A positional ordinal (`GROUP BY 1`) as a 0-based index into a SELECT list
/// of `n` items, or `None` if `e` isn't an in-range integer literal.
fn positional_ref(e: &ast::Expr, n: usize) -> Option<usize> {
    if let ast::Expr::Value(v) = e
        && let ast::Value::Number(s, _) = &v.value
        && let Ok(pos) = s.parse::<usize>()
        && (1..=n).contains(&pos)
    {
        Some(pos - 1)
    } else {
        None
    }
}

/// The underlying expression of a SELECT item (positional GROUP BY resolves
/// to it). `SELECT *` can't be grouped positionally.
fn select_item_expr(item: &ast::SelectItem) -> Result<&ast::Expr, String> {
    match item {
        ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => Ok(e),
        other => Err(format!("positional GROUP BY can't reference {other}")),
    }
}

/// Map a `date_trunc` unit string to [`DateTruncUnit`] (DuckDB spellings).
fn bind_trunc_unit(s: &str) -> Result<DateTruncUnit, String> {
    Ok(match s.to_lowercase().as_str() {
        "year" | "years" | "yr" => DateTruncUnit::Year,
        "quarter" | "quarters" => DateTruncUnit::Quarter,
        "month" | "months" | "mon" => DateTruncUnit::Month,
        "week" | "weeks" => DateTruncUnit::Week,
        "day" | "days" => DateTruncUnit::Day,
        other => return Err(format!("unsupported date_trunc unit '{other}'")),
    })
}

/// Map a parsed `EXTRACT` field to the engine's [`DateField`] (DuckDB
/// spellings). Time-of-day fields are out of scope — the engine has only
/// Date32, no timestamps.
fn bind_date_field(field: &ast::DateTimeField) -> Result<DateField, String> {
    use ast::DateTimeField as F;
    Ok(match field {
        F::Year => DateField::Year,
        F::Month => DateField::Month,
        F::Day => DateField::Day,
        F::Quarter => DateField::Quarter,
        F::Dow => DateField::Dow,
        F::Isodow => DateField::IsoDow,
        F::Doy => DateField::Doy,
        F::Week(_) => DateField::Week,
        other => return Err(format!("unsupported EXTRACT field: {other}")),
    })
}

fn bind_typed_string(ts: &ast::TypedString) -> Result<Expr, String> {
    match &ts.data_type {
        ast::DataType::Date => {
            let ast::Value::SingleQuotedString(s) = &ts.value.value else {
                return Err(format!("DATE literal must be a quoted string: {ts}"));
            };
            Ok(Expr::Literal(ScalarValue::Date32(parse_date32(s)?)))
        }
        other => Err(format!("unsupported typed literal: {other} '…'")),
    }
}

/// `"YYYY-MM-DD"` → days since the Unix epoch (proleptic Gregorian).
fn parse_date32(s: &str) -> Result<i32, String> {
    let mut it = s.splitn(3, '-');
    let (Some(y), Some(m), Some(d)) = (it.next(), it.next(), it.next()) else {
        return Err(format!("bad date literal '{s}' (want YYYY-MM-DD)"));
    };
    let bad = |_| format!("bad date literal '{s}' (want YYYY-MM-DD)");
    let y: i32 = y.parse().map_err(bad)?;
    let m: u32 = m.parse().map_err(bad)?;
    let d: u32 = d.parse().map_err(bad)?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(format!("bad date literal '{s}' (month/day out of range)"));
    }
    Ok(days_from_civil(y, m, d))
}

/// Howard Hinnant's `days_from_civil`: civil date → days since 1970-01-01.
fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe as i32 - 719_468
}

/// An exact decimal literal: `mant × 10⁻ˢᶜᵃˡᵉ`. The binder's constant-fold
/// arithmetic runs here so `0.06 + 0.01` is exactly `0.07`, not the f64
/// `0.069999999999999996`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Dec {
    mant: i128,
    scale: u32,
}

impl Dec {
    /// Parse a SQL numeric literal (`"24"`, `"0.06"`, `"-1.5"`).
    fn parse(s: &str) -> Result<Self, String> {
        let bad = || format!("bad numeric literal '{s}'");
        let (int, frac) = match s.split_once('.') {
            Some((i, f)) => (i, f),
            None => (s, ""),
        };
        if frac.contains(['-', '+']) {
            return Err(bad());
        }
        let digits: String = format!("{int}{frac}");
        let mant: i128 = digits.parse().map_err(|_| bad())?;
        Ok(Dec {
            mant,
            scale: frac.len() as u32,
        })
    }

    fn neg(self) -> Self {
        Dec {
            mant: -self.mant,
            ..self
        }
    }

    /// Rescale both to the larger scale.
    fn align(a: &Dec, b: &Dec) -> (i128, i128, u32) {
        let scale = a.scale.max(b.scale);
        let am = a.mant * 10i128.pow(scale - a.scale);
        let bm = b.mant * 10i128.pow(scale - b.scale);
        (am, bm, scale)
    }

    fn add(&self, o: &Dec) -> Dec {
        let (a, b, scale) = Self::align(self, o);
        Dec { mant: a + b, scale }
    }

    fn sub(&self, o: &Dec) -> Dec {
        let (a, b, scale) = Self::align(self, o);
        Dec { mant: a - b, scale }
    }

    fn mul(&self, o: &Dec) -> Dec {
        Dec {
            mant: self.mant * o.mant,
            scale: self.scale + o.scale,
        }
    }

    /// The decimal→leaf-type cast: integers stay integer; fractional values
    /// become the nearest f64 of the exact decimal (`mant / 10^scale`, one
    /// correctly-rounded division).
    fn to_scalar(self) -> ScalarValue {
        if self.scale == 0 {
            if let Ok(i) = i64::try_from(self.mant) {
                return ScalarValue::Int64(i);
            }
        }
        ScalarValue::Float64(self.to_f64())
    }

    /// The exact decimal value as the nearest f64 — used when a context
    /// (e.g. `CAST(0 AS DECIMAL)`) forces a floating result even at scale 0.
    fn to_f64(self) -> f64 {
        self.mant as f64 / 10f64.powi(self.scale as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date32_matches_known_days() {
        assert_eq!(parse_date32("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date32("1994-01-01").unwrap(), 8766);
        assert_eq!(parse_date32("1995-01-01").unwrap(), 9131);
        assert_eq!(parse_date32("1996-12-31").unwrap(), 9861);
        assert!(parse_date32("1994-13-01").is_err());
        assert!(parse_date32("nope").is_err());
    }

    #[test]
    fn decimal_fold_is_exact_where_f64_is_not() {
        let d06 = Dec::parse("0.06").unwrap();
        let d01 = Dec::parse("0.01").unwrap();
        // The exact fold: 0.06 + 0.01 = Dec(7, 2), NOT 0.0699…96.
        assert_eq!(d06.add(&d01), Dec { mant: 7, scale: 2 });
        assert_eq!(d06.sub(&d01), Dec { mant: 5, scale: 2 });
        // Cast at the leaf: nearest f64 of 7/100 == the stored 0.07.
        assert_eq!(d06.add(&d01).to_scalar(), ScalarValue::Float64(0.07));
        // The f64 fold really is one ULP low — the bug this design avoids.
        assert_ne!(0.06_f64 + 0.01_f64, 0.07_f64);

        // Integers stay integer; mixed scales align.
        assert_eq!(
            Dec::parse("24").unwrap().to_scalar(),
            ScalarValue::Int64(24)
        );
        assert_eq!(
            Dec::parse("1.5").unwrap().add(&Dec::parse("2").unwrap()),
            Dec { mant: 35, scale: 1 }
        );
        // Multiply adds scales: 0.5 * 0.5 = 0.25 exactly.
        let half = Dec::parse("0.5").unwrap();
        assert_eq!(half.mul(&half), Dec { mant: 25, scale: 2 });
    }
}
