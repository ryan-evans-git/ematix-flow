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
use crate::expr::{BinaryOp, Expr, ScalarValue};
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
/// (name, type)). CTE references materialize as derived tables.
type CteMap = HashMap<String, (BoundQuery, Vec<(String, LogicalType)>)>;

fn bind_query(
    query: &ast::Query,
    catalog: &Catalog,
    set_semantics: bool,
    outer_ctes: &CteMap,
) -> Result<BoundQuery, String> {
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
                (bq, names.into_iter().zip(tys).collect()),
            );
        }
    }
    let ctes = &ctes;

    if matches!(query.body.as_ref(), ast::SetExpr::SetOperation { .. }) {
        return bind_set_query(query, catalog, ctes);
    }
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("only plain SELECT is supported here".into());
    };

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
                    let bound = b.bind_scalar(e)?;
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
        if !b.views.is_empty() {
            return Err("SELECT * over an inlined subquery is not yet supported".into());
        }
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
                Attribution::Single(t) => filters[t].push(e),
                Attribution::Multi => post.push(e),
            }
        }
    }
    edges.append(&mut b.extra_edges);
    // Decorrelation may have appended tables after `filters` was sized.
    filters.resize(b.tables.len(), Vec::new());
    let post_filter = post.into_iter().reduce(and);

    // Every table must be reachable through join edges — a disconnected
    // table would be a silent cross join.
    {
        let n = b.tables.len();
        let mut seen = vec![false; n];
        let mut stack = vec![0usize];
        seen[0] = true;
        while let Some(t) = stack.pop() {
            for e in &edges {
                let (ta, tb) = (b.slots[e.a].table, b.slots[e.b].table);
                for (x, y) in [(ta, tb), (tb, ta)] {
                    if x == t && !seen[y] {
                        seen[y] = true;
                        stack.push(y);
                    }
                }
            }
        }
        if let Some(missing) = seen.iter().position(|s| !s) {
            return Err(format!(
                "table '{}' is not connected to the rest of the query (missing join condition)",
                b.tables[missing].display
            ));
        }
    }

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
        Expr::ExtractYear(i) | Expr::CastInt(i) => remap_window_cols(i, win_base, group_base),
        Expr::Like { expr, .. }
        | Expr::InSub { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::InSetStr { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Substr { expr, .. } => remap_window_cols(expr, win_base, group_base),
        Expr::Concat(parts) => {
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
        let bq = bind_query(&qside, catalog, false, ctes)?;
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
    derived: Vec<BoundQuery>,
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
                if let Some((cte_bq, cols)) = self.ctes.get(&tname) {
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
                    matches!(
                        &inner.group_by,
                        ast::GroupByExpr::Expressions(g, m) if g.is_empty() && m.is_empty()
                    ) && inner.having.is_none()
                        && subquery.order_by.is_none()
                        && subquery.limit_clause.is_none()
                        && inner.distinct.is_none()
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
                    self.derived.push(bq);
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
            let def = if let Some((_, cols)) = self.ctes.get(&tname) {
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
        self.derived.push(bq);
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
        self.derived.push(bq);
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

        let pred = if !negated {
            // __m = 1 AND (cd >= 2 OR ms <> outer_s)
            and(
                binary(BinaryOp::Eq, m, lit(1)),
                or(
                    binary(BinaryOp::GtEq, cd, lit(2)),
                    binary(BinaryOp::NotEq, ms, outer_s),
                ),
            )
        } else {
            // __m = 0 OR (cd = 1 AND ms = outer_s)
            or(
                binary(BinaryOp::Eq, m, lit(0)),
                and(
                    binary(BinaryOp::Eq, cd, lit(1)),
                    binary(BinaryOp::Eq, ms, outer_s),
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
            name => {
                let af = match name {
                    "sum" => AggFunc::Sum,
                    "avg" => AggFunc::Avg,
                    "min" => AggFunc::Min,
                    "max" => AggFunc::Max,
                    "count" => AggFunc::Count,
                    "stddev_samp" => AggFunc::StddevSamp,
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
            "stddev_samp" => AggFunc::StddevSamp,
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
                Bound::Expr(_) => Err("unary minus on non-literals is not yet supported".into()),
            },
            ast::Expr::TypedString(ts) => bind_typed_string(ts).map(Bound::Expr),
            ast::Expr::Extract { field, expr, .. } => {
                if !matches!(field, ast::DateTimeField::Year) {
                    return Err(format!("unsupported EXTRACT field: {field}"));
                }
                let inner = materialize(self.bind(expr)?);
                Ok(Bound::Expr(Expr::ExtractYear(Box::new(inner))))
            }
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
                if let Some(rewritten) = rewrite_scalar_fn(f)? {
                    return self.bind(&rewritten);
                }
                Err("aggregate calls are only allowed in the SELECT list (so far)".into())
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
            WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::RowNumber => LogicalType::Int64,
            WindowFunc::Agg(_) => LogicalType::Float64,
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
        Expr::ExtractYear(_) | Expr::CastInt(_) => LogicalType::Int64,
        Expr::Substr { .. } | Expr::Concat(_) => LogicalType::Utf8,
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
                    WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::RowNumber => {
                        LogicalType::Int64
                    }
                    WindowFunc::Agg(_) => LogicalType::Float64,
                }
            }
        }
        Expr::Literal(v) => literal_type(v),
        Expr::Binary { op, lhs, rhs } => binary_type(
            *op,
            infer_row_type(q, key_tys, lhs),
            infer_row_type(q, key_tys, rhs),
        ),
        Expr::ExtractYear(_) | Expr::CastInt(_) => LogicalType::Int64,
        Expr::Substr { .. } | Expr::Concat(_) => LogicalType::Utf8,
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
                "sum" | "count" | "min" | "max" | "avg" | "stddev_samp"
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
                "sum" | "count" | "min" | "max" | "avg" | "stddev_samp"
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
        Expr::ExtractYear(i) | Expr::CastInt(i) => references_columns(i),
        Expr::Like { expr, .. } => references_columns(expr),
        Expr::ScalarSub(_) => false,
        Expr::InSub { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::InSetStr { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Substr { expr, .. } => references_columns(expr),
        Expr::Concat(parts) => parts.iter().any(references_columns),
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
