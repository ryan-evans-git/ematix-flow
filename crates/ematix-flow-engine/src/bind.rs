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

use std::collections::BTreeSet;

use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::{Catalog, TableDef};
use crate::expr::{BinaryOp, Expr, ScalarValue};
use crate::logical::{
    AggExpr, AggFunc, BoundQuery, GroupExpr, JoinEdge, OrderByKey, OutputExpr, ScanColumn, Slot,
    TableInput,
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
    bind_query(query, catalog, false)
}

/// Bind one query level (the top level, or a subquery). `set_semantics`
/// marks an IN-subquery: an aggregate-less, group-less inner SELECT is then
/// rewritten as GROUP BY its select items — membership only cares about the
/// value SET, so the dedup is semantics-preserving (and gives the executor
/// its grouped path).
fn bind_query(
    query: &ast::Query,
    catalog: &Catalog,
    set_semantics: bool,
) -> Result<BoundQuery, String> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("only plain SELECT is supported (no set operations yet)".into());
    };

    // FROM: comma-separated plain tables (the TPC-H canonical form), with
    // optional aliases for self-joins (`nation n1, nation n2`).
    if select.from.is_empty() {
        return Err("a FROM clause is required".into());
    }
    let mut b = Binder {
        catalog,
        tables: Vec::new(),
        slots: Vec::new(),
        touched: BTreeSet::new(),
        subs: Vec::new(),
    };
    for twj in &select.from {
        if !twj.joins.is_empty() {
            return Err("JOIN … ON syntax is not yet supported (use comma joins + WHERE)".into());
        }
        let ast::TableFactor::Table { name, alias, .. } = &twj.relation else {
            return Err("only plain table names are supported in FROM".into());
        };
        let tname = name.to_string();
        let def = catalog
            .table(&tname)
            .ok_or_else(|| format!("unknown table '{tname}'"))?;
        let display = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tname.clone());
        if b.tables.iter().any(|t| t.display == display) {
            return Err(format!(
                "duplicate table name/alias '{display}' — alias one of them"
            ));
        }
        b.tables.push(BoundTable {
            display,
            def,
            used: Vec::new(),
        });
    }

    // GROUP BY first (slot space) — SELECT items match against these. Keys
    // may be integer, float, or string valued (the executor's typed group
    // keys); booleans group as 0/1.
    let group: Vec<GroupExpr> = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            let mut out = Vec::new();
            for e in exprs {
                let bound = b.bind_scalar(e)?;
                out.push(GroupExpr { expr: bound });
            }
            out
        }
        ast::GroupByExpr::Expressions(..) => {
            return Err("GROUP BY modifiers (ROLLUP/CUBE/…) are not yet supported".into());
        }
        other => return Err(format!("unsupported GROUP BY form: {other:?}")),
    };

    // An aggregate-less, group-less IN-subquery SELECT becomes GROUP BY its
    // items (set semantics — see fn docs).
    let group: Vec<GroupExpr> = if group.is_empty()
        && set_semantics
        && select.projection.iter().all(|it| match it {
            ast::SelectItem::UnnamedExpr(e) => !contains_function(e),
            ast::SelectItem::ExprWithAlias { expr, .. } => !contains_function(expr),
            _ => false,
        }) {
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

    // SELECT: each item becomes a row-space output projection; aggregate
    // calls inside it are extracted into `aggs`.
    let mut aggs: Vec<AggExpr> = Vec::new();
    let mut output: Vec<OutputExpr> = Vec::new();
    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(e) => (e, None),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            other => return Err(format!("unsupported select item: {other}")),
        };
        let row_expr = b.bind_output(expr, &group, &mut aggs)?;
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
    if aggs.is_empty() && group.is_empty() {
        return Err(
            "SELECT list must contain an aggregate or a GROUP BY (plain row queries are not \
             yet supported)"
                .into(),
        );
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
    if let Some(where_expr) = &select.selection {
        let mut raw = Vec::new();
        split_and(where_expr, &mut raw);
        let mut conjuncts: Vec<ast::Expr> = Vec::new();
        for conj in raw {
            factor_or(conj, &mut conjuncts);
        }
        for conj in &conjuncts {
            if let ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } = conj
            {
                if let (Some(lp), Some(rp)) = (ident_parts(left), ident_parts(right)) {
                    let a = b.resolve_parts(&lp)?;
                    let bb = b.resolve_parts(&rp)?;
                    let (ta, tb) = (b.slots[a].table, b.slots[bb].table);
                    if ta != tb {
                        let (ka, kb) = (b.slot_col(a), b.slot_col(bb));
                        if !is_integer_family(ka.ty) || !is_integer_family(kb.ty) {
                            return Err(format!(
                                "join keys '{}' = '{}' must be integer-typed",
                                ka.name, kb.name
                            ));
                        }
                        edges.push(JoinEdge { a, b: bb });
                        continue;
                    }
                    // Same table ⇒ an ordinary filter; falls through.
                }
            }
            let (e, t) = b.bind_multi(conj)?;
            match t {
                Attribution::None => {
                    return Err(format!(
                        "constant WHERE predicate '{conj}' is not supported"
                    ));
                }
                Attribution::Single(t) => filters[t].push(e),
                Attribution::Multi => post.push(e),
            }
        }
    }
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

    // ORDER BY: each key must name an output column (alias or column name).
    let mut order_by = Vec::new();
    if let Some(ob) = &query.order_by {
        let ast::OrderByKind::Expressions(exprs) = &ob.kind else {
            return Err("unsupported ORDER BY form".into());
        };
        for oe in exprs {
            let Some(parts) = ident_parts(&oe.expr) else {
                return Err(format!(
                    "ORDER BY must name an output column (so far), got '{}'",
                    oe.expr
                ));
            };
            let name = parts.last().expect("nonempty ident");
            let idx = output
                .iter()
                .position(|o| o.name == *name)
                .ok_or_else(|| format!("ORDER BY '{name}' does not match an output column"))?;
            order_by.push(OrderByKey {
                output: idx,
                desc: oe.options.asc == Some(false),
            });
        }
    }

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
        .map(|(bt, fs)| TableInput {
            name: bt.display,
            path: bt.def.path.clone(),
            projection: bt.used,
            filter: fs.into_iter().reduce(and),
        })
        .collect();

    Ok(BoundQuery {
        tables,
        edges,
        slots: b.slots,
        post_filter,
        group,
        aggs,
        having,
        output,
        order_by,
        limit,
        subqueries: b.subs,
    })
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

/// One table in scope: its display name (alias-aware), definition, and the
/// columns referenced so far (its scan projection, in first-use order).
struct BoundTable<'a> {
    display: String,
    def: &'a TableDef,
    used: Vec<ScanColumn>,
}

/// Per-query binding state.
struct Binder<'a> {
    catalog: &'a Catalog,
    tables: Vec<BoundTable<'a>>,
    /// The global slot space: slot `s` = `(table, col-in-projection)`.
    slots: Vec<Slot>,
    /// Tables touched by the expression currently being bound (single-table
    /// attribution for filters).
    touched: BTreeSet<usize>,
    /// Subqueries bound so far (referenced by `Expr::ScalarSub` / `InSub`).
    subs: Vec<BoundQuery>,
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
    fn slot_col(&self, s: usize) -> &ScanColumn {
        let Slot { table, col } = self.slots[s];
        &self.tables[table].used[col]
    }

    /// Resolve a (possibly qualified) column name to its global slot,
    /// extending the owning table's scan projection on first use.
    fn resolve_parts(&mut self, parts: &[&str]) -> Result<usize, String> {
        let (t, cname) = match parts {
            [c] => {
                let mut hit = None;
                for (t, bt) in self.tables.iter().enumerate() {
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
                (
                    hit.ok_or_else(|| format!("unknown column '{c}'"))? as usize,
                    *c,
                )
            }
            [tbl, c] => {
                let t = self
                    .tables
                    .iter()
                    .position(|bt| bt.display == *tbl)
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
        let col = match bt.used.iter().position(|c| c.name == cname) {
            Some(i) => i,
            None => {
                let def = bt.def.column(cname).expect("column just checked");
                bt.used.push(ScanColumn {
                    name: def.name.clone(),
                    leaf: def.leaf,
                    ty: def.ty,
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
        if !contains_function(e) {
            let bound = materialize(self.bind(e)?);
            if let Some(g) = group.iter().position(|ge| ge.expr == bound) {
                return Ok(Expr::Column(g));
            }
            if !references_columns(&bound) {
                return Ok(bound);
            }
            return Err(format!("'{e}' is neither an aggregate nor a GROUP BY key"));
        }
        match e {
            ast::Expr::Function(_) => {
                let agg = self.bind_aggregate(e)?;
                aggs.push(agg);
                Ok(Expr::Column(group.len() + aggs.len() - 1))
            }
            ast::Expr::Nested(inner) => self.bind_output(inner, group, aggs),
            ast::Expr::Subquery(_) => Ok(materialize(self.bind(e)?)),
            ast::Expr::BinaryOp { left, op, right } => {
                let op = bind_op(op)?;
                let l = self.bind_output(left, group, aggs)?;
                let r = self.bind_output(right, group, aggs)?;
                Ok(binary(op, l, r))
            }
            other => Err(format!("unsupported expression over aggregates: {other}")),
        }
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
        Ok(AggExpr { func, arg })
    }

    /// Bind an AST expression bottom-up (slot space), folding literal
    /// arithmetic in decimal.
    fn bind(&mut self, e: &ast::Expr) -> Result<Bound, String> {
        match e {
            ast::Expr::Identifier(id) => {
                let s = self.resolve_parts(&[&id.value])?;
                Ok(Bound::Expr(Expr::Column(s)))
            }
            ast::Expr::CompoundIdentifier(ids) => {
                let parts: Vec<&str> = ids.iter().map(|i| i.value.as_str()).collect();
                let s = self.resolve_parts(&parts)?;
                Ok(Bound::Expr(Expr::Column(s)))
            }
            ast::Expr::Nested(inner) => self.bind(inner),
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(s, _) => Ok(Bound::Dec(Dec::parse(s)?)),
                ast::Value::SingleQuotedString(s) => Ok(Bound::Expr(Expr::Literal(
                    ScalarValue::Utf8(s.as_str().into()),
                ))),
                other => Err(format!("unsupported literal: {other}")),
            },
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
                if operand.is_some() {
                    return Err("CASE <operand> WHEN … is not yet supported".into());
                }
                let else_ = else_result
                    .as_ref()
                    .ok_or("CASE requires an ELSE branch (no NULLs yet)")?;
                let whens = conditions
                    .iter()
                    .map(|cw| {
                        Ok((
                            materialize(self.bind(&cw.condition)?),
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
                let bound = materialize(self.bind(expr)?);
                let lo = materialize(self.bind(low)?);
                let hi = materialize(self.bind(high)?);
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
                    .map(|item| Ok(binary(cmp, bound.clone(), materialize(self.bind(item)?))))
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
                    let Expr::Literal(ScalarValue::Date32(d)) = base else {
                        return Err("intervals apply to date literals only (so far)".into());
                    };
                    let signed = match op {
                        ast::BinaryOperator::Plus => 1,
                        ast::BinaryOperator::Minus => -1,
                        other => {
                            return Err(format!("unsupported interval operator: {other}"));
                        }
                    };
                    return Ok(Bound::Expr(Expr::Literal(ScalarValue::Date32(shift_date(
                        d, iv, signed,
                    )?))));
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
                Ok(Bound::Expr(binary(op, materialize(l), materialize(r))))
            }
            ast::Expr::Subquery(sq) => {
                let bq = bind_query(sq, self.catalog, false)?;
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
                let bq = bind_query(subquery, self.catalog, true)?;
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
            ast::Expr::Function(_) => {
                Err("aggregate calls are only allowed in the SELECT list (so far)".into())
            }
            other => Err(format!("unsupported expression: {other}")),
        }
    }
}

/// Flatten an `AND` tree into its conjuncts (leaves kept in source order).
fn split_and<'e>(e: &'e ast::Expr, out: &mut Vec<&'e ast::Expr>) {
    if let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOperator::And,
        right,
    } = e
    {
        split_and(left, out);
        split_and(right, out);
    } else {
        out.push(e);
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
        ast::Expr::Subquery(_) | ast::Expr::InSubquery { .. } => false,
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
        Expr::ExtractYear(i) => references_columns(i),
        Expr::Like { expr, .. } => references_columns(expr),
        Expr::ScalarSub(_) => false,
        Expr::InSub { expr, .. } | Expr::InSet { expr, .. } => references_columns(expr),
        Expr::Case { whens, else_ } => {
            whens
                .iter()
                .any(|(c, v)| references_columns(c) || references_columns(v))
                || references_columns(else_)
        }
    }
}

/// Shift a Date32 day count by an interval (`'90' day`, `'3' month`,
/// `'1' year`), folding at bind time.
fn shift_date(days: i32, iv: &ast::Interval, sign: i32) -> Result<i32, String> {
    let ast::Expr::Value(v) = iv.value.as_ref() else {
        return Err(format!("unsupported interval value: {}", iv.value));
    };
    let n: i32 = match &v.value {
        ast::Value::SingleQuotedString(s) => s.trim().parse(),
        ast::Value::Number(s, _) => s.parse(),
        other => return Err(format!("unsupported interval value: {other}")),
    }
    .map_err(|_| format!("bad interval count in {iv}"))?;
    let n = n * sign;
    match iv.leading_field {
        Some(ast::DateTimeField::Day) => Ok(days + n),
        Some(ast::DateTimeField::Month) => Ok(shift_months(days, n)),
        Some(ast::DateTimeField::Year) => Ok(shift_months(days, n * 12)),
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
        ScalarValue::Float64(self.mant as f64 / 10f64.powi(self.scale as i32))
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
