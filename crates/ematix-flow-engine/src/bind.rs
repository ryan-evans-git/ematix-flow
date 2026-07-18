//! P3 binder: SQL text → bound, typed [`LogicalPlan`].
//!
//! `sqlparser` supplies tokenize→AST only (the sanctioned bootstrap — a
//! standalone lib, not DataFusion); everything from the AST inward is owned
//! engine code. The binder resolves names against the [`Catalog`] into chunk
//! positions, desugars `BETWEEN`, resolves `date '…'` literals to `Date32`
//! days, and — the first real correctness obligation — **constant-folds
//! literal arithmetic in decimal**, casting to the target type only at the
//! leaf. Folding `0.06 + 0.01` in f64 yields `0.069999999999999996`, one ULP
//! below the stored `0.07`, silently dropping the whole 0.07 bucket (~1/3 of
//! Q6's matches) — the `lib.rs:62` lesson, now owned by the binder.
//!
//! Multi-table (4b): FROM may list tables comma-style (the TPC-H canonical
//! form); WHERE splits into conjuncts — an equality between columns of two
//! different tables is a **join condition**, anything else must attribute to
//! exactly one table and becomes that table's filter. SELECT / GROUP BY
//! expressions must all attribute to one table (the fact side); the other
//! table joins in key-only (its columns appear only in its own filters).
//!
//! Scope grows slice by slice; each unsupported construct errors by name
//! rather than mis-binding. Not yet: `JOIN … ON` syntax, >2 tables,
//! dim-payload columns in SELECT, ORDER BY / LIMIT, scalar functions.

use std::collections::BTreeSet;

use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::{Catalog, TableDef};
use crate::expr::{BinaryOp, Expr, ScalarValue};
use crate::logical::{AggExpr, AggFunc, GroupExpr, LogicalPlan, ScanColumn};
use crate::vector::LogicalType;

/// Parse `sql` and bind it against `catalog` into a typed plan.
pub fn bind_sql(sql: &str, catalog: &Catalog) -> Result<LogicalPlan, String> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| format!("parse: {e}"))?;
    let [stmt] = stmts.as_slice() else {
        return Err(format!("expected one statement, got {}", stmts.len()));
    };
    let ast::Statement::Query(query) = stmt else {
        return Err("only SELECT queries are supported".into());
    };
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("only plain SELECT is supported (no set operations yet)".into());
    };

    // FROM: comma-separated plain tables (the TPC-H canonical form).
    if select.from.is_empty() {
        return Err("a FROM clause is required".into());
    }
    let mut b = Binder {
        tables: Vec::new(),
        touched: BTreeSet::new(),
    };
    for twj in &select.from {
        if !twj.joins.is_empty() {
            return Err("JOIN … ON syntax is not yet supported (use comma joins + WHERE)".into());
        }
        let ast::TableFactor::Table { name, .. } = &twj.relation else {
            return Err("only plain table names are supported in FROM".into());
        };
        let tname = name.to_string();
        let def = catalog
            .table(&tname)
            .ok_or_else(|| format!("unknown table '{tname}'"))?;
        b.tables.push(BoundTable {
            name: tname,
            def,
            used: Vec::new(),
        });
    }

    // SELECT items first (so their arguments claim the first chunk
    // positions). Non-aggregate items are group keys and must precede the
    // aggregates. Everything here must attribute to ONE table — the fact
    // side.
    let mut fact: Option<usize> = None;
    let note_fact = |fact: &mut Option<usize>, t: Option<usize>| -> Result<(), String> {
        match (t, *fact) {
            (None, _) => Ok(()),
            (Some(t), None) => {
                *fact = Some(t);
                Ok(())
            }
            (Some(t), Some(f)) if t == f => Ok(()),
            (Some(_), Some(_)) => {
                Err("SELECT / GROUP BY must reference exactly one table (so far)".into())
            }
        }
    };

    let mut keys: Vec<GroupExpr> = Vec::new();
    let mut aggs: Vec<AggExpr> = Vec::new();
    for item in &select.projection {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(e) => (e, None),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            other => return Err(format!("unsupported select item: {other}")),
        };
        if matches!(expr, ast::Expr::Function(_)) {
            let (agg, t) = b.bind_aggregate(expr, alias)?;
            note_fact(&mut fact, t)?;
            aggs.push(agg);
        } else {
            if !aggs.is_empty() {
                return Err("group keys must precede aggregates in SELECT (so far)".into());
            }
            let (bound, t) = b.bind_single(expr)?;
            note_fact(&mut fact, t)?;
            let name = alias.unwrap_or_else(|| match (&bound, t) {
                (Expr::Column(i), Some(t)) => b.tables[t].used[*i].name.clone(),
                _ => format!("key{}", keys.len()),
            });
            keys.push(GroupExpr { expr: bound, name });
        }
    }
    if aggs.is_empty() {
        return Err("SELECT list must contain at least one aggregate (so far)".into());
    }

    // GROUP BY: the non-aggregate select items must BE the group keys, in
    // order.
    let group_exprs = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            let mut out = Vec::new();
            for e in exprs {
                let (bound, t) = b.bind_single(e)?;
                note_fact(&mut fact, t)?;
                out.push(bound);
            }
            out
        }
        ast::GroupByExpr::Expressions(..) => {
            return Err("GROUP BY modifiers (ROLLUP/CUBE/…) are not yet supported".into());
        }
        other => return Err(format!("unsupported GROUP BY form: {other:?}")),
    };
    if keys.len() != group_exprs.len() || keys.iter().zip(&group_exprs).any(|(k, g)| k.expr != *g) {
        let named = keys
            .iter()
            .map(|k| k.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "non-aggregate select items [{named}] must match GROUP BY exprs in order"
        ));
    }
    let fact = fact.ok_or("SELECT must reference a table column")?;
    // Group keys route through the i64 hash-agg path: integer-family
    // columns only (so far), checked here so execution can't mis-key.
    for k in &keys {
        let Expr::Column(i) = k.expr else {
            return Err(format!(
                "group key '{}' must be a plain column (so far)",
                k.name
            ));
        };
        if !is_integer_family(b.tables[fact].used[i].ty) {
            return Err(format!(
                "group key '{}' must be integer-typed (so far), is {:?}",
                k.name, b.tables[fact].used[i].ty
            ));
        }
    }
    let group = keys;

    // WHERE: split the conjunct tree. A `colA = colB` across two tables is
    // a join condition; every other conjunct must attribute to one table
    // and becomes that table's filter.
    let mut filters: Vec<Vec<Expr>> = vec![Vec::new(); b.tables.len()];
    let mut join_conds: Vec<(usize, usize, usize, usize)> = Vec::new(); // (tl, cl, tr, cr)
    if let Some(where_expr) = &select.selection {
        let mut conjuncts = Vec::new();
        split_and(where_expr, &mut conjuncts);
        for conj in conjuncts {
            if let ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::Eq,
                right,
            } = conj
            {
                if let (Some(ln), Some(rn)) = (as_ident(left), as_ident(right)) {
                    let (tl, cl) = b.resolve(ln)?;
                    let (tr, cr) = b.resolve(rn)?;
                    if tl != tr {
                        let (kl, kr) = (&b.tables[tl].used[cl], &b.tables[tr].used[cr]);
                        if !is_integer_family(kl.ty) || !is_integer_family(kr.ty) {
                            return Err(format!(
                                "join keys '{}' = '{}' must be integer-typed",
                                kl.name, kr.name
                            ));
                        }
                        join_conds.push((tl, cl, tr, cr));
                        continue;
                    }
                    // Same table ⇒ an ordinary filter; falls through.
                }
            }
            let (e, t) = b.bind_single(conj)?;
            let t = t.ok_or("constant WHERE predicates are not supported")?;
            filters[t].push(e);
        }
    }

    // Assemble the input tree.
    let input = match b.tables.len() {
        1 => {
            if !join_conds.is_empty() {
                unreachable!("single-table query cannot have cross-table conditions");
            }
            table_plan(&b.tables[0], std::mem::take(&mut filters[0]))
        }
        2 => {
            let dim = 1 - fact;
            let [(tl, cl, _tr, cr)] = join_conds.as_slice() else {
                return Err(format!(
                    "expected exactly one join condition between the two tables, got {}",
                    join_conds.len()
                ));
            };
            let (left_key, right_key) = if *tl == fact { (*cl, *cr) } else { (*cr, *cl) };
            // The dim side must contribute only its key — its other columns
            // may appear only in its own filters (already guaranteed: any
            // SELECT/GROUP reference to it would have failed fact
            // attribution above).
            let left = table_plan(&b.tables[fact], std::mem::take(&mut filters[fact]));
            let right = table_plan(&b.tables[dim], std::mem::take(&mut filters[dim]));
            LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                left_key,
                right_key,
            }
        }
        n => return Err(format!("{n} tables are not yet supported (P3 4c)")),
    };

    Ok(LogicalPlan::Aggregate {
        input: Box::new(input),
        group,
        aggs,
    })
}

/// One table in the query's scope, accumulating its referenced columns (the
/// scan projection, in first-use order — that table's `Expr::Column(i)`
/// space).
struct BoundTable<'a> {
    name: String,
    def: &'a TableDef,
    used: Vec<ScanColumn>,
}

/// Per-query binding state.
struct Binder<'a> {
    tables: Vec<BoundTable<'a>>,
    /// Tables touched by the expression currently being bound — how a bound
    /// expression is attributed to (exactly) one table.
    touched: BTreeSet<usize>,
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
    /// Resolve a column name to `(table, chunk position)`, extending that
    /// table's scan projection on first use. Ambiguous names error.
    fn resolve(&mut self, name: &str) -> Result<(usize, usize), String> {
        let mut hit: Option<usize> = None;
        for (t, bt) in self.tables.iter().enumerate() {
            if bt.def.column(name).is_some() {
                if let Some(prev) = hit {
                    return Err(format!(
                        "column '{name}' is ambiguous (in '{}' and '{}')",
                        self.tables[prev].name, bt.name
                    ));
                }
                hit = Some(t);
            }
        }
        let t = hit.ok_or_else(|| format!("unknown column '{name}'"))?;
        self.touched.insert(t);
        let bt = &mut self.tables[t];
        if let Some(i) = bt.used.iter().position(|c| c.name == name) {
            return Ok((t, i));
        }
        let col = bt.def.column(name).expect("column just found");
        bt.used.push(ScanColumn {
            name: col.name.clone(),
            leaf: col.leaf,
            ty: col.ty,
        });
        Ok((t, bt.used.len() - 1))
    }

    /// Bind a scalar expression that must attribute to at most one table.
    /// Returns the bound expression and the table it references (`None` for
    /// pure literals).
    fn bind_single(&mut self, e: &ast::Expr) -> Result<(Expr, Option<usize>), String> {
        self.touched.clear();
        let bound = materialize(self.bind(e)?);
        match self.touched.len() {
            0 => Ok((bound, None)),
            1 => Ok((bound, self.touched.first().copied())),
            _ => Err(
                "expression must reference exactly one table (cross-table expressions are \
                 only supported as equi-join conditions)"
                    .into(),
            ),
        }
    }

    /// Bind one SELECT item as an aggregate call (`sum(<expr>)`).
    fn bind_aggregate(
        &mut self,
        e: &ast::Expr,
        alias: Option<String>,
    ) -> Result<(AggExpr, Option<usize>), String> {
        let ast::Expr::Function(f) = e else {
            return Err(format!(
                "select item must be an aggregate call (so far), got: {e}"
            ));
        };
        let fname = f.name.to_string().to_lowercase();
        let func = match fname.as_str() {
            "sum" => AggFunc::Sum,
            other => return Err(format!("unsupported aggregate function '{other}'")),
        };
        let ast::FunctionArguments::List(args) = &f.args else {
            return Err(format!("aggregate '{fname}' needs an argument list"));
        };
        let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))] = args.args.as_slice()
        else {
            return Err(format!("aggregate '{fname}' takes exactly one argument"));
        };
        let (bound, t) = self.bind_single(arg)?;
        Ok((
            AggExpr {
                func,
                arg: bound,
                alias,
            },
            t,
        ))
    }

    /// Bind an AST expression bottom-up, folding literal arithmetic in
    /// decimal.
    fn bind(&mut self, e: &ast::Expr) -> Result<Bound, String> {
        match e {
            ast::Expr::Identifier(id) => {
                let (_, i) = self.resolve(&id.value)?;
                Ok(Bound::Expr(Expr::Column(i)))
            }
            ast::Expr::Nested(inner) => self.bind(inner),
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(s, _) => Ok(Bound::Dec(Dec::parse(s)?)),
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
            ast::Expr::BinaryOp { left, op, right } => {
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
            ast::Expr::Function(_) => {
                Err("aggregate calls are only allowed in the SELECT list (so far)".into())
            }
            other => Err(format!("unsupported expression: {other}")),
        }
    }
}

/// Build a table's input plan: its scan plus its own filters (AND-folded).
fn table_plan(bt: &BoundTable, filters: Vec<Expr>) -> LogicalPlan {
    let mut plan = LogicalPlan::Scan {
        table: bt.name.clone(),
        path: bt.def.path.clone(),
        projection: bt.used.clone(),
    };
    if let Some(predicate) = filters.into_iter().reduce(and) {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    plan
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

/// A plain (possibly parenthesized) identifier, if that's all `e` is.
fn as_ident(e: &ast::Expr) -> Option<&str> {
    match e {
        ast::Expr::Identifier(id) => Some(&id.value),
        ast::Expr::Nested(inner) => as_ident(inner),
        _ => None,
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

fn bind_op(op: &ast::BinaryOperator) -> Result<BinaryOp, String> {
    use ast::BinaryOperator as A;
    Ok(match op {
        A::Plus => BinaryOp::Add,
        A::Minus => BinaryOp::Sub,
        A::Multiply => BinaryOp::Mul,
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
