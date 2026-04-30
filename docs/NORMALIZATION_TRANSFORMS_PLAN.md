# ematix-flow — Normalization + Transformations Plan

A planning doc parallel to `docs/ERGONOMICS_PLAN.md`. Tackles two
distinct user-facing needs the v0.1 core doesn't address:

1. **Normalization** — lightweight cleanup of source values before they
   hit the target. Empty strings → NULL, timestamp parsing, trim,
   dedupe, etc. This is per-column or per-pipeline, runs as part of
   the source SELECT, and compiles to plain SQL so Postgres does the
   work (no row-by-row Python).

2. **Transformations** — separate steps that produce derived data
   *after* the main load completes. Refresh a materialized view,
   recompute a roll-up table, populate a derived dimension. This is
   pipeline-level, runs after success, and is composable as standalone
   `@ematix.transform`-decorated callables.

The key principle: keep both expressed declaratively in Python, compile
to SQL, run in-database. No row-by-row Python over user data.

---

## 1. Goals

- **Easy to use.** A user wanting "trim whitespace and treat empty as
  NULL" should write one line, not ten. Common cases get one-liners;
  uncommon cases drop to raw SQL.
- **Fast.** All normalizers compile to SQL expressions Postgres
  evaluates inline. No materialization through Python.
- **Composable.** Per-column markers + per-pipeline transforms layer
  cleanly. Order is deterministic and inspectable via `preview()`.
- **Scoped.** Normalization is "before write," transformations are
  "after write." Distinct decorators / kwargs so the user knows which
  bucket they're in.

Specifically asked-for cases (all addressed):
- Empty strings → NULL
- Timestamp / date format standardization
- Pre-write deduplication (preventing PK / UNIQUE violations)
- String replacement
- Concatenation (lands as derived columns; see §3.6)

Plus the high-value additions a real pipeline needs:
- Trim, lower, upper, regex_replace, truncate
- NULL-default handling (`COALESCE` sugar)
- Email/phone canonicalization
- Boolean coercion (`'yes'/'no'/'1'/'0'` → bool)
- Numeric parsing (`'$1,234.56'` → numeric)
- Filter / limit / sample at the pipeline level
- Post-load: SQL strings, decorated callables, or another registered pipeline

---

## 2. Two concepts, one diagram

```
                                 ┌─────────────────────┐
   User source query   ─────►   │  Normalization      │   ─────►   target table
   (str / Source /                 (compiled to SQL)            
    source_table)                  • per-column markers
                                   • pipeline-level transforms_pre
                                 └─────────────────────┘
                                          │
                                          ▼ load succeeds
                                 ┌─────────────────────┐
                                 │  Transformations    │
                                 │  (post-load)        │
                                 │  • SQL strings      │
                                 │  • @ematix.transform│
                                 │  • run sequentially │
                                 └─────────────────────┘
```

Two clear edges:
- **Normalization runs inside the load transaction.** A normalizer
  failing fails the load. The normalized rows are what the target
  sees — there is no "raw" copy.
- **Transformations run after commit, not in the load transaction.**
  A transformation failing does *not* roll back the load. Each
  transformation gets its own transaction. Sequential, halt-on-first
  by default with a `continue_on_failure` opt-out (matching multi-target
  semantics from Phase 24b).

---

## 3. Normalization API

### 3.1 Per-column normalizers via `Annotated[T, ...]`

Normalizers are markers in PEP 593 `Annotated[...]`, just like `pk()` /
`natural_key()` / `nullable()`. They live alongside the column they
operate on.

```python
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.normalize import (
    trim, lower, empty_to_null, parse_timestamp, default
)
from ematix_flow.types import BigInt, String, Text, TimestampTZ


@ematix.table(schema="warehouse")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256], lower(), trim(), empty_to_null()]
    name: Annotated[Text | None, trim(), empty_to_null()]
    signup_at: Annotated[TimestampTZ, parse_timestamp(format="YYYY-MM-DD")]
    region: Annotated[String[8], default("US")]
```

When the framework synthesizes the source SELECT (whether from
`source_table=` or wrapping a function-body return value), each
column's normalizers compile to SQL applied left-to-right:

```sql
SELECT
    customer_id,
    NULLIF(trim(lower(email)), '')                       AS email,
    NULLIF(trim(name), '')                               AS name,
    to_timestamp(signup_at, 'YYYY-MM-DD')                AS signup_at,
    COALESCE(region, 'US')                               AS region
FROM (<original source>) src
```

**Order matters.** `[lower(), trim()]` produces `trim(lower(col))`
(read right-to-left as function composition). `[trim(), lower()]`
would produce `lower(trim(col))` — same result for ASCII whitespace,
but the contract is "left = applied first".

### 3.2 Pipeline-level pre-transforms via `transforms_pre=`

Cross-column or cross-row work — deduplication, filters, sampling —
declared on the pipeline:

```python
from ematix_flow.normalize import deduplicate_by, filter_where


@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="merge",
    transforms_pre=[
        deduplicate_by("customer_id", order_by="updated_at DESC"),
        filter_where("region IS NOT NULL"),
    ],
)
def sync_customers(conn):
    return "SELECT customer_id, email, name, region, signup_at FROM source.users"
```

These wrap the (already column-normalized) source query in order. The
final SQL the strategy sees:

```sql
WITH _src_user AS (<original source>),
     _src_normalized AS (
        SELECT customer_id,
               NULLIF(trim(lower(email)), '') AS email, ...
        FROM _src_user
     ),
     _src_dedup AS (
        SELECT DISTINCT ON (customer_id) *
        FROM _src_normalized
        ORDER BY customer_id, updated_at DESC
     ),
     _src_filtered AS (
        SELECT * FROM _src_dedup WHERE region IS NOT NULL
     )
SELECT * FROM _src_filtered
```

CTE-stacked rather than nested-subquery so Postgres `EXPLAIN` is
readable and `preview()` can render each layer separately.

### 3.3 Concrete normalizer catalogue

#### String

| Marker | SQL | Notes |
|---|---|---|
| `trim()` | `trim(col)` | leading + trailing whitespace |
| `lower()` | `lower(col)` | |
| `upper()` | `upper(col)` | |
| `empty_to_null()` | `NULLIF(col, '')` | |
| `whitespace_to_null()` | `NULLIF(trim(col), '')` | combined common case |
| `replace(old, new)` | `replace(col, old, new)` | |
| `regex_replace(pattern, replacement, flags='g')` | `regexp_replace(col, pattern, replacement, flags)` | |
| `truncate(n)` | `left(col, n)` | |

#### Casing / canonicalization

| Marker | Composed SQL |
|---|---|
| `email_normalize()` | `lower(trim(col))` (and validates not empty after) |
| `phone_normalize()` | `regexp_replace(col, '[^0-9+]', '', 'g')` |

#### Date / time

| Marker | SQL | Notes |
|---|---|---|
| `parse_timestamp(format)` | `to_timestamp(col, format)` | `format` uses Postgres patterns (`YYYY-MM-DD HH24:MI:SS`) |
| `parse_date(format)` | `to_date(col, format)` | |
| `to_timezone(tz)` | `col AT TIME ZONE tz` | |
| `date_trunc(precision)` | `date_trunc(precision, col)` | `precision = 'day' / 'hour' / ...` |

#### Numeric

| Marker | SQL |
|---|---|
| `parse_int()` | `col::bigint` |
| `parse_numeric(precision, scale)` | `col::numeric(precision, scale)` |
| `round(precision)` | `round(col, precision)` |
| `clamp(min, max)` | `least(greatest(col, min), max)` |

#### Boolean

| Marker | SQL |
|---|---|
| `parse_bool(truthy=('true','1','yes','y'), falsy=('false','0','no','n'))` | `CASE WHEN lower(col) IN (...) THEN true WHEN lower(col) IN (...) THEN false END` |

#### NULL handling

| Marker | SQL |
|---|---|
| `default(value)` | `COALESCE(col, value)` |
| `nullif(value)` | `NULLIF(col, value)` |
| `not_null_or(value)` | `COALESCE(col, value)` (alias for clarity) |

#### Pipeline-level (transforms_pre)

| Helper | Composed SQL |
|---|---|
| `deduplicate_by(*keys, order_by=...)` | `SELECT DISTINCT ON (keys) * ... ORDER BY keys, order_by` |
| `filter_where(expr)` | `WHERE expr` |
| `limit(n)` | `LIMIT n` |
| `sample_pct(p)` | `WHERE random() < p` (or `TABLESAMPLE BERNOULLI(p*100)` if `from_table=True`) |

### 3.4 Custom / escape-hatch normalizer

```python
from ematix_flow.normalize import sql

email: Annotated[
    String[256],
    sql("CASE WHEN col LIKE '%@example.com' THEN replace(col, '@', '+spam@') ELSE col END"),
]
```

`sql(expression)` takes any Postgres expression with `col` as the
placeholder for the column name. Compiles to that expression with
`col` substituted. The user gets full SQL power for the cases the
named normalizers don't cover.

### 3.5 Inspection via `preview()`

Phase 25's `preview()` renders the full normalized + transformed SQL
plan, showing each CTE layer with the normalizer / transform that
produced it. Users debug "why did this column get blanked" by reading
the preview output instead of guessing.

### 3.6 Concatenation / derived columns (deferred to Phase 27)

```python
full_name: Annotated[Text, derive("first_name || ' ' || last_name")]
```

`derive()` is conceptually different from a normalizer — it doesn't
modify an existing source column, it produces a new one. We'll ship
it in Phase 27 alongside transformations because the surface area is
similar (any SQL expression). For Phase 26, users with derived-column
needs put the expression in the source SELECT directly.

---

## 4. Transformations API (post-load)

### 4.1 Inline SQL strings via `transforms_post=`

Simplest case — refresh a materialized view, run an `ANALYZE`:

```python
@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="merge",
    transforms_post=[
        "REFRESH MATERIALIZED VIEW CONCURRENTLY warehouse.customer_segments",
        "ANALYZE warehouse.customer_dim",
    ],
)
def sync_customers(conn):
    return "SELECT ..."
```

Each entry runs in its own transaction on the pipeline's
`target_connection`. Sequential. Halt on first failure by default;
`continue_on_failure_post=True` opts in to running every step.

### 4.2 Reusable transformations via `@ematix.transform`

When a transformation is reused across pipelines or wants its own
schedule:

```python
from ematix_flow import ematix


@ematix.transform(
    target_connection="warehouse",
    name="recompute_segments",
)
def recompute_segments(conn):
    conn.execute("""
        INSERT INTO warehouse.customer_segments
        SELECT customer_id, ...
        FROM warehouse.customer_dim
        WHERE _loaded_at > now() - interval '1 day'
        ON CONFLICT (customer_id) DO UPDATE SET ...
    """)


@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="merge",
    transforms_post=[recompute_segments],   # callable, not string
)
def sync_customers(conn):
    return "SELECT ..."
```

`@ematix.transform`-decorated functions:
- Get registered in a parallel `pipeline._TRANSFORMS_REGISTRY`.
- Are runnable standalone via `flow transform run <name>` and
  `flow transform list`.
- Can themselves declare a `schedule=` to run on their own cron
  independent of any pipeline.

### 4.3 Transformation as a chained pipeline

When the post-load step is heavy enough to want its own SCD2 / merge
semantics, just write another `@ematix.pipeline` and reference it from
the upstream pipeline's `transforms_post`. The framework looks up the
registered pipeline by name and runs its full sync:

```python
@ematix.pipeline(
    target=CustomerDim,
    transforms_post=["recompute_ltv_pipeline"],   # name of another pipeline
    schedule="0 * * * *",
    mode="merge",
)
def sync_customers(conn): ...


@ematix.pipeline(
    target=CustomerLifetimeValue,
    schedule=None,                                # triggered, not scheduled
    mode="merge",
    name="recompute_ltv_pipeline",
)
def recompute_ltv(conn):
    return "SELECT customer_id, sum(amount) AS ltv FROM orders GROUP BY 1"
```

`schedule=None` keeps the pipeline registered but unscheduled — it only
runs when invoked from another pipeline's `transforms_post` or directly
via `flow run <name>`.

### 4.4 Order of operations

Within a single pipeline run:

1. **Pre-flight**: `ensure_table` for the augmented target spec.
2. **Source query**: user's function body or `source_table=` synthesis.
3. **Per-column normalization**: applied as SQL inside the source CTE.
4. **Pipeline-level pre-transforms**: applied as additional CTEs.
5. **Strategy execution**: append / merge / scd2 against the normalized source.
6. **Watermark advance + run_history success row** (existing).
7. **Post-load transformations**: each in its own transaction,
   sequentially. Failures recorded in `run_history` as `transform_failed`
   rows distinct from the main load's `success` row.

Crucially: **steps 1–6 are atomic to each other**, **step 7 is not
atomic with anything**. If a post-transform fails, the load itself
remains committed. The user sees a clear log of which post-transforms
ran and which didn't.

### 4.5 `flow transform` CLI subcommands

```bash
flow transform list --module my_pipelines
flow transform run --module my_pipelines <name>
```

For pipelines that *only* contain a transform, no separate flow needed.

---

## 5. Worked examples

### 5.1 The "messy CSV upload" pipeline

```python
@ematix.table(schema="raw")
class CustomerLanding:
    customer_id: Annotated[BigInt, pk()]
    email: Annotated[String[256], lower(), trim(), empty_to_null()]
    phone: Annotated[String[32], phone_normalize(), empty_to_null()]
    signup_at: Annotated[TimestampTZ, parse_timestamp("YYYY-MM-DD HH24:MI:SS")]
    region: Annotated[String[8], default("US")]
    notes: Annotated[Text | None, trim(), empty_to_null()]


@ematix.pipeline(
    target=CustomerLanding,
    source_table="raw.csv_upload",
    schedule="*/15 * * * *",
    mode="merge",
    transforms_pre=[
        deduplicate_by("customer_id", order_by="signup_at DESC"),
        filter_where("email IS NOT NULL"),
    ],
    transforms_post=[
        "REFRESH MATERIALIZED VIEW analytics.customer_summary",
    ],
)
def ingest_customers():
    pass
```

Six lines of normalizers + two lines of transforms get the user from
"raw CSV with messy values and dupes" to "clean dim table with a
refreshed downstream view," with no Python written for the data path.

### 5.2 SCD2 dimension with feature-store-style cleanup

```python
@ematix.table(schema="features")
class UserFeatures:
    user_id: Annotated[BigInt, pk()]
    email: Annotated[String[256], lower(), trim(), empty_to_null()]
    last_seen: Annotated[TimestampTZ, parse_timestamp("YYYY-MM-DD HH24:MI:SS")]


@ematix.pipeline(
    target=UserFeatures,
    schedule="0 */4 * * *",
    mode="scd2",
    event_timestamp_column="last_seen",
    transforms_pre=[
        deduplicate_by("user_id", order_by="last_seen DESC"),
    ],
)
def sync_user_features(conn):
    return """
        SELECT user_id, email, last_seen
        FROM events.user_logins
        WHERE last_seen >= now() - interval '7 days'
    """
```

`deduplicate_by(..., order_by="last_seen DESC")` keeps the latest event
per user before SCD2 sees it — exactly what Phase 15's event-time
SCD2 needs to behave deterministically when source has multiple rows
per natural key.

---

## 6. Implementation approach

### 6.1 Where the SQL compilation happens

All normalizers compile to plain Postgres SQL strings on the **Python**
side. The output flows through the existing `pipeline.sync` →
`Connection.run_*` → Rust executor path with no Rust changes.

This is the right layer because:
- Normalizers are user-facing — Python makes the API ergonomic.
- The SQL output is what Postgres evaluates; Rust just ships the
  string.
- Adding a new named normalizer is a Python-only PR.

### 6.2 Marker → SQL contract

Each normalizer marker is a frozen dataclass with a `to_sql(col)`
method that returns the SQL expression for `col` after this
normalizer applies:

```python
@dataclass(frozen=True)
class _Trim:
    def to_sql(self, col: str) -> str:
        return f"trim({col})"


def trim() -> _Trim:
    return _Trim()
```

For composite normalizers (e.g., `whitespace_to_null`), the SQL is
composed:
```python
@dataclass(frozen=True)
class _WhitespaceToNull:
    def to_sql(self, col: str) -> str:
        return f"NULLIF(trim({col}), '')"
```

For escape-hatch:
```python
@dataclass(frozen=True)
class _SqlNormalizer:
    expression: str   # uses 'col' as the placeholder
    def to_sql(self, col: str) -> str:
        return self.expression.replace("col", col)
```

The `@ematix.table` decorator collects normalizer markers per column.
The `@ematix.pipeline` decorator wraps the user's source SELECT in a
CTE that applies each column's chain.

### 6.3 Pipeline-level transforms_pre

Each entry is a class with a `to_sql_wrap(inner_cte_name)` method:

```python
@dataclass(frozen=True)
class _DeduplicateBy:
    keys: tuple[str, ...]
    order_by: str | None
    def to_sql_wrap(self, inner: str) -> str:
        order = f", {self.order_by}" if self.order_by else ""
        return (
            f"SELECT DISTINCT ON ({', '.join(self.keys)}) * "
            f"FROM {inner} ORDER BY {', '.join(self.keys)}{order}"
        )
```

Pipeline emits one CTE per transform_pre. Order is preserved from
declaration order.

### 6.4 Validation

- Normalizer SQL output is **never** sanitized (we trust SQL strings).
  Users could sneak SQL injection through `default("'; DROP TABLE...")`
  if they wanted to, but they're already authoring SQL in the source
  query — this is no different.
- For named normalizers with simple values (`default("US")`),
  framework-side quoting kicks in: `default(value: str)` quotes via
  `'value'` and escapes `'` to `''`.
- `sql("...")` is a marker the user explicitly opted into, no quoting.

### 6.5 Type-checking the normalizer chain

Each normalizer is annotated to indicate compatible column types:
- `trim` / `lower` / `upper` / `empty_to_null` — String/Text only
- `parse_timestamp` — String → TimestampTZ
- `clamp` — numeric only
- etc.

At `@ematix.table` decoration time, validate that the chain's expected
input type matches the column's declared type. Misuses (`trim()` on a
`BigInt` column) raise at decoration time, not at run time.

---

## 7. Phased rollout

### Phase 26 — Normalization (≈2–3d)

- `ematix_flow.normalize` module with the catalogue from §3.3.
- Marker dataclasses with `to_sql(col)` methods.
- `@ematix.table` decorator collects normalizers per column.
- `@ematix.pipeline` decorator gains `transforms_pre=[...]` kwarg.
- Source query synthesis builds CTE-stacked SQL.
- Validation: type-compatible chains, schema-qualified `sql()` markers.
- `preview()` renders the CTE chain with each layer's normalizer.

Tests:
- Unit tests per normalizer (Python → SQL string match).
- Composition tests (chain of 3+ normalizers).
- Pipeline-level transforms_pre (dedup, filter, limit).
- `sql()` escape-hatch tests.
- Type-mismatch validation tests.
- Integration: messy CSV → clean target via testcontainers.

### Phase 27 — Transformations + derived columns (≈1.5–2d)

- `transforms_post=[...]` kwarg on `@ematix.pipeline`. Accepts:
  - SQL strings → run on `target_connection` in own tx
  - Callables → run with `target_connection` as arg
  - Names of other registered pipelines / transforms
- `@ematix.transform(target_connection=..., name=..., schedule=None)`
  decorator. Registers in `pipeline._TRANSFORMS_REGISTRY`.
- Transform results recorded as separate `run_history` rows:
  `transform_started` / `transform_success` / `transform_failed`,
  linked to the parent pipeline's run_id.
- `flow transform list / run` CLI subcommands.
- `derive("expression")` marker for derived columns (concatenation,
  computed values).

Tests:
- Per-pipeline transforms_post string + callable.
- Halt-on-first vs `continue_on_failure_post`.
- run_history captures per-transform status.
- `@ematix.transform` registers and runs standalone.
- `flow transform` CLI subcommands.

---

## 8. Open questions

1. **Normalizer validation timing.** Type-mismatch errors at
   decoration time would be ideal, but we'd need to introspect the
   column type without running the decorator. Punt to first run for
   v0.1?
2. **Should `derive()` count as normalization or transformation?**
   It produces a new column from existing ones. I argue it's a
   transformation (Phase 27) because it's "computing a new value,"
   while normalizers "fix an existing value." But the user-facing
   API could treat them as a single bucket if simpler.
3. **Default value escaping.** `default("US")` vs `default(42)` vs
   `default(None)`. Current proposal: framework quotes strings,
   inlines numerics literally, treats `None` as `NULL`. Acceptable?
4. **`sql()` injection vector.** If user writes
   `default(user_input)` and `user_input` is attacker-controlled,
   that's classic SQL injection. Document, don't try to defend.
5. **Per-column vs per-pipeline `transforms_pre`.** Should
   `deduplicate_by()` also be available as a column marker? Probably
   not — it's intrinsically cross-row, but the user-facing question
   is whether to confuse users by surfacing both forms.
6. **Idempotency.** `transforms_post=["INSERT INTO summary ..."]` is
   not idempotent. Should we recommend `INSERT ... ON CONFLICT` or
   provide an `@ematix.transform` decorator that takes a target table
   and handles the merge semantics for the user? Probably v0.3.

---

## 9. Out of scope (for now)

- Row-level Python transformations. If you need to call a Python
  function on every row, write it in your source query as a Postgres
  function or do it in upstream code.
- Multi-table joins as transforms. Use a separate `@ematix.pipeline`
  with the join in the source SELECT.
- Streaming normalization (Kafka → DB). v0.1 is batch + cron.
- Schema-changing transforms (ALTER TABLE during a load). The drift
  comparator in Phase 4 forbids this for safety; transforms shouldn't
  bypass it.
- "Transform DAGs" (A then B-and-C-in-parallel then D). v0.1 is
  sequential. If users need DAG semantics they're using the wrong
  framework — Airflow, Prefect, Dagster.

---

## 10. Summary

Normalization (Phase 26) and transformations (Phase 27) layer cleanly
on top of the existing decorator API.

**Normalization** — per-column markers + pipeline-level transforms_pre
— compiles to CTE-stacked SQL inside the load transaction. Common
cases (empty→null, trim, parse_timestamp, dedup) get one-line markers
the user can read at a glance. Custom cases drop to `sql()` with full
SQL access.

**Transformations** — pipeline-level `transforms_post` + standalone
`@ematix.transform` decorator — run after the load commits, each in
their own transaction, sequentially with halt-on-first by default.
Three flavors: SQL strings (refresh / analyze), callables, and
references to other registered pipelines (chaining).

The split keeps the user's mental model crisp: "before write =
normalization (declarative, atomic with the load)," "after write =
transformation (composable, post-commit, recorded separately)."
