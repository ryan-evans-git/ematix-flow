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
| `parse_timestamp(formats=[...], on_failure="null"\|"error"\|"default", default=...)` | regex-pre-filtered CASE chain over `to_timestamp(col, fmt)` | Phase 26 ships position β (multi-format with built-in regex catalogue). `on_failure="null"` is default — preserves rows when one bad value arrives. `formats=[...]` accepts Postgres format codes (`YYYY-MM-DD HH24:MI:SS`, `MM/DD/YYYY`, etc.). Single-format short form: `parse_timestamp(format=...)` is sugar over `formats=[format]`. |
| `parse_date(formats=[...], on_failure=..., default=...)` | regex-pre-filtered CASE chain over `to_date` | Same shape as `parse_timestamp`. |
| `to_timezone(tz)` | `col AT TIME ZONE tz` | |
| `date_trunc(precision)` | `date_trunc(precision, col)` | `precision = 'day' / 'hour' / ...` |

#### Numeric

| Marker | SQL |
|---|---|
| `parse_int(on_failure="null"\|"error"\|"default", default=...)` | regex-validated CASE → `col::bigint` |
| `parse_numeric(precision, scale, on_failure=..., default=...)` | regex-validated CASE → `col::numeric(p, s)` |
| `round(precision)` | `round(col, precision)` |
| `clamp(min, max)` | `least(greatest(col, min), max)` |

`parse_int` / `parse_numeric` follow the same `on_failure` protocol as
`parse_timestamp`/`parse_date` — `"null"` default preserves rows.

#### Boolean

| Marker | SQL |
|---|---|
| `parse_bool(truthy=('true','1','yes','y'), falsy=('false','0','no','n'))` | `CASE WHEN lower(col) IN (...) THEN true WHEN lower(col) IN (...) THEN false END` |

#### NULL handling

| Marker | SQL | Notes |
|---|---|---|
| `default(value)` | `COALESCE(col, <quoted>)` | Framework quotes per Python type: strings get `''`-escaped single quotes, numerics inlined as literals, booleans → `true`/`false`, `date`/`datetime` → ISO string with `::date`/`::timestamptz` cast. **`default(None)` raises at decoration time** ("use `T \| None` for nullability"). Lists/dicts/bytes raise — punt to `sql()`. |
| `nullif(value)` | `NULLIF(col, <quoted>)` | Same quoting rules as `default`. |
| `not_null_or(value)` | `COALESCE(col, <quoted>)` | Alias for `default()` when readability calls for "this column must never be null." |

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

**Safety contract (locked Q4):**
- The framework does **not** quote, validate, or escape `sql()`
  expressions. Users who construct the string from untrusted input
  own the SQL-injection risk.
- `preview()` output flags every column carrying a `sql()` marker so
  the raw expression can be audited before deploy.
- Use a named normalizer (`default()`, `regex_replace()`, etc.) for
  parameter-style safety whenever possible.

### 3.5 Inspection via `preview()`

Phase 25's `preview()` renders the full normalized + transformed SQL
plan, showing each CTE layer with the normalizer / transform that
produced it. Users debug "why did this column get blanked" by reading
the preview output instead of guessing.

### 3.6 Concatenation / derived columns (Phase 26, locked Q2)

```python
full_name: Annotated[Text, derive("first_name || ' ' || last_name"), trim()]
```

`derive(expression)` produces a new column from an arbitrary SQL
expression rather than modifying an existing source column. Ships in
Phase 26 alongside other normalizers because it's the only way to add
a computed column when using `source_table=` (no function body to
modify).

**Position contract (locked Q2 → A.1):** `derive()` must be the
**first** marker in the chain when present. Subsequent normalizers
wrap the derived expression naturally:

```python
# OK — derive first, then trim wraps it
full_name: Annotated[Text, derive("first_name || ' ' || last_name"), trim()]
# → trim(first_name || ' ' || last_name) AS full_name

# ERROR at decoration time — derive must be first
full_name: Annotated[Text, trim(), derive("first_name || ' ' || last_name")]
```

The framework treats columns with `derive()` differently in source SQL
synthesis: the column doesn't need to exist in the source query result;
the derive expression IS the column's value, optionally wrapped by
later normalizers in the chain.

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

### 4.6 DataFrame interop inside `@ematix.transform`

For transforms that need to do real Python work between read and write —
ML feature engineering, joining against a parquet file, calling a
scikit model — the connection grows two helpers that auto-detect the
caller's preferred DataFrame library:

```python
@ematix.transform(target_connection="warehouse")
def score_customers(conn):
    df = conn.read_df("SELECT customer_id, features FROM warehouse.customer_dim")
    # df is a polars.DataFrame if polars is installed; else a pandas.DataFrame
    df = df.with_columns(score=predict_with_model(df["features"]))
    conn.write_df(df, "warehouse.customer_scores", mode="merge", keys=["customer_id"])
```

**Auto-detect rules (locked Q7 → A — to be confirmed):**

- `conn.read_df(sql, *, prefer="auto")`:
  - `"auto"` returns polars if `import polars` succeeds, else pandas.
  - `"polars"` / `"pandas"` force the choice; raises ImportError if missing.
- `conn.write_df(df, qualified_name, *, mode, keys=None, ...)`:
  - Detects the input by `isinstance` — polars.DataFrame, pandas.DataFrame.
  - PyArrow is the on-the-wire format; both libraries convert through it.
  - Goes through the same strategy executor as `pipeline.sync` so
    `mode="merge"` / `"append"` / `"truncate"` / `"scd1"` / `"scd2"` all
    work identically. Behind the scenes: write the DataFrame to a
    temporary Postgres table via COPY BINARY, then run the chosen strategy
    against it.

**Why this layer and not normalization:**

Normalization (Phase 26) compiles to in-database SQL on purpose — keeps
loads fast and atomic with the strategy. DataFrame interop is the
"escape hatch" for genuine Python work. Different scope, different
constraints. The split keeps the v0.1 fast-path intact while letting
ML / advanced workflows pull data into Python when they need to.

**Out of scope here (deferred to Phase 28 — see §7):** PySpark / Spark
DataFrame interop. PySpark is distributed (JVM, JDBC, cluster config),
so it ships as `pip install ematix-flow[spark]` with its own
`conn.read_spark_df` / `conn.write_spark_df` helpers in a later phase.

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
- `derive()` marker in Phase 26 (locked Q2 → A.1 — must be first).
- `default()` strict quoting per Python type (locked Q3 → A); dates +
  datetimes ship in Phase 26 (locked Q3.2).
- `parse_timestamp(formats=[...], on_failure=...)` with regex pre-filter
  catalogue per format (locked Q3.5 → β; default `on_failure="null"`).
  Same shape for `parse_date`, `parse_int`, `parse_numeric`.
- `sql()` escape hatch with documented warning + `preview()` flagging
  (locked Q4 → A — no validation).
- Pipeline-only `transforms_pre` for cross-row ops (locked Q5 → A —
  no column-level sugar for dedup/filter/limit/sample).
- **No** decoration-time type checking on normalizer chains (locked
  Q1 → A). Postgres runtime errors are the safety net.
- `preview()` renders the CTE chain with each layer's normalizer.

Plus the Q1 mitigation:

- New `flow validate <pipeline>` CLI subcommand. Runs `EXPLAIN` against
  the resolved target connection on the synthesized source SQL, surfacing
  type/syntax errors at user-controlled times (CI, pre-deploy) without
  taxing decoration. ~30 LOC.

Tests:
- Unit tests per normalizer (Python → SQL string match).
- Composition tests (chain of 3+ normalizers, `derive()` position).
- `default()` quoting per type (string, int, float, bool, date, datetime,
  None-rejected, unsupported-type-rejected).
- `parse_timestamp(formats=[...])` with on_failure=null/error/default.
- Pipeline-level transforms_pre (dedup, filter, limit).
- `sql()` escape-hatch tests + `preview()` flag rendering.
- Integration: messy CSV → clean target via testcontainers.
- `flow validate` smoke tests (good pipeline → exit 0; bad type chain
  → exit nonzero with clear pointer).

### Phase 27 — Transformations + DataFrame interop (≈2–3d)

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
- **DataFrame interop (§4.6):**
  - `Connection.read_df(sql, *, prefer="auto")` — returns polars or
    pandas DataFrame. Auto-detects via importable libraries.
  - `Connection.write_df(df, qualified_name, *, mode, keys=None, ...)`
    — accepts polars or pandas DataFrame, dispatches via `isinstance`.
    Goes through the same strategy executor as `pipeline.sync` so
    every mode works identically.
  - Hard requirement: at least one of polars / pandas installed; the
    framework declares them as `[df]` extras (`pip install
    ematix-flow[df]`) and raises a clear ImportError if a user calls
    `read_df` / `write_df` without either.
- **Idempotency contract (locked Q6 → A):** the framework guarantees
  idempotency only via the chained-pipeline pattern — write the
  transformation as `@ematix.pipeline(schedule=None, mode="merge", ...)`
  and reference by name in `transforms_post`. Raw SQL strings in
  `transforms_post` are reserved for naturally-idempotent maintenance
  (`REFRESH MATERIALIZED VIEW`, `ANALYZE`, `VACUUM`). Documentation
  steers users to the right tool with a clear example.

Note: `derive()` lives in Phase 26 (locked Q2 → A.1) — moved up since
it unlocks `source_table=` for derived-column use cases.

Tests:
- Per-pipeline transforms_post string + callable.
- Chained-pipeline pattern resolves `transforms_post=["other_pipe"]`
  by name and runs that pipeline's full sync.
- Halt-on-first vs `continue_on_failure_post`.
- run_history captures per-transform status linked to parent run_id.
- `@ematix.transform` registers and runs standalone.
- `flow transform` CLI subcommands.
- `read_df` / `write_df` with both polars and pandas (matrix tests).
- Auto-detect picks polars when both installed; pandas-only when only
  pandas; clear ImportError when neither.

### Phase 28 — Spark interop (shipped)

Available via `pip install ematix-flow[spark]`. The `ematix_flow.spark`
module monkey-patches `_core.Connection` on import:

- `conn.read_spark_df(spark_session, sql)` — returns a Spark DataFrame
  backed by the Postgres JDBC driver.
- `conn.write_spark_df(df, qualified_name, *, mode, target=, keys=, ...)`
  — stages via JDBC into a uuid-named table, then routes through the
  strategy executor (same pattern as Phase 27e write_df). Every mode
  (append/truncate/merge/scd1/scd2) works for free with a ManagedTable.

The user owns the SparkSession (Spark is heavy: JVM, JDBC jar, cluster
config). Document the lift, don't hide it. The Postgres JDBC jar is
typically wired via `SparkSession.builder.config("spark.jars.packages",
"org.postgresql:postgresql:42.7.x")`.

Distinct from Phase 27e because Spark's distributed model needs separate
plumbing (JDBC, SparkSession, cluster config) that doesn't share much
with the polars/pandas in-process path.

Tests under `@pytest.mark.spark` are opt-in (`pytest -m spark`) since
they require pyspark + JDBC jar download.

---

## 8. Locked decision log

| ID | Decision | Recorded |
|---|---|---|
| Q1 | No decoration-time type checking. Postgres errors at runtime + `preview()` shows the generated SQL + new `flow validate <pipeline>` CLI subcommand for `EXPLAIN`-based pre-deploy checks. | §6 (Phase 26 plan) |
| Q2 | `derive(expression)` ships in Phase 26 as a normalization marker. **Must be first** in the chain when present (Position A.1). | §3.6 |
| Q3 | `default()` uses strict per-type quoting. Strings escape `'` → `''`; numerics inlined; booleans → `true`/`false`; dates/datetimes get ISO + `::date`/`::timestamptz` casts. `default(None)` and unsupported types raise at decoration. | §3.3 (NULL handling) |
| Q3.5 | `parse_timestamp(formats=[...], on_failure="null"\|"error"\|"default", default=...)` ships in Phase 26 (Position β). Default `on_failure="null"` to preserve rows. Same shape for `parse_date`, `parse_int`, `parse_numeric`. Position γ (auto-detect) shipped as a follow-up: `parse_timestamp()` / `parse_date()` with no args fall back to a curated `DEFAULT_TIMESTAMP_FORMATS` / `DEFAULT_DATE_FORMATS` catalogue (first-match-wins on ambiguity; explicit `format=`/`formats=` overrides). | §3.3 (date/time, numeric) |
| Q4 | `sql()` escape hatch is documented as user-owned-risk. No validation, no escaping. `preview()` flags every `sql()` marker for audit. | §3.4 |
| Q5 | Cross-row operations (`deduplicate_by`, `filter_where`, `limit`, `sample_pct`) live only at the pipeline level via `transforms_pre=[...]`. No column-level sugar. | §3.2 |
| Q6 | Idempotency for write-many post-load work goes through the chained-pipeline pattern (Phase 27 §4.3). Raw SQL `transforms_post=` is reserved for naturally-idempotent maintenance ops. Strong docs, no separate decorator. | §4.3, Phase 27 |

### Phase 27 design log (locked)

| ID | Decision | Notes |
|---|---|---|
| 27-Q1 | `transforms_post=` accepts strings (always SQL), function refs (passed directly), and `ematix.transform_ref("name")` for name-based lookup of pipelines/transforms. No verb-prefix heuristic. | C |
| 27-Q2 | Reuse `run_history` table. Add nullable `run_id` UUID (shared by parent + transform rows in one run) and `step_name` (NULL for the main load row, set for transforms). Broaden `status` to include `transform_success` / `transform_failed`. | A |
| 27-Q3.1 | Transform callable may return a dict of metrics; framework merges it into the `run_history` row. Returning None → `transform_success` with empty metrics. | B |
| 27-Q3.2 | Transform callable arity is auto-detected via `inspect.signature`: 1-arg gets `(conn,)`, 2-arg gets `(conn, parent)` where `parent` carries `run_id`, `pipeline_name`, `target`, parent metrics; `parent` is None when invoked standalone. | γ |
| 27-Q4.1 | A `@ematix.transform(schedule=...)` that is also referenced from a pipeline's `transforms_post` fires from both — pipeline post-load AND its own cron, independently. User opted into both. | A |
| 27-Q4.2 | `flow list` shows pipelines + transforms with a `type=pipeline\|transform` column; `flow transform list` is the filtered shortcut. | β |
| 27-Q5 | `transform_ref("name")` runs the chained pipeline's full sync independently — fresh connection resolution, own `run_history` row (with shared `run_id`), recursive transforms_post. No implicit batch passing. | A |
| 27-Q6 | `Connection.read_df` / `write_df` are attached via monkey-patch from `ematix_flow.df` on import. Lazy: importing the df module attaches the methods. Keeps `_core` Rust extension free of polars/pandas coupling. | A |
| 27-Q7 | `prefer="auto"` returns polars when both polars + pandas are installed; falls back to pandas if only pandas is available. `prefer="pandas"` / `"polars"` force the choice (raises ImportError if missing). | A |
| 27-Q8.1 | `write_df` accepts a `target=ManagedTable` for production paths (validates df shape, runs full strategy pipeline). Without `target=`, infers schema from df dtypes (ad-hoc / Jupyter convenience). | C |
| 27-Q8.2 | ManagedTable path auto-augments `_loaded_at` / `_batch_id` like `pipeline.sync`. Inferred path takes the df as-is — no metadata columns auto-added. | γ |
| 27-Q9 | Transport: convert df → Arrow → COPY BINARY into a temp table, then run the chosen strategy against it. Reuses Phase 9 cross-DB COPY plumbing. Every strategy mode (append/truncate/merge/scd1/scd2) works for free. | A |
| 27-Q10 | `preview()` lists transforms_post entries (text only, no execution). `dry_run()` skips them entirely (load only). `validate()` EXPLAINs string transforms; callables and `transform_ref` skipped with a note. | B |

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
on top of the existing decorator API. All seven design questions in
§8 are locked.

**Normalization** — per-column markers + pipeline-level transforms_pre
— compiles to CTE-stacked SQL inside the load transaction. Common
cases (empty→null, trim, parse_timestamp with multi-format tolerance,
dedup) get one-line markers the user reads at a glance. The catalogue
covers every case the user listed plus the high-value extensions a
real pipeline needs.

`derive()` joins the catalogue (Q2) so `source_table=` users can add
computed columns without dropping to a function body. Position-required
first in the chain so SQL composition is unambiguous.

`default()` and the parser markers ship in Phase 26 with strict quoting
(Q3) and format-resilient `on_failure="null"` defaults (Q3.5) — one bad
row can't take down a whole load.

`sql()` is the no-validation escape hatch (Q4); `preview()` flags it for
audit so users can catch their own mistakes before deploy. The new
`flow validate` CLI (Q1 mitigation) catches type/syntax errors against
a real connection at user-controlled times.

**Transformations** — pipeline-level `transforms_post` + standalone
`@ematix.transform` decorator — run after the load commits, each in
their own transaction, sequentially with halt-on-first by default.
The chained-pipeline pattern (Q6) is the canonical idempotent path:
write the transformation as `@ematix.pipeline(schedule=None, mode="merge")`
and reference by name. Raw SQL is reserved for naturally-idempotent
maintenance (REFRESH, ANALYZE, VACUUM).

The split keeps the user's mental model crisp: "before write =
normalization (declarative, atomic with the load)," "after write =
transformation (composable, post-commit, recorded separately)."
