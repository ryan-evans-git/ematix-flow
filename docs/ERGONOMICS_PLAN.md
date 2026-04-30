# ematix-flow — Python Ergonomics Plan

A planning doc parallel to `docs/ML_FEATURE_STORE_PLAN.md`. Reviews the
current Python surface against three goals:

1. **Shift complexity to the framework.** Rust does the heavy lifting; Python
   should look almost trivial.
2. **Decorator-based declarative API** as the primary path, matching modern
   Python style (FastAPI, FastMCP, Pydantic v2, dlt).
3. **Auto-detect everything that's already in the DDL.** Merge keys,
   nullability, types — the user shouldn't repeat themselves.

Plus three specific gaps surfaced in design review:

- **Unique-constraint-driven upsert** for tables that don't use a single PK.
- **Connection / credential configuration** — currently the user threads
  raw URL strings through their code.
- **Multi-target pipelines** for the common "one source feeds many feature
  views" pattern.

This doc is the **post-design-review spec**. All open questions from the
v1 of this plan are now locked. See §8 for the decision log.

---

## 1. Where we are today

A complete Phase-15 example with everything we've shipped looks like this:

```python
# my_pipelines.py
from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import BigInt, Column, String, Text, TimestampTZ


class CustomerDim(ManagedTable):
    __schema__ = "warehouse"
    __tablename__ = "customer_dim"

    customer_id = Column(BigInt(), nullable=False, primary_key=True)
    email = Column(String(256), nullable=False)
    name = Column(Text())
    last_seen = Column(TimestampTZ(), nullable=False)


@pipeline.register(name="customer_sync", schedule="0 * * * *")
def customer_sync():
    conn = _core.connect("postgres://user:pass@host:5432/warehouse")
    return pipeline.sync(
        target=CustomerDim,
        source=Source.postgres_query(
            conn,
            "SELECT id AS customer_id, email, name, last_seen FROM source.users",
        ),
        target_connection=conn,
        mode="scd2",
        keys=("customer_id",),
        compare_columns=("email", "name"),
        event_timestamp_column="last_seen",
    )
```

That's ~25 substantive lines, most of it plumbing the framework already
knows: the connection URL belongs in a config, `keys=` is already declared
as `primary_key=True`, `compare_columns` is the same pattern of "all
non-key columns" most users want, and the imports + ceremony are ~8 lines
on their own.

---

## 2. Where we want to be

The same pipeline, after Phases 21–25:

```python
# my_pipelines.py
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, String, Text, TimestampTZ


@ematix.table(schema="warehouse")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]
    email: String[256]
    name: Text | None
    last_seen: TimestampTZ


@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    event_timestamp_column="last_seen",
)
def customer_sync(conn):
    return "SELECT id AS customer_id, email, name, last_seen FROM source.users"
```

~10 substantive lines. The user wrote: a class with type-annotated
attributes, one decorator declaring it a managed table, a function
returning a SQL string, one decorator wiring it up. Everything else
(connection, key inference, compare columns, target connection wiring,
schedule registration) is the framework's job.

For the trivial 1:1 mirror case, even shorter:

```python
@ematix.pipeline(
    target=CustomerDim,
    source_table="source.users",
    column_map={"customer_id": "id"},
    schedule="0 * * * *",
    mode="merge",
)
def customer_sync():
    pass
```

---

## 3. Decorator surface (`ematix` namespace)

The package exports a single namespace handle named `ematix`:

```python
from ematix_flow import ematix
```

Used in code as `@ematix.table(...)` / `@ematix.pipeline(...)` /
`ematix.target(...)`. Distinct from the `flow` CLI shipped in Phase 12.
No `flow` alias — pick one name and teach it consistently.

### `@ematix.table`

A class decorator that turns a Python class with type-annotated attributes
into a `ManagedTable` subclass. Style: PEP 593 `Annotated[T, marker()]`,
matching FastAPI / FastMCP / Pydantic v2 conventions.

```python
@ematix.table(schema="warehouse", name="customer_dim")
class CustomerDim:
    customer_id: Annotated[BigInt, pk()]
    order_date: Annotated[Date, pk()]              # composite PK
    email: String[256]
    name: Text | None                              # T | None → nullable
    balance: Numeric[12, 2] | None
    is_active: Boolean
    created_at: TimestampTZ
```

Conventions baked in:
- Bare class for parameter-less types (`BigInt`, `Boolean`, `Text`, `Date`,
  `TimestampTZ`).
- Subscript for parameterized types (`String[256]`, `Numeric[12, 2]`) via
  `__class_getitem__` returning the matching instance.
- `Annotated[T, marker()]` only when a column carries flags. Markers are
  always called (e.g., `pk()`, never bare `pk`), matching Pydantic's
  `Field()` style.
- `T | None` (or `Optional[T]`) infers `nullable=True`. Default is
  `nullable=False` — the safer choice.
- `pk()` marks a primary-key column. Composite PK = multiple `pk()` columns
  in declaration order.
- `natural_key()` marks a column as part of a composite UNIQUE constraint.
  Same group label = same constraint. `natural_key()` (no arg) is "default"
  group; `natural_key("legacy")` joins others with that label.
- `__unique_constraints__` dunder is the escape hatch for power users:
  `(("col1", "col2"), ("col3", "col4"))`.
- `name=` kwarg overrides the auto-snake-cased class name; `schema=` is
  required.
- All `@ematix.table` kwargs (`schema=`, `mode=`, `event_timestamp_column=`,
  `ttl=`, etc.) become class-level defaults that `@ematix.pipeline` reads.

**Stacking with `@dataclass` / `@attrs.define` / Pydantic `BaseModel` is
forbidden.** `@ematix.table` raises a clear error at decoration time when
those markers are present:

> "Customer is already decorated with @dataclass. @ematix.table provides
> field-collection on its own; remove @dataclass or use the imperative
> `class X(ManagedTable)` form instead."

### `@ematix.pipeline`

A function decorator that combines target + source + sync + schedule into
one declaration, building on Phase 12's `@pipeline.register`.

```python
@ematix.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    event_timestamp_column="last_seen",
)
def customer_sync(conn):
    return "SELECT id AS customer_id, email, name, last_seen FROM source.users"
```

**Function signatures inspected:**

| Signature | Behavior |
|---|---|
| `def f()` / `async def f()` | OK only when `source_table=` is set |
| `def f(conn)` / `async def f(conn)` | Single connection (same-DB by default) |
| `def f(src_conn, tgt_conn)` / `async def f(src_conn, tgt_conn)` | Cross-DB |

Both `def` and `async def` accepted; async bodies run via `asyncio.run(...)`
inside the existing tokio-block-on path. Async is for users with async-only
upstream APIs (e.g., async HTTP clients). It's not a performance feature.

**Function return values:**

| Returns | Treatment |
|---|---|
| `str` | Wrap as `Source.postgres_query(conn, returned)` |
| `Source` | Use directly |
| `dict` | Assume the user did their own `pipeline.sync` and use the dict as the result |
| `None` (and `source_table=` is set) | Synthesize from `source_table` + `column_map` |
| `None` (and `source_table=` is unset) | Raise: pipeline has no source declaration |

**Connection resolution:**

Connections are named (Phase 21). `target_connection="warehouse"` is the
default; `source_connection="oltp"` is the cross-DB second connection.
Both are symbolic names resolved via the connection registry. No URL
strings in pipeline definitions.

**Multi-target via `targets=[...]`** — see §3.4.

**Preview / dry-run methods on the decorated function:**

```python
customer_sync.preview()   # plan only, no DB
customer_sync.dry_run()   # execute in tx, ROLLBACK
```

See §3.5.

### `pk()`, `natural_key()`, `nullable()` markers

Sentinels used inside `Annotated[T, marker()]`. All callable (Pydantic
`Field()` style) so future params land cleanly:

```python
pk()                          # primary key
pk(autoincrement=True)        # future-proofed for IDENTITY columns
natural_key()                 # default-group UNIQUE constraint
natural_key("legacy")         # named-group UNIQUE constraint
nullable()                    # alias for `T | None`
```

`T | None` is preferred over `nullable()` — Pythonic, type-checker-friendly.
`nullable()` is provided as a sometimes-clearer marker for explicit cases.

### `ematix.target(...)`

A small dataclass wrapping per-target options for multi-target pipelines:

```python
ematix.target(
    Customer,                          # target class
    mode="scd2",                       # required
    target_connection="alt_warehouse", # optional override
    event_timestamp_column="ts",       # optional, scd2 only
    ttl=timedelta(days=7),             # optional, scd2 only
    handle_deletes="hard",             # optional
    keys=("a", "b"),                   # optional override
    compare_columns=(...),             # optional override
    update_columns=(...),              # optional override
)
```

---

## 3.1 Connection registry (Phase 21)

Pipelines declare *symbolic* connection names; the registry resolves them.
Position F from the design discussion: registry-only override, no CLI flag
for runtime redirects.

**Resolution order** (highest priority first):
1. Environment variable `EMATIX_FLOW_DSN_<NAME>` (e.g., `EMATIX_FLOW_DSN_WAREHOUSE`).
2. `EMATIX_FLOW_DSN` for the connection named `default`.
3. Project-local `./.ematix-flow.toml` `[connections.<name>]`.
4. User-global `~/.ematix-flow/connections.toml` `[connections.<name>]`.
5. Explicit `connect(url=...)` in code (low-level escape hatch).

**Config file format** (TOML, `tomllib` from py 3.11):

```toml
[connections.default]
dsn = "postgres://user:pass@host/db"

[connections.warehouse]
dsn = "${WAREHOUSE_DSN}"   # env-var interpolation supported
```

**User-facing API:**

```python
from ematix_flow import connect

conn = connect()                        # default
conn = connect("warehouse")             # named
conn = connect(url="postgres://...")    # explicit URL escape hatch
```

**Redirect to staging without code changes** — the canonical operational
pattern:

```bash
EMATIX_FLOW_DSN_WAREHOUSE=postgres://staging-host/db flow run-due ...
```

The pipeline keeps declaring `target_connection="warehouse"`. What
"warehouse" *means* is what changed.

**CLI subcommands:**

```bash
flow connections list                       # what's configured
flow connections check [name]               # verify reachability
flow connections set NAME=postgres://...    # write to user config
```

`connections set` writes to `~/.ematix-flow/connections.toml`. Useful for
ad-hoc testing without exporting env vars by hand.

**Backwards compat:** `_core.connect(url)` continues to work for users who
want to pass an explicit URL.

---

## 3.2 UNIQUE constraints + auto-detected merge keys (Phases 22–23)

### Phase 22 — UNIQUE constraints in DDL + drift

`TableSpec` gains `unique_constraints: Vec<Vec<String>>` (default empty).
- `create_table_sql` emits `UNIQUE (col, ...)` clauses.
- `read_existing_columns` is supplemented by `read_existing_unique_constraints`
  (information_schema.table_constraints + key_column_usage).
- `compare_table` learns `UniqueConstraintMissing` / `UniqueConstraintExtra`
  variants of `Difference`.
- `ManagedTable` gains `__unique_constraints__: tuple[tuple[str, ...], ...]`
  dunder (default empty).
- `@ematix.table` derives `__unique_constraints__` from `natural_key()`
  markers grouped by label.

### Phase 23 — Auto-detected merge keys

Resolution order for "what columns does merge upsert against?":

1. `keys=` kwarg passed to `@ematix.pipeline` or `pipeline.sync` — explicit
   override, always wins.
2. `__merge_keys__` dunder on the class — explicit class-level default.
3. First entry of `__unique_constraints__` (or the `natural_key()`-marked
   columns) — natural key takes precedence over surrogate PK.
4. `__primary_keys__` (current default, derived from `pk()` / Phase 0
   `primary_key=True`).

If none resolve to a non-empty list, raise with an actionable error
listing all four options.

**Same resolution applies to SCD1/SCD2** (which already require keys for
the ON CONFLICT / hash-join targets).

**Warning at sync time** when `natural_key()` columns differ from `pk()`
columns:

> Pipeline customer_sync: merge key resolved to `(natural_key columns)`
> from `natural_key()`, which differs from declared primary key
> `(pk columns)`. Pass `keys=` explicitly to silence this warning.

Fires once per pipeline-run, not once per row.

---

## 3.3 `source_table=` + `column_map=` (Phase 24)

Trivial 1:1 mirrors don't need a function body:

```python
@ematix.pipeline(
    target=Customer,                  # declares: customer_id, email, signup_at
    source_table="public.users",      # has: id, email, created_at
    column_map={
        "customer_id": "id",
        "signup_at": "created_at",
    },
    schedule="0 * * * *",
    mode="merge",
)
def sync_customers():
    pass
```

Framework synthesizes:

```sql
SELECT id AS customer_id, email, created_at AS signup_at FROM public.users
```

For each target column: in `column_map` → use mapped source column with
`AS` alias; else → use target column name verbatim (must exist in source).

**Ambiguity rejected at decoration time:**
- `source_table="users"` → `ValueError("source_table requires schema.table format")`
- `source_table="public.users.x"` → `ValueError`

**Validation timing:**
- Decorator just stores the string and the map.
- At first run / preview / dry-run: validate that target columns exist in
  source schema. Errors are clear and point at the missing column.

**Function body wins** if both `source_table=` and a returning body are
present. The body is the always-available escape hatch for joins, filters,
and computed columns.

---

## 3.4 Multi-target pipelines (Phase 24)

One source query feeding multiple targets — common for ML feature views
fanning out from a shared event stream:

```python
@ematix.pipeline(
    targets=[
        ematix.target(DailyFeatures, mode="scd2", event_timestamp_column="event_ts"),
        ematix.target(WeeklyFeatures, mode="scd2"),
        ematix.target(EventLog, mode="append"),
    ],
    schedule="0 1 * * *",
    target_connection="warehouse",
)
def sync_user_events(conn):
    return "SELECT user_id, event_ts, ... FROM events.user_events"
```

**Semantics:**

| Concern | Decision |
|---|---|
| Atomicity across targets | Independent transactions per target. Target #2 failing doesn't roll back target #1's commit. |
| Failure handling | Halt on first failure by default. Opt out via `continue_on_failure=True` on the pipeline. |
| Per-target connection override | Allowed: `ematix.target(Cls, target_connection="other")`. Source connection stays pipeline-level. |
| Source extraction | Re-runs per target for v0.1 (simplest). Optimization to "stage once, fan out" deferred to a later phase. |
| Scheduling | Per-pipeline, not per-target. All targets fire together when the cron triggers. |
| Run history | One row per target per fire (so observability stays tidy). |

**Single-target API** — `target=Cls` — keeps working unchanged; the two
shapes coexist.

---

## 3.5 `preview()` + `dry_run()` (Phase 25)

Two methods on every decorator-decorated pipeline + matching CLI subcommands:

```python
sync_customers.preview()   # build the plan, don't execute
sync_customers.dry_run()   # execute inside a tx, ROLLBACK
```

```bash
flow preview <name> --module my_pipelines [-v|--verbose] [--format json]
flow dry-run <name> --module my_pipelines [-v|--verbose] [--format json]
```

**`preview()` shows for each target:**
- Resolved connection (name → host/db/user, no password).
- Path decision (same-DB / cross-DB / forced) with the *reason* (`source
  connection == target connection`).
- Augmented `TableSpec` (auto-added `_loaded_at`, `_batch_id`, `valid_from`,
  `valid_to`, `is_current`, `row_hash` rendered dim so users see what they
  declared vs. what we added).
- Resolved merge / compare keys with reasons (`[from primary_key=True]`,
  `[from natural_key()]`, `[from explicit keys=]`).
- Watermark state if applicable.
- The full SQL plan (1 to N statements) that would execute.

**`dry_run()` does the above plus actually executes inside a transaction**,
catching missing columns, syntax errors, permission issues, etc., then
ROLLBACK. Output adds row-counts that *would* have been affected.

**Output format:**

| | Default | With `--format json` |
|---|---|---|
| Single-target | Verbose plan | Full JSON of plan + decisions |
| Multi-target | **Compact summary** (one line per target + key facts) | Full JSON of all targets |
| Multi-target with `-v` | Verbose plan per target | Same JSON |

**Compact summary example:**

```
sync_user_events   schedule: 0 1 * * *   targets: 3

  ✓ features.daily_features    mode=scd2    keys=(user_id, period_start)   path=same_db
  ✓ features.weekly_features   mode=scd2    keys=(user_id, period_start)   path=same_db
  ✓ events.event_log           mode=append  keys=—                         path=same_db

  --verbose for SQL plans per target
```

**Color** via `rich` (added as a runtime dep — Phase 25). Auto-disables when
piped to a non-tty. Manual override via `--no-color` / `NO_COLOR=1` env var.

**Dry-run on multi-target pipelines: always continues through every
target**, regardless of `continue_on_failure`. The whole point of dry-run
is to surface every problem in one shot. Output marks errors per target
clearly with row-counts on the successful ones.

**Side-effect caveat:** `preview()` / `dry_run()` invoke the wrapped
function to obtain the source query (or `Source` object). If the function
makes external API calls or mutates state in its body, those side effects
fire. Documented; not over-engineered.

---

## 4. Putting it all together (worked example)

A real moderately-complex SCD2 feature view:

**Today (Phase 15):**

```python
from ematix_flow import _core, pipeline
from ematix_flow.source import Source
from ematix_flow.table import ManagedTable
from ematix_flow.types import (
    BigInt, Boolean, Column, Date, Numeric, String, Text, TimestampTZ,
)


class UserPurchaseFeatures(ManagedTable):
    __schema__ = "features"
    __tablename__ = "user_purchase_features_v1"

    user_id = Column(BigInt(), nullable=False, primary_key=True)
    period_start = Column(Date(), nullable=False, primary_key=True)
    total_spend = Column(Numeric(12, 2))
    order_count = Column(BigInt())
    is_subscriber = Column(Boolean(), nullable=False)
    last_event_ts = Column(TimestampTZ(), nullable=False)


@pipeline.register(
    name="user_purchase_features_sync",
    schedule="0 */6 * * *",
)
def sync_user_purchase_features():
    src = _core.connect("postgres://oltp_user:pass@oltp/main")
    tgt = _core.connect("postgres://wh_user:pass@warehouse/wh")
    return pipeline.sync(
        target=UserPurchaseFeatures,
        source=Source.postgres_query(src, """
            SELECT user_id, date_trunc('day', event_ts)::date AS period_start,
                   sum(amount) AS total_spend, count(*) AS order_count,
                   bool_or(is_sub) AS is_subscriber, max(event_ts) AS last_event_ts
            FROM events.purchases GROUP BY 1, 2
        """),
        target_connection=tgt,
        mode="scd2",
        keys=("user_id", "period_start"),
        compare_columns=("total_spend", "order_count", "is_subscriber"),
        event_timestamp_column="last_event_ts",
    )
```

**After Phases 21–25:**

```python
from typing import Annotated
from ematix_flow import ematix, pk
from ematix_flow.types import BigInt, Boolean, Date, Numeric, TimestampTZ


@ematix.table(schema="features", name="user_purchase_features_v1")
class UserPurchaseFeatures:
    user_id: Annotated[BigInt, pk()]
    period_start: Annotated[Date, pk()]
    total_spend: Numeric[12, 2] | None
    order_count: BigInt | None
    is_subscriber: Boolean
    last_event_ts: TimestampTZ


@ematix.pipeline(
    target=UserPurchaseFeatures,
    schedule="0 */6 * * *",
    mode="scd2",
    event_timestamp_column="last_event_ts",
    source_connection="oltp",
    target_connection="warehouse",
)
def sync_user_purchase_features(src_conn, tgt_conn):
    return """
        SELECT user_id, date_trunc('day', event_ts)::date AS period_start,
               sum(amount) AS total_spend, count(*) AS order_count,
               bool_or(is_sub) AS is_subscriber, max(event_ts) AS last_event_ts
        FROM events.purchases GROUP BY 1, 2
    """
```

The function body went from ~15 lines of orchestration to one SELECT.
Keys, compare columns, connection wiring all inferred. Same SCD2 plan,
same atomicity, same speed.

---

## 5. Auto-inference budget

The framework now auto-infers, with explicit overrides always available:

| Thing inferred | From | Override |
|---|---|---|
| Merge keys | `pk()` → `natural_key()` → `__merge_keys__` → `__unique_constraints__[0]` → PK columns | `keys=` kwarg |
| Compare columns | Non-key, non-metadata columns of the target | `compare_columns=` / `update_columns=` kwarg |
| Connection URL | Symbolic name → env var → config file | `_core.connect(url=...)` low-level escape hatch |
| Same-DB vs cross-DB | Function signature (1 vs 2 `conn` params) | `force_path=` kwarg |
| Source SQL | Function return value, or synthesized from `source_table` + `column_map` | Function body always wins if both set |
| SCD2 metadata cols | Auto-augmented (valid_from/to, is_current, row_hash) | n/a |
| Append metadata cols | Auto-augmented (`_loaded_at`, `_batch_id`) | n/a |
| Watermark filter | Synthesized from prior watermark + column type | `incremental_column=` kwarg |
| Unique constraints | Derived from `natural_key()` markers grouped by label | `__unique_constraints__` dunder |

**Mitigation against "why did it pick that?" debugging pain** — the
`preview()` / `dry_run()` path shows every decision alongside its reason.
No separate `.explain()` method needed; preview *is* the explainer.

---

## 6. Phased rollout

Each phase TDD-style with both Rust and Python tests; same discipline as
Phases 0–15.

### Phase 21 — Connection registry (≈0.5–1d)

- `ematix_flow.config` module: `connect(name="default", url=None)` resolves
  via env vars → config file → explicit url.
- `~/.ematix-flow/connections.toml` and `./.ematix-flow.toml` parser.
- Env-var interpolation in TOML values (`${VAR_NAME}`).
- `flow connections list` / `check` / `set` subcommands.
- Existing `_core.connect(url)` stays as the low-level escape hatch.
- Tests: env-var resolution precedence, project-local vs user-global,
  missing-name error, env-var interpolation, CLI subcommand smoke tests.

### Phase 22 — UNIQUE constraints in DDL + drift (≈1–1.5d)

- `TableSpec.unique_constraints: Vec<Vec<String>>`.
- `create_table_sql` emits `UNIQUE (col, ...)`.
- `read_existing_unique_constraints` reflects them.
- `compare_table` learns new `Difference` variants.
- `ManagedTable.__unique_constraints__` dunder.
- Tests: DDL emission, reflection round-trip, drift detection across all
  combinations.

### Phase 23 — Auto-detect merge keys (≈0.5d)

- `pipeline.sync` resolution order documented in §3.2.
- Same resolution for SCD2 keys.
- Warning emitted at sync time when `natural_key()` differs from PK.
- Tests: each level of resolution; clear error when none resolve.

### Phase 24 — `@ematix.table`, `@ematix.pipeline`, multi-target, source_table (≈2–3d)

- `ematix_flow.ematix` namespace.
- `@ematix.table` class decorator using PEP 593 `Annotated[T, marker()]`.
- `pk()`, `natural_key()`, `nullable()` markers.
- `String[256]` / `Numeric[p, s]` via `__class_getitem__`.
- Mutual-exclusion check against `@dataclass` / `@attrs.define` /
  Pydantic `BaseModel`.
- `@ematix.pipeline` function decorator:
  - Inspects signature (0 / 1 / 2 conn params).
  - Accepts both `def` and `async def`.
  - Resolves connection names via Phase 21.
  - Returns wrapper that calls `pipeline.sync(...)` with right arguments
    and registers it via `pipeline.register(...)`.
- `ematix.target(...)` named-constructor for per-target options.
- `targets=[...]` multi-target support; halt-on-first by default with
  `continue_on_failure=True` opt-out.
- `source_table=` + `column_map=` kwargs, schema-qualified validation
  at decoration time, source-schema validation at first run.
- Tests:
  - Side-by-side example from §4 produces identical normalized spec to
    pre-decorator version.
  - Both styles resolve to identical SQL plans.
  - Decoration-time errors (missing `pk()`, `@dataclass` stacked,
    unqualified `source_table=`) raise.
  - Multi-target: independent commits; halt-on-first with opt-out;
    per-target connection override; one run_history row per target.
  - `async def` body executes via `asyncio.run`.

### Phase 25 — `preview()` + `dry_run()` (≈1–1.5d)

- `rich` added as runtime dep.
- `pipeline.preview(name)` and `pipeline.dry_run(name)` module-level
  helpers.
- Decorator-decorated pipelines gain `.preview()` / `.dry_run()` methods.
- CLI subcommands `flow preview <name>` / `flow dry-run <name>`.
- `--format json` machine-readable output.
- Multi-target compact mode by default; `-v` / `--verbose` for full.
- Auto-color detection (no color when piped); `--no-color` / `NO_COLOR`.
- Dry-run always continues through every target.
- Tests:
  - Preview against decorator-defined pipeline matches imperative
    equivalent's plan.
  - Dry-run rolls back cleanly even with `handle_deletes='hard'` and
    multi-statement SCD2 plans.
  - JSON output is stable across runs (no timestamps in machine output).
  - Multi-target compact text passes a snapshot test.

---

## 7. Imperative API stays as escape hatch

None of this removes `class X(ManagedTable)` + `pipeline.sync(...)` +
`_core.connect(url)`. Decorators are pure sugar producing the same
`ManagedTable` subclasses and `ScheduledPipeline` registrations. Users
who want full control or programmatic class construction stay on the
imperative path.

---

## 8. Locked decision log

| ID | Decision | Recorded |
|---|---|---|
| Q1 | Annotated style (PEP 593) for type hints; `Annotated[BigInt, pk()]` | §3 |
| Q2 | Option A (sugar) + Option C (dunder escape hatch); `natural_key()` + `__unique_constraints__`; warn when natural key differs from PK | §3.2 |
| Q3 | Parameterized SQL types via `__class_getitem__` (`String[256]`, `Numeric[12, 2]`) | §3 |
| Q4 | `@ematix.table` fails loudly when stacked with `@dataclass`/`@attrs`/Pydantic | §3 |
| Q5 | Both `def` and `async def` accepted | §3 |
| Q6 | `ematix` namespace (no `flow` alias); defer PEP 420 restructure | §3 |
| Extra A | Position F: registry-only override; no CLI flag for runtime override; `flow connections set/list/check` subcommands | §3.1 |
| Extra B | Multi-target via `targets=[ematix.target(...)]`; schedule per pipeline; independent tx; halt on first failure (opt-out via `continue_on_failure`); per-target connection override; re-run source per target for v0.1 | §3.4 |
| Extra C | `preview()` + `dry_run()` ship together as Phase 25; `rich` as runtime dep; compact mode for multi-target; dry-run always continues through every target | §3.5 |
| Extra D | `source_table=` + `column_map=` shipped together; reject unqualified table names; function body always wins if both set | §3.3 |
| Meta | Maximally inferred with `preview()` as the explainer | §5 |

---

## 9. Out of scope (for now)

- Async-pipeline DAG construction. ematix-flow is not Airflow; the user
  composes pipelines by writing more `@ematix.pipeline` functions and
  running them on independent schedules. DAG dependencies are a v0.2+
  concern.
- IDE plugins / language servers. The framework should produce types good
  enough that mypy/pyright work; bespoke tooling can wait.
- GUI for connection / pipeline management. CLI is the v0.1 surface.
- Auto-deriving the source query from the target schema (some FS tools do
  this for "feature DAGs"). Out of scope until users ask.
- Stage-source-once-and-fanout optimization for multi-target pipelines.
  Phase 24 ships re-run-per-target; optimize transparently in a later
  phase if profiling shows it matters.
- `column_map` chaining / transforms. If you need anything beyond a name
  rename, write the SELECT in the function body.

---

## 10. Summary

The current Python surface is fine but verbose. Five clean simplifications
all locked:

1. **Named connections** end the URL-in-source-code anti-pattern.
2. **Decorators with Annotated type hints** match modern Python style and
   roughly halve every example.
3. **Auto-detected merge keys** plus **UNIQUE constraint support** mean
   the user declares each fact about a table exactly once.
4. **Multi-target pipelines** support the "one source, many feature views"
   pattern without forcing users to repeat the source SQL.
5. **`preview()` + `dry_run()`** make "what would this do?" a one-line
   answer, and the colored output makes it pleasant.

None of this changes the Rust core. None of it sacrifices speed. All of it
lands as additive Python-only changes in Phases 21–25. The imperative API
(current `ManagedTable` subclassing + `pipeline.sync` + `_core.connect`)
is preserved as a fully-supported escape hatch.
