# ematix-flow — Python Ergonomics Plan

A planning doc parallel to `docs/ML_FEATURE_STORE_PLAN.md`. Reviews the
current Python surface against three goals:

1. **Shift complexity to the framework.** Rust does the heavy lifting; Python
   should look almost trivial.
2. **Decorator-based declarative API** as the primary path, matching modern
   Python style (FastAPI, Pydantic, dlt, Prefect 2 task flow).
3. **Auto-detect everything that's already in the DDL.** Merge keys,
   nullability, types — the user shouldn't repeat themselves.

Plus two specific gaps surfaced in design review:

- **Unique-constraint-driven upsert** for tables that don't use a single PK.
- **Connection / credential configuration** — currently the user threads
  raw URL strings through their code.

This doc is design-only. Implementation lands in Phases 21–24 if approved.

---

## 1. Where we are today

A complete Phase-12 example with everything we've shipped looks like this:

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

That's ~25 substantive lines. Most of it is plumbing the framework already
knows: the connection URL belongs in a config, `keys=("customer_id",)` is
already declared as `primary_key=True`, `compare_columns` is the same pattern
of "all non-key columns" most users want, and the imports + ceremony are
~8 lines on their own.

---

## 2. Where we want to be

The same pipeline, after this plan:

```python
# my_pipelines.py
from ematix_flow import flow, pk, BigInt, String, Text, TimestampTZ


@flow.table(schema="warehouse")
class CustomerDim:
    customer_id: BigInt = pk()
    email: String(256)
    name: Text | None
    last_seen: TimestampTZ


@flow.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    event_timestamp_column="last_seen",
)
def customer_sync(conn):
    return "SELECT id AS customer_id, email, name, last_seen FROM source.users"
```

~10 substantive lines. The user wrote: a class with type hints, one decorator
declaring it as a managed table, a function returning a SQL string, one
decorator wiring it up. Everything else (connection, key inference, compare
columns, target connection wiring, schedule registration) is the framework's
job.

---

## 3. Three areas of improvement

### 3.1 Connection / credential configuration

**Problem.** Users currently embed `_core.connect("postgres://...")` calls
in their code, which forces either hard-coded URLs (bad), `os.environ`
plumbing (verbose), or a shared `get_conn()` helper they have to write
themselves (boilerplate every project repeats).

**Proposal.** A small `ematix_flow.config` module that resolves connections
by name from three sources, in priority order:

1. **Environment variables.** `EMATIX_FLOW_DSN` for the default;
   `EMATIX_FLOW_DSN_<NAME>` (uppercased) for named connections. Simplest
   path; works with secret managers, CI runners, k8s secrets.
2. **Config file.** `~/.ematix-flow/connections.toml` or
   `./.ematix-flow.toml` (project-local takes precedence over user-global):
   ```toml
   [connections.default]
   dsn = "postgres://user:pass@host/db"

   [connections.warehouse]
   dsn = "${WAREHOUSE_DSN}"   # env-var interpolation supported
   ```
3. **Explicit url=** to `connect(...)` (current behavior, preserved).

User-facing:

```python
from ematix_flow import connect

conn = connect()                     # default; reads EMATIX_FLOW_DSN
conn = connect("warehouse")          # named; reads EMATIX_FLOW_DSN_WAREHOUSE
conn = connect(url="postgres://...") # explicit; current behavior
```

Backwards-compat shim: `_core.connect(url)` keeps working.

**CLI integration.** `flow run ... --connection warehouse` and a new
`flow connections list` / `flow connections check` for verifying that
configured connections actually reach a Postgres.

### 3.2 Decorator-based declarative API

**Proposal.** Add a top-level `flow` namespace exposing two decorators —
`flow.table` and `flow.pipeline` — that produce the same underlying
`ManagedTable` subclasses and `ScheduledPipeline` registrations the
imperative API uses today.

#### `@flow.table`

A class decorator that turns a Python class with type-annotated attributes
into a `ManagedTable`. Type hints carry the column type; helper sentinels
(`pk()`, `natural_key()`, `nullable()`) attach optional flags.

```python
@flow.table(schema="warehouse")
class CustomerDim:
    customer_id: BigInt = pk()
    order_date: Date = natural_key()  # joins customer_id in a UNIQUE constraint
    email: String(256)
    name: Text | None                  # `T | None` → nullable=True
    total: Numeric(12, 2) = nullable() # explicit alias for `T | None`
```

Decisions encoded in the decorator:
- Class name → `__tablename__` snake-cased (`CustomerDim` → `customer_dim`),
  override with `@flow.table(name="...")`.
- Type annotations become columns in declaration order (already supported
  by Python ≥ 3.7 dict ordering).
- `T | None` (or `Optional[T]`) infers `nullable=True`. Default is
  `nullable=False` — the safer choice.
- `pk()` marks a primary-key column.
- `natural_key()` marks a column as part of a composite UNIQUE constraint —
  framework collects all such columns into one tuple. Multiple separate
  unique constraints can be declared via `__unique_constraints__` (escape
  hatch for advanced cases).
- All `flow.table` kwargs (`schema=`, `mode=`, `event_timestamp_column=`,
  `ttl=`, etc.) become class-level defaults that `flow.pipeline` reads.

Why decorators rather than expanding the existing `class X(ManagedTable)`?
Two reasons:

1. The annotation form lets us drop the `Column(...)` wrappers entirely.
   `customer_id: BigInt = pk()` is shorter than `customer_id =
   Column(BigInt(), primary_key=True)` and reads like a stdlib `dataclass`
   or Pydantic model — already familiar to Python users.
2. `T | None` for nullability is idiomatic; users don't need to learn our
   `nullable=` kwarg.

We keep `class X(ManagedTable)` working for advanced users who want full
control or programmatic class construction.

#### `@flow.pipeline`

A function decorator that combines target + source + sync + schedule into
one declaration, building on Phase 12's `@pipeline.register`. The decorated
function returns the source SQL (or a `Source` for advanced cases); the
framework handles connect/ensure/sync.

```python
@flow.pipeline(
    target=CustomerDim,
    schedule="0 * * * *",
    mode="scd2",
    event_timestamp_column="last_seen",
)
def customer_sync(conn):
    return "SELECT id AS customer_id, email, name, last_seen FROM source.users"
```

Behavior:
- Decorator inspects the function signature. A single `conn` param means
  "give me the resolved connection"; two params `(src_conn, tgt_conn)` mean
  cross-DB. Zero params means "use the default connection for both".
- Function returns:
  - A `str` → wrap as `Source.postgres_query(conn, returned)`.
  - A `Source` → use directly (for `postgres_table` etc).
  - A `dict` already → assume the user did their own `pipeline.sync` and
    just return it (escape hatch for fully imperative work).
- Decorator registers the wrapped function in `_REGISTRY` (Phase 12), so
  `flow run` / `flow run-due` find it.
- Connection naming: `connection="warehouse"` kwarg picks the named
  connection from §3.1's resolver.

Cross-DB example:

```python
@flow.pipeline(
    target=CustomerDim,
    schedule="*/5 * * * *",
    mode="merge",
    source_connection="oltp",
    target_connection="warehouse",
)
def customer_sync(src_conn, tgt_conn):
    return "SELECT id AS customer_id, email FROM public.customers"
```

The framework wires `Source.postgres_query(src_conn, ...)` and
`target_connection=tgt_conn` automatically.

### 3.3 Auto-detect merge keys (PK + UNIQUE)

**Problem.** Today `pipeline.sync(mode="merge", keys=...)` makes the user
restate keys that are already declared on the table. Worse, tables backed
by a UNIQUE constraint (no PK, or PK is a synthetic surrogate) can't
participate in merge at all unless the user passes `keys=` explicitly.

**Proposal.** Two pieces:

1. **Allow declaring `__unique_constraints__`** on the table. Each entry is
   a tuple of column names. The framework:
   - Emits a Postgres `UNIQUE (col1, col2, ...)` clause in the
     `CREATE TABLE` plan.
   - Reflects it back via `read_existing_columns` when checking drift
     (Phase 4 needs a small extension for this — see Phase 22 below).
   - Uses the first declared unique constraint as the default merge key
     when `keys=` is not specified.
2. **`natural_key()` sentinel** in the decorator API — sugar that adds a
   column to a single composite unique constraint without writing
   `__unique_constraints__` by hand:
   ```python
   @flow.table(schema="warehouse")
   class CustomerOrder:
       id: BigInt = pk()                       # synthetic surrogate
       customer_id: BigInt = natural_key()     # joins...
       order_date: Date = natural_key()        # ...this in a UNIQUE constraint
       total: Numeric(12, 2)
   ```
   becomes equivalent to declaring `__unique_constraints__ =
   [("customer_id", "order_date")]`.

**Resolution order** for "what columns does merge upsert against?":

1. `keys=` kwarg passed to `pipeline.sync` — user override, always wins.
2. `__merge_keys__` dunder on the class — explicit class-level default.
3. First entry of `__unique_constraints__` (or the `natural_key()`-marked
   columns) — natural key takes precedence over surrogate PK.
4. `__primary_keys__` (current default, derived from `primary_key=True`).

If none of these resolve to a non-empty list, raise with an actionable
error pointing the user at the four options above.

Same resolution applies to SCD1/SCD2 (which already require keys for the
ON CONFLICT / hash-join targets).

**Reflection caveat.** Step 3 requires the drift comparator to recognize
UNIQUE constraints — currently `compare_table` only tracks PK membership.
Phase 22 expands `ReflectedColumn` and `compare_table` to track unique
constraints too.

---

## 4. Putting all three together

Side-by-side, picking a moderately complex SCD2 feature view:

**Today (Phase 15):**

```python
from datetime import timedelta
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

**After this plan:**

```python
from ematix_flow import flow, pk, BigInt, Boolean, Date, Numeric, TimestampTZ


@flow.table(schema="features", name="user_purchase_features_v1")
class UserPurchaseFeatures:
    user_id: BigInt = pk()
    period_start: Date = pk()
    total_spend: Numeric(12, 2) | None
    order_count: BigInt | None
    is_subscriber: Boolean
    last_event_ts: TimestampTZ


@flow.pipeline(
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

The function body went from ~15 lines of orchestration to one SELECT. Keys
are inferred from `pk()`. Compare columns default to "every non-key
non-metadata column". Connections are named, not URL-strings. Same
under-the-hood behavior — same SCD2 plan, same atomicity, same speed.

---

## 5. Phased rollout

Each phase TDD-style with both Rust and Python tests; same discipline as
Phases 0–15.

### Phase 21 — Connection registry (≈0.5–1d)

- `ematix_flow.config` module: `connect(name="default", url=None)` resolves
  via env vars → config file → explicit url.
- `~/.ematix-flow/connections.toml` parser (TOML stdlib in py3.11+).
- `flow connections list` and `flow connections check` subcommands.
- Existing `_core.connect(url)` stays as the low-level escape hatch; the
  high-level `connect()` wraps it.
- Tests: env-var resolution, config-file precedence, missing-name error,
  CLI subcommand smoke tests.

### Phase 22 — UNIQUE constraints in DDL + drift (≈1–1.5d)

- `TableSpec` gains `unique_constraints: Vec<Vec<String>>` (default empty).
- `create_table_sql` emits `UNIQUE (col, ...)` clauses.
- `read_existing_columns` is supplemented by `read_existing_unique_constraints`
  (information_schema.table_constraints + key_column_usage).
- `compare_table` learns a `UniqueConstraintMissing` / `UniqueConstraintExtra`
  variant of `Difference`.
- `ManagedTable` gains `__unique_constraints__: tuple[tuple[str, ...], ...]`
  dunder. Existing classes ignore it (default empty).
- Tests: DDL emission, reflection round-trip, drift detection across all
  combinations.

### Phase 23 — Auto-detect merge keys (≈0.5d)

- `pipeline.sync` resolution order: explicit `keys=` → `__merge_keys__` →
  `__unique_constraints__[0]` → primary keys → error.
- Same resolution for SCD2 keys.
- Documented in `docs/IMPLEMENTATION_PLAN.md`.
- Tests: each level of the resolution order works; clear error when none
  resolve.

### Phase 24 — `flow.table` and `flow.pipeline` decorators (≈1–2d)

- `ematix_flow.flow` namespace.
- `flow.table` class decorator using `__init_subclass__` over a
  hidden `_DecoratedTable` base (or a metaclass — TBD; class decorator on
  a plain class is probably cleanest because it sees annotations).
- `pk()`, `natural_key()`, `nullable()` sentinels.
- `flow.pipeline` function decorator:
  - Inspects the wrapped function's signature to decide same-DB vs
    cross-DB.
  - Resolves connection names via Phase 21.
  - Returns a wrapper that calls `pipeline.sync(...)` with the right
    arguments and registers it via `pipeline.register(...)`.
- All existing Phase 1–15 mechanics continue to work; decorators are
  pure sugar producing the same `ManagedTable` subclasses and
  `ScheduledPipeline` registrations.
- Tests: the side-by-side example from §4 produces an identical normalized
  spec to the pre-decorator version; both styles resolve to the same SQL
  plan; decorator-level errors (missing `pk()`, conflicting `pk()` and
  `natural_key()`) raise at class-decoration time, not at sync time.

---

## 6. Open questions

- **`pk()` vs `Annotated[BigInt, pk]`.** The `Annotated` form is more "type-
  checker friendly" but uglier. The `default-value sentinel` form (`pk()`)
  reads better. Recommend `pk()` and let mypy stay happy via a tiny stub.
- **Composite PK in decorators.** Two columns with `pk()` form a composite
  PK in declaration order. No need for `pk(order=...)` — declaration order
  is enough.
- **`String(256)` as a type annotation.** This works (it's a class instance,
  Python doesn't object), but mypy will complain. Provide a `Varchar[256]`
  generic alias for type-checker happiness. Both forms accepted.
- **Decorator class syntax conflicts with `dataclass`.** Users who want
  both should pick one. Document that `@flow.table` is the right one for
  ematix-flow tables.
- **`flow.pipeline` running synchronously vs returning a deferred handle.**
  Today `pipeline.sync` is synchronous. Decorator preserves that. v0.2
  could add `@flow.pipeline(async_mode="background")` returning a future,
  but not now.
- **Naming.** `flow.table` collides with the `flow` CLI in tutorials
  (`from ematix_flow import flow` then `flow.table(...)` looks like the
  CLI). Alternatives: `ef` (cryptic), `etl` (overloaded), `ematix` (long).
  Recommend keeping `flow` and disambiguating in docs.

---

## 7. Out of scope (for now)

- Async-pipeline decorators / DAG construction. ematix-flow is not Airflow;
  the user composes pipelines by writing more `@flow.pipeline` functions
  and running them on independent schedules. DAG dependencies (run B after
  A succeeds) is a v0.2+ concern.
- IDE plugins / language servers. The framework should produce types good
  enough that mypy/pyright work; bespoke tooling can wait.
- GUI for connection / pipeline management. CLI is the v0.1 surface.
- Auto-deriving the source query from the target schema (some FS tools do
  this for "feature DAGs"). Out of scope until users ask.

---

## 8. Summary

The current Python surface is fine but verbose. Three clear simplifications:

1. **Named connections** end the URL-in-source-code anti-pattern.
2. **Decorators with type-annotated tables** cut every example to roughly
   half its current size and match modern Python style.
3. **Auto-detected merge keys** plus **UNIQUE-constraint support** mean the
   user declares each fact about a table exactly once.

None of this changes the Rust core. None of it sacrifices speed. All of it
lands as additive Python-only changes in Phases 21–24, with the imperative
API (current `ManagedTable` subclassing + `pipeline.sync` + `_core.connect`)
preserved as a fully-supported escape hatch.
