# Σ.B spike — connector trait shape

**Question.** Σ.B requires the connector trait to be serializable across
Arrow Flight (executors instantiate connectors lazily on remote nodes
from URL-only config). Σ.A2 and Σ.D have follow-on needs that, if
addressed in a later refactor, mean two breaking changes instead of
one. What's the unified trait shape that absorbs all three needs in a
single rework?

**Output.** Concrete proposal + 6 open questions to resolve before
opening Σ.B PR 1.

---

## What each sub-phase needs from the trait

### Σ.A2 — dialect translator

**No new methods on `Backend`.** The translator sits *above* the
Backend dispatch — it rewrites SQL → SQL before the SQL ever reaches
`Backend::read_arrow_stream` / `Backend::execute`. The translator is a
free function (`dialect::translate(sql, from)`). Trait shape unchanged.

### Σ.B — Ballista distributed batch

Three needs, all centered on shipping a backend instance to a remote
executor:

1. **`'static` bound.** Today: `pub trait Backend: Send + Sync`. To
   put backend instances inside `Arc<dyn Backend>` and ship them via
   the Ballista scheduler → executor channel, they must outlive the
   call (`'static`). Most impls already meet this transitively (`Arc<Mutex<…>>`,
   `Arc<Pool>`, owned `String`s) — adding the bound is non-breaking
   for in-tree impls.
2. **Serializable config.** Today: backends carry live state
   (`Arc<Mutex<ConsumerSession>>`, `deadpool_postgres::Pool`,
   `aws_sdk_kinesis::Client`). These can't ship over the wire. Need
   a separate **config blob** that's `Serialize + DeserializeOwned`
   and from which a fresh backend instance can be reconstructed on
   the executor side.
3. **Lazy resource init.** Once a backend lands on an executor, it
   shouldn't open pools / consumer sessions until the first call.
   Today's `KafkaBackend::consumer_session: Arc<Mutex<ConsumerSession>>`
   is already lazy; `PgPool` opens eagerly in the constructor.
   Refactor makes laziness uniform across all 11 backends.

### Σ.D — distributed streaming

Two follow-on needs that should land in the Σ.B trait shape so we
don't break it again:

4. **Per-key partitioning hint.** For windowed joins distributed
   across executors, the framework needs to ask "if you read this
   query, can you produce results partitioned by key K?" Today's
   read API is unpartitioned. Σ.D needs `read_arrow_stream_partitioned`
   or a `partitioning_hint(query) -> Option<KeyRange>` method.
5. **Distributed state hooks.** Σ.D extends the existing `seek_to` /
   `offset_snapshot` (Phase 39.5a) into per-key state shipped across
   shuffle boundaries. The trait shape for this lands as part of
   Σ.D itself, but the surface should already be plumbing-compatible
   with Σ.B's serializable-config model.

---

## Current trait at a glance

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    fn dialect(&self) -> Dialect;
    fn connection_info(&self) -> ConnectionInfo;
    fn dsn(&self) -> Option<String>;
    fn as_postgres(&self) -> Option<&PgPool> { None }

    async fn ping(&self) -> Result<(), BackendError>;
    async fn execute(&self, statement: &str) -> Result<u64, BackendError>;
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError>;
    async fn write_arrow_stream(&self, target: &TargetTable, ..., mode: WriteMode) -> Result<u64, BackendError>;

    async fn run_append(&self, ..., source_backend: Option<&dyn Backend>, ...) -> Result<...>;
    async fn run_truncate(&self, ..., source_backend: Option<&dyn Backend>, ...) -> Result<...>;
    async fn run_merge(&self, ..., source_backend: Option<&dyn Backend>, ...) -> Result<...>;
    async fn run_scd2(&self, ..., source_backend: Option<&dyn Backend>, ...) -> Result<...>;

    async fn commit_offsets(&self) -> Result<(), BackendError> { Ok(()) }
    fn supports_seek_to(&self) -> bool { false }
    async fn seek_to(&self, offset_bytes: &[u8]) -> Result<(), BackendError> { ... default error }
    async fn offset_snapshot(&self) -> Result<Option<Vec<u8>>, BackendError> { Ok(None) }
}
```

11 impls: `PostgresBackend` (in `backend.rs`), `KafkaBackend`,
`MySqlBackend`, `SqliteBackend`, `DuckDbBackend`, `DeltaBackend`,
`ObjectStoreBackend`, `PubSubBackend`, `RabbitMqBackend`,
`KinesisBackend`, plus `streaming.rs`'s test stub.

---

## Proposed shape

The split: a single `Backend` operation interface (today's, plus a
`'static` bound), a **separate** `BackendConfig` serializable struct
per backend, and a `BackendFactory` registry that bridges the two.

```rust
// 1. Operation surface. Same as today, plus 'static.
//    Note `as_postgres` becomes deprecated — Σ.B PR 1 removes it.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    fn dialect(&self) -> Dialect;
    fn connection_info(&self) -> ConnectionInfo;
    fn dsn(&self) -> Option<String>;

    /// NEW: serializable form of this backend's constructor + builder
    /// state. Must NOT contain live connections / pools / file
    /// descriptors. Used by Ballista to ship the backend to executors.
    fn config(&self) -> BackendConfig;

    async fn ping(&self) -> Result<(), BackendError>;
    async fn execute(&self, statement: &str) -> Result<u64, BackendError>;
    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError>;
    async fn write_arrow_stream(&self, target: &TargetTable, ..., mode: WriteMode) -> Result<u64, BackendError>;

    // run_append / run_truncate / run_merge / run_scd2 unchanged.
    // commit_offsets / supports_seek_to / seek_to / offset_snapshot unchanged.

    /// NEW (Σ.D-ready): can this backend partition reads by key?
    /// `None` means single-stream only. Default `None` means existing
    /// backends opt in by override; Σ.B can ship without any override.
    fn partitioning_hint(&self, _query: &str) -> Option<KeyPartitioning> { None }
}

// 2. Serializable config. Single tagged enum so `serde_json::Value`
//    is the wire format (matches the existing TOML config shape;
//    JSON is a strict superset).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackendConfig {
    Postgres(PostgresConfig),
    MySql(MySqlConfig),
    Sqlite(SqliteConfig),
    DuckDb(DuckDbConfig),
    Kafka(KafkaConfig),
    Kinesis(KinesisConfig),
    PubSub(PubSubConfig),
    RabbitMq(RabbitMqConfig),
    Delta(DeltaConfig),
    ObjectStore(ObjectStoreConfig),
}

// One concrete config per backend. Example:
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: Option<String>,
    pub auth: AuthConfig,           // already a serde-shaped enum
    pub payload_format: KafkaPayloadFormat,
    pub schema_registry_url: Option<String>,
    pub schema_registry_basic_auth: Option<SrBasicAuthConfig>,
    pub message_key_column: Option<String>,
    pub batch_config: KafkaBatchConfig,
    pub delivery_semantics: KafkaDeliverySemantics,
}

// 3. Reverse direction: config → backend instance. Doesn't fit on
//    the Backend trait (would need `Self: Sized`, breaking dyn dispatch).
//    Free function dispatching on the config tag.
pub fn backend_from_config(cfg: BackendConfig) -> Result<Arc<dyn Backend>, BackendError> {
    match cfg {
        BackendConfig::Postgres(c) => Ok(Arc::new(PostgresBackend::open(c)?)),
        BackendConfig::Kafka(c) => Ok(Arc::new(KafkaBackend::open(c)?)),
        // ... 10 arms
    }
}

// 4. Each backend's constructor takes its config struct directly.
//    No more builder methods on the public surface — config is the
//    canonical input. (Builders stay as private convenience wrappers
//    inside tests, but production code constructs from config.)
impl KafkaBackend {
    pub fn open(cfg: KafkaConfig) -> Result<Self, BackendError> {
        Ok(Self {
            cfg,                                // own the config
            consumer_session: Arc::new(Mutex::new(ConsumerSession::not_yet_opened())),
            producer_session: Arc::new(Mutex::new(ProducerSession::not_yet_opened())),
        })
    }
}
```

The config blob is the canonical wire form. PG-side: `PostgresConfig {
dsn: String, ssl_mode: SslMode, ... }`. Kafka-side: `KafkaConfig`
above. Anything that today's `with_*` builder methods set lives in
the config struct.

### Why this shape

- **One breaking change.** All 11 backends migrate to constructor-
  takes-config + `fn config()` returns the same struct. After Σ.B
  PR 1, no public builder methods (`with_*`) exist on backend types
  — the config struct is the API. Π-block users construct
  `Backend` from a config they got from TOML or built in-process;
  Ballista users serialize the config across the wire.
- **Σ.A2 is unaffected.** Translator runs above the trait.
- **Σ.D's per-key partitioning is plumbed in.** Default returns
  `None`; Σ.D adds overrides for Kafka (partition-keyed reads) +
  Postgres (range-partitioned reads) without breaking the trait
  again.
- **Lazy init falls out naturally.** The constructor stores the
  config; pools/sessions construct on first method call. Every
  backend uses the same pattern.
- **Credentials story is unchanged.** Today config carries DSNs +
  passwords. Tomorrow same — over Arrow Flight (TLS) instead of
  in-process. Threat model: same as Spark shipping JDBC URLs to
  Spark executors, which is industry-standard.

### Migration cost — per backend

Each of the 11 backends:

1. Define a `<Backend>Config` struct that's `Serialize + Deserialize`.
   Field-by-field copy of today's struct; replace live state
   (`Arc<Mutex<…>>`, pools) with `not_yet_opened()` placeholders.
2. Refactor constructor: `Backend::open(cfg: Config) -> Result<Self>`.
   Builder methods (`with_sasl_plain`, etc.) reduce to mutators on the
   `Config` struct *before* `::open`.
3. Implement `Backend::config(&self) -> BackendConfig`. Just clones
   the stored `Config`.
4. Add a match arm to `backend_from_config`.
5. Update existing tests that call builders directly — they
   construct a `Config` first.

Estimated **2 days/backend × 11 = ~3 weeks**, but mostly mechanical.
PR strategy below stages it across 4 commits to keep diffs reviewable.

### Σ.B PR 1 commit plan

a. **Trait + dispatch surface.** Add `BackendConfig` enum, `fn config()`
   trait method (default `unimplemented!()` for now), `backend_from_config`
   stub. No backend impls touched yet — this commit only changes
   `backend.rs` + adds the tagged enum.

b. **Migrate database backends.** PostgresBackend, MySqlBackend,
   SqliteBackend, DuckDbBackend. These are the simplest (no live
   sessions; just pools). Convert each to `Config` shape; populate
   `fn config()`; add to `backend_from_config`.

c. **Migrate object-store + delta.** ObjectStoreBackend, DeltaBackend.
   Configs include S3 credentials.

d. **Migrate streaming backends.** KafkaBackend, KinesisBackend,
   PubSubBackend, RabbitMqBackend. These are the heaviest because
   they carry live session state — but the laziness pattern is
   already mostly in place. KafkaBackend is the longest commit
   (largest backend file in the workspace).

After commit (d), `Backend::config()` is implemented for every backend
and `backend_from_config(cfg)` round-trips for every kind.

### Acceptance test for the refactor

A single integration test that, for every backend kind, exercises:

```rust
let original = backend_kind_specific_constructor();
let cfg = original.config();
let json = serde_json::to_string(&cfg)?;
let cfg_round_trip: BackendConfig = serde_json::from_str(&json)?;
let recovered = backend_from_config(cfg_round_trip)?;
assert_eq!(original.dialect(), recovered.dialect());
assert_eq!(original.connection_info(), recovered.connection_info());
recovered.ping().await?;  // verify live ops still work after round-trip
```

This is the failing test driving the refactor (TDD). One test
function per backend kind; CI matrix covers each.

---

## Open questions to resolve before Σ.B PR 1

1. **Wire format: JSON or postcard?** JSON is human-readable, debuggable,
   matches today's TOML config shape, and is universal across the
   Arrow Flight metadata channel. Postcard is more compact (~2× smaller)
   but binary-only. **Recommend JSON** — config blobs are tiny (a
   few KB), debug-ability matters more than bytes-on-wire.

2. **Where does the credential live in the config blob?** Two options:
   - **(a) Inline (today's pattern).** `PostgresConfig { dsn: "postgres://user:pass@host" }`.
     Simple; the executor process needs to handle the password the
     same way the driver does today.
   - **(b) Indirect via env-var ref.** `PostgresConfig { dsn: "postgres://user:${PASSWORD}@host" }`,
     resolved on the executor with its own environment. Aligns with
     Π.5 deprecation of inline credentials.
   - **Recommend (a) for Σ.B PR 1.** Keep it simple; lock down via
     TLS at the Arrow Flight transport layer. Add (b) as a Σ.B follow-
     up once a user requests it. Document the threat model in
     `docs/SECURITY.md`.

3. **`as_postgres()` escape hatch — keep, deprecate, or remove?**
   Today: `fn as_postgres(&self) -> Option<&PgPool>` lets the PG↔PG
   COPY BINARY fast path peek under the trait. **Recommend remove
   in PR 1.** The cross-DB Arrow path is already production-tested
   (Phase 30b/c); the COPY BINARY fast path is a micro-optimization.
   Removing it eliminates a serialization snag (you can't ship a
   `PgPool` reference over the wire) and forces the universal path,
   which is what Ballista will use anyway.

4. **Out-of-tree custom backends — registry extension API?**
   Today no user can add a backend without a fork. After Σ.B,
   `backend_from_config` is closed (match-on-tag). Two options:
   - **(a) Closed registry, document forking.** Simplest; users
     who need custom backends fork. Aligns with how `parquet`-rs and
     `arrow` handle out-of-tree extensions.
   - **(b) Open registry via `inventory` or `linkme` crate.**
     Compile-time backend registration; `BackendConfig` becomes
     `serde_json::Value` + a registered factory function.
   - **Recommend (a) for Σ.B; revisit in Σ.D.** Ship value first;
     adding the open registry later is non-breaking (the closed
     match becomes a lookup against a default registry, third
     parties register additions).

5. **`partitioning_hint` shape — bake now or defer?** Σ.D needs
   per-key partitioning info, but the exact shape (range partitioning,
   hash partitioning, partition-by-Kafka-partition?) isn't locked
   yet. **Recommend default-`None` in Σ.B PR 1**, override schema
   designed in Σ.D. This adds a method but doesn't commit to its
   semantics.

6. **`run_append` / `run_merge` / `run_scd2` `source_backend` parameter
   — ship-of-Theseus over Ballista?** These methods take
   `Option<&dyn Backend>` for the source. If a strategy is dispatched
   to a Ballista executor, the source backend is also a `dyn Backend`
   on that executor. Ergonomic question: do strategy methods become
   "backend pair" methods (`run_append(target_cfg, source_cfg, ...)`)
   so the executor reconstructs both sides from config blobs? Or
   does the scheduler reconstruct on its side and ship live
   `Arc<dyn Backend>`s? **Recommend the latter** — the scheduler
   constructs both backends from config, then dispatches the strategy
   over Ballista with the live references. Keeps the strategy
   interface unchanged.

---

## What this spike does NOT decide

- **Wire-protocol details for Arrow Flight beyond the config blob.**
  Σ.B PR 2 (the `ematix-flow-ballista` crate) handles RPC plumbing.
- **Ballista version pin.** Σ.B PR 2.
- **Streaming distribution.** That's Σ.D.

---

## Recommendation

Lock answers to all 6 questions above (or accept the recommended
defaults), then open Σ.B PR 1 with the 4-commit plan. Estimated
effort: **3 weeks for one engineer**, mostly mechanical migration of
11 backends to the `Config` shape. After PR 1 lands, Σ.B PR 2
(`ematix-flow-ballista` crate) can start in parallel with Σ.A1 / Σ.A2
work since it doesn't touch backend internals further.

Next: confirm the recommended defaults and queue Σ.B PR 1 as the
first concrete piece of distributed-compute work after v0.1.0
publishes.
