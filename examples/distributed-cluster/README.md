# Distributed-cluster example (Σ.B PR 3)

Three `flow-worker` peer pods + a coordinator. Demonstrates the
`engine = "distributed"` SQL-transform path across multiple ematix-
flow processes.

Plan: [`docs/PHASE_SIGMA_PLAN.md`](../../docs/PHASE_SIGMA_PLAN.md) Σ.B.
Spike: [`docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`](../../docs/PHASE_SIGMA_B_TRAIT_SPIKE.md).

## What this directory contains

| File | Purpose |
|---|---|
| `Dockerfile.worker` | Builds the `flow-worker` binary into a slim Debian image |
| `docker-compose.yml` | 3-worker bridge-network cluster on ports 50051/52/53 |
| `pipeline.toml` | Sample coordinator config — Kafka → distributed SQL → Postgres |

## Bring up the cluster

```sh
docker compose -f examples/distributed-cluster/docker-compose.yml up --build
```

First build is ~3–5 min (Rust workspace compile). Subsequent
`docker compose up` reuses the cached image.

The three workers listen on:

```
http://localhost:50051
http://localhost:50052
http://localhost:50053
```

## Run a coordinator against the cluster

Any process that constructs a `DistributedBackend` (or routes a
`LazySqlTransform` through `engine = "distributed"`) acts as the
coordinator. Three options:

### 1. `flow consume` with the sample TOML

```sh
flow consume examples/distributed-cluster/pipeline.toml
```

Replace `[source]` / `[target]` with your own backends; the
`[transform] engine = "distributed"` + `peers = [...]` block stays
identical.

### 2. Rust integration

```rust
use ematix_flow_core::backend::DistributedConfig;
use ematix_flow_distributed::DistributedSqlTransform;

let xform = DistributedSqlTransform::open(
    "SELECT user_id, SUM(amount) FROM source GROUP BY user_id",
    DistributedConfig {
        peers: vec![
            "http://localhost:50051".into(),
            "http://localhost:50052".into(),
            "http://localhost:50053".into(),
        ],
    },
)?;
```

### 3. Native run (no Docker)

If you don't want Docker, run three `flow-worker` instances
directly:

```sh
cargo run --release -p ematix-flow-distributed --bin flow-worker -- --port 50051 &
cargo run --release -p ematix-flow-distributed --bin flow-worker -- --port 50052 &
cargo run --release -p ematix-flow-distributed --bin flow-worker -- --port 50053 &
```

The coordinator config stays the same.

## Tear down

```sh
docker compose -f examples/distributed-cluster/docker-compose.yml down
```

## Verifying the cluster works without Docker

The integration test
`crates/ematix-flow-distributed/tests/cross_pod.rs` spawns N
in-process workers on free localhost ports and runs SUM(1..10)
through the distributed plan, asserting the result is 55. Same
code path you'd hit with the docker-compose stack — just no Docker
involved.

```sh
cargo test -p ematix-flow-distributed --test cross_pod
```

## TPC-H benchmarks against the compose stack

> **What you get.** Multi-process distributed numbers on your local
> machine. The "workers" are containers sharing your kernel, CPU,
> memory, and disk; the network between them is loopback. These
> numbers are honest as a regression baseline and as evidence that
> the distributed planner parallelises correctly — they are *not*
> cross-host scaling numbers and should not be cited as such.
> Real cross-host numbers need network-separated machines (homelab
> k3s across multiple boxes, or rented bare-metal).

### 1. Generate Parquet (once per scale factor)

```sh
# SF=1 (~1 GB, seconds)
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1  --out examples/tpch/data/sf1

# SF=10 (~10 GB, ~minute on M-series; 32 GB RAM recommended)
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 10 --out examples/tpch/data/sf10
```

`examples/tpch/data/` is git-ignored. The compose stack mounts it
read-only at `/data` inside each worker, so workers see exactly
the same Parquet paths as the host coordinator.

### 2. Bring up the cluster

```sh
docker compose -f examples/distributed-cluster/docker-compose.yml up --build -d
```

Three workers on `localhost:50051` / `:50052` / `:50053`.

### 3. Run the bench

The host coordinator must reference Parquet at the same path the
workers see. The cleanest fix is a symlink so `/data/sf<N>` exists
on both sides:

```sh
sudo ln -s "$PWD/examples/tpch/data" /data   # one-time
```

Then point the bench at the compose stack:

```sh
EMATIX_DISTRIBUTED_PEERS=http://localhost:50051,http://localhost:50052,http://localhost:50053 \
TPCH_DATA_DIR=/data/sf10 \
    cargo bench -p ematix-flow-distributed --bench tpch_distributed
```

Two configurations run per query (Q1 / Q3 / Q6 / Q19):

| Group label | What it measures |
|---|---|
| `tpch_sf10_distributed_of_one`        | Coordinator-only, no peers — trait-surface overhead |
| `tpch_sf10_distributed_external_3_peers` | Plan fans out across the 3 compose workers |

Compare against the single-node bench (`cargo bench -p
ematix-flow-core --bench tpch`) for a 1-process baseline on the
same hardware.

### 4. Tear down

```sh
docker compose -f examples/distributed-cluster/docker-compose.yml down
```

## Known limitations (Σ.B follow-ups)

- **Static peer membership.** `StaticWorkerResolver` carries a
  fixed `Vec<Url>`. Dynamic membership (k8s pods discovered via DNS,
  service-mesh integration) is a Σ.B follow-up.
- **No authentication.** The worker tonic endpoint is plaintext
  HTTP/2. Production deployments should put a TLS-terminating
  ingress (envoy, nginx) in front + restrict at the network layer.
- **No window/join wrapping.** `engine = "distributed"` rejects
  `[transform.window]` + `[transform.join]` at config-load. Rooting
  the windowed/joined wrappers' types in `Arc<dyn BatchTransform>`
  is a Σ.B follow-up.
- ~~**No lookups.**~~ ✓ Lookups now ship to peer workers via Arrow
  Flight as part of the distributed plan (broadcast joins).
  Configure `[transform.lookups.<name>]` blocks alongside
  `engine = "distributed"`; they Just Work.
