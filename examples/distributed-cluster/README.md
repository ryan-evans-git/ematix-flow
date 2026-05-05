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
- **No lookups.** `engine = "distributed"` + `transform.lookups` is
  rejected (would need cross-pod table registration).
