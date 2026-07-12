# Delivery semantics & recovery

This is the reference for **what ematix-flow guarantees under failure** —
crashes, restarts, rebalances, and slow or unavailable targets. If you're
evaluating ematix-flow for a production streaming pipeline, read this
before you design around it, and validate the behavior that matters to you
against your own broker and target.

ematix-flow is **beta** (`0.14.x`). The semantics below reflect the
current implementation in the Kafka backend and streaming runtime. They
are stable and intended, but the API around them may still evolve before
1.0.

## TL;DR

| Concern | Behavior |
| --- | --- |
| Default guarantee | **Exactly-once when the pipeline is eligible, at-least-once otherwise** (`delivery = "auto"`). Eligible = one transactional SQL target (SQLite, DuckDB, Postgres, MySQL), no CDC apply mode, no stateful transform. |
| Exactly-once mechanism | The batch **and** the source offsets commit in **one target-database transaction** (`_ematix_offsets` table in the target). A crash can re-deliver from the broker, but recovery seeks past everything the target already committed — zero duplicate rows. |
| Demanding it | `delivery = "exactly_once"` on an ineligible pipeline is a **hard error at startup** listing every blocker. It never silently downgrades. |
| Forcing the old contract | `delivery = "at_least_once"` restores the historical behavior even on an eligible pipeline. |
| Ineligible pipelines | **At-least-once**: source offsets are committed *after* the target write; a crash between them re-delivers → make target writes idempotent (`merge`/`scd2`). |
| Kafka-native exactly-once | Still available: Kafka producer sinks via Kafka transactions, and Kafka → Kafka read-process-write EOS via `send_offsets_to_transaction`. |
| Offset commits | **Manual**, framework-controlled — not `enable.auto.commit`. Under resolved exactly-once, the broker commit is advisory; the target is the authority. |
| Poison / failing records | Routed to a **DLQ** (topic or table) after `dlq_max_attempts`; `transform_on_error` = `fail` / `drop` / `dlq`. |
| Rebalance | Cooperative or eager protocol; recovered offsets are applied into the assignment, falling back to the broker-committed offset. |
| Stalled pipeline | Caught by a **freshness SLO** (run-history based, target-agnostic), not by delivery. |

## Exactly-once to transactional SQL targets (Σ.XO)

When a pipeline writes to a **single transactional SQL target** —
SQLite, DuckDB, Postgres, or MySQL today — the write path commits each
batch **and** the post-batch source offsets in **one database
transaction**:

1. `BEGIN`
2. insert the batch rows into the target table
3. upsert one row per source into `_ematix_offsets(pipeline_id,
   source_id, offsets_json, updated_at)` (created lazily inside the same
   transaction, in the connection's default schema)
4. `COMMIT`

Either everything in steps 2–3 becomes durable together, or none of it
does. On restart, the runtime reads `_ematix_offsets` back from the
target and **seeks every source to the committed position before the
first read** — these target-committed offsets are authoritative over
both broker-committed and StateStore-recovered offsets. The broker can
re-deliver as much as it wants; recovery skips everything the target
already owns.

The source-side offset commit still happens after each iteration, but it
is **advisory** under resolved exactly-once: its failure logs a warning
instead of failing the pipeline, because it no longer carries the
guarantee.

### Eligibility

`delivery = "auto"` (the default) resolves to exactly-once when ALL of
these hold, and to at-least-once otherwise:

- **Exactly one target**, whose backend supports atomic batch+offsets
  writes (SQLite, DuckDB, Postgres, MySQL). Fan-out stays at-least-once
  in this release: N targets would need N offset tables plus a
  reconciliation rule for targets that disagree after a partial fan-out
  failure.
- **No CDC apply mode** — CDC has its own per-event idempotency gate and
  does not route through this write path.
- **No stateful transform** (windows/sessions/streaming joins). This
  exclusion is load-bearing, not conservatism: offsets stamped at write
  time would cover rows still buffered in window state, so recovery
  would seek past rows that never reached the target — data loss, which
  is worse than a duplicate.
- **Every source supports seeking** (Kafka does). A source that cannot
  seek cannot be recovered to the target's position.

`delivery = "exactly_once"` demands the guarantee: if any condition
above fails, the pipeline errors **at startup** with the complete list
of blockers. It never runs with a silently weaker guarantee.

### What it costs

The offsets upsert rides the same transaction as the batch insert — one
extra statement per batch, no extra round-trip protocol. There is no
two-phase commit and no coordinator: the target database's own
transaction is the whole mechanism.

## At-least-once (ineligible pipelines, or by request)

The streaming loop reads a batch from the source, runs the transform,
writes to the target, and **only then** commits the source offset. The
commit position is the last consumed offset + 1, per the Kafka protocol.

The consequence is the standard at-least-once contract:

- If the process dies **after** the target write but **before** the
  offset commit, those records are read again on restart and re-written.
- Therefore **duplicates are possible**, and the operator is responsible
  for making the effect of a re-write harmless.

### Making writes idempotent

Pair at-least-once delivery with an idempotent load mode so a re-delivery
is a no-op instead of a double-insert:

- **`merge` (SCD1)** — upsert on the merge key. A re-delivered row
  overwrites itself; net effect is exactly-once *state*.
- **`scd2`** — versioned upsert with merge-key resolution and watermarks.
- Use a **deterministic key** derived from the event (not an
  auto-increment or wall-clock value) so the same event always resolves to
  the same target row.

`append` mode does **not** de-duplicate — only use it when the pipeline
resolved exactly-once, duplicates are acceptable, or they are removed
downstream.

## Kafka-native exactly-once

Two additional paths exist for Kafka sinks; know which one you're
getting.

1. **Kafka producer sink (write-side EOS).** When a streaming pipeline
   writes to a Kafka topic, each write batch can be wrapped in a Kafka
   transaction: `begin_transaction` → produce all rows →
   `commit_transaction` (or `abort_transaction` on failure). This requires
   a **unique `transactional_id`** per process. The transactional producer
   is reused across batches — `init_transactions` is once per producer
   lifetime, and two producers sharing a `transactional.id` are fenced by
   the broker (only the newest is allowed to proceed). Cost: an extra
   broker round-trip per batch.

2. **Kafka → Kafka read-process-write EOS.** The full exactly-once flow —
   where the *consumer offset commit* is itself part of the producer
   transaction via `send_offsets_to_transaction` — is handled by the
   dedicated EOS pipeline. Here the offset advance and the output produce
   commit atomically, so a crash can't leave the output written but the
   input un-consumed (or vice versa).

For targets that are neither Kafka nor a transactional SQL database
(object stores, Delta), exactly-once *state* comes from at-least-once +
an idempotent `merge`/`scd2` write.

## Failure & recovery reference

This section maps the failure modes you should test to the actual
behavior.

### Crash between target write and offset commit
- **Resolved exactly-once**: the target write already committed the
  offsets atomically with the rows. On restart the runtime seeks the
  sources to the target-committed position; the broker's redelivery
  window lands **zero duplicate rows**. (Pinned by
  `redelivery_after_crash_lands_no_duplicates` and its at-least-once
  contrast test in `streaming.rs`.)
- **At-least-once**: records re-deliver on restart. Idempotent
  `merge`/`scd2` writes absorb the duplicate; `append` does not.

### Partition rebalance while a batch is in flight
Rebalance is handled through the consumer's rebalance callback using the
negotiated protocol — **cooperative** (`incremental_assign` /
`incremental_unassign`) or **eager**. Any offsets recovered by the runtime
are injected into the assignment's topic-partition list *before* the
assign call; if recovery can't set an offset, that partition falls back to
the **broker-stored committed offset**. Because commits only happen after a
successful write, a partition reassigned mid-batch resumes from the last
committed (fully-written) position — re-processing the in-flight batch
rather than skipping it.

### Poison message / partial batch
`transform_on_error` selects the policy:
- `fail` — the batch errors and the offset is not advanced (the pipeline
  stops / retries from the last commit).
- `drop` — the offending record is dropped and processing continues.
- `dlq` — the record is routed to the dead-letter destination.

The DLQ store is a **Kafka topic** (`dead_letter_topic`) or a **table**
(`dlq_store = "topic" | "table" | "auto"`). A record is sent to the DLQ
after `dlq_max_attempts` (its original emission counts as attempt 1), so
you get bounded retries before quarantine rather than an infinite poison
loop.

### Multi-target fan-out
When a single source fans out to multiple sinks (`targets=[...]`), the
framework writes **every** target before advancing the source offset. A
failure on any target leaves the offset un-committed, so the whole batch is
retried — **at-least-once across the fan-out** (fan-out is excluded from
exactly-once resolution; see Eligibility above). A partial fan-out
failure re-writes the targets that *did* succeed; those writes must be
idempotent too.

### Target slow or unavailable (backpressure)
Reads are gated by the batch limits (size and time window), so a slow
target naturally throttles consumption rather than building an unbounded
in-memory backlog. If the target is *down*, the write fails, the offset
isn't committed, and the batch retries from the last committed position —
under exactly-once the failed transaction rolled back whole, so nothing
partial ever lands.

### Pipeline stops running entirely
A delivery guarantee can't catch a pipeline that isn't running — no batch
means no error to alert on. That's what **freshness SLOs** are for:
`freshness_sla="6h"` is evaluated both at run-end and on a schedule
(`flow freshness-check`), so a stalled pipeline breaches its SLO and fires
`sla_breached`. Freshness is computed from run history and is
**target-agnostic** — it works regardless of connection kind.

## Running multiple replicas

Offset commits are manual and per-consumer-group. Running multiple
replicas in the **same consumer group** distributes partitions across them
via the rebalance protocol above — standard Kafka scaling. For the
exactly-once Kafka producer path, each replica needs its **own unique
`transactional_id`**; sharing one causes broker fencing (only one producer
survives). The transactional-SQL exactly-once path keys `_ematix_offsets`
by `(pipeline_id, source_id)` — replicas of the same pipeline against the
same target share those rows, which is correct only while partitions are
disjoint (the consumer group guarantees that). Validate replica scaling
under a forced rebalance before relying on it.

## What to validate yourself

Documentation is not a substitute for testing against *your* broker,
*your* target, and *your* load. Before trusting a critical stream, we
recommend exercising:

- a hard kill between target write and source commit — verify zero
  duplicates on an exactly-once pipeline, and duplicate absorption via
  `merge`/`scd2` on an at-least-once one,
- `delivery = "exactly_once"` against your actual config — the startup
  error lists every blocker if the pipeline is ineligible,
- rebalance while batches are in flight (scale replicas up/down under
  load),
- poison-message routing and `dlq_max_attempts` behavior,
- late / out-of-order events for your window type,
- window/state recovery after a restart,
- backpressure when the target is throttled or down,
- broker outage and reconnect.

If you hit a case where the behavior differs from what's documented here,
please [open an issue](https://github.com/ryan-evans-git/ematix-flow/issues)
with a reproduction — that's exactly the feedback that hardens a beta.
