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
| Default guarantee | **At-least-once.** Source offsets are committed *after* the target write succeeds. |
| On crash between write and commit | Records are **re-delivered** on restart → duplicates are possible. Make target writes idempotent. |
| Exactly-once | Available for **Kafka producer** sinks via Kafka transactions (unique `transactional_id`), and for the full **Kafka → Kafka** read-process-write flow via the dedicated EOS pipeline. |
| Offset commits | **Manual**, framework-controlled — not `enable.auto.commit`. |
| Poison / failing records | Routed to a **DLQ** (topic or table) after `dlq_max_attempts`; `transform_on_error` = `fail` / `drop` / `dlq`. |
| Rebalance | Handled via the cooperative or eager protocol; recovered offsets are applied into the assignment, falling back to the broker-committed offset. |
| Stalled pipeline | Caught by a **freshness SLO** (run-history based, target-agnostic), not by delivery. |

## At-least-once (the default)

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

`append` mode does **not** de-duplicate — only use it when duplicates are
acceptable or removed downstream.

## Exactly-once

Two distinct paths exist; know which one you're getting.

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

Exactly-once end-to-end into a **non-Kafka** target (Postgres, Delta, S3)
is not a Kafka-transaction property — for those, use at-least-once + an
idempotent `merge`/`scd2` write, which gives exactly-once *state*.

## Failure & recovery reference

This section maps the failure modes you should test to the actual
behavior.

### Crash between target write and offset commit
Records re-delivered on restart (at-least-once). Idempotent `merge`/`scd2`
writes absorb the duplicate; `append` does not.

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
retried — at-least-once across the fan-out. (This means a partial fan-out
failure re-writes the targets that *did* succeed; those writes must be
idempotent too.)

### Target slow or unavailable (backpressure)
Reads are gated by the batch limits (size and time window), so a slow
target naturally throttles consumption rather than building an unbounded
in-memory backlog. If the target is *down*, the write fails, the offset
isn't committed, and the batch retries from the last committed position.

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
survives). Validate replica scaling under a forced rebalance before
relying on it.

## What to validate yourself

Documentation is not a substitute for testing against *your* broker,
*your* target, and *your* load. Before trusting a critical stream, we
recommend exercising:

- duplicate handling after a hard kill between write and commit,
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
