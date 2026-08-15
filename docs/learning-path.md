# Learning path

A staged route through the `nodejs-kafka` repo, from Kafka concepts to the reliability patterns this codebase demonstrates. Work the stages in order — each one builds on the previous. This page is the anchor that the Phase 2 and Phase 3 guide docs link back to.

## Stage 0 — Concepts

Start with the two concept pages before touching any code:

1. [docs/concepts/kafka-fundamentals.md](./concepts/kafka-fundamentals.md) — topics, partitions, offsets, ordering, retention and compaction, delivery semantics. Suggested reading time: ~15 minutes.
2. [docs/concepts/consumer-groups-and-offsets.md](./concepts/consumer-groups-and-offsets.md) — consumer groups, partition assignment and rebalancing, manual vs auto-commit, at-least-once delivery. Suggested reading time: ~10 minutes.

Both pages describe real Kafka semantics and point out where this repo's in-memory driver emulates them, so the theory maps directly onto the demo you run in Stage 1.

## Stage 1 — Run the demo

Follow the README quickstart, in the recommended order:

1. **Docker path first** — [Standard usage — Docker stack](../README.md#standard-usage--docker-stack). Brings up Kafka (KRaft), the producer, the consumer and the web flow tracker; watch a message move through `parse -> retry -> DLQ -> commit` end to end, and trigger a retry + DLQ with an oversized order.
2. **In-memory path second** — [Alternative — in-memory driver (no Docker)](../README.md#alternative--in-memory-driver-no-docker). The same port contract with zero dependencies; the consumer self-generates a 5-event sample workload, so the whole pipeline runs in one process.

For what changes when you switch drivers, see the [driver-switching guide](./guides/driver-switching.md).

## Stage 2 — Read the code

Now read the code that made the demo work, in this order — each file is the dependency of the next:

1. `packages/broker/src/port.ts` — the `IMessageBroker` interface: the single seam everything talks to, and the contract both drivers implement.
2. `packages/broker/src/factory.ts` — `createBroker()`: how `BROKER_DRIVER` picks an adapter behind the port.
3. `packages/broker/src/adapters/in-memory.ts` — the zero-dependency `EventEmitter` broker that emulates topics, partitions and offsets so the same app code runs without Kafka.
4. `apps/consumer/src/handler-runner.ts` — `createHandlerRunner`: the production pipeline `parse -> retry -> DLQ -> commit`.
5. `packages/infra/src/dlq.ts` — `DlqManager`: how failed messages become `DlqEntry` records on `orders.dlq` instead of being silently dropped.
6. `packages/infra/src/publisher.ts` — `TypedPublisher`: compile-time topic-to-schema binding plus runtime validation, so the broker never sees an invalid envelope.

## Stage 3 — Patterns

The reliability patterns this repo demonstrates, in the order a learner should approach them. The guides below land in Phase 2; the paths are placeholders to fill in then:

1. `docs/guides/retry-topics.md` (Phase 2) — moving retries out of the process and into Kafka, replacing the in-process backoff you saw in Stage 1.
2. `docs/guides/outbox.md` (Phase 2) — the transactional outbox, so writes and events are committed together for exactly-once publishing.
3. `docs/guides/schema-registry.md` (Phase 2) — Schema Registry + Avro for schema evolution.
4. `docs/guides/compacted-topics.md` (Phase 2) — key-based compaction for reference data and the customer-360 view.

## Stage 4 — Platform

Operational concerns for running a Kafka system in production. Guides land in Phase 3:

1. `docs/guides/observability.md` (Phase 3) — OpenTelemetry tracing and metrics across the pipeline.
2. `docs/guides/multi-cluster.md` (Phase 3) — MirrorMaker 2 and multi-cluster topologies.
3. `docs/guides/security.md` (Phase 3) — SASL, TLS and ACLs.

## Keeping it real

Every pattern in this repo is implemented to production depth — idempotent producer, manual offset commit, retry with exponential backoff + jitter, DLQ with diagnostics, graceful shutdown — but it all runs locally, and most of it works against the in-memory driver alone. Only the driver-dependent capabilities (retry topics, transactions/outbox, Schema Registry, compacted topics, multi-cluster, SASL/TLS/ACL) need a real Kafka. Check the capability matrix in the [driver-switching guide](./guides/driver-switching.md) to see which capabilities require the Confluent driver and which the in-memory driver only simulates.
