# Learning path

A staged route through the `typekafka` repo, from Kafka concepts to the reliability patterns this codebase demonstrates. Work the stages in order — each one builds on the previous. This page links to the implemented examples and the guides available in this repository.

## Stage 0 — Concepts

Start with the two concept pages before touching any code:

1. [docs/concepts/kafka-fundamentals.md](./concepts/kafka-fundamentals.md) — topics, partitions, offsets, ordering, retention and compaction, delivery semantics. Suggested reading time: ~15 minutes.
2. [docs/concepts/consumer-groups-and-offsets.md](./concepts/consumer-groups-and-offsets.md) — consumer groups, partition assignment and rebalancing, manual vs auto-commit, at-least-once delivery. Suggested reading time: ~10 minutes.

Both pages describe real Kafka semantics and point out where this repo's in-memory driver emulates them, so the theory maps directly onto the demo you run in Stage 1.

## Stage 1 — Run the demo

Follow the README quickstart, in the recommended order:

1. **Docker path first** — [Standard usage — Docker stack](../README.md#standard-usage--docker-stack). Brings up Kafka (KRaft), the producer, the consumer and the web flow tracker; watch a message move through `parse → retry → DLQ → commit` end to end, and trigger a retry + DLQ with an oversized order.
2. **In-memory path second** — [Alternative — in-memory driver (no Docker)](../README.md#alternative--in-memory-driver-no-docker). The same port contract with zero dependencies; the consumer self-generates a 5-event sample workload, so the whole pipeline runs in one process.

For what changes when you switch drivers, see the [driver-switching guide](./guides/driver-switching.md).

## Stage 2 — Read the code

Now read the code that made the demo work, in this order — each file is the dependency of the next:

1. `packages/broker/src/port.ts` — the `IMessageBroker` interface: the single seam everything talks to, and the contract both drivers implement.
2. `packages/broker/src/factory.ts` — `createBroker()`: how `BROKER_DRIVER` picks an adapter behind the port.
3. `packages/broker/src/adapters/in-memory.ts` — the zero-dependency `EventEmitter` broker that emulates topics, partitions and offsets so the same app code runs without Kafka.
4. `apps/consumer/src/handler-runner.ts` — `createHandlerRunner`: the consumer pipeline `parse → retry → DLQ → commit`.
5. `packages/infra/src/dlq.ts` — `DlqManager`: how failed messages become `DlqEntry` records on `orders.dlq` instead of being silently dropped.
6. `packages/infra/src/publisher.ts` — `TypedPublisher`: compile-time topic-to-schema binding plus runtime validation, so the broker never sees an invalid envelope.

## Stage 3 — Patterns

The reliability patterns this repo demonstrates, in the order a learner should approach them. Start with the existing README explanations, then follow the dedicated guides:

1. [Retry topics and delayed requeue](../README.md#consumer-pipeline) — the Confluent pipeline escalates exhausted in-process retries to a Kafka retry topic with scheduled redelivery.
2. [Transactional outbox](../README.md#web-producer--transactional-outbox) — local data and pending events are recorded atomically; the Kafka relay publishes at least once, so duplicate events remain possible.
3. [Schema Registry + Avro](./guides/schema-registry.md) — schema evolution for serialized events.
4. [Compacted topics](./guides/compacted-topics.md) — key-based compaction and the customer-360 view.

## Stage 4 — Platform

Operational capabilities demonstrated by optional local Docker Compose profiles, not production deployment validation:

1. [Observability](./guides/observability.md) — OpenTelemetry tracing and metrics across the pipeline.
2. [Multi-cluster mirroring](./guides/multi-cluster-mirroring.md) — MirrorMaker 2 and multi-cluster topologies.
3. [Security](./guides/security.md) — SASL, TLS and ACLs.

## Keeping it real

This repository demonstrates an idempotent producer, manual offset commits, exponential-backoff retries and DLQ diagnostics under automated tests; it is a reference implementation, not a production-validated service. Graceful shutdown is implemented but is not covered by a dedicated shutdown test. Most non-Kafka-specific flows also run with the in-memory driver. Retry topics, transactions/outbox, Schema Registry, multi-cluster and SASL/TLS/ACL require real Kafka; compacted topics and customer-360 have an in-memory simulation. Check the capability matrix in the [driver-switching guide](./guides/driver-switching.md) for the differences between drivers.
