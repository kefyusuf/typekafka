# Kafka fundamentals

An orientation to Kafka's core ideas, written for readers new to event streaming. Everything here is real Kafka semantics; this repo's in-memory broker emulates them so the same application code runs without a broker.

## What is Kafka?

Kafka is a **distributed commit log**: a durable, append-only sequence of records that many independent services can read and write. Producers append events, consumers read them back in order, and Kafka keeps them on disk long after the producer has moved on.

It is also a **publish-subscribe** system at scale — many producers and many consumers can share the same topics without knowing about each other. Because events are stored rather than passed through, a consumer can join late or restart and read history from where it left off.

That last point is what makes Kafka an **event source of truth**: the log is the authoritative record of what happened. UI state, caches, and read models are derived views that can be rebuilt by replaying the log — not the other way around.

## Core primitives

| Primitive | What it is | In this repo |
|---|---|---|
| **Topic** | A named stream of records; the unit of publish-subscribe. | `orders.created`, `payments.completed`, `orders.dlq`, `telemetry.events` — bound to Zod schemas in `packages/domain/src/events/index.ts`. |
| **Partition** | A shard of a topic; each partition is an ordered, append-only log. | Partition count configured via `TopicConfig.numPartitions`; the in-memory driver emulates partitions per topic. |
| **Offset** | The position of a message within a partition, starting at 0. Kafka stores the **committed offset as the offset of the NEXT message**. | The confluent adapter commits `nextOffset(offset)` = `offset + 1` (`packages/broker/src/adapters/confluent.ts`). |
| **Key** | Optional routing key; records with the same key land on the same partition. | Producer `options.key`; `partitionForKey()` hash in the in-memory driver. |
| **Value** | The payload of the message. | A JSON event, validated against its topic's Zod schema before the broker ever sees it. |
| **Headers** | Optional metadata key/value pairs attached to a record. | DLQ entries carry `dlq.original-topic` and `dlq.error-type` (`packages/infra/src/dlq.ts`). |

## Ordering

Order is guaranteed **within a partition, not across partitions**. Records that share a key land in the same partition, so a consumer sees them in produce order; records in different partitions have no global order.

The in-memory broker emulates exactly this: offsets are assigned per `(topic, partition)`, so ordering behavior matches real Kafka even though there is no broker behind it (`packages/broker/src/adapters/in-memory.ts`).

## Retention and compaction

**Retention** is time- or size-based deletion: by default a topic keeps records for a configured window (for example 7 days) and then discards the oldest. It bounds storage cost and is the default cleanup mode.

**Compaction** keeps the latest value per key instead of deleting by age: for a given key only the most recent record survives, so the topic converges to the current state of every key. It suits topics that hold reference data or a "customer 360" view. Compaction is covered in `docs/guides/compacted-topics.md` (Phase 2).

## Delivery semantics

**At-most-once** commits the offset before the message is processed. If the consumer crashes mid-work, the message is skipped — the handler may not have run, but nothing is reprocessed.

**At-least-once** commits the offset only after the message was processed successfully. A crash between processing and commit causes the message to be delivered again, so handlers must tolerate duplicates. This is what this repo uses: the consumer pipeline only commits after the handler (or DLQ write) succeeded, so no message is lost.

**Exactly-once** makes each message processed exactly once — in practice at-least-once delivery combined with an idempotency mechanism that renders duplicates harmless, or a transaction that pairs the processing side effect with the offset commit. Kafka's transactional producer can do this broker-side; the transactional outbox pattern is the common application-level alternative and lands in `docs/guides/outbox.md` (Phase 3).

## Consumers and consumer groups

A consumer reads messages from a topic, and consumers that share a **group id** cooperate as one logical worker, splitting partitions so each partition is handled by exactly one member. Groups give both scaling (add members to parallelize) and fault tolerance (a departing member's partitions are reassigned). Offsets, rebalancing, and the group ids used in this repo are covered in [consumer groups and offsets](./consumer-groups-and-offsets.md).

## How this repo maps concepts to code

- `packages/broker/src/port.ts` — the `IMessageBroker` port: the single seam every app talks to.
- `packages/broker/src/adapters/in-memory.ts` — zero-dependency broker that emulates topics, partitions, offsets, and key routing for dev, tests, and CI.
- `packages/broker/src/adapters/confluent.ts` — the real Kafka driver: idempotent producer, consumer groups, manual offset commit (`offset + 1`).
- `packages/domain/src/events/` — the event vocabulary: Zod schemas and the `EventPayload` union bound to topics at compile time.
- `packages/infra/src/dlq.ts` — the dead-letter queue that preserves failed messages so nothing is silently dropped.
