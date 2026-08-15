# Consumer groups and offsets

How the consumers in this repo cooperate, and why offset bookkeeping matters. Read [Kafka fundamentals](./kafka-fundamentals.md) first if the primitives are unfamiliar.

## What is a consumer group

A consumer group is a set of consumers that share a **group id**. The broker treats them as one logical worker: the topic's partitions are divided among the members, and each partition is assigned to **exactly one member** at any time. Because the group tracks its own offsets, it reads the topic from its own position regardless of what other groups are doing — two groups reading the same topic are fully independent.

## Partition assignment and rebalancing

When a group is created, the broker assigns partitions to its members. The assignment is **rebalanced** whenever membership changes: when a consumer joins, partitions are redistributed so the newcomer gets work; when a consumer leaves (gracefully or by timing out), its partitions are reassigned to the survivors so no partition is left unread.

This repo runs two groups with the same mechanics:

- `notification-service` — the order and payment consumers (`apps/consumer`).
- `web-telemetry` — the web UI's telemetry stream (`apps/web`).

Each group tracks its offsets independently, so the same topic can be consumed for two different purposes without interference.

## Offsets

An offset is the position of a message within a partition. Kafka's convention is that the **committed offset is the offset of the NEXT message to read** — committing `5` means "I am done with everything below 5". The confluent adapter encodes this as `nextOffset(offset)` = `offset + 1` (`packages/broker/src/adapters/confluent.ts`), and the in-memory driver mirrors the same semantics per `(topic, partition)`.

Offsets can be committed two ways:

- **Auto-commit** — the client commits periodically, behind your back. Simple, but a crash can commit work that did not complete.
- **Manual commit** — your code calls `context.commit()` when the message is truly done. The `notification-service` consumer runs with `manualCommit: true`, so a handler crash does not advance the offset. (The `web-telemetry` group instead passes `manualCommit: false`, so its telemetry offsets auto-commit.)

## At-least-once in this repo

The `notification-service` consumer runs each message through the pipeline in `apps/consumer/src/handler-runner.ts`: `parse → retry → DLQ → commit`.

The offset is committed **only after the handler succeeds**. If the message fails to parse, or the handler exhausts its retries, the message is written to `orders.dlq` (`DlqManager.deadLetter`, `packages/infra/src/dlq.ts`) and only then committed. The DLQ keeps the original payload plus diagnostics (error, attempts, and headers such as `dlq.original-topic`), so no message is silently dropped — it is either processed, or dead-lettered for inspection and replay.

This is **at-least-once** delivery: a crash between processing and commit redelivers the message, so handlers must tolerate duplicates. The trade-off is deliberate — losing a message is worse than seeing one twice.

## Concurrency

One consumer processes the messages of a given partition **serially** — that is what preserves per-partition order. Parallelism in a group comes from having more partitions and more members, not from running one member's partitions concurrently.

`ConsumeOptions` (`packages/broker/src/types.ts`) exposes two knobs:

- `concurrency` — declared in `ConsumeOptions` as the number of concurrent handler invocations, defaulting to `1`. Today neither adapter reads this field: the confluent adapter never forwards it to `consumer.run`, so serial processing comes from the kafka.js `eachMessage` default (`concurrency: 1`), and the in-memory driver dispatches every message fire-and-forget with no serialization at all.
- `manualCommit` — when `true`, offsets are committed only after the handler resolves; when `false`, the kafka.js auto-commit path commits periodically (`packages/broker/src/adapters/confluent.ts`).

In practice: the `notification-service` consumer runs with `manualCommit: true`, committing each offset only after the `parse → retry → DLQ → commit` pipeline finishes, while the `web-telemetry` group auto-commits. Per-partition serial processing on real Kafka comes from the kafka.js `eachMessage` default, not from `ConsumeOptions.concurrency`.
