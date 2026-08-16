# Compacted topics + customer-360

This guide explains how `nodejs-kafka` builds a **KTable-style read model** the Node.js way: the producer aggregates per-customer state into a full-state changelog on a **compacted** `customers` topic, and a dedicated `customer-view` service replays that changelog into an in-memory store and serves it over REST. No Kafka Streams, no state stores — just log compaction, tombstones, and a `Map`.

## Why compacted topics

A normal Kafka topic keeps every record. That is the right model for an event log (`orders.created`, `payments.completed`), where the history is the point. For a **per-entity snapshot** (the current state of a customer) the history is irrelevant — only the latest record per key matters.

Log compaction changes the retention semantics:

- The broker keeps **the most recent record per key** and periodically removes older ones for the same key.
- Each record's **key** (`customerId`) is what the broker compacts on — records with the same key collapse to the latest.
- A record whose **value is `null`** is a **tombstone**: it tells compacting consumers the key was deleted, and the broker eventually removes the key entirely.
- Readers get **latest-wins** semantics for free: replay the topic in order and apply each record, and you always end up with the current snapshot.

This is exactly how Kafka Streams' KTable works under the hood — a keyed changelog topic in front of an in-memory store.

## How it fits together

```
apps/producer  ── CustomerUpdated (key = customerId) ──►  customers  (cleanup.policy=compact)
                                                              │  consumed in group `customer-view`,
                                                              │  fromBeginning: true
                                                              ▼
                                              CustomerStore (in-memory Map)
                                                              │
                                                              ▼
                                          Express REST API  GET/DELETE /customers[/:id]
```

- **Write side (producer)** — `apps/producer` keeps a per-customer aggregate in memory as it publishes order + payment events. After each event it emits a full-state `CustomerUpdated` record, keyed by `customerId`, to the `customers` topic. The changelog is **idempotent**: because every record carries the complete state, replaying or duplicating it converges to the same snapshot — latest record per key wins.
- **Read side (`customer-view`)** — `apps/customer-view` consumes `customers` in the `customer-view` consumer group with `fromBeginning: true` and applies every record to an in-memory `Map` (`CustomerStore`). On startup it replays the changelog from the earliest offset and rebuilds the snapshot; on restart it just replays again. This is the Node approach to a KTable: the topic is the source of truth, the `Map` is a disposable cache.
- **Driver symmetry** — with the `confluent` driver the broker performs real log compaction and delivers real tombstones. With the `in-memory` driver, compaction is **simulated**: latest record per key wins and `null` values are stored as deletes, so the same application code behaves identically without a Kafka broker.

## Aggregation semantics

The aggregation lives in `packages/domain/src/aggregation/customer-state.ts` — a **pure reducer** with no Kafka imports, so it is unit-testable in isolation.

- `applyOrder(state, order)` bumps `orderCount` and sets `lastOrderAt` to the order's timestamp. It does **not** touch `totalSpentCents`.
- `applyPayment(state, payment)` adds `amountCents` to `totalSpentCents`. **Spend comes from payments only** — an order is booked, a payment is what a customer actually paid.
- `toCustomerUpdated(state)` builds the full-state `CustomerUpdated` changelog event from the current aggregate.

The producer owns the write side: it folds orders and payments for a customer into one aggregate, then publishes the full state. Downstream consumers never see the individual events — they read one record per customer that is always the whole truth.

## Tombstones

A `null` value on a keyed topic is the standard Kafka delete signal. Here it flows end to end:

- **Codec** — `JsonCodec.serialize(null)` returns `null` (one line in `packages/broker/src/codec/json.ts`). Previously a `null` payload became the string `'null'`; now it passes through so the confluent driver sends a real Kafka tombstone and the in-memory driver stores/returns `null` (simulated compaction).
- **Read model** — `CustomerStore.apply` treats a record with a `null` value as a tombstone and **deletes the key** from the `Map`.
- **Write-through delete** — `DELETE /customers/:id` on the REST API first publishes a tombstone (a `null`-value record keyed by `customerId`) to `customers`, then removes the key from the local store. The delete is thus durable in the changelog, and any future consumer replays it.
- **Schema Registry note** — with `SCHEMA_REGISTRY_URL` set, `customers` still works: `AvroCodec` falls back to the `JsonCodec` for topics without an Avro schema, and `customers` has no Avro schema (its payload is `CustomerUpdated` JSON, exactly like the codec default).

## Configuration

| Variable | Default | Description |
|---|---|---|
| `CUSTOMER_VIEW_PORT` | `3001` | `customer-view` service: HTTP port for the customer read model |

The `customers` topic is created by the producer at startup with `cleanup.policy=compact` (see `apps/producer/src/main.ts`), so the broker compacts it from the start.

## Run it

In-memory self-demo — one process, no Docker, with the whole tombstone story visible:

```bash
npm run dev:customer-view
```

The app self-generates a sample changelog (records for CUST-1002, CUST-1003, CUST-1001) **plus one tombstone for CUST-1002**, then serves the read model on `:3001` — so the live store shows only the survivors.

With the full Docker stack:

```bash
docker compose up --build
```

The `customer-view` service consumes the real compacted `customers` topic (group `customer-view`, from beginning) and serves the same REST API on `:3001`.

## Verify

```bash
curl http://localhost:3001/customers              # the aggregated customers
curl http://localhost:3001/customers/CUST-1003    # one customer (200)
curl -X DELETE http://localhost:3001/customers/CUST-1003   # tombstone it
curl http://localhost:3001/customers/CUST-1003    # now 404
```

In the in-memory demo, `GET /customers` shows CUST-1003 + CUST-1001 (CUST-1002 was tombstoned by the self-generated sample), and `GET /customers/CUST-1002` returns 404. With Docker, the tombstone also shows up in Kafka UI (`:8080`) under the `customers` topic as a record with a `null` payload.

## What changes, what doesn't

- **No port signature changes** — the `IMessageBroker` port, adapters, and types are untouched; only the one codec line in `JsonCodec.serialize` changed.
- **No behavior change by default** — the producer still publishes the same `orders.created` / `payments.completed` events; the compacted changelog is an additional write.
- **No new dependencies** — the aggregation reducer and the in-memory `CustomerStore` are plain TypeScript (the store is a `Map`).
- **Pure domain logic** — `customer-state.ts` imports no Kafka code, keeping the aggregation unit-testable in isolation.
- **What changes** — `customers` is a compacted, JSON-only changelog topic (no Avro schema); the broker compacts it and the `customer-view` service rebuilds the snapshot from the log on every start.
