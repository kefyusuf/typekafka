# nodejs-kafka

Type-safe **Kafka messaging on Node.js** — built with **TypeScript**, **Zod**, and a plug-and-play **Ports & Adapters** architecture.

This repo is a portfolio / reference project showing production-grade, event-driven engineering in TypeScript + Node.js + Kafka — not just a "produce a message" snippet. It covers:

- A broker **port** (`IMessageBroker`) with **two swappable adapters** — an in-memory broker (zero deps) and a real Kafka driver (Confluent).
- **End-to-end type safety**: Zod schemas bound to topics at compile time, validated at runtime before the broker ever sees a message.
- A production-shaped **consumer pipeline**: `parse → retry (exponential backoff + jitter) → DLQ → commit`.
- **Graceful shutdown**, **structured JSON logging** (pino), **fail-fast env validation**, and a CI pipeline.
- A full **Docker Compose** stack with a **KRaft-mode Kafka** (no ZooKeeper) and **Kafka UI**.
- A **live flow tracker** — a web UI that places orders, watches the message move through the pipeline in real time, and streams per-step telemetry over SSE.

---

## Table of contents

- [Why an adapter pattern?](#why-an-adapter-pattern)
- [Architecture](#architecture)
  - [Consumer pipeline](#consumer-pipeline)
- [Quickstart](#quickstart)
  - [Option A — in-memory driver (no Docker)](#option-a--in-memory-driver-no-docker)
  - [Option B — full Docker stack](#option-b--full-docker-stack)
  - [Option C — Web UI (recommended for learning)](#option-c--web-ui-recommended-for-learning)
- [Configuration](#configuration)
- [Message contract](#message-contract)
  - [Topic-safe generic types](#topic-safe-generic-types)
  - [Events](#events)
- [What this repo demonstrates](#what-this-repo-demonstrates)
- [Repository layout](#repository-layout)
- [Commands](#commands)
- [Tests](#tests)
- [Roadmap](#roadmap)

---

## Why an adapter pattern?

Kafka client libraries for Node.js are in flux:

- **`kafkajs`** — the long-standing community default — is no longer actively maintained.
- **`@confluentinc/kafka-javascript`** — Confluent's official client — is a native (`librdkafka`) dependency with a large binary footprint, but it ships a KafkaJS-compatible facade and is actively maintained.

Instead of hard-coding one client, the whole application talks to a single **port** (`IMessageBroker`, see `packages/broker/src/port.ts`). Concrete drivers are swapped behind it via one environment variable:

| Driver | Description | Use case |
|---|---|---|
| `in-memory` | Zero-dependency `EventEmitter` broker with the same semantics (topics, partitions, offsets) | Local dev, CI, demos — runs with **no Kafka** |
| `confluent` | Real Kafka via `@confluentinc/kafka-javascript` (KafkaJS facade) | Production / Docker |

```ts
const broker = createBroker({
  driver: process.env.BROKER_DRIVER,      // 'in-memory' | 'confluent'
  connection: { brokers, clientId },
  logger,
});
```

Swap the driver tomorrow and the domain + apps stay untouched.

### The port

```ts
export interface IMessageBroker {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  readonly isConnected: boolean;

  createTopics(topics: TopicConfig[]): Promise<void>;
  listTopics(): Promise<string[]>;

  produce<T>(topic: string, value: T, options?: ProduceOptions): Promise<ProduceResult>;

  consume<T>(
    topics: string[],
    handler: ConsumeHandler<T>,
    options?: ConsumeOptions,
  ): Promise<Disposer>;

  /** Subscribe, but only deliver messages written after subscription. */
  consumeFromNow<T>(topics: string[], handler: ConsumeHandler<T>, options?: ConsumeOptions): Promise<Disposer>;
}
```

The adapters translate between the native client shape and a broker-agnostic `KafkaMessage` envelope (`packages/broker/src/types.ts`), so business code never sees `librdkafka` buffers or KafkaJS internals.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        apps/                                 │
│          producer (CLI)          consumer (worker)           │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┴───────────────────────────────────┐
│              domain/  —  pure business logic                 │
│   Zod schemas · EventPayload union · topic->schema registry  │
│   notification handler (no Kafka imports)                    │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┴───────────────────────────────────┐
│         infra/  —  config (Zod) · pino logger · retry        │
│         DlqManager · TypedPublisher · graceful shutdown      │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┴───────────────────────────────────┐
│   broker/  —  the PORT (IMessageBroker)                      │
│        ├── InMemoryBrokerAdapter   (dev/test, no deps)       │
│        └── ConfluentKafkaAdapter   (real Kafka, Phase 2)     │
└──────────────────────────────────────────────────────────────┘
```

Hexagonal architecture: business logic in `domain/` never imports a Kafka client. The port in `broker/` is the only seam, and `infra/` provides cross-cutting concerns (config, logging, retry, DLQ, publish).

Every pipeline step in the consumer emits a **telemetry event** to `telemetry.events`, and the web app (`apps/web`) adds its own `produced` telemetry when an order is placed. The web server consumes that topic in the `web-telemetry` group and broadcasts each event to connected browsers over **Server-Sent Events** (`GET /api/events`), so the flow diagram and event log update live.

### Consumer pipeline

Each consumed message runs through a production pipeline (`apps/consumer/src/handler-runner.ts`):

```
raw message
   │
   ├─ 1. parse   → Zod schema validation
   │              └─ invalid  → DLQ (original payload + diagnostics)
   ├─ 2. handler → business logic with exponential backoff retry
   │              └─ exhausted → DLQ
   └─ 3. commit  → offset committed only on success
```

The runner (`createHandlerRunner`) wraps a single domain handler with the full pipeline:

1. **parse** — validates the raw payload against the topic's Zod schema. Invalid messages go straight to the DLQ (`DlqManager.deadLetter`).
2. **retry** — transient failures are retried with exponential backoff + full jitter (`withRetry` in `packages/infra/src/retry.ts`; the consumer uses `attempts: 3`, `baseDelayMs: 50`).
3. **dlq** — after attempts are exhausted, the message is written to `orders.dlq` with diagnostics (`error`, `errorType`, `attempts`, `failedAt`) plus the original payload and headers (`dlq.original-topic`, `dlq.error-type`).
4. **commit** — the offset is committed only when the handler (or DLQ write) succeeded, giving **at-least-once** delivery.

The consumer app runs **two consumers** in the same `notification-service` group:

- `orders.created` → `createHandlerRunner` with the notification handler.
- `payments.completed` → inline handler that logs `payment recorded` and commits.

With `BROKER_DRIVER=in-memory`, the consumer self-generates a 5-event sample workload so the whole pipeline is visible end to end in one process. With `BROKER_DRIVER=confluent`, events come from the standalone producer service.

---

## Quickstart

Requirements: **Node.js 22+** (Option A), **Docker** (Option B).

### Option A — in-memory driver (no Docker)

```bash
npm install
npm run build
npm run dev:consumer     # self-generates 5 orders → consumes → retries → DLQ
```

You'll see the full pipeline in the JSON logs: orders processed, one oversized order retried (`handler failed, will retry`) and dead-lettered (`handler exhausted retries, sending to DLQ`), and payments recorded.

```bash
npm run dev:producer -- --count 10 --delay 300
```

In-memory mode uses the same port contract as real Kafka, so the semantics (topics, partition key routing, offsets) are exercised identically.

### Option B — full Docker stack

```bash
docker compose up --build
```

This starts:

- **Kafka** (`apache/kafka:3.7.0`, KRaft mode, no ZooKeeper) on `localhost:9092`
- **Kafka UI** on [http://localhost:18080](http://localhost:18080)
- **consumer** app (confluent driver, reads the `notification-service` group)
- **producer** app (publishes order + payment events and exits)

> The apps talk through the real Kafka cluster (`BROKER_DRIVER=confluent`). The broker keeps its KRaft logs inside the container, so `docker compose down` resets Kafka state and the next `up` replays from the beginning. Use `docker compose up --build` again to re-run the demo.

Watch the consumer process orders and payments while the Kafka UI shows the topics, partitions, messages, and consumer group offsets live.

### Option C — Web UI (recommended for learning)

```bash
docker compose up --build
```

Open [http://localhost:3000](http://localhost:3000):

- **Place an order** from the form (SKU / quantity / unit price). The web
  service publishes `orders.created` + `payments.completed` and emits
  telemetry events for what it did.
- **Watch the flow diagram** light up as the message moves
  `producer → orders.created → consumer → (retry) → DLQ`.
- **Read the live event log** — every pipeline step (`consumed`, `parsed`,
  `retrying`, `dead-lettered`, `committed`, `payment-recorded`) with the
  topic, partition, offset, and an educational concept tag.
- Try a **total above 100000 cents** to trigger the simulated provider
  timeout → retry → DLQ flow.

The web UI requires real Kafka (`BROKER_DRIVER=confluent`, set by compose).
For a no-Docker learning path, `npm run dev:consumer` still self-demos the
in-memory broker.

---

## Configuration

All environment variables are validated **at startup** by a Zod schema (`packages/infra/src/config.ts`) — an invalid or missing value fails fast with a descriptive error instead of failing later at runtime.

| Variable | Default | Description |
|---|---|---|
| `BROKER_DRIVER` | `in-memory` | Broker adapter to use: `in-memory` or `confluent` |
| `BROKER_BROKERS` | `kafka:9092` | Comma-separated bootstrap server list |
| `BROKER_CLIENT_ID` | `nodejs-kafka-demo` | Kafka client id |
| `BROKER_SASL_USERNAME` | — | SASL/PLAIN username (only if required) |
| `BROKER_SASL_PASSWORD` | — | SASL/PLAIN password (only if required) |
| `BROKER_MEMORY_AUTO_COMMIT` | `true` | In-memory driver: commit offsets automatically after handler resolve |
| `CONSUMER_GROUP_ID` | `notification-service` | Consumer group id for both consumers |
| `CONSUMER_FROM_BEGINNING` | `true` | Start reading from the earliest offset when no committed offset exists |
| `LOG_LEVEL` | `info` | pino level: `trace` / `debug` / `info` / `warn` / `error` / `fatal` |
| `SERVICE_NAME` | `nodejs-kafka` | Tag used in structured log records |
| `WEB_PORT` | `3000` | Web UI HTTP port |
| `TELEMETRY_GROUP_ID` | `web-telemetry` | Consumer group id for the telemetry event stream |

See [`.env.example`](.env.example) for a documented copy-paste template.

---

## Message contract

Events are typed end-to-end. A single registry (`packages/domain/src/events/index.ts`) maps topic → Zod schema → TypeScript type:

```ts
// packages/domain/src/events/order-created.ts
export const OrderCreatedSchema = z.object({
  type: z.literal('order.created'),
  eventId: z.string().uuid(),
  occurredAt: z.string().datetime(),
  orderId: z.string().min(1),
  customerId: z.string().min(1),
  items: z.array(OrderItemSchema).min(1),
  totalCents: z.number().int().nonnegative(),
});
export type OrderCreated = z.infer<typeof OrderCreatedSchema>;
```

### Topic-safe generic types

```ts
export type EventTopic = keyof typeof topicToType;             // 'orders.created' | 'payments.completed'
export type EventOf<Topic extends EventTopic> = Extract<
  EventPayload,
  { type: (typeof topicToType)[Topic] }
>;
```

The `TypedPublisher` facade (`packages/infra/src/publisher.ts`) binds each topic to its schema at compile time **and** re-validates at runtime (fail-fast on the producer side):

```ts
const publisher = new TypedPublisher(broker);
await publisher.publish('orders.created', order);       // TS: accepts OrderCreated only
await publisher.publish('orders.created', payment);     // ❌ compile error
```

### Events

| Topic | Event | Trigger |
|---|---|---|
| `orders.created` | `OrderCreated` | Order placed (items + totals) |
| `payments.completed` | `PaymentCompleted` | Payment captured for an order |
| `orders.dlq` | `DlqEntry` | Failed messages (parse failure or handler exhaustion) |
| `orders.retry` | — | Reserved for retry topics (see roadmap) |

The sample generators (`packages/domain/src/sample/sample-events.ts`) create deterministic, spec-compliant events. Every third order is deliberately **oversized** (> 100 000 cents) so the notification handler's simulated provider timeout fires — demonstrating the retry + DLQ pipeline. Payment events mirror each order (`orderId`, `amountCents`, `method`).

---

## What this repo demonstrates

| Concept | Where |
|---|---|
| **Ports & Adapters** (hexagonal) | `packages/broker/src/port.ts`, `packages/broker/src/factory.ts` |
| **Type-safe messaging** (compile + runtime) | Zod schemas + `parseEvent()` + `TypedPublisher` |
| **Discriminated union events** | `EventPayload`, `EventOf<Topic>`, `topicToType` |
| **Dead Letter Queue** | `packages/infra/src/dlq.ts`, `apps/consumer/src/handler-runner.ts` |
| **Retry with exponential backoff + jitter** | `packages/infra/src/retry.ts` |
| **Manual offset commit (at-least-once)** | `ConfluentKafkaAdapter.consume`, `handler-runner.ts` |
| **Idempotent producer** (`acks=all`, `enable.idempotence`) | `ConfluentKafkaAdapter.getProducer` |
| **Graceful shutdown** (drain + commit + exit) | `packages/infra/src/shutdown.ts` |
| **Structured logging** (pino, JSON) | `packages/infra/src/logger.ts`, logger bridge to the Kafka client |
| **Env validation** (fail-fast config) | `packages/infra/src/config.ts` |
| **Partition key routing** | `partitionForKey()` hash in `InMemoryBrokerAdapter`, producer `key` |
| **Consumer groups / offsets** | `ConsumeOptions` (manual commit, group id, concurrency) |
| **Multi-stage Docker builds** | `apps/*/Dockerfile` |
| **KRaft Kafka (no ZooKeeper)** | `docker-compose.yml` |
| **Unit + integration tests** | Vitest, 42 tests, no Kafka required |

---

## Repository layout

```
apps/
  producer/          CLI that publishes order + payment events, then exits
  consumer/          long-running worker with parse → retry → DLQ → commit pipeline
  web/               Express REST + SSE server and React flow-tracker UI
packages/
  broker/            IMessageBroker port + in-memory / confluent adapters
  domain/            Zod schemas, event union, sample generators, notification handler
  infra/             config, logging, retry, DLQ, publisher, shutdown
docker-compose.yml   Kafka (KRaft) + Kafka UI + app services
.env.example         Documented environment template
```

---

## Commands

| Command | Description |
|---|---|
| `npm install` | Install workspace dependencies |
| `npm run build` | Compile all packages (topological order) |
| `npm run typecheck` | `tsc --noEmit` across all packages |
| `npm run lint` | ESLint (flat config + typescript-eslint) |
| `npm test` | Vitest — 42 tests, runs without any Kafka |
| `npm run dev:producer -- --count N` | Produce N order+payment pairs (`--delay` also accepted, ms) |
| `npm run dev:consumer` | Consumer worker (in-memory self-demo) |
| `npm run dev:web` | Web UI — Express API on :3000, Vite dev UI on :5173 (needs real Kafka) |
| `docker compose up --build` | Full stack with Kafka UI on :18080 and web UI on :3000 |

---

## Tests

Vitest, configured in `vitest.config.ts`. All 42 tests run **without Kafka** — they use the in-memory driver and mocks:

| Suite | File | Tests |
|---|---|---|
| Retry behaviour | `packages/infra/test/retry.test.ts` | 4 |
| Event schemas | `packages/domain/test/schemas.test.ts` | 6 |
| Telemetry schema | `packages/domain/test/telemetry.test.ts` | 4 |
| Confluent adapter (mocked driver) | `packages/broker/test/confluent.test.ts` | 12 |
| In-memory broker | `packages/broker/test/in-memory.test.ts` | 6 |
| Telemetry client | `packages/infra/test/telemetry.test.ts` | 3 |
| Dead-letter queue | `packages/infra/test/dlq.test.ts` | 1 |
| Consumer pipeline | `apps/consumer/test/pipeline.test.ts` | 3 |
| Web server | `apps/web/test/server.test.ts` | 3 |

CI (`.github/workflows/ci.yml`) runs `npm ci` → `build` → `typecheck` → `lint` → `test` on Node 22.

---

## Roadmap

- [x] `ConfluentKafkaAdapter` — idempotent producer (`acks=all`, `enable.idempotence`), manual offset commit, consumer group rebalancing, headers
- [x] Wire producer ↔ consumer through real Kafka in Docker (`BROKER_DRIVER=confluent`)
- [x] KRaft-mode Kafka (no ZooKeeper) via the official `apache/kafka` image
- [ ] Retry topic + scheduled retry (instead of in-process backoff only)
- [ ] Kafka Streams–style aggregation / compacted topics (customer 360 view)
- [ ] Exactly-once / transactional outbox pattern
- [ ] Schema Registry + Avro serialization for schema evolution
