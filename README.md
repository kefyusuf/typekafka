# nodejs-kafka

Type-safe **Kafka messaging on Node.js** — built with **TypeScript**, **Zod**, and a plug-and-play **Ports & Adapters** architecture.

This repo is a portfolio / reference project showing production-grade, event-driven engineering in TypeScript + Node.js + Kafka — not just a "produce a message" snippet. It covers:

- A broker **port** (`IMessageBroker`) with **two swappable adapters** — an in-memory broker (zero deps) and a real Kafka driver (Confluent).
- **End-to-end type safety**: Zod schemas bound to topics at compile time, validated at runtime before the broker ever sees a message.
- A production-shaped **consumer pipeline**: `parse → retry (in-process backoff + jitter) → retry topic (escalating delay) → DLQ → commit` (confluent driver); in-memory keeps in-process retry → DLQ.
- A **transactional outbox** in the web app — `POST /api/orders` writes the order + outbox rows to a local **SQLite** file in one transaction (`node:sqlite`, built into Node 22, zero native deps); an outbox relay publishes committed rows to Kafka **transactionally**, and consumers run with `read_committed` (confluent driver).
- **Graceful shutdown**, **structured JSON logging** (pino), **fail-fast env validation**, and a CI pipeline.
- A full **Docker Compose** stack with a **KRaft-mode Kafka** (no ZooKeeper) and **Kafka UI**.
- Curated **Schema Registry + Avro** schemas (schema evolution with backward compatibility) behind the same codec seam.
- **Compacted topics / customer-360** — the producer aggregates per-customer state into a KTable-style changelog on a compacted `customers` topic; the `customer-view` service replays it into an in-memory read model, deletes via tombstones (null-value records), and serves a small REST API.
- A **live flow tracker** — a web UI that places orders, watches the message move through the pipeline in real time, and streams per-step telemetry over SSE.
- **Observability** — OpenTelemetry **manual spans** around `produce` / `consume` in both broker adapters (no auto-instrumentation), a **Prometheus `/metrics`** endpoint per app, and an optional `observability` compose profile (`otel-collector` → debug exporter, Prometheus scraping all apps, Grafana dashboard at `:3002`).
- **Multi-cluster / MirrorMaker 2** — an optional `mirror` compose profile with a second single-node KRaft cluster (`kafka-b`) and a **MirrorMaker 2** worker replicating `orders.*` (`orders.created`, `orders.payment.*`, `orders.dlq`) from the primary cluster, visible as a second `mirror` cluster in Kafka UI.
- **Security** — an optional `security` compose profile with a **SASL_SSL**-secured KRaft cluster (`kafka-secured`, PLAIN auth: `admin` super user + `app`) guarded by **topic-scoped ACLs**; self-signed certs are generated inside the container, and the confluent driver auto-selects `security.protocol` from `BROKER_SASL_*` / `BROKER_SSL_*` env vars — no app code changes.

---

## Table of contents

- [Why an adapter pattern?](#why-an-adapter-pattern)
- [Architecture](#architecture)
  - [Consumer pipeline](#consumer-pipeline)
- [Switching drivers](#switching-drivers)
- [Quickstart](#quickstart)
  - [Standard usage — Docker stack](#standard-usage--docker-stack)
    - [What's running](#whats-running)
    - [How the stack works](#how-the-stack-works)
    - [What to check](#what-to-check)
  - [Alternative — in-memory driver (no Docker)](#alternative--in-memory-driver-no-docker)
- [Configuration](#configuration)
- [Message contract](#message-contract)
  - [Topic-safe generic types](#topic-safe-generic-types)
  - [Events](#events)
  - [Telemetry events (flow tracker)](#telemetry-events-flow-tracker)
  - [Web API](#web-api)
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

Which driver supports which capability is the difference that matters — see the [driver capability matrix](#switching-drivers) and the full [driver-switching guide](docs/guides/driver-switching.md).

```ts
const broker = createBroker({
  driver: process.env.BROKER_DRIVER,      // 'in-memory' | 'confluent'
  connection: { brokers, clientId },
  logger,
});
```

Swap the driver tomorrow and the domain + apps stay untouched.

Message (de)serialization is pluggable via a `MessageCodec` (`packages/broker/src/codec/`); `JsonCodec` is the default, and the port also supports Kafka transactions via `beginTransaction()` (confluent driver only — see the driver matrix).

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

  beginTransaction(options?: TransactionOptions): Promise<MessageTransaction>;
}
```

The adapters translate between the native client shape and a broker-agnostic `KafkaMessage` envelope (`packages/broker/src/types.ts`), so business code never sees `librdkafka` buffers or KafkaJS internals.

Producer/consumer serialization, retry topics, transactions and security capabilities differ per driver — see [Switching drivers](#switching-drivers).

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
│        ├── ConfluentKafkaAdapter   (real Kafka)              │
│        └── codec/ — MessageCodec (JsonCodec default)         │
└──────────────────────────────────────────────────────────────┘
```

Hexagonal architecture: business logic in `domain/` never imports a Kafka client. The port in `broker/` is the only seam, and `infra/` provides cross-cutting concerns (config, logging, retry, DLQ, publish).

Every pipeline step in the consumer emits a **telemetry event** to `telemetry.events`. In the web app (`apps/web`) an order is first written to a local **SQLite outbox** — the order row and the outbox rows in one transaction (`packages/infra/src/outbox.ts`). An `OutboxRelay` then publishes the committed rows to `orders.created` / `payments.completed` inside a **Kafka transaction** and emits the `produced` telemetry. The web server consumes that topic in the `web-telemetry` group and broadcasts each event to connected browsers over **Server-Sent Events** (`GET /api/events`), so the flow diagram and event log update live.

### Consumer pipeline

Each consumed message runs through a production pipeline (`apps/consumer/src/handler-runner.ts`):

```
raw message
   │
   ├─ 1. parse   → Zod schema validation
   │              └─ invalid  → DLQ (original payload + diagnostics)
   ├─ 2. handler → business logic with exponential backoff retry
   │              └─ exhausted → orders.retry (retry-count + next-deliver-at headers)
   ├─ 3. retry topic consumer → parks not-yet-due messages, re-processes due ones
   │              └─ max deliveries → DLQ
   └─ 4. commit  → offset committed only on success
```

The runners (`createHandlerRunner`, `createRetryTopicRunner`) wrap a single domain handler with the full pipeline. The confluent driver uses the **hybrid model** — in-process retry, then a retry-topic hop, then DLQ; in-memory keeps in-process retry → DLQ.

1. **parse** — validates the raw payload against the topic's Zod schema. Invalid messages go straight to the DLQ (`DlqManager.deadLetter`).
2. **retry (in-process)** — transient failures are retried with exponential backoff + full jitter (`withRetry` in `packages/infra/src/retry.ts`). The confluent pipeline uses `attempts: 2`; in-memory uses `attempts: 3` (then DLQ).
3. **retry topic** — on exhaustion (confluent only), the message is published to `orders.retry` with `retry-count`, `next-deliver-at` (ISO), and `retry.original-topic` headers (`RetryTopicScheduler` in `packages/infra/src/retry-topic.ts`). Escalating delays `[2s, 10s, 60s]`, max 3 deliveries (`retry-count >= 3`). The retry-topic consumer parks not-yet-due messages (head-of-line blocked on that partition — ordered retry) and re-processes due ones (`createRetryTopicRunner` in `apps/consumer/src/retry-runner.ts`); the retry topic carries JSON (not Avro).
4. **dlq** — reached only after max retry-topic deliveries (or parse failure). The message is written to `orders.dlq` with diagnostics (`error`, `errorType`, `attempts`, `failedAt`) plus the original payload and headers (`dlq.original-topic`, `dlq.error-type`).
5. **commit** — the offset is committed on success, schedule, or DLQ write, giving **at-least-once** delivery.

The consumer app runs **two consumers** in the same `notification-service` group:

- `orders.created` → `createHandlerRunner` with the notification handler.
- `payments.completed` → inline handler that logs `payment recorded` and commits.

With `BROKER_DRIVER=in-memory`, the consumer self-generates a 5-event sample workload so the whole pipeline is visible end to end in one process. With `BROKER_DRIVER=confluent`, events come from the standalone producer service.

### Web producer — transactional outbox

The web app is the outbox producer. `POST /api/orders` (`apps/web/src/order-store.ts`) writes the
`orders` row and two outbox rows (`orders.created`, `payments.completed`) to a local **SQLite**
database (`node:sqlite`, built into Node 22 — zero native deps, no Alpine/CI build risk) in **one
transaction**: the order is durable even while Kafka is down, and the HTTP response does not depend
on a broker round-trip.

An `OutboxRelay` (`packages/infra/src/outbox.ts`) polls the `outbox` table and publishes each batch
inside a broker transaction (`beginTransaction()`, Phase 1 transactions): every message in the batch
commits or aborts together. Rows are marked published only **after** the commit — a crash between
commit and mark re-publishes the row, so delivery is **at-least-once** and consumers must be
idempotent (the notification handler is). The confluent consumer is pinned to
`isolation.level = read_committed`, so messages from aborted transactions are never delivered.
`node:sqlite` is experimental in Node 22 and logs an `ExperimentalWarning`; it needs no flag on
Node ≥ 22.13 (Docker / CI images resolve to the latest 22.x).

---

## Quickstart

Requirements: **Docker** (standard usage) or **Node.js 22+** (no-Docker path).

### Standard usage — Docker stack

```bash
docker compose up --build
```

This is the recommended way to run the project: it starts Kafka, all app services and both UIs, and shows real message flow end to end.

#### What's running

| Service | Container | What it does | Where to look |
|---|---|---|---|
| **Kafka** | `nodejs-kafka-kafka` | KRaft-mode broker (no ZooKeeper), listens on `9092` | — |
| **Kafka UI** | `nodejs-kafka-ui` | Browse topics, partitions, messages and consumer-group offsets | [http://localhost:8080](http://localhost:8080) |
| **producer** | `nodejs-kafka-producer` | Publishes a batch of order + payment events, then exits | `docker compose logs producer` |
| **consumer** | `nodejs-kafka-consumer` | Long-running worker: `parse → retry → retry topic → DLQ → commit` on `orders.created` + `payments.completed` | `docker compose logs -f consumer` |
| **web** | `nodejs-kafka-web` | REST + SSE server, React flow-tracker UI, and the SQLite transactional outbox | [http://localhost:3000](http://localhost:3000) |
| **customer-view** | `nodejs-kafka-customer-view` | KTable-style read model over the compacted `customers` topic (`GET/DELETE /customers`) | [http://localhost:3001](http://localhost:3001) |
| **otel-collector** | `nodejs-kafka-otel-collector` | Receives OTLP traces over HTTP on `4318` and prints them via its debug exporter (*observability profile*) | `docker compose logs otel-collector` |
| **prometheus** | `nodejs-kafka-prometheus` | Scrapes the four apps' `/metrics` endpoints plus the collector's own metrics (*observability profile*) | [http://localhost:9090](http://localhost:9090) |
| **grafana** | `nodejs-kafka-grafana` | Visualizes the metrics with a provisioned Prometheus datasource + "Node.js Kafka" dashboard (*observability profile*) | [http://localhost:3002](http://localhost:3002) |
| **kafka-b** | `nodejs-kafka-kafka-b` | Second single-node KRaft cluster (`apache/kafka:3.7.0`) — the mirror target, listens on `9094` (*mirror profile*) | — |
| **mirror-maker** | `nodejs-kafka-mirror-maker` | MirrorMaker 2 worker replicating `orders.*` from `kafka` to `kafka-b` (*mirror profile*) | `docker compose logs mirror-maker` |
| **kafka-secured** | `nodejs-kafka-kafka-secured` | SASL_SSL-secured KRaft broker (PLAIN auth + `AclAuthorizer`, topic ACLs), listens on `9095` (*security profile*) | — |
| **security-init** | `nodejs-kafka-security-init` | One-shot job granting the `app` user topic-scoped ACLs on the secured cluster, then exits (*security profile*) | `docker compose logs security-init` |
| **producer-secured** | `nodejs-kafka-producer-secured` | Publishes the demo batch over SASL_SSL as the `app` user (*security profile*) | `docker compose logs producer-secured` |
| **consumer-secured** | `nodejs-kafka-consumer-secured` | Long-running worker over SASL_SSL as the `app` user: `parse → retry → retry topic → DLQ → commit` (*security profile*) | `docker compose logs -f consumer-secured` |

#### How the stack works

1. **Startup order** — `kafka` starts first and is health-checked (it must answer `kafka-topics.sh --list`). `kafka-ui`, `producer`, `consumer`, `web` and `customer-view` wait for it via `depends_on: condition: service_healthy`, so nothing connects before the broker is ready.
2. **Topics** — the apps create their topics at startup through the broker's admin client: `orders.created`, `payments.completed`, `orders.dlq`, `telemetry.events`, and `customers` (compacted — created with `cleanup.policy=compact`). `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` keeps the cluster explicit.
3. **Produce** — the `producer` service publishes deterministic order + payment events and exits; orders placed from the web UI go through the **SQLite transactional outbox** instead (see [Web producer — transactional outbox](#web-producer--transactional-outbox)). Every third order is deliberately **oversized** (> 100 000 cents) to fire the simulated provider timeout. Each order also feeds a per-customer aggregate: the producer publishes full-state `CustomerUpdated` records (key = `customerId`) to the compacted `customers` topic.
4. **Consume** — the `consumer` reads them in the `notification-service` group and runs each message through `parse → retry → DLQ → commit`, emitting a telemetry event per step to `telemetry.events`.
5. **Visualise** — the `web` service consumes `telemetry.events` in the `web-telemetry` group and broadcasts each event to browsers over Server-Sent Events, so the flow diagram and the event log update live.
6. **Query the read model** — the `customer-view` service consumes `customers` in the `customer-view` group and serves the current per-customer totals over REST (`GET /customers/:id`); `DELETE /customers/:id` publishes a tombstone, so the read model evicts the customer.
7. **Observe (optional)** — run the observability stack with `docker compose --profile observability up` alongside the base stack: the apps export OTel spans to `otel-collector` (visible in its logs via the debug exporter) and expose Prometheus metrics at `/metrics`, Prometheus scrapes all four apps (see its targets on `:9090`), and Grafana visualizes them with a provisioned datasource + dashboard on `:3002`. The default `docker compose up` (no profile) is unchanged and starts no observability infrastructure; an app only activates tracing/metrics when `OTEL_EXPORTER_OTLP_ENDPOINT` / `METRICS_PORT` are set.
8. **Mirror (optional)** — run the mirror stack with `docker compose --profile mirror up` alongside the base stack: a second single-node KRaft cluster (`kafka-b`, host port `9094`) starts and a MirrorMaker 2 worker replicates `orders.*` (`orders.created`, `orders.payment.*`, `orders.dlq`) from the primary `kafka` cluster to it. Kafka UI on `:8080` then lists both clusters — `local` and `mirror`. The default `docker compose up` (no profile) is unchanged and starts no mirror infrastructure; the compacted `customers` topic is intentionally not mirrored.
9. **Secure (optional)** — run the secured stack with `docker compose --profile security up` alongside the base stack: a second single-node KRaft cluster (`kafka-secured`, host port `9095`) starts with a **SASL_SSL** listener (PLAIN auth, `admin` super user + `app`), generates self-signed certs **inside the container** via the JRE's `keytool` (no host scripts), and a one-shot `security-init` job provisions the `app` user's topic-scoped ACLs. Only then do `producer-secured` / `consumer-secured` start and round-trip messages over SASL_SSL. The default `docker compose up` (no profile) is unchanged and the base `kafka` cluster stays plaintext.

> State is container-local: the broker keeps its KRaft logs inside the container (no volumes). `docker compose down` therefore **resets all Kafka state**, and the next `docker compose up --build` replays the demo from the beginning.

#### What to check

1. **Containers are up** — `docker compose ps` shows the six services running (`Up` / `healthy`). The first build takes a while (the apps compile TypeScript inside the image); `docker compose logs -f web` prints `web UI listening` when it is ready.
2. **Kafka is healthy** — open [http://localhost:8080](http://localhost:8080) (Kafka UI):
   - *Topics* → `orders.created`, `payments.completed`, `orders.dlq`, `telemetry.events`, `customers`. Open one → *Browse messages* → the JSON payloads.
   - *Consumer groups* → `notification-service` advances its offset as messages are consumed; `web-telemetry` advances with each telemetry event; `customer-view` advances with each changelog record.
3. **The pipeline ran** — `docker compose logs consumer` shows `consumers running`, and per message: `handler failed, will retry` → `handler exhausted retries, sending to DLQ` for the oversized order, then the offset commit. `docker compose logs producer` shows `event produced` lines with `topic`, `eventId`, `partition`, `offset`.
4. **Watch it live** — open [http://localhost:3000](http://localhost:3000) and place an order from the form. The flow diagram lights up `producer → orders.created → consumer → committed`, and the live event log streams each step (`consumed`, `parsed`, `committed`, `payment-recorded`) with topic, partition, offset and a concept tag.
5. **Trigger retry → DLQ** — place an order with **total above 100000 cents**; the log shows `retrying` → `dead-lettered`, and the message lands in `orders.dlq` (visible in Kafka UI).
6. **Clean up / reset** — `docker compose down` stops everything and wipes Kafka state; `docker compose up --build` starts a fresh run.
7. **Exercise the read model** — `curl http://localhost:3001/customers` lists the aggregated customers; `curl http://localhost:3001/customers/CUST-1003` shows one customer; `curl -X DELETE http://localhost:3001/customers/CUST-1003` tombstones it (subsequent GET returns 404, and the record appears in Kafka UI's `customers` topic).

### Alternative — in-memory driver (no Docker)

Same port contract, zero dependencies — good for a quick look or CI. The consumer self-generates a 5-event sample workload, so the whole pipeline runs in one process:

```bash
npm install
npm run build
npm run dev:consumer     # self-generates 5 orders → consumes → retries → DLQ
```

You'll see the full pipeline in the JSON logs: orders processed, one oversized order retried (`handler failed, will retry`) and dead-lettered (`handler exhausted retries, sending to DLQ`), and payments recorded. You can also publish explicitly:

```bash
npm run dev:producer -- --count 10 --delay 300
npm run dev:customer-view   # in-memory demo: self-generates a sample changelog + tombstone, serves the read model on :3001
```

In-memory mode uses the same port contract as real Kafka, so the semantics (topics, partition key routing, offsets) are exercised identically — but there is **no web UI** in this mode (the `customer-view` REST API works on :3001). The customer view self-generates records for three customers and tombstones one (CUST-1002), so `GET /customers` shows the survivors.

---

## Switching drivers

`BROKER_DRIVER=in-memory|confluent` selects the adapter behind the port — apps and domain code never change.

| Capability | `in-memory` | `confluent` |
|---|---|---|
| Core pipeline (produce/consume/offsets/DLQ) | ✅ | ✅ |
| In-process retry (backoff + jitter) | ✅ | ✅ |
| Retry topic + scheduled retry | ❌ (in-process only) | ✅ |
| Transactions / transactional outbox (SQLite + relay) | ❌ | ✅ |
| Schema Registry + Avro | ❌ | ✅ |
| Compacted topics / customer-360 | ✅ (simulated) | ✅ |
| Observability (OTel + metrics) | ✅ | ✅ |
| MirrorMaker 2 / multi-cluster | ❌ | ✅ |
| SASL/TLS/ACL | ❌ | ✅ |

Step-by-step: [docs/guides/driver-switching.md](docs/guides/driver-switching.md)
Schema evolution with Schema Registry + Avro: [docs/guides/schema-registry.md](docs/guides/schema-registry.md)
Compacted topics (customer-360) with tombstones: [docs/guides/compacted-topics.md](docs/guides/compacted-topics.md)
Observability (OTel spans + Prometheus metrics): [docs/guides/observability.md](docs/guides/observability.md)
Multi-cluster mirroring (MirrorMaker 2): [docs/guides/multi-cluster-mirroring.md](docs/guides/multi-cluster-mirroring.md)
Security (SASL_SSL + PLAIN auth + topic ACLs): [docs/guides/security.md](docs/guides/security.md)

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
| `BROKER_SSL_CA_PATH` | (empty) | Path to a PEM CA file used to verify the broker's TLS certificate (confluent driver) |
| `BROKER_SSL_CERT_PATH` | (empty) | Path to a PEM client certificate (confluent driver; only needed for mTLS) |
| `BROKER_SSL_KEY_PATH` | (empty) | Path to the PEM private key for the client certificate (confluent driver; only needed for mTLS) |
| `SCHEMA_REGISTRY_URL` | (empty) | Schema Registry URL; enables the Avro codec (driver must be `confluent`); empty → JSON codec |
| `BROKER_MEMORY_AUTO_COMMIT` | `true` | In-memory driver: commit offsets automatically after handler resolve |
| `CONSUMER_GROUP_ID` | `notification-service` | Consumer group id for both consumers |
| `CONSUMER_FROM_BEGINNING` | `true` | Start reading from the earliest offset when no committed offset exists |
| `LOG_LEVEL` | `info` | pino level: `trace` / `debug` / `info` / `warn` / `error` / `fatal` |
| `SERVICE_NAME` | `nodejs-kafka` | Tag used in structured log records |
| `WEB_PORT` | `3000` | Web UI HTTP port |
| `TELEMETRY_GROUP_ID` | `web-telemetry` | Consumer group id for the telemetry event stream |
| `OUTBOX_DB_PATH` | `data/outbox.db` | Web app: SQLite file for the `orders` + `outbox` tables (parent dir is created on start; compose mounts a named volume at `/data`) |
| `CUSTOMER_VIEW_PORT` | `3001` | `customer-view` service: HTTP port for the customer read model |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (empty) | OTel OTLP/HTTP traces endpoint (e.g. `http://otel-collector:4318/v1/traces`); empty → tracing disabled |
| `OTEL_SERVICE_NAME` | (empty) | OTel resource `service.name` (falls back to `nodejs-kafka` when empty) |
| `METRICS_PORT` | (empty) | Port for the app's Prometheus `/metrics` HTTP server; empty → metrics server disabled |

> Metric endpoints in the compose stack: producer → `producer:9464/metrics`, consumer → `consumer:9465/metrics` (both via `METRICS_PORT`), web → `web:3000/metrics`, customer-view → `customer-view:3001/metrics` (both served on their own Express port).

> SASL + TLS protocol selection: `BROKER_SASL_USERNAME` / `BROKER_SASL_PASSWORD` enable **PLAIN** auth; combined with the `BROKER_SSL_*` paths, the confluent driver auto-selects `security.protocol` — `sasl_ssl` (SASL + TLS), `sasl_plaintext` (SASL only), `ssl` (TLS only), `plaintext` (neither). All empty → `plaintext`, unchanged. See the [security guide](docs/guides/security.md).

> The MirrorMaker 2 flow is configured in `compose/mirror/mm2.properties` (cluster aliases, `source->target.topics = orders.*`, internal-topic replication factors) — no new user env vars; the profile is enabled with `--profile mirror`.

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
export type EventTopic = keyof typeof topicToType;             // 'orders.created' | 'payments.completed' | 'customers'
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
| `customers` | `CustomerUpdated` | Full-state per-customer changelog — compacted topic, key = `customerId`, latest record wins, null value = tombstone (delete) |
| `orders.dlq` | `DlqEntry` | Failed messages (parse failure or handler exhaustion) |
| `orders.retry` | Original payload + headers | Scheduled retries — `retry-count` + `next-deliver-at` headers, escalating delay, max 3 deliveries → DLQ. JSON codec. |
| `telemetry.events` | `TelemetryEvent` | Per-step pipeline telemetry for the live flow tracker |

The sample generators (`packages/domain/src/sample/sample-events.ts`) create deterministic, spec-compliant events. Every third order is deliberately **oversized** (> 100 000 cents) so the notification handler's simulated provider timeout fires — demonstrating the retry + DLQ pipeline. Payment events mirror each order (`orderId`, `amountCents`, `method`).

### Telemetry events (flow tracker)

Every pipeline step emits a `TelemetryEvent` to `telemetry.events` so the web UI can replay the flow live. The consumer publishes one per step it executes (`packages/consumer` → `TelemetryClient`); the web app's outbox relay adds a `produced` event per published outbox row.

```ts
// packages/domain/src/events/telemetry.ts
export const TELEMETRY_TOPIC = 'telemetry.events';

type TelemetryEventType =
  | 'produced'        // outbox relay published a queued order
  | 'consumed'        // consumer picked up the message
  | 'parsed'          // payload passed Zod validation
  | 'retrying'        // handler failed, backing off
  | 'retry-parked'    // retry message held until next-deliver-at
  | 'retry-scheduled' // published to the retry topic (orders.retry)
  | 'dead-lettered'   // exhausted retries → DLQ
  | 'committed'       // offset committed after success
  | 'payment-recorded'// payments.completed processed
  | 'invalid-to-dlq'; // parse failure → DLQ

interface TelemetryEvent {
  type: TelemetryEventType;
  topic: string;        // origin topic (e.g. 'orders.created')
  eventId: string;      // id of the source event
  orderId: string;      // grouped per order for the flow diagram
  partition: number;
  offset: string;
  attempt?: number;     // set for retrying / dead-lettered
  message: string;      // human-readable summary shown in the event log
  concept: string;      // educational tag (e.g. 'retry', 'dlq', 'at-least-once')
  occurredAt: string;   // ISO-8601 timestamp
}
```

The web server consumes the topic in the `web-telemetry` group (`TELEMETRY_GROUP_ID`) and broadcasts every validated event to connected browsers over SSE.

### Web API

| Route | Method | Description |
|---|---|---|
| `/api/health` | `GET` | Liveness probe |
| `/api/orders` | `POST` | Places an order — writes the order + outbox rows to SQLite in one transaction (published asynchronously by the outbox relay). Request body is validated server-side (Zod) and returns `400` with a field error list when invalid |
| `/api/events` | `GET` | Server-Sent Events stream of `TelemetryEvent`s (`text/event-stream`), used by the flow diagram and event log |

The React UI is served statically on the same origin (see `apps/web/src/ui`).

---

## What this repo demonstrates

| Concept | Where |
|---|---|
| **Ports & Adapters** (hexagonal) | `packages/broker/src/port.ts`, `packages/broker/src/factory.ts` |
| **Pluggable serialization** (JSON default; Avro via Schema Registry) | `packages/broker/src/codec/` |
| **Schema Registry + Avro** (curated schemas, BACKWARD compat) | `packages/broker/src/codec/avro.ts`, `packages/domain/src/events/avro-schemas.ts` |
| **Type-safe messaging** (compile + runtime) | Zod schemas + `parseEvent()` + `TypedPublisher` |
| **Discriminated union events** | `EventPayload`, `EventOf<Topic>`, `topicToType` |
| **Dead Letter Queue** | `packages/infra/src/dlq.ts`, `apps/consumer/src/handler-runner.ts` |
| **Retry with exponential backoff + jitter** | `packages/infra/src/retry.ts` |
| **Manual offset commit (at-least-once)** | `ConfluentKafkaAdapter.consume`, `handler-runner.ts` |
| **Transactional outbox** (SQLite write + relay, `read_committed`) | `packages/infra/src/outbox.ts`, `apps/web/src/order-store.ts` |
| **Compacted topics / customer-360** (KTable read model, tombstones) | `apps/customer-view/`, `packages/domain/src/aggregation/customer-state.ts` |
| **Idempotent producer** (`acks=all`, `enable.idempotence`) | `ConfluentKafkaAdapter.getProducer` |
| **Graceful shutdown** (drain + commit + exit) | `packages/infra/src/shutdown.ts` |
| **Structured logging** (pino, JSON) | `packages/infra/src/logger.ts`, logger bridge to the Kafka client |
| **Env validation** (fail-fast config) | `packages/infra/src/config.ts` |
| **Partition key routing** | `partitionForKey()` hash in `InMemoryBrokerAdapter`, producer `key` |
| **Consumer groups / offsets** | `ConsumeOptions` (manual commit, group id, concurrency) |
| **Multi-stage Docker builds** | `apps/*/Dockerfile` |
| **KRaft Kafka (no ZooKeeper)** | `docker-compose.yml` |
| **Unit + integration tests** | Vitest, 140 tests, no Kafka required |

---

## Repository layout

```
apps/
  producer/          CLI that publishes order + payment events, then exits
  consumer/          long-running worker with parse → retry → DLQ → commit pipeline
  web/               Express REST + SSE server and React flow-tracker UI
  customer-view/     Express read model over the compacted customers topic (tombstones)
packages/
  broker/            IMessageBroker port + in-memory / confluent adapters
  domain/            Zod schemas, event union, sample generators, notification handler
  infra/             config, logging, retry, DLQ, outbox, publisher, shutdown
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
| `npm test` | Vitest — 140 tests, runs without any Kafka |
| `npm run dev:producer -- --count N` | Produce N order+payment pairs (`--delay` also accepted, ms) |
| `npm run dev:consumer` | Consumer worker (in-memory self-demo) |
| `npm run dev:web` | Web UI — Express API on :3000, Vite dev UI on :5173 (needs real Kafka) |
| `npm run dev:customer-view` | Customer read model — REST API on :3001 (in-memory self-demo) |
| `docker compose up --build` | Full stack with Kafka UI on :8080 and web UI on :3000 |

---

## Tests

Vitest, configured in `vitest.config.ts`. All 140 tests across 22 files run **without Kafka** — they use the in-memory driver, `node:sqlite` `:memory:` databases, and mocks:

| Suite | File | Tests |
|---|---|---|
| Retry behaviour | `packages/infra/test/retry.test.ts` | 4 |
| Retry topic scheduler | `packages/infra/test/retry-topic.test.ts` | 6 |
| Outbox store + relay | `packages/infra/test/outbox.test.ts` | 10 |
| Event schemas | `packages/domain/test/schemas.test.ts` | 6 |
| Customer changelog (aggregation) | `packages/domain/test/customer.test.ts` | 6 |
| Avro schemas (curated) | `packages/domain/test/avro-schemas.test.ts` | 3 |
| Avro codec (Schema Registry) | `packages/broker/test/avro.test.ts` | 7 |
| Broker config wiring (incl. SASL/TLS protocol selection) | `packages/infra/test/broker.test.ts` | 12 |
| Telemetry schema | `packages/domain/test/telemetry.test.ts` | 4 |
| Confluent adapter (mocked driver; incl. transactions + SASL/TLS config mapping) | `packages/broker/test/confluent.test.ts` | 23 |
| In-memory broker | `packages/broker/test/in-memory.test.ts` | 8 |
| Broker tracing (spans) | `packages/broker/test/tracing.test.ts` | 3 |
| Codec (JSON + wiring) | `packages/broker/test/codec.test.ts` | 9 |
| Telemetry client | `packages/infra/test/telemetry.test.ts` | 3 |
| Metrics registry + server | `packages/infra/test/metrics.test.ts` | 6 |
| Tracing bootstrap | `packages/infra/test/tracing.test.ts` | 1 |
| Publisher | `packages/infra/test/publisher.test.ts` | 2 |
| Dead-letter queue | `packages/infra/test/dlq.test.ts` | 1 |
| Consumer pipeline | `apps/consumer/test/pipeline.test.ts` | 4 |
| Retry topic pipeline | `apps/consumer/test/retry-topic.test.ts` | 6 |
| Web server | `apps/web/test/server.test.ts` | 7 |
| Customer view (read model + HTTP) | `apps/customer-view/test/customer-view.test.ts` | 9 |

CI (`.github/workflows/ci.yml`) runs `npm ci` → `build` → `typecheck` → `lint` → `test` on Node 22.

---

## Roadmap

- [x] `ConfluentKafkaAdapter` — idempotent producer (`acks=all`, `enable.idempotence`), manual offset commit, consumer group rebalancing, headers
- [x] Wire producer ↔ consumer through real Kafka in Docker (`BROKER_DRIVER=confluent`)
- [x] KRaft-mode Kafka (no ZooKeeper) via the official `apache/kafka` image
- [x] Retry topic + scheduled retry (escalating delay, max deliveries → DLQ)
- [x] Kafka Streams–style aggregation / compacted topics (customer 360 view)
- [x] Transactional outbox (SQLite order + outbox rows in one write, transactional relay, `read_committed` consumers)
- [x] Schema Registry + Avro serialization for schema evolution
- [x] Observability — OpenTelemetry manual spans (`produce` / `consume`) + Prometheus `/metrics` per app + optional `observability` compose profile (collector / Prometheus / Grafana)
- [x] Multi-cluster mirroring — MirrorMaker 2 replicating `orders.*` to a second single-node KRaft cluster via the optional `mirror` compose profile
- [x] Security — SASL_SSL (TLS + PLAIN auth) + topic-scoped ACLs on the optional `security` compose profile, with self-signed certs generated in-container (`keytool`, no host scripts) and confluent-driver TLS support via `BROKER_SSL_*` env vars
