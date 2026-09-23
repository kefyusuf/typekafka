# Driver switching

This guide explains how `typekafka` swaps between its two broker drivers behind the `IMessageBroker` port, and which driver supports which capability.

## The two drivers

| Driver | Adapter class | When to use |
|---|---|---|
| `in-memory` | `InMemoryBrokerAdapter` | Local dev, CI, demos — zero dependencies, no Kafka required |
| `confluent` | `ConfluentKafkaAdapter` | Production / Docker — real Kafka via `@confluentinc/kafka-javascript` |

## How switching works

`createBroker({ driver })` reads the `BROKER_DRIVER` environment variable and returns the matching adapter. The port (`packages/broker/src/port.ts`) is the only seam — apps and domain code never change.

```ts
const broker = createBroker({
  driver: process.env.BROKER_DRIVER,      // 'in-memory' | 'confluent'
  connection: { brokers, clientId },
  logger,
});
```

The adapters translate between the native client shape and a broker-agnostic `KafkaMessage` envelope, so business code never sees `librdkafka` buffers or KafkaJS internals.

## Switch to the in-memory driver

The in-memory driver needs no Kafka at all. The consumer self-generates a 5-event sample workload, so the whole pipeline runs in one process:

```bash
npm install
npm run build
npm run dev:consumer     # self-generates 5 orders → consumes → retries → DLQ
```

## Switch to the Confluent driver

The Docker stack runs the real Kafka (KRaft mode) and every service connects to it:

```bash
docker compose up --build
```

What each service connects to:

| Service | Connects to | Purpose |
|---|---|---|
| `kafka` | — (broker, PLAINTEXT on `9092`) | KRaft-mode Kafka, no ZooKeeper |
| `kafka-ui` | `kafka:9092` | Browse topics, partitions, messages and consumer-group offsets on `:8080` |
| `producer` | `kafka:9092` (`BROKER_DRIVER=confluent`) | Publishes order + payment events, then exits |
| `consumer` | `kafka:9092` (`BROKER_DRIVER=confluent`) | Runs `parse → retry → DLQ → commit` on `orders.created` + `payments.completed` |
| `web` | `kafka:9092` (`BROKER_DRIVER=confluent`) | REST + SSE server and the React flow-tracker UI on `:3000` |

## Capability matrix

| Capability | `in-memory` | `confluent` |
|---|---|---|
| Core pipeline (produce/consume/offsets/DLQ) | ✅ | ✅ |
| In-process retry (backoff + jitter) | ✅ | ✅ |
| Retry topic + scheduled retry | ❌ (in-process only) | ✅ |
| Transactions / exactly-once outbox | ❌ | ✅ |
| Schema Registry + Avro | ❌ | ✅ |
| Compacted topics / customer-360 | ✅ (simulated) | ✅ |
| Observability (OTel + metrics) | ✅ | ✅ |
| MirrorMaker 2 / multi-cluster | ❌ | ✅ |
| SASL/TLS/ACL | ❌ | ✅ |

## What changes, what doesn't

- Apps and domain code are untouched — they talk to the port, not to a specific driver.
- The `BROKER_*` environment variables (`BROKER_DRIVER`, `BROKER_BROKERS`, `BROKER_CLIENT_ID`, `BROKER_SASL_USERNAME`, `BROKER_SASL_PASSWORD`, `BROKER_MEMORY_AUTO_COMMIT`) are documented in `.env.example` and validated at startup.
- The consumer pipeline (parse, retry, DLQ, commit) behaves identically on both drivers.
- Only serialization, transaction, and retry-topic capabilities differ — see the matrix above.

## Troubleshooting

- **`Broker is not connected`** — the adapter was used before `connect()` was called. Ensure the app awaits `broker.connect()` before producing or consuming.
- **SASL mismatch** — `BROKER_SASL_USERNAME` / `BROKER_SASL_PASSWORD` are not set while the broker requires SASL. Set both (they are only used when both are present), or configure the broker without SASL for local testing.
- **Driver mismatch** — `in-memory` is used but you expected the compose Kafka cluster. Check `docker compose ps` to confirm the stack is up, and verify `BROKER_DRIVER=confluent` in the service environment.
