# Schema Registry + Avro

This guide explains how `typekafka` adds Schema Registry + Avro serialization behind the existing `MessageCodec` seam — curated Avro schemas, explicit registration with `BACKWARD` compatibility, and the Confluent wire format — without changing any application or domain code.

## Why Schema Registry + Avro

Plain JSON messages carry no schema, so producer and consumer must agree on the shape out of band. If the producer adds a field, an old consumer can still read the record, but a changed or renamed field silently breaks everything downstream.

Schema Registry fixes that by storing a **versioned schema per subject** and enforcing **compatibility rules** when a new version is registered:

- **Schema evolution safety** — the registry rejects a change that would break existing consumers, before the new producer even ships.
- **Backward compatibility** — a consumer compiled against the old schema can read records written with the new schema. We pin `BACKWARD` on every subject because it matches how real consumer pipelines evolve: new producers can start writing new data while older consumers keep reading it.
- **A single source of truth** — the schema lives in one place (the registry), and every Avro payload carries its schema id in the wire format, so the reader always knows which schema version to use.

`FORWARD` compatibility flips the direction (old producers, new consumers); `FULL` requires both; `NONE` turns the check off. For this reference project, `BACKWARD` is the right balance — consumers are the long-lived part of a Kafka pipeline.

## What you need

- The **separate** `@confluentinc/schemaregistry` npm package (`@confluentinc/schemaregistry@^1.10.0`) — Confluent removed the registry client from the driver package, so it lives on its own and stays a pure-JS dependency.
- The **Docker `schema-registry` profile** in `docker-compose.yml`, which runs `confluentinc/cp-schema-registry:7.7.0` on `:8081` alongside Kafka. The base stack keeps working without it.

## How it fits together

```
packages/domain/src/events/avro-schemas.ts     topicToAvroSchema (curated schemas)
        │
        ▼
packages/broker/src/codec/avro.ts              AvroCodec (register + BACKWARD + wire format)
        │
        ▼
packages/infra/src/broker.ts                   buildBrokerConfig (SCHEMA_REGISTRY_URL → codec)
        │
        ▼
apps/*/src/main.ts                             createBroker(buildBrokerConfig(...))
```

- **Curated schemas** — `topicToAvroSchema` in `packages/domain/src/events/avro-schemas.ts` maps every event topic to an Avro record authored beside its Zod schema, so the validation schema and the wire schema stay in the same place.
- **`AvroCodec`** (`packages/broker/src/codec/avro.ts`) sits behind the existing `MessageCodec` seam. On first use it registers each schema under the Confluent `{topic}-value` subject (e.g. `orders.created-value`) and pins `BACKWARD` compatibility. Payloads use the **Confluent wire format**: a `0x00` magic byte, a 4-byte big-endian schema id, then the Avro payload.
- **Wiring** — `buildBrokerConfig` in `packages/infra/src/broker.ts` composes the `AvroCodec` only when `driver === 'confluent'` **and** `SCHEMA_REGISTRY_URL` is set; otherwise it returns a JSON codec, identical to today.
- **Per-topic fallback** — topics without an Avro schema (DLQ, retry, telemetry) keep the exact `JsonCodec` behavior, so mixed payloads flow through the same codec.

## Configuration

`SCHEMA_REGISTRY_URL` (in `.env.example`) is the only knob:

| Variable | Default | Description |
|---|---|---|
| `SCHEMA_REGISTRY_URL` | (empty) | Schema Registry URL; enables the Avro codec (driver must be `confluent`); empty → JSON codec |

- `BROKER_DRIVER=in-memory` → JSON codec, regardless of the URL.
- `BROKER_DRIVER=confluent` + empty URL → JSON codec (default, unchanged).
- `BROKER_DRIVER=confluent` + URL → Avro codec with `BACKWARD` compat.

## Run it

```bash
docker compose --profile schema-registry up -d
```

This starts Kafka, the Schema Registry, and the three app services. The producer and consumer then round-trip `orders.created` / `payments.completed` in Avro over the wire:

1. The `AvroCodec` registers both schemas on first produce (see `init()`), and `docker compose logs producer` shows the events publishing normally — the codec is invisible to app code.
2. Kafka UI on `:8080` shows the same messages, now as Avro (browse via the "Avro" rendering). DLQ and telemetry topics stay plain JSON.
3. The consumer decodes the Avro payloads using the schema id embedded in each record.

## Verify

The registry exposes a REST API. After the stack is up:

```bash
curl http://localhost:8081/subjects
```

Expected output — both value subjects registered, per the `{topic}-value` convention:

```json
["orders.created-value","payments.completed-value"]
```

Check compatibility and the schema itself:

```bash
curl http://localhost:8081/config/orders.created-value
curl http://localhost:8081/subjects/orders.created-value/versions/latest
```

## Schema evolution

To add a field to an event:

1. Add it to the Zod schema in `packages/domain/src/events/*.ts` **and** to the matching Avro record in `packages/domain/src/events/avro-schemas.ts`.
2. Give the new Avro field a **default** (or make it optional) — an append-only change stays `BACKWARD`-compatible, so old consumers keep reading.
3. The sync test (`packages/domain/test/avro-schemas.test.ts`) enforces that the Avro field names always equal the Zod schema keys, so the two can't drift silently.

Change the compatibility level (e.g. to `FULL` or `NONE`) in `AvroCodec.registerAll()` only when you understand the trade-off — see [Why Schema Registry + Avro](#why-schema-registry--avro).

## Troubleshooting

- **Registration failure** — a `BrokerError` ("schema registry registration failed for subject ...") means the registry was unreachable at first use. Check the registry container (`docker compose ps schema-registry`) and `SCHEMA_REGISTRY_URL`.
- **Value does not match the schema** — a `MessageParseError` on serialize/deserialize means the payload violated the Avro record. Zod already validated it against the same field set, so this points at a drift between the two schemas — the sync test catches that.
- **`expected a Buffer (Confluent wire format)`** — an Avro topic received a non-Buffer payload. Consumers hand the codec a `Buffer`; if a message was written as JSON, re-publish it through the Avro codec.
- **Registry unreachable but the app still starts** — registration is lazy (first produce/consume on an Avro topic), matching how apps without SR config keep working with plain JSON.

## What changes, what doesn't

- **No port signature changes** — `produce<T>` / `consume` / `beginTransaction` are untouched; apps and domain code are unchanged.
- **No behavior change by default** — `BROKER_DRIVER=in-memory` or an empty `SCHEMA_REGISTRY_URL` keeps the JSON codec exactly as before.
- **What changes** — the `confluent` driver with a registry URL serializes event topics in Avro and registers schemas with `BACKWARD` compatibility. Everything else (pipeline, DLQ, retries, telemetry, transactions) behaves identically.
