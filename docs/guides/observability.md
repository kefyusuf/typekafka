# Observability — OTel spans + Prometheus metrics

This guide explains how `nodejs-kafka` adds OpenTelemetry **manual spans** around produce and consume plus a **Prometheus `/metrics`** endpoint per app — with an optional `observability` Docker profile (`otel-collector` → Prometheus → Grafana) — without changing the broker port, the pipeline, or any domain code.

## Why observability

A Kafka pipeline is a black box without telemetry: messages leave the producer, hop topics, get retried, dead-lettered, and consumed — and you can't tell from the code where the time goes or where messages stall. This repo ships two complementary signals:

- **Traces** — one span per `produce` and per `consume`, so you can see the messaging operations the apps actually perform. There is **no distributed-trace UI** shipped here: the collector's **debug exporter** prints exported spans to its own logs, which is enough to inspect them locally.
- **Metrics** — a small set of Prometheus counters/histograms per app, scraped by Prometheus and visualized in a provisioned **Grafana** dashboard.

## How it fits together

```
packages/broker/src/trace.ts                    withSpan (tracer '@nodejs-kafka/broker')
        │  manual spans: produce / consume
        ▼
packages/broker/src/adapters/*.ts               InMemoryBrokerAdapter, ConfluentKafkaAdapter
        │
        ▼
packages/infra/src/tracing.ts                   initTracing (NodeSDK + OTLP HTTP exporter)
        │                                        no-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set
        ▼
apps/*/src/main.ts                              startMetricsServer / metricsMiddleware (/metrics)
```

- **Spans are manual** — `withSpan` in `packages/broker/src/trace.ts` wraps the produce and consume paths in both adapters. It calls `trace.getTracer('@nodejs-kafka/broker')` from `@opentelemetry/api`, which is a **no-op without an SDK**, so an app that never calls `initTracing` pays a few nanoseconds per message and ships zero telemetry.
- **SDK bootstrap** — each app calls `initTracing` (`packages/infra/src/tracing.ts`) at startup. With an endpoint set it starts a `NodeSDK` with an `OTLPTraceExporter`; with an empty endpoint (or on init failure) it returns a no-op and logs a warning.
- **Metrics** — `createMetrics` (`packages/infra/src/metrics.ts`) builds a per-app `prom-client` registry. Apps with a `METRICS_PORT` serve the registry on a dedicated HTTP server at `/metrics`; the web and customer-view apps mount the same registry as an Express middleware on their own port.

> kafka-javascript (like kafkajs) has **no official auto-instrumentation**, which is exactly why these spans are manual: the two adapter methods are the one place every message passes through, so wrapping them captures all messaging activity without touching app code.

## Span points

Both adapters emit the same two spans:

| Span | Where | Attributes |
|---|---|---|
| `produce` (`produce <topic>`) | `InMemoryBrokerAdapter.produce`, `ConfluentKafkaAdapter.produce` | `messaging.system=kafka`, `messaging.destination=<topic>`; the in-memory adapter also records `messaging.destination_partition` (it knows the partition before "send"; the confluent driver only learns it from the broker's produce response) |
| `consume` (`consume <topic>`) | `InMemoryBrokerAdapter` handler dispatch, `ConfluentKafkaAdapter` message handler | `messaging.destination=<topic>` plus `topic`, `partition`, `offset` context attributes |

The span name is the literal string `produce` / `consume` (span names are static in OpenTelemetry); the topic lives in `messaging.destination`.

## Metrics

All counters are labeled by `topic`. `packages/infra/test/metrics.test.ts` asserts the registry wiring.

| Metric | Type | Labels | Emitted by |
|---|---|---|---|
| `nodejs_kafka_messages_produced_total` | Counter | `topic` | producer |
| `nodejs_kafka_messages_consumed_total` | Counter | `topic` | consumer, customer-view |
| `nodejs_kafka_handler_duration_ms` | Histogram | `topic` | consumer, customer-view |
| `nodejs_kafka_retries_total` | Counter | `topic` | consumer |
| `nodejs_kafka_dlq_total` | Counter | `topic` | consumer |
| `nodejs_kafka_outbox_published_total` | Counter | `topic` | web (outbox relay) |

## Configuration

Three env vars control observability (`packages/infra/src/config.ts`):

| Variable | Default | Effect |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (empty) | OTel OTLP/HTTP traces endpoint (e.g. `http://otel-collector:4318/v1/traces`); empty → tracing disabled (no-op) |
| `OTEL_SERVICE_NAME` | (empty) | OTel resource `service.name`; falls back to `nodejs-kafka` when empty |
| `METRICS_PORT` | (empty) | Port for the app's `/metrics` HTTP server; empty → no metrics server |

In the compose stack these are preset: `METRICS_PORT=9464` (producer) / `9465` (consumer), `OTEL_SERVICE_NAME` matches the service, and `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces` — but all three are inert unless the collector is running.

### In-memory demos with metrics (no OTLP)

You don't need a collector to see metrics. Run the in-memory demo with a metrics port and scrape it yourself:

```bash
METRICS_PORT=9465 npm run dev:consumer
curl http://localhost:9465/metrics
```

Tracing stays off (empty `OTEL_EXPORTER_OTLP_ENDPOINT`), which is exactly the default behavior.

## Run it

```bash
docker compose --profile observability up --build
```

This starts the normal stack **plus** the observability services:

| Service | Where to look |
|---|---|
| `otel-collector` | OTLP HTTP on `:4318`; exported spans printed to its logs (`docker compose logs otel-collector`) |
| `prometheus` | UI + targets on [http://localhost:9090](http://localhost:9090) |
| `grafana` | Provisioned Prometheus datasource + "Node.js Kafka" dashboard on [http://localhost:3002](http://localhost:3002) |

## Verify

1. **Metrics are scraped** — open Prometheus at `:9090` → *Status → Targets*. The `nodejs-kafka` job should show `producer:9464`, `consumer:9465`, `web:3000`, `customer-view:3001` **UP**, and the `otel-collector` job should show `otel-collector:8888` **UP**. (Prometheus scrapes over the compose network; the individual ports are not published to the host.)
2. **Generate traffic** — `docker compose up producer` publishes a batch and exits; the consumer keeps running. `curl http://localhost:3000/api/orders` (POST) exercises the web outbox path.
3. **See spans** — `docker compose logs otel-collector` shows `produce` / `consume` spans with their `messaging.*` attributes.
4. **See metrics** — open Grafana at `:3002` and open the **Node.js Kafka** dashboard (datasource + dashboard are provisioned, no setup). Panels: rate of messages produced/consumed, retries, DLQ total, handler duration p95, outbox published.

## Troubleshooting

- **No spans in collector logs** — the apps export over HTTP only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set and reachable; check the app logs for `OTel tracing enabled (OTLP HTTP exporter)` and confirm the collector container is up.
- **Exporter failures are non-fatal** — an unreachable collector logs `warn`-level export errors and the apps keep running; tracing is best-effort.
- **Empty metrics / no targets** — the `/metrics` server only exists when `METRICS_PORT` is set; Prometheus only sees apps on the compose network, not host `curl` of unpublished ports.
- **Grafana panel shows "No data"** — the dashboards are provisioned but start empty; produce a few messages (step 2 above) and wait one scrape interval (15s).

## What changes, what doesn't

- **No port signature changes** — `produce<T>` / `consume` / `beginTransaction` are untouched; the spans live inside the adapter methods.
- **No behavior change by default** — with `OTEL_EXPORTER_OTLP_ENDPOINT` and `METRICS_PORT` unset, apps behave exactly as before: no SDK, no spans, no `/metrics` server.
- **No auto-instrumentation** — spans are created manually in the two adapters only; HTTP/Express and Kafka client internals are not instrumented.
- **Metrics are in-memory** — counters and histograms live in a per-app `prom-client` registry and **reset on restart**; nothing is persisted or shipped to a metrics backend.
