# ADR-0001: Trace context propagation across Kafka (W3C traceparent)

- Status: Accepted
- Date: 2026-08-18

## Context

The system spans an HTTP API, an outbox relay, a consumer pipeline, a retry
topic and a DLQ. A single logical `orders.created` event crosses several process
and Kafka-boundary hops. Without propagating the OpenTelemetry trace context,
each hop starts a brand-new trace and operators lose the end-to-end causality
needed to debug latency and failures in production.

Kafka has no first-class tracing; the only portable carrier is a message header.
The W3C `traceparent` header (`00-<traceId>-<spanId>-<flags>`) is the standard
way to serialize a span context across an async boundary, and the OpenTelemetry
`@opentelemetry/api` primitives already exist in the dependency graph.

We already implement the broker as a hexagonal port (`IMessageBroker`,
`packages/broker/src/port.ts`) with two drivers: a real `confluent` driver
(`packages/broker/src/adapters/confluent.ts`) and an in-memory driver used for
tests/local dev (`packages/broker/src/adapters/in-memory.ts`). Trace propagation
must work identically in both, so the logic belongs in the shared broker layer,
not in application code.

## Decision

Implement trace propagation in `packages/broker/src/trace.ts` and wire it into
both drivers:

- `injectTraceContext(headers)` — called by every `produce`/`beginTransaction`
  send. If an OpenTelemetry span is active it serializes it into a `traceparent`
  header; if no span is active (no SDK registered) it returns the headers
  unchanged, so producers outside a tracing setup pay nothing.
- `extractParentContext(headers)` — parses a `traceparent` header (strict
  `^[0-9a-f]{2}-...` regex) into a remote `SpanContext` wrapped as an OTel
  `Context`.
- `withSpan(name, attributes, fn, parentContext?)` — runs `fn` inside a child
  span of the active context, or of `parentContext` when supplied. Continues the
  trace that crossed the Kafka boundary; no-op when no SDK is registered.

Wiring:

- `packages/broker/src/adapters/confluent.ts` — `produce` injects the context
  into outgoing headers (the `toKafkaHeaders(injectTraceContext(options.headers))`
  call); in `consume`, each delivered message's `traceparent` is extracted via
  `extractParentContext` and passed as `withSpan(..., parentContext)` around the
  application `handler` so the consumer span is parented on the producer span.
- `packages/broker/src/adapters/in-memory.ts` — `produce` injects, and `dispatch`
  extracts + runs the handler under `withSpan(..., parentContext)`, so the exact
  same propagation contract is exercised in tests.

## Consequences

- End-to-end traces survive the HTTP → outbox → Kafka → consumer → retry/DLQ
  hops; a failure in the consumer shows up under the originating request trace.
- Both drivers share one propagation implementation; swapping `BROKER_DRIVER`
  does not change observability behavior.
- Producers and consumers with no OTel SDK attached are unaffected (no headers
  added, no spans created) — zero behavioral change for non-tracing deployments.
- The `traceparent` header is a public contract on the wire; changing the format
  would require a coordinated producer/consumer rollout (mitigated by strict
  parsing that simply returns `undefined` on malformed input).

## Summary (EN)

A trace that starts in an HTTP request stopped at the Kafka boundary, so a consumer failure
could not be read in the context of the request that caused it. The decision is to
propagate W3C `traceparent` headers inside the broker layer — `packages/broker/src/trace.ts`
— rather than in application code, so every producer and consumer gets it for free.
`injectTraceContext` writes the header on send, `extractParentContext` parses it with a
strict regex, and `withSpan` parents the consumer span on the producer span. Both the
Confluent and the in-memory driver run the same three functions, which is why the contract
is exercised by the fast in-memory tests too. With no OTel SDK registered the functions
return their input unchanged, so a deployment that does not trace pays nothing.

## Verification

- `packages/broker/test/propagation.test.ts` — `injects a well-formed traceparent and
  reconstructs the parent context`, `is a no-op without an active span and rejects malformed
  input`.
- `packages/broker/test/tracing.test.ts` — `exports a produce span with messaging
  attributes`, `exports a consume span around the handler invocation`, `marks a failing span
  as error and records the exception`.
- `packages/infra/test/tracing.test.ts` — `returns a noop tracing when the endpoint is empty
  or whitespace`.

