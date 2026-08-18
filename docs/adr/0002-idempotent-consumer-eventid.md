# ADR-0002: Idempotent consumer keyed on eventId

- Status: Accepted
- Date: 2026-08-18

## Context

Kafka delivery is at-least-once. A message can be redelivered after a commit
failure, a consumer rebalance, or a crash; the transactional outbox relay can
also re-publish the same logical event if its `markPublished` write fails before
the broker acks. A downstream handler that mutates state (sends a notification,
updates a read model) therefore risks running twice for one logical event.

We must tolerate seeing the same event more than once and still produce the same
effect exactly once. The natural key is the event's own `eventId` (a UUID carried
on every event payload via `packages/domain/src/events/*.ts` schemas), which is
stable across redeliveries and outbox re-publishes — unlike the Kafka offset,
which is per-partition and meaningless for deduplication.

The consumer pipeline already wraps every handler in
`createHandlerRunner` (`apps/consumer/src/handler-runner.ts`) and
`createRetryTopicRunner` (`apps/consumer/src/retry-runner.ts`), so the guard
belongs in those wrappers, keyed on `eventId` extracted by
`extractMessageMeta` (`apps/consumer/src/pipeline-shared.ts`).

## Decision

Provide an in-process idempotency filter in
`packages/infra/src/idempotency.ts`:

- `createIdempotencyFilter({ maxSize, ttlMs })` returns an `IdempotencyFilter`
  with `isDuplicate(eventId)` and `mark(eventId)`.
- It is an LRU-bounded, optionally TTL'd in-memory `Map`. Default capacity
  100k; `maxSize` evicts the oldest entry, so memory stays bounded for a single
  instance over its uptime.
- A duplicate is only treated as such after the event has been **successfully
  processed** — `mark(eventId)` is called only on the success path. A transient
  in-flight retry has not been marked yet, so it is never suppressed; only a true
  re-delivery of an already-completed event is skipped.

Wiring:

- `apps/consumer/src/handler-runner.ts` — after parse, if
  `idempotency?.isDuplicate(meta.eventId)` (and `eventId !== 'unknown'`), the
  message is committed and skipped (telemetry `duplicate-skipped`,
  `idempotencySkipped` metric). On success, `idempotency.mark(meta.eventId)` runs
  immediately before `context.commit()` (lines ~109-131 and ~225).
- `apps/consumer/src/retry-runner.ts` — the same guard wraps the retry-topic
  pipeline (lines ~135-156, `mark` at ~241), so redeliveries off the retry topic
  are also deduped.

The filter is supplied per-runner via `HandlerConfig.idempotency` /
`RetryHandlerConfig.idempotency`; the integration suite
(`apps/consumer/test/integration/real-kafka.test.ts`) reuses the same filter
instance across a consumer "restart" to prove reprocessing is suppressed.

## Consequences

- At-least-once delivery is made effectively exactly-once for the notification /
  read-model side effects without touching business logic.
- Genuine transient retries are never suppressed (mark-on-success), so the retry
  and DLQ machinery still works for real failures.
- The store is in-memory and instance-local: it does not survive a process
  restart. A restart relies on committed offsets (no redelivery) for correctness,
  and the filter is a same-instance defense-in-depth layer. For cross-restart
  guarantees the doc/ADR notes back this with a durable store (unique
  `eventId` constraint on the downstream write, or Redis/SQLite).
- Memory is bounded by `maxSize`; under heavy throughput with a long-lived
  instance, a TTL (`ttlMs`) can be set to trade some replay protection for lower
  footprint.
