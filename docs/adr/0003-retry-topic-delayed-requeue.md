# ADR-0003: Retry-topic delayed-requeue pattern

- Status: Accepted
- Date: 2026-08-18

## Context

Transient downstream failures (provider timeouts, 5xx) should be retried with
backoff, but an unbounded in-place `await sleep(backoff)` inside a Kafka
`eachMessage` handler is dangerous:

- It blocks the partition for the whole backoff. Kafka only commits the offset
  after the handler returns, so the partition is effectively head-of-line
  blocked and a rebalance can stall consumption for every other message on it.
- A single very long sleep ties up the event loop and denies the broker liveness
  heartbeats, risking session timeouts.

We instead keep failures on a dedicated `orders.retry` topic
(`RETRY_TOPIC` in `packages/domain/src/constants.ts`) with `retry-count` and
`next-deliver-at` headers, and let the existing consumer-group machinery redeliver
the message when it is due. This needs a consumer that can "park" a not-yet-due
message without blocking its partition.

`packages/infra/src/retry-topic.ts` already provides `RetryTopicScheduler` with
`schedule` (escalating delays, `nextDeliverAt`, `retry-count`) and `requeue`
(re-publish preserving headers), plus `parkDelayMs` / `parseRetryHeaders` for the
park decision. The question is how the retry-topic runner handles a not-yet-due
message.

## Decision

In `apps/consumer/src/retry-runner.ts` the throttled-requeue strategy replaces
the in-place park:

- When a retry message arrives before its `next-deliver-at`
  (`parkDelayMs(retryCount, nextDeliverAtMs) > 0`), the runner:
  1. sleeps `Math.min(parkMs, REQUEUE_THROTTLE_MS)` where
     `REQUEUE_THROTTLE_MS = 1_000`,
  2. calls `scheduler.requeue(message, retryCount, nextDeliverAtMs)` to
     re-publish the message onto `orders.retry` with the **same** `retry-count`
     and `next-deliver-at`,
  3. commits the offset and returns — freeing the partition immediately.
- `RetryTopicScheduler.requeue` (line ~125) does NOT escalate `retry-count` or
  advance the scheduled time; it simply holds the message until its
  `next-deliver-at`, at which point the next delivery is processed (not parked).
- `RetryTopicScheduler.schedule` (line ~99) is used only when a handler truly
  fails: it increments `retry-count`, pushes `next-deliver-at` forward by the
  next escalating delay (`delaysMs`, default `[2000, 10000, 60000]`), and the
  `isMaxRetries(retryCount)` bound (default `maxDeliveries = delaysMs.length`)
  routes an exhausted message to the DLQ instead of requeueing forever.

### Why a bounded sleep — not a busy-loop, not one long sleep

- **Single long sleep** (`await sleep(parkMs)`): would block the partition for
  the full backoff (up to 60s), reproducing the head-of-line / session-timeout
  problem this design exists to avoid.
- **No sleep (immediate requeue)**: on a low-latency broker the message would
  redeliver almost instantly, busy-looping the retry topic and burning CPU while
  never honoring the backoff window.
- **Bounded sleep** (`min(parkMs, REQUEUE_THROTTLE_MS)`): caps each hop at 1s so
  the partition is never blocked longer than one second, while `requeue` preserves
  the real `next-deliver-at` so the cumulative wait still tracks the escalating
  schedule. Across hops the total backoff is preserved, and `maxDeliveries` bounds
  the number of hops so a permanently-stuck message escalates to the DLQ rather
  than requeueing forever.

## Consequences

- Partitions are never blocked for the backoff duration; redelivery is delegated
  to Kafka's own consumer-group machinery, which also survives consumer crashes
  (the message is just another retry-topic record with a committed offset).
- Backoff is honored across retries without busy-looping, and is observable
  (telemetry `retry-parked`, `retry-scheduled`).
- Cost: an extra copy of the message on the retry topic per hop (storage/IO), and
  a small amount of duplicate delivery within the throttle window — acceptable and
  already neutralized by the idempotency filter (ADR-0002) keyed on `eventId`.
- Tuning knobs: `REQUEUE_THROTTLE_MS` (per-hop cap) and the
  `RetryTopicPolicy` (`delaysMs`, `maxDeliveries`) in `retry-topic.ts`.
