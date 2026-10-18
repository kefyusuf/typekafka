# ADR-0005: Reuse of this package by the next build is deferred, with the criteria fixed now

- Status: Accepted
- Date: 2026-10-15

## Context

This repo produces a broker port (`IMessageBroker`), two drivers, a consumer pipeline
(`parse → retry → retry topic → DLQ → commit`) and a transactional outbox with a relay. Every
piece of it was built to be consumed by a service, and every piece was exercised by 152 unit tests
plus 3 Docker-gated integration tests when this decision was accepted. What no piece has is an external consumer: development
paused on 2026-09-23 and nothing outside this repository imports it.

That gap is the kind that gets papered over. The tempting follow-ups were a demo consumer written
in-repo to make the README claim a "system", or an unprompted rewrite of the relay in the next
project because nobody re-read this code. Both would destroy the only signal this repo carries:
that the delivery semantics here were chosen deliberately, and can be defended.

The next build in this plan is a retail copilot whose money path produces events (order accepted,
payment captured, reconciliation deltas). Whether it consumes this package or writes its own relay
has to be decided when that service exists, because the inputs of the decision — tenancy model,
transaction boundary, delivery guarantee — live there, not here.

## Decision

Defer the reuse decision to that service's own ADR set, and fix the deciding criteria here now, so
the future ADR is a comparison rather than a mood:

- **Reuse this package** when the consumer needs at-least-once delivery with an outbox, a
  per-event idempotency filter and escalating retry topics — i.e. the semantics implemented here —
  and runs on Node ≥ 22 with the confluent driver. In that case the package is a dependency, added
  through the port, and the service contributes only its event schemas and handlers.
- **Write a separate relay** only if the service's transaction boundary cannot be a local SQLite
  write (ADR-level requirement: a named datastore and a named commit point), or if its delivery
  guarantee must be exactly-once at the broker rather than idempotency-filtered at-least-once. The
  ADR that makes that call must cite which of the two conditions failed.
- **Never** copy the relay code into the service. Fork-by-copy would strand both copies.

This repo stays a reference implementation with its test suite as the contract. It is not archived:
the port, the retry-topic pattern and the outbox relay are the documented answer to questions the
next build will ask, and a paused repo that is still green is evidence, not failure.

## Consequences

- The README's status line can say what is true ("no external consumer yet") instead of implying a
  deployment that never happened.
- The reuse decision is made once, in the service that owns the requirement, against criteria
  written down before anyone had a preference.
- Cost: while no consumer exercises the package across a repository boundary, API friction in the
  broker port remains unverified outside this repo. The port includes connection and topic
  management, publishing, consumption, and transactions; its fit must be checked by the consumer.
- If the deferred decision eventually lands on "own relay", that ADR must link back to this file and
  state which criterion above failed; otherwise this ADR stays correct and the package keeps its
  slot in the showcase.

## Summary (EN)

This package has no consumer outside its own repository, and "unused" is information, not a defect
to hide. Rather than inventing a deployment or quietly archiving the repo, the reuse question is
recorded here as a deferred decision with the criteria fixed now. If the next build's relay need
matches what the port already offers, it consumes the package as a dependency; if it genuinely needs
something else, that service writes its own ADR naming which criterion failed. Copying the relay code
into the service is forbidden, because two copies drift and neither keeps the contract. The package
stays green and unarchived, and its automated tests remain the available verification evidence
while no external consumer exists. The suite had 152 unit tests when this ADR was accepted.

## Verification

No test holds a deferral. At acceptance, verification evidence comprised 152 unit tests across
25 files ([CI run #38712438308](https://github.com/kefyusuf/typekafka/actions/runs/38712438308)), including `packages/infra/test/outbox.test.ts`,
`packages/infra/test/retry-topic.test.ts`, `packages/infra/test/idempotency.test.ts`, and
`packages/broker/test/propagation.test.ts`, plus three Docker-gated integration tests in
`apps/consumer/test/integration/real-kafka.test.ts`. The latest verified `main` CI at the time
of this clarification ([run #39078357391](https://github.com/kefyusuf/typekafka/actions/runs/39078357391))
passed 160 unit tests across 27 files and 3 Kafka integration tests. These counts are evidence
snapshots, not permanent contract values; consult current CI for later changes. The check that
belongs to this ADR is procedural: when the flagship's outbox relay is first written, its ADR
must cite either the reuse or the own-relay condition from above.

