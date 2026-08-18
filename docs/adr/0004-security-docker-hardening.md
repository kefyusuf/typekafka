# ADR-0004: Security and Docker hardening

- Status: Accepted
- Date: 2026-08-18

## Context

The reference deployment runs as a set of Docker containers via
`docker-compose.yml` (Kafka in KRaft mode plus the producer, consumer, web and
customer-view apps, with optional schema-registry / mirror / security /
observability profiles). A demo that "just runs" is a common starting point, but
shipping it as production guidance means the images and compose file should follow
least-privilege container hygiene and the HTTP surfaces should not be world-open
by default.

Two distinct concerns:

1. **Container hardening** — run as non-root, read-only root filesystem, dropped
   capabilities, no privilege escalation, resource limits, and healthchecks so
   orchestrators only route to healthy brokers/apps.
2. **HTTP auth** — the web UI (`apps/web`) and customer-view (`apps/customer-view`)
   expose `/metrics` and admin-ish HTTP endpoints; leaving these unauthenticated is
   fine for a demo but not for a shared deployment. Auth must be opt-in so demos
   stay zero-config.

## Decision

### Container hardening

All app images use a non-root runtime user. `apps/consumer/Dockerfile` (and the
sibling `apps/producer`, `apps/web`, `apps/customer-view` Dockerfiles) build in a
`node:22-alpine` multi-stage image, then in the runtime stage:

- `addgroup -S appuser && adduser -S appuser -G appuser`,
- `chown -R appuser:appuser /app`,
- `USER appuser`.

`docker-compose.yml` applies defense-in-depth to every service:

- `read_only: true` with a `tmpfs: /tmp` (and `/etc/kafka/secrets` for the
  secured broker) so the root filesystem is immutable,
- `cap_drop: ["ALL"]` and `security_opt: ["no-new-privileges:true"]` — no Linux
  capabilities, no privilege escalation,
- `deploy.resources.limits` bounding `cpus` (1.0) and `memory` (512M–1G) per
  service to contain noisy neighbors and OOM blast radius,
- `healthcheck` blocks on the broker (`kafka-topics.sh --list`) and on the web /
  customer-view apps (`/api/health`, `/health`) so `depends_on:
  condition: service_healthy` gates dependents on a ready dependency,
- the broker and secured broker also `cap_drop`/`no-new-privileges` and limit
  resources; the secured broker runs its one-shot keystore/JAAS provisioning as
  root only to write into the `security-certs` volume, then drops to `appuser`
  for the long-running broker process (documented inline as a production
  simplification to be replaced with a pre-provisioned keystore).

### Optional HTTP basic-auth

`packages/infra/src/http-auth.ts` provides env-gated auth:

- `validateBasicCredentials(authHeader, credentials)` — constant-time comparison
  (`timingSafeEqual`, length-checked) of `user:password`, never logs the
  password.
- `createBasicAuthMiddleware(credentials?)` — when `credentials` is undefined or
  empty it is a **no-op passthrough**; otherwise every request must carry a valid
  `Authorization: Basic` header or receives `401` with a `WWW-Authenticate`
  challenge.

Wiring (keep the health probe unauthenticated so orchestrators can still probe):

- `apps/web/src/app.ts` mounts `app.get('/api/health', ...)` before the gate, then
  `app.use(createBasicAuthMiddleware(httpBasicAuth))` (line ~63). `httpBasicAuth`
  comes from the `HTTP_BASIC_AUTH` env var, supplied in compose as
  `HTTP_BASIC_AUTH: ${HTTP_BASIC_AUTH:-}` — unset means open, set means protected.
- `apps/customer-view/src/app.ts` follows the same pattern with its `/health`
  probe opened and the same `HTTP_BASIC_AUTH` gate.

## Consequences

- Images run as an unprivileged user with an immutable root FS and no capabilities;
  a compromised process cannot escalate or write to the image layer.
- Resource limits and healthchecks make the stack safe to run under an
  orchestrator and bound failure blast radius.
- HTTP auth is strictly opt-in: demos with no `HTTP_BASIC_AUTH` behave exactly as
  before (no breaking change), while a real deployment can enable it via a single
  env var without code changes.
- Remaining demo trade-offs (documented in compose): the keystore password and
  `GF_SECURITY_ADMIN_PASSWORD` / `KAFKA_UI_PASSWORD` defaults are non-production
  placeholders, and the secured broker's root entrypoint is a provisioning
  convenience to be replaced by a pre-provisioned keystore + non-root user in
  production. These are explicitly called out as demo-only in the compose file.
