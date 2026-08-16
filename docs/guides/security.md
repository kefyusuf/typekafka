# Security — SASL_SSL + PLAIN auth + topic ACLs

This guide explains how `nodejs-kafka` secures its Kafka stack: a **SASL_SSL** listener (TLS transport encryption + SASL/PLAIN username/password authentication) on a dedicated `kafka-secured` cluster, with **topic-scoped ACLs** so the app user can only touch what it needs — all without changing any application or domain code.

## Why TLS + auth + ACLs

A plaintext Kafka cluster trusts whatever connects to it and lets any client read or write every topic. That is fine for a demo broker on `localhost`, but a poor template for a real deployment, where you want three separate guarantees:

- **Confidentiality** — TLS encrypts the wire traffic, so credentials and payloads can't be sniffed in transit.
- **Authentication** — SASL/PLAIN proves a client is who it claims to be; the broker rejects anything without valid credentials.
- **Authorization** — an authenticated principal still only gets the **topic-scoped ACLs** you grant it. The `app` user can publish to and consume from `orders.*` topics and read consumer groups — nothing else.

## What this profile ships

Everything lives behind the `security` compose profile (`docker compose --profile security up`):

| Service | What it is |
|---|---|
| `kafka-secured` | A second single-node KRaft cluster (`apache/kafka:3.7.0`) with a **SASL_SSL** listener on `9095`, PLAIN users `admin` (super user) + `app`, and `AclAuthorizer` enforcing topic-scoped ACLs |
| `security-init` | One-shot job that provisions the `app` user's ACLs against the secured cluster as `admin`, then exits |
| `producer-secured` | The normal producer image pointed at `kafka-secured:9095` with the `app` credentials + CA path |
| `consumer-secured` | The normal consumer image, same credentials, long-running worker |

The base stack is untouched — the primary `kafka` cluster stays plaintext and the default `docker compose up` starts nothing secured.

## How it fits together

```
producer-secured / consumer-secured        security-init (one-shot)
   apps (BROKER_SASL_* + BROKER_SSL_CA_PATH)   admin creds (admin-client.properties)
              │  SASL_SSL                            │  SASL_SSL (admin)
              ▼                                       ▼
   kafka-secured:9095  ── AclAuthorizer ──►  User:app: ALL on orders.* + READ on groups
        ├── certs: keystore.p12 + ca.pem (security-certs volume)
        └── listeners: SASL_SSL://:9095 (clients) · CONTROLLER://:9093 (internal)
```

## Authentication — SASL/PLAIN

The SASL_SSL listener runs **PLAIN** (username + password), with two principals defined via the broker's JAAS config env:

```
KAFKA_LISTENER_NAME_SASL_SSL_PLAIN_SASL_JAAS_CONFIG=...PlainLoginModule required \
  username="admin" password="admin-secret" user_admin="admin-secret" user_app="app-secret";
```

- `admin` — the super user that runs the init job and admin tooling.
- `app` — the identity the secured producer/consumer connect as.

### Protocol auto-selection (Node driver)

The confluent driver selects `security.protocol` from two inputs: SASL is enabled by `BROKER_SASL_USERNAME` / `BROKER_SASL_PASSWORD`, TLS by any of the `BROKER_SSL_*` paths:

| `BROKER_SASL_*` | `BROKER_SSL_*` | `security.protocol` |
|---|---|---|
| set | set | `sasl_ssl` |
| set | empty | `sasl_plaintext` |
| empty | set | `ssl` |
| empty | empty | `plaintext` |

This mapping lives in `ConfluentKafkaAdapter.buildGlobalConfig` and is covered by `packages/infra/test/broker.test.ts` and `packages/broker/test/confluent.test.ts`. The in-memory driver has no TLS support at all — it never touches the network.

## Certificates

`kafka-secured`'s entrypoint (`compose/security/kafka-entrypoint.sh`) generates a self-signed server certificate **inside the container**, once per `security-certs` volume lifetime:

- `keystore.p12` — a PKCS12 keystore (alias `server`, CN `kafka-secured`, SAN `DNS:kafka-secured,DNS:localhost,IP:127.0.0.1`) used by the broker.
- `ca.pem` — the self-signed CA exported from that keystore in PEM form, mounted into the apps at `/certs/ca.pem` as their trust anchor.

> **Why `keytool` and not `openssl`?** The original plan called for openssl, but it is **not guaranteed** in the temurin-based `apache/kafka` image. The JRE's `keytool` always is, so the entrypoint uses it instead — the Windows-safe equivalent, and it needs **no host scripts and nothing extra installed** in the image.

Clients do **server-auth only**: they trust the broker's certificate via the CA PEM and never present a certificate of their own. Client certificates (`BROKER_SSL_CERT_PATH` + `BROKER_SSL_KEY_PATH`) are supported by the driver but unused here — they are only needed for mutual TLS.

## Authorization — topic-scoped ACLs

`kafka-secured` runs the `AclAuthorizer` with a deny-by-default posture:

```
KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
KAFKA_SUPER_USERS: User:admin;User:ANONYMOUS
```

- `allow.everyone.if.no.acl.found=false` means an authenticated principal with **no matching ACL is denied** — the authorizer is the enforcement point, not a bystander.
- `User:admin` is super, so the init job and admin tooling bypass the ACL table.
- `User:ANONYMOUS` is also super because the **internal CONTROLLER listener is PLAINTEXT**: in KRaft the controller principal has no SASL identity, so it must be treated as a trusted super user or the cluster's own metadata operations would be denied.

`security-init` (`compose/security/provision-acls.sh`) runs as `admin` and grants the `app` user exactly two ACLs:

```bash
kafka-acls.sh --add --allow-principal User:app --operation All \
  --topic orders --resource-pattern-type prefixed
kafka-acls.sh --add --allow-principal User:app --operation Read --group '*'
```

- **`All` on `orders.*` (prefixed)** — the apps create their topics at startup, so **CREATE** must be allowed (it's part of `--operation All`) or topic auto-creation fails with a cluster-authorization error. The prefix also covers `orders.created`, `orders.payment.*`, `orders.retry`, and `orders.dlq`.
- **`Read` on `'*'` consumer groups** — offset commits go through `__consumer_offsets`, which is governed by the **group** ACL. Without the group READ, the consumer can read messages but fails to commit offsets.

## Node app configuration

Securing an app is env-only. The secured producer/consumer set four variables over the base image:

| Env | Value in the profile | Meaning |
|---|---|---|
| `BROKER_BROKERS` | `kafka-secured:9095` | Point at the SASL_SSL listener |
| `BROKER_SASL_USERNAME` | `app` | PLAIN username → enables SASL |
| `BROKER_SASL_PASSWORD` | `app-secret` | PLAIN password |
| `BROKER_SSL_CA_PATH` | `/certs/ca.pem` | Trust anchor for the broker's cert (from the `security-certs` volume) |

The driver maps `BROKER_SSL_CA_PATH` → `ssl.ca.location` (TLS on → protocol `sasl_ssl` when SASL is also set). A client **truststore is not needed** — the CA PEM is the trust anchor and the handshake is server-auth only. `BROKER_SSL_CERT_PATH` / `BROKER_SSL_KEY_PATH` are only mapped when you want mTLS.

## Run it

The secured cluster is self-contained — you don't need the base stack up:

```bash
docker compose --profile security up
```

This starts `kafka-secured` (which generates the certs and bootstraps KRaft), runs the one-shot `security-init` ACL job, then starts `producer-secured` and `consumer-secured` once the ACLs are in place.

## Verify

1. **ACLs are listed** — from inside the secured broker, with the admin client:
   ```bash
   docker compose exec kafka-secured /opt/kafka/bin/kafka-acls.sh \
     --bootstrap-server localhost:9095 --command-config /tmp/security/admin-client.properties --list
   ```
   You should see the two `User:app` entries (prefixed `orders` + group `'*'`).
2. **The pipeline ran over SASL_SSL** — `docker compose logs producer-secured` shows `event produced` lines, and `docker compose logs -f consumer-secured` shows `consumers running` then per-message `committed` — all authenticated as `app` over TLS.
3. **Topic access is enforced** — list topics with the admin creds (works, `admin` is super), then confirm the `app` user's view is limited to `orders.*` by the ACLs above.

## Demo the ACL denial

ACLs are real — prove it by running a consumer with a wrong password:

```bash
docker compose run --rm -e BROKER_SASL_PASSWORD=wrong consumer-secured
```

Expect `SASL authentication failed` / `SaslAuthenticationException` in the logs. To prove **authorization** (not just auth) is enforced, remove the grant (or run as a user with no ACL) — the client authenticates fine but hits `TOPIC_AUTHORIZATION_FAILED` on its first produce/consume. Either way the broker refuses, which is exactly what the profile is for.

## Troubleshooting

- **`unable to find valid certification path` / PKIX errors** — the client can't verify the broker's certificate. `BROKER_SSL_CA_PATH` is missing, wrong, or points at an empty path; confirm `/certs/ca.pem` exists in the app container (`docker compose exec producer-secured cat /certs/ca.pem`).
- **`SASL authentication failed`** — credentials don't match a JAAS user. Check `BROKER_SASL_USERNAME` / `BROKER_SASL_PASSWORD` against the `user_*` entries in `KAFKA_LISTENER_NAME_SASL_SSL_PLAIN_SASL_JAAS_CONFIG`.
- **`TOPIC_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED`** — the client authenticated but has no ACL for the resource. Check `docker compose logs security-init` completed, or rerun `docker compose run --rm security-init`; remember `allow.everyone.if.no.acl.found=false` denies everything not explicitly granted.
- **CLI tools can't connect** — they don't read the app env vars. The repo ships `compose/security/admin-client.properties` (SASL_SSL + PEM truststore `ssl.truststore.type=PEM` → `ca.pem`) for `kafka-acls.sh` / `kafka-topics.sh`.
- **Healthcheck keeps failing / stale certs** — the `security-certs` volume is reused across runs; `docker compose down -v` to regenerate certs and start clean.

## What changes, what doesn't

- **No code changes** — securing an app is env-only: point `BROKER_BROKERS` at the secured listener, add `BROKER_SASL_*` + `BROKER_SSL_CA_PATH`. Apps and domain code are untouched.
- **No behavior change by default** — with the SSL paths and SASL creds empty, the driver keeps `security.protocol=plaintext` exactly as before.
- **Confluent-driver only** — TLS/auth/ACL support lives in `ConfluentKafkaAdapter`; the in-memory driver has no network, no TLS, and no notion of principals.
- **Not a CA or PKI** — the certs are self-signed for the demo; a real deployment swaps in your own CA and the same env vars still work.
