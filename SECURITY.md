# Security

## Reporting a vulnerability

Do **not** open a public issue for a security vulnerability. Please report it privately to [security contact — to be filled when a public channel exists].

You can expect:

- An acknowledgement within 72 hours
- A status update at least every 7 days until the issue is resolved

## Scope

This project is a reference and learning example, not production software. The demo stack (single-node KRaft, as configured in `docker-compose.yml`) has no production hardening, and the threat model covers local development and learning scenarios only. Do not deploy it as-is to production.

## Supported drivers

The Kafka driver `@confluentinc/kafka-javascript` receives security fixes from its upstream maintainers, and Node.js LTS releases receive security fixes through the official Node.js security process. Keep your dependencies and your Node.js version current to pick up those fixes.
