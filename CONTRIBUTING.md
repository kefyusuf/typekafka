# Contributing

## Welcome

`typekafka` is a TypeScript + Node.js reference project that demonstrates how to build a small, type-safe Kafka application using an adapter pattern over pluggable drivers (`in-memory` for local development and `@confluentinc/kafka-javascript` for real brokers). It is written to be a friendly starting point for newcomers to Kafka, Node.js, or TypeScript, so every contribution that improves the example, its tests, or its documentation is welcome, no matter how small.

## Prerequisites

- Node.js 22+ and npm
- `npm install` in the repository root to install all workspace dependencies

## Project layout

```
apps/                 runnable applications (producer, consumer, web)
packages/             shared libraries (broker, domain, infra)
docs/                 concept guides and driver-switching guide
docker-compose.yml    local Kafka stack for manual verification
```

## Development workflow

- Create a branch from `main` for every change
- Implement the change, following the existing structure and conventions
- Keep CI green: all checks below must pass before a pull request is ready

## Commits

- Use Conventional Commits with one of: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`
- Add a scope where it helps, for example `feat(consumer): ...`
- Keep one logical change per commit

## Tests

- `npm test` runs the Vitest suite (140 tests; no Kafka required — tests use the in-memory driver and `node:sqlite` `:memory:` databases)
- Add a test for any new behavior
- Run `npm run typecheck` and `npm run lint` before pushing

## Pull requests

- Use the template in `.github/PULL_REQUEST_TEMPLATE.md`
- Describe the change, the driver used for manual verification (`in-memory` or `confluent`), and the test result

## Reporting issues

- Use the issue templates in `.github/ISSUE_TEMPLATE/`
- For security vulnerabilities, follow [SECURITY.md](SECURITY.md) and do not open a public issue
