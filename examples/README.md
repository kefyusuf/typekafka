# Examples

Standalone example projects that consume typekafka **the way an external
consumer would**: they install the packages from `npm pack` tarballs, not from
the npm workspaces. Nothing here is a workspace member — each example has its
own `package.json` and `tsconfig.json` and runs its own `npm install`, so the
packages' `files` / `exports` / `dist` packaging is exercised for real.

## Why tarballs?

Inside the monorepo, workspace links resolve `@typekafka/*` to the source
directories, which hides packaging mistakes (missing `dist` output, a wrong
`exports` map, files excluded from the tarball). Installing the packed tarball
via a `file:` dependency reproduces what `npm install @typekafka/broker` would
do from a registry.

## hello-roundtrip

Minimal produce → consume → commit roundtrip on the in-memory driver — no
Docker, no Kafka, no environment variables:

1. `createBroker({ driver: 'in-memory', ... })` and `connect()`
2. `createTopics([{ name: TOPIC_ORDER_CREATED }])`
3. `consume()` with `manualCommit: true` — the handler validates the payload
   with `parseEvent()` and commits the offset
4. `produce()` a `createSampleOrder(1)` event keyed by `orderId`
5. await the receipt (with a timeout guard), check the consumed event equals
   the produced one, then dispose the consumer and disconnect

`src/roundtrip.ts` exports `runRoundtrip()`; `src/main.ts` is a thin CLI wrapper
so the tests never auto-run the demo.

### Run it

From the repo root:

```bash
npm install
npm run build
npm run pack:packages          # writes .pack/*.tgz

cd examples/hello-roundtrip
npm install                    # installs the tarballs via file: deps
npm run build
npm test                       # vitest — 2 tests
npm start                      # CLI roundtrip
```

CI runs exactly this sequence in the `examples` job
(`.github/workflows/ci.yml`) on every push and pull request.

### Notes

- The example depends only on `@typekafka/broker` and `@typekafka/domain`.
  `@typekafka/infra` declares `"*"` dependencies on its sibling packages, which
  only resolve inside the workspace — it is packed, but not consumable from a
  registry until those versions are pinned at publish time.
- Example lockfiles are git-ignored: tarball integrity hashes change on every
  `npm pack`, so a committed lockfile would break fresh installs.
- Re-run `npm run pack:packages` after changing package source, then
  `npm install` again inside the example to pick up the new tarballs.