# Multi-cluster mirroring with MirrorMaker 2

This guide explains how `nodejs-kafka` adds a second single-node KRaft cluster (`kafka-b`) and uses **MirrorMaker 2 (MM2)** to replicate the `orders.*` topics from the primary cluster into it — via the optional `mirror` Docker profile — without changing any application or domain code.

## Why multi-cluster + MM2

A single Kafka cluster is fine for one app. Real deployments usually run several, and there are three common reasons to copy topics between them:

- **Aggregation** — several regional clusters feed one central cluster so analytics can read everything in one place.
- **Disaster recovery** — a warm standby cluster holds a copy of the source topics so consumers can fail over if the primary dies.
- **Read-heavy regions** — a second cluster close to the readers keeps load off the primary's brokers.

MirrorMaker 2 is the standard tool for this: it is a Kafka consumer + producer pair that re-reads a set of topics on a **source** cluster and re-writes them on a **target** cluster. It also replicates consumer-group offsets via its `checkpoints` topic, so a failover consumer can resume where the source left off.

## Topology

```
apps ── produce / consume ──► kafka:9092      (source, "local")
                                  │
          MirrorMaker 2  ─────────┤  orders.*
                                  ▼
                          kafka-b:9092        (target, "mirror")
```

Kafka UI (`:8080`) is configured with **two** clusters: `local` (the primary, `kafka:9092`) and `mirror` (`kafka-b:9092`), so you can watch both topic lists side by side.

## How it fits in this repo

Mirroring is **cluster-level tooling** — it sits entirely in `docker-compose.yml` and `compose/mirror/mm2.properties`. The Node apps keep talking to the primary cluster (`kafka:9092` via `BROKER_BROKERS`) and are not aware a second cluster exists. `mirror-maker` is not a Node app either — it is the Confluent JVM image, and from the repo's point of view it is just another compose service with a mounted config file. That is why the [driver matrix](../README.md#switching-drivers) marks MirrorMaker 2 / multi-cluster as `❌ | ✅`: it is not a capability of either Node driver, it needs **real Kafka clusters**, so it cannot be exercised by the in-memory driver or the unit tests.

## Config walkthrough

The whole flow lives in `compose/mirror/mm2.properties`, mounted read-only into the container at `/tmp/config/mm2.properties` (the image's default command is `connect-mirror-maker /tmp/config/mm2.properties`):

| Lines | What they do |
|---|---|
| `clusters = source, target` | The two cluster **aliases** MM2 manages. Everything else in the file names a cluster by alias, not by host. |
| `source.bootstrap.servers = kafka:9092` | The primary cluster — the same broker the apps already use. |
| `target.bootstrap.servers = kafka-b:9092` | The second cluster. On the compose network its port is still `9092`; `9094` is only the host-side mapping. |
| `source->target.enabled = true` | Turns the source→target **flow** on. A flow without `enabled` and a `topics` selector does nothing. |
| `target->source.enabled = false` | No reverse mirror — this repo is one-way. Without this, MM2 would also try to copy target→source. |
| `source->target.topics = orders.*` | The topic filter. `orders.*` matches `orders.created`, `orders.payment.*` and `orders.dlq`. The compacted `customers` topic is deliberately **not** matched, so it is not mirrored. |
| `replication.factor = 1` × 4 | MM2's **internal** topics — `heartbeats`, `checkpoints`, `mm2-offset-syncs` — must use RF 1 because both clusters are single-node. On a multi-broker cluster these should be raised (default is 3). |
| `tasks.max = 1` | One MM2 task per flow — fine for a demo; more tasks spread the copying of topics/partitions across parallel workers. |
| `sync.topic.acls.enabled = false` | No ACL sync. There are no ACLs here, and enabling it only fails on a cluster that has no ACLs configured. |
| `refresh.topics.interval.seconds = 10` | MM2 rescans the source for new topics matching `orders.*` every 10 seconds — this is the latency before a newly created topic appears on the target. |

## Run it

The mirror profile is optional, like the observability profile. Start the base stack first, then bring up the mirror services:

```bash
docker compose up --build
docker compose --profile mirror up
```

The second command starts `kafka-b` (a second single-node KRaft cluster on host port `9094`) and `mirror-maker` (the Confluent MM2 container, config mounted from `compose/mirror/mm2.properties`). `mirror-maker` waits for **both** brokers to be healthy before starting. The two commands can be combined — `docker compose --profile mirror up --build` brings up the base stack and the mirror services together. The default `docker compose up` (no profile) is unchanged and starts nothing mirror-related.

## Verify / expected output

1. **Produce against the primary** — `docker compose up producer` publishes the order + payment batch to `kafka:9092`. This works before or after the mirror profile is up; MM2 mirrors whatever lands on `orders.*`.
2. **List topics on kafka-b** — exec into the target broker (host port `9094` maps to `9092` inside the container):

```bash
docker compose exec kafka-b /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

   Expect `orders.created`, `orders.payment.*` and `orders.dlq` (mirrored copies — MM2 names the mirrored topics after their originals with default remote-topic naming), plus MM2's internal topics `heartbeats`, `checkpoints` and `mm2-offset-syncs`. **`customers` must not appear** — it is not matched by `orders.*`.
3. **Kafka UI** — [http://localhost:8080](http://localhost:8080) lists both clusters. The `mirror` cluster's topic list shows the same mirrored `orders.*` topics.
4. **Read the copy** — consume a mirrored record straight from the target to prove the copy exists:

```bash
docker compose exec kafka-b /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.created \
  --from-beginning --max-messages 1
```

   Expect the same JSON payload the producer wrote to the primary.

## Troubleshooting

- **`mirror` cluster shows down / connection error in Kafka UI** — kafka-ui is always configured with both clusters, but `kafka-b` only runs when the mirror profile is up. Start it with `docker compose --profile mirror up`. (This mirrors how the schema-registry cluster entry can also be listed while the registry is not running.)
- **`Could not create internal topics ... replication factor larger than the number of brokers`** — MM2's internal topics default to RF 3; on a single-node cluster they must stay 1. All four `*.replication.factor` lines in `mm2.properties` are already set to 1 — keep them that way.
- **The `heartbeats` topic is expected** — MM2 writes a heartbeat topic on each cluster and uses it to detect that the other side is reachable; seeing it is not a failure.
- **A mirrored topic is missing** — `refresh.topics.interval.seconds = 10`, so a topic created on the source takes up to ~10s to appear on the target. Check `docker compose logs mirror-maker` for errors and confirm the topic name matches `orders.*`.
- **Nothing mirrored at all** — MM2 only copies topics that already exist on the source and match `orders.*`. If the primary has no orders yet, run `docker compose up producer` once to create and populate them, then wait one refresh interval.

## What changes, what doesn't

- **No Node code changes** — the apps, the broker port, the pipeline and the domain are untouched; mirroring lives entirely in compose configuration.
- **In-memory driver unaffected** — `BROKER_DRIVER=in-memory` and the unit tests are unchanged; MM2 needs real clusters (`❌ | ✅` in the driver matrix).
- **No new user env vars** — the mirror flow is configured solely in `compose/mirror/mm2.properties`; the profile is enabled with `--profile mirror`.
