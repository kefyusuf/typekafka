#!/bin/sh
# Live verification for the security (SASL_SSL + ACL) profile.
# Brings up the secured cluster, confirms the pipeline runs over SASL_SSL,
# lists the app ACLs, and demonstrates two real denials (bad password + ACL).
#
# Usage: scripts/verify-security.sh
# Requires Docker + Docker Compose and that no leftover typekafka
# containers from a previous run are holding the cert volume / ports.

set -u

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

echo "=== cleaning previous security containers ==="
# Scope cleanup to the security-profile containers only (do not delete
# unrelated typekafka containers such as the base kafka/web/consumer).
for c in typekafka-kafka-secured typekafka-security-init typekafka-producer-secured typekafka-consumer-secured; do
  docker rm -f "$c" 2>/dev/null || true
done
docker network prune -f >/dev/null 2>&1 || true

echo "=== UP (security profile: producer-secured + consumer-secured pulls kafka-secured + security-init) ==="
docker compose --profile security up -d producer-secured consumer-secured

echo "=== wait kafka-secured healthy (max ~3 min) ==="
H=
for i in $(seq 1 40); do
  H=$(docker inspect --format '{{.State.Health.Status}}' typekafka-kafka-secured 2>/dev/null)
  [ "$H" = "healthy" ] && break
  sleep 5
done
[ -n "$H" ] || { echo "FATAL: kafka-secured never became healthy"; exit 1; }
echo "kafka-secured=$H"

echo "=== wait security-init completed ==="
S=
for i in $(seq 1 12); do
  S=$(docker inspect --format '{{.State.Status}}' typekafka-security-init 2>/dev/null)
  [ "$S" = "exited" ] && break
  sleep 5
done
[ -n "$S" ] || { echo "FATAL: security-init did not complete"; exit 1; }
echo "security-init=$S"

echo "=== wait producer-secured finished ==="
for i in $(seq 1 30); do
  S=$(docker inspect --format '{{.State.Status}}' typekafka-producer-secured 2>/dev/null)
  [ "$S" = "exited" ] && break
  sleep 5
done

echo "=== producer-secured logs ==="
docker logs typekafka-producer-secured
echo "=== consumer-secured logs (tail) ==="
docker logs typekafka-consumer-secured | tail -25

echo "=== ACL LIST (admin) ==="
MSYS_NO_PATHCONV=1 docker exec typekafka-kafka-secured /opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:9095 --command-config /tmp/security/admin-client.properties --list

echo "=== ACL denial #1: app user, WRONG password (SASL auth failure) ==="
MSYS_NO_PATHCONV=1 docker exec typekafka-kafka-secured /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9095 --topic orders.created \
  --from-beginning --max-messages 1 --timeout-ms 15000 \
  --consumer.config /tmp/security/admin-client.properties \
  --consumer-property 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="WRONG";' \
  || echo '(expected non-zero exit: auth failure)'

echo "=== ACL denial #2: app user on test.deny (not whitelisted) ==="
MSYS_NO_PATHCONV=1 docker exec typekafka-kafka-secured /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9095 --topic test.deny \
  --from-beginning --max-messages 1 --timeout-ms 15000 \
  --consumer.config /tmp/security/admin-client.properties \
  --consumer-property 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="app-secret";' \
  || echo '(expected non-zero exit: ACL denial)'

echo "=== teardown ==="
docker compose --profile security down
echo "=== DONE ==="
