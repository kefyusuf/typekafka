#!/bin/sh
set -e

# Provision topic-scoped ACLs for the `app` user (PLAIN) on the secured
# cluster. The `admin` user is super (see KAFKA_SUPER_USERS) and drives this.
# Topics are created on demand by the apps, so CREATE (via --operation All) on
# the `orders` prefix must be allowed too. Consumer groups are opened for READ
# so offset commits work (`__consumer_offsets` is governed via the group ACL).

/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka-secured:9095 \
  --command-config /tmp/security/admin-client.properties \
  --add --allow-principal User:app --operation All \
  --topic orders --resource-pattern-type prefixed

/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka-secured:9095 \
  --command-config /tmp/security/admin-client.properties \
  --add --allow-principal User:app --operation Read --group '*'

echo 'ACLs provisioned for app'
