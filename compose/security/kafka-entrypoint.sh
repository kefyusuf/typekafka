#!/bin/sh
set -e

KEYSTORE=/tmp/certs/keystore.p12
CA_PEM=/tmp/certs/ca.pem
TRUSTSTORE=/tmp/certs/truststore.p12
KEY_CREDS=/tmp/certs/key_creds
KS_CREDS=/tmp/certs/keystore_creds
TRUST_CREDS=/tmp/certs/truststore_creds
JAAS=/tmp/certs/kafka_jaas.conf
# Password for the GENERATED, self-signed demo keystore. This is a
# non-production demo secret — replace with a real keystore in production.
PASS=changeit

# Demo-only SASL credentials (NON-PRODUCTION defaults). Override via compose
# variables; they must match the app services' BROKER_SASL_* values.
KAFKA_ADMIN_PASSWORD="${KAFKA_ADMIN_PASSWORD:-admin-secret}"
KAFKA_APP_PASSWORD="${BROKER_SASL_APP_PASSWORD:-app-secret}"

if [ ! -f "$KEYSTORE" ]; then
  keytool -genkeypair \
    -keyalg RSA \
    -alias server \
    -dname "CN=kafka-secured" \
    -validity 3650 \
    -storetype PKCS12 \
    -keystore "$KEYSTORE" \
    -storepass "$PASS" \
    -keypass "$PASS" \
    -ext "SAN=DNS:kafka-secured,DNS:localhost,IP:127.0.0.1"
  keytool -exportcert \
    -alias server \
    -rfc \
    -keystore "$KEYSTORE" \
    -storepass "$PASS" \
    -file "$CA_PEM"
  keytool -importcert \
    -alias ca \
    -file "$CA_PEM" \
    -keystore "$TRUSTSTORE" \
    -storetype PKCS12 \
    -storepass "$PASS" \
    -noprompt
  printf '%s' "$PASS" > "$KEY_CREDS"
  printf '%s' "$PASS" > "$KS_CREDS"
  printf '%s' "$PASS" > "$TRUST_CREDS"
  cat > "$JAAS" <<EOF
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="$KAFKA_ADMIN_PASSWORD"
  user_admin="$KAFKA_ADMIN_PASSWORD"
  user_app="$KAFKA_APP_PASSWORD";
};
EOF
fi

cp /tmp/certs/keystore.p12 /etc/kafka/secrets/
cp /tmp/certs/truststore.p12 /etc/kafka/secrets/
cp /tmp/certs/key_creds /etc/kafka/secrets/
cp /tmp/certs/keystore_creds /etc/kafka/secrets/
cp /tmp/certs/truststore_creds /etc/kafka/secrets/
cp /tmp/certs/kafka_jaas.conf /etc/kafka/secrets/

# The security-certs volume is mounted root-owned, but the apache/kafka image
# runs the broker as the non-root 'appuser'. When we start as root (needed to
# generate the keystore + JAAS above), chown the cert paths to that user so the
# broker can read them — then drop to 'appuser' so the long-running broker
# process itself is not root.
if [ "$(id -u)" = "0" ]; then
  CERT_UID=$(id -u appuser 2>/dev/null || echo 1000)
  chown -R "$CERT_UID" /tmp/certs /etc/kafka/secrets 2>/dev/null || true
  exec su appuser -s /bin/sh -c 'exec /etc/kafka/docker/run "$@"' -- "$@"
fi

exec /etc/kafka/docker/run "$@"
