#!/bin/sh
set -e

KEYSTORE=/tmp/certs/keystore.p12
CA_PEM=/tmp/certs/ca.pem
KEY_CREDS=/tmp/certs/key_creds
KS_CREDS=/tmp/certs/keystore_creds
JAAS=/tmp/certs/kafka_jaas.conf
PASS=changeit

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
  printf '%s' "$PASS" > "$KEY_CREDS"
  printf '%s' "$PASS" > "$KS_CREDS"
  cat > "$JAAS" <<'EOF'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_app="app-secret";
};
EOF
fi

rm -rf /etc/kafka/secrets
ln -s /tmp/certs /etc/kafka/secrets

exec /etc/kafka/docker/run "$@"
