#!/bin/sh
set -e

# Generate a self-signed server certificate for the SASL_SSL listener, once per
# volume lifetime. Uses the JRE's keytool (guaranteed present in the
# temurin-based apache/kafka image) instead of openssl — no host scripts, and
# nothing extra to install in the image (Windows-safe).
KEYSTORE=/tmp/certs/keystore.p12
CA_PEM=/tmp/certs/ca.pem
STORE_PASSWORD=changeit

if [ ! -f "$KEYSTORE" ]; then
  keytool -genkeypair \
    -keyalg RSA \
    -alias server \
    -dname "CN=kafka-secured" \
    -validity 3650 \
    -storetype PKCS12 \
    -keystore "$KEYSTORE" \
    -storepass "$STORE_PASSWORD" \
    -keypass "$STORE_PASSWORD" \
    -ext "SAN=DNS:kafka-secured,DNS:localhost,IP:127.0.0.1"
  keytool -exportcert \
    -alias server \
    -rfc \
    -keystore "$KEYSTORE" \
    -storepass "$STORE_PASSWORD" \
    -file "$CA_PEM"
fi

# Hand off to the standard apache/kafka entrypoint (env -> server.properties ->
# Kafka start). The KAFKA_SSL_KEYSTORE_* envs reference the files above.
exec /etc/kafka/docker/run "$@"
