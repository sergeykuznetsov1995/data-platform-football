#!/bin/bash
set -e
CERT_DIR="configs/trino/certs"
mkdir -p "$CERT_DIR"
KEYSTORE_PASS="${TRINO_KEYSTORE_PASSWORD:-changeit}"

# Generate key and certificate
openssl req -x509 -newkey rsa:4096 -keyout "$CERT_DIR/trino.key" \
  -out "$CERT_DIR/trino.crt" -days 365 -nodes \
  -subj "/CN=trino/O=DataPlatform"

# PKCS12
openssl pkcs12 -export -in "$CERT_DIR/trino.crt" -inkey "$CERT_DIR/trino.key" \
  -out "$CERT_DIR/trino.p12" -name trino -passout "pass:$KEYSTORE_PASS"

# JKS keystore
keytool -importkeystore -srckeystore "$CERT_DIR/trino.p12" -srcstoretype PKCS12 \
  -srcstorepass "$KEYSTORE_PASS" -destkeystore "$CERT_DIR/keystore.jks" \
  -deststoretype JKS -deststorepass "$KEYSTORE_PASS" -noprompt

chmod 644 "$CERT_DIR/keystore.jks"
echo "Certificates generated in $CERT_DIR"
echo "Keystore: $CERT_DIR/keystore.jks"
