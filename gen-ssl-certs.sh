#!/bin/bash

# Documentation:
# https://kafka.apache.org/documentation.html#security_ssl
# Content was compiled from:
# https://medium.com/@ahosanhabib.974/enabling-ssl-tls-encryption-for-kafka-with-jks-8186ccc82dd1
# https://developer.confluent.io/courses/security/hands-on-setting-up-encryption/

set -e

PASS="abcdefgh"

# Generate CA self-signed certificate
openssl genpkey -algorithm RSA -out ca.key

cat > ca.cnf <<EOF
[ ca ]
default_ca    = CA_default      # The default ca section

[ CA_default ]
x509_extensions = ca_extensions # The extensions to add to the cert

[ req ]
distinguished_name = ca_distinguished_name
x509_extensions    = ca_extensions

[ ca_distinguished_name ]
commonName         = Common Name (e.g. server FQDN or YOUR name)
commonName_default = Test Name

[ ca_extensions ]
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always, issuer
basicConstraints       = critical, CA:true
keyUsage               = keyCertSign, cRLSign
EOF

openssl req -x509 -new -key ca.key -days 3650 -out ca.crt -config ca.cnf \
    -subj "/CN=CA"

# Generate Server Private Key and CSR
openssl req -new -newkey rsa:4096 -nodes -keyout server.key -out server.csr \
    -subj "/CN=server"

# Generate the server certificate using the CSR, the CA cert, and private key
cat > sign-ext.cnf <<EOF
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid,issuer
basicConstraints       = CA:FALSE
keyUsage               = digitalSignature, keyEncipherment
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 3650 -extfile sign-ext.cnf

# Create Kafka format Keystore
openssl pkcs12 -export -in server.crt -inkey server.key -name kafka-broker \
    -out kafka.p12 -password "pass:$PASS"

# Create Kafka Java KeyStore (JKS)
keytool -importkeystore -noprompt \
    -deststoretype pkcs12 -destkeystore br_server.keystore.jks -deststorepass "$PASS" \
    -srcstoretype pkcs12 -srckeystore kafka.p12 -srcstorepass "$PASS"

# Create Kafka TrustStore
keytool -keystore br_server.truststore.jks -alias CARoot -import -file ca.crt \
    -storepass "$PASS" -noprompt


# To check:
# keytool -list -v -keystore br_server.keystore.jks -storepass "$PASS"
# keytool -list -v -keystore br_server.truststore.jks -storepass "$PASS"


# Generate client Private Key and CSR
openssl genpkey -algorithm RSA -out client.key -pass "pass:$PASS"
openssl req -new -nodes -key client.key -passin "pass:$PASS" -out client.csr \
    -subj "/CN=client"

# Generate the client certificate using the CSR, the CA cert, and private key
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client.crt -days 3650 -extfile sign-ext.cnf
