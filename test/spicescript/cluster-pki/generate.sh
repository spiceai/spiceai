#!/bin/sh
# Generates the test-only PKI the cluster scripts use for scheduler/executor
# mTLS: a CA and one certificate per node, valid for 127.0.0.1 and localhost,
# for both server and client authentication. Run from this directory.
set -eu
days=36500
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca.key
openssl req -x509 -new -key ca.key -days "$days" -subj "/CN=Spice.ai spicescript test CA - DO NOT USE IN PRODUCTION" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign,digitalSignature" -out ca.crt
for node in scheduler executor; do
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$node.key"
  openssl req -new -key "$node.key" -subj "/CN=$node" -out "$node.csr"
  printf '%s\n' "basicConstraints=CA:FALSE" "keyUsage=critical,digitalSignature,keyEncipherment" \
    "extendedKeyUsage=serverAuth,clientAuth" "subjectAltName=DNS:localhost,IP:127.0.0.1" > "$node.ext"
  openssl x509 -req -in "$node.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days "$days" \
    -extfile "$node.ext" -out "$node.crt"
  rm "$node.csr" "$node.ext"
done
rm -f ca.srl ca.key
