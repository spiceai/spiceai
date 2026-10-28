---
name: verify-cli-tls
description: Live-check that the spice CLI's HTTP commands (status, datasets, refresh, models, ...) honor the global --tls-root-certificate-file and report certificate failures as TLS errors. Use after touching bin/spice/src/context.rs HTTP client construction, bin/spice/src/error.rs runtime_unreachable, or TLS flag handling.
---

# Verify the spice CLI against a TLS runtime

Unit tests (`cargo test -p spice status_ refresh_reports test_with_args_`) cover this with an
in-process HTTPS server. This skill is the end-to-end check against a real `spiced`.

## 1. PKI in a scratch dir (never in the repo)

```bash
D=$(mktemp -d); cd "$D"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=verify CA" -keyout ca.key -out ca.pem
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=unrelated CA" -keyout other.key -out wrongca.pem
openssl req -newkey rsa:2048 -nodes -subj "/CN=127.0.0.1" -keyout server.key -out server.csr
printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -extfile san.ext -out server.pem
```

## 2. A TLS runtime with an API key

`spicepod.yaml` in `$D`:

```yaml
version: v1
kind: Spicepod
name: verify-cli-tls
runtime:
  tls:
    enabled: true
    certificate_file: server.pem
    key_file: server.key
  auth:
    api-key:
      enabled: true
      keys: [ro-key:ro, rw-key:rw]
datasets: []
```

Start it on unused ports: `spiced --http 127.0.0.1:28090 --flight 127.0.0.1:25051 --metrics 127.0.0.1:29090 &`.

## 3. Checks (use the CLI built from the branch: `target/debug/spice`)

| Command | Expected |
|---|---|
| `spice status --http-endpoint https://127.0.0.1:28090 --tls-root-certificate-file ca.pem --api-key ro-key` | component table, exit 0 |
| same with `--tls-root-certificate-file wrongca.pem` | `TLS certificate verification failed for https://127.0.0.1:28090: unknown issuer. Pass --tls-root-certificate-file ...`, exit 1 |
| same with no `--tls-root-certificate-file` | same TLS error (the CA is not in the system roots) |
| `--tls-root-certificate-file missing.pem` | `Could not load the TLS root certificate file 'missing.pem': ...` before any request |
| `spice datasets ... ca.pem --api-key ro-key` | dataset table (empty is fine), exit 0 |
| `spice refresh <dataset> ... wrongca.pem --api-key rw-key` | the TLS error, not `Is it running?` |
| `spice status --http-endpoint https://127.0.0.1:1 --tls-root-certificate-file ca.pem` | still `The Spice runtime is unavailable ... Is it running?` (nothing listening) |

Kill `spiced` and remove `$D` afterwards.
