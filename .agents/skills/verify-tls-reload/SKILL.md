---
name: verify-tls-reload
description: Verify spiced's TLS certificate hot-reload against a running runtime — good rotations swap once, bad ones (malformed PEM, cert that doesn't match the key) keep the old cert, log a WARN and tick runtime_tls_reload_total. Use after changing crates/runtime-tls/src/reload.rs or the TLS wiring in bin/spiced.
---

# Verify TLS hot-reload live

Unit tests (`cargo test -p runtime-tls`) and the `tls_reload::` integration
tests (`cargo test -p runtime --test integration -- tls_reload`) cover the
logic. This is the live check against a real `spiced`.

## 1. PKI in a scratch dir (never in the repo)

```bash
D=$(mktemp -d); cd "$D"
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 2 -subj "/CN=reload test CA"
printf "subjectAltName=DNS:localhost,IP:127.0.0.1\n" > ext.cnf
leaf() { # leaf <name>: key + cert for localhost signed by the CA
  openssl req -new -newkey rsa:2048 -nodes -keyout "$1.key" -subj "/CN=localhost" -out "$1.csr"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 -extfile ext.cnf -out "$1.pem"
}
leaf a; leaf b; leaf other
cp a.pem server.pem; cp a.key server.key
```

## 2. Start spiced with TLS from those paths

```bash
cat > spicepod.yaml <<'YAML'
version: v1
kind: Spicepod
name: reload
runtime:
  tls:
    enabled: true
    certificate_file: ./server.pem
    key_file: ./server.key
YAML
spiced --http 127.0.0.1:18443 --metrics 127.0.0.1:19090 > spiced.log 2>&1 &
```

Use a `spiced` built from the branch under test (`cargo build -p spiced`,
one build at a time). Check `spiced.log` for the TLS startup line.

## 3. Exercise the reload paths

The watcher polls every 2s; wait ~5s after each write.

```bash
fp()  { echo | openssl s_client -connect 127.0.0.1:18443 -CAfile ca.pem 2>/dev/null | openssl x509 -noout -fingerprint -sha256; }
ok()  { curl -sS --cacert ca.pem https://127.0.0.1:18443/health; echo " exit=$?"; }
met() { curl -sS --cacert ca.pem https://127.0.0.1:19090/metrics | grep runtime_tls_reload_total; }
```

| Step | Action | Expect |
|---|---|---|
| baseline | — | `ok` succeeds, `fp` = a.pem |
| cert only | `cp other.pem server.pem` | `ok` still succeeds, `fp` still a.pem, `result="key_mismatch"` +1, one WARN naming both files |
| restore | `cp a.pem server.pem` | no swap, no new INFO line, no counter change (material already served) |
| rotation | write b.pem then b.key (`cp x tmp && mv tmp dst` each) | `fp` = b.pem, `result="ok"` +1, exactly one `TLS reload: swapped server certificate` |
| garbage | `echo nope > server.pem` | old cert keeps serving, `result="parse_error"` +1 |

A rotation whose two writes straddle a poll boundary can log one
`key_mismatch` WARN before the second write lands and the swap succeeds;
that is expected.

## 4. Clean up

Kill `spiced`, `rm -rf "$D"`.
