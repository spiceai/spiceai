# Spice.ai Security Audit

Scope: workspace-wide audit of the Spice.ai runtime, focused on authentication,
secrets, HTTP/Flight surface, AI/LLM tooling, data connectors, and the
container/build pipeline. Findings list file paths and line numbers verified
against the tree at audit time.

Severity legend:

- **Critical** — direct auth bypass, RCE, or credential exfil with low effort.
- **High** — practical exploit path with meaningful impact (MITM, SSRF to
  cloud metadata, brute-force).
- **Medium** — exploitable under realistic-but-narrower conditions, or a
  hardening gap with clear mitigation.
- **Low** — defense-in-depth issue, info disclosure, or operator-controlled
  surface.

---

## Critical

### C1. API key comparison is not constant-time (timing oracle)

`crates/runtime-auth/src/api_key/mod.rs:68`, `:82`, `:94`, `:116`

All four `ApiKeyAuth` paths (HTTP, Flight basic auth, Flight bearer, gRPC
metadata) compare the presented key to the configured allowlist with `==` /
`PartialEq`:

```rust
if let Some(api_key) = self.api_keys.iter().find(|key| *key == api_key) { ... }
```

`ApiKey`'s `PartialEq` resolves to byte-wise `String::eq`, which short-circuits
on the first mismatched byte. Over a network this is observable and lets an
attacker recover the key one byte at a time. This applies to every
authenticated endpoint in the product.

**Fix.** Compare with a constant-time primitive (e.g., `subtle::ConstantTimeEq`
on the bytes, or `constant_time_eq`). Iterate over all keys without
short-circuiting, or hash incoming keys and look them up in a `HashSet`
of stored hashes.

### C2. Remote UDF endpoints have no SSRF allowlist

`crates/runtime-datafusion-udfs/src/user_functions/remote.rs:971-982`

`parse_endpoint` only validates the URL scheme; any operator- or
LLM-influenced UDF declaration can target loopback, private RFC1918 ranges, or
the cloud metadata service:

```rust
fn parse_endpoint(from: &str) -> Result<Url> {
    let url = Url::parse(from)?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        ...
    }
}
```

The same `parse_endpoint` is used by scalar, aggregate, and table UDFs
(`:180`, `:236`, `:292`). On AWS/GCP/Azure deployments this is a practical
path to IAM credential exfiltration via `169.254.169.254`.

**Fix.** Resolve the host and reject loopback, link-local (`169.254.0.0/16`,
`fe80::/10`), RFC1918, ULA, and the cloud metadata IPs unless an explicit
opt-in flag is set. Re-validate after redirects.

---

## High

### H1. MySQL catalog connector silently disables TLS verification in `preferred` mode

`crates/runtime/src/catalogconnector/mysql.rs:242-246`

```rust
if ssl_mode == "preferred" {
    opts = opts
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true);
}
```

`preferred` is one of three accepted values and is selected via a parameter
that may come from spicepod config. Both certificate validation and hostname
validation are disabled with no warning, enabling MITM against any MySQL
metadata fetch using this mode.

**Fix.** Either remove `preferred` (the libmysql semantics — encrypt if
offered, fall back to plaintext — should not require disabling validation),
or split it into an explicit `tls_skip_verify` flag with a startup warning.

### H2. HashiCorp Vault `tls_skip_verify` honored for non-loopback hosts

`crates/runtime-secrets/src/stores/hashicorp_vault.rs` (TLS skip verify
parameter handling)

The Vault store accepts `hashicorp_vault_tls_skip_verify=true` for any
address. Disabling cert verification against a remote Vault inverts the
trust model of the secrets backend itself: an on-path attacker that can
intercept Vault traffic now harvests every secret the runtime requests.

**Fix.** Reject `tls_skip_verify=true` unless the Vault address resolves to a
loopback host, mirroring the existing `build_base_url` loopback gate.

### H3. AWS Secrets Manager falls back to default credential chain without
identity verification at config time

`crates/runtime-secrets/src/stores/aws_secrets_manager.rs` (init path)

When neither `key` nor `secret` is configured, the SDK's default chain is
used (env, profile, IMDS, IRSA, container creds). The first successful
secret lookup is what surfaces the identity, by which point the runtime may
have read cross-tenant secrets. No early `sts:GetCallerIdentity` is run when
the chain is implicit.

**Fix.** When no explicit credentials are supplied, run `GetCallerIdentity`
during store initialization and log the principal. Optionally require an
`aws_expected_account_id` parameter and refuse to start if it does not
match.

### H4. No brute-force / rate-limit on auth failures

`crates/runtime-auth/src/layer/{http,grpc,flight}.rs`

Auth verdicts feed back to the caller without any per-IP or per-key
rate-limiting. Combined with C1, this makes online key recovery much more
practical; even if C1 is fixed, it allows credential stuffing and
password-spray-style probing of any configured keys.

**Fix.** Apply a rate-limit / lockout middleware (e.g., `governor`, which is
already in the dependency tree) on auth-failure events keyed on remote IP,
with structured logging to support alerting.

---

## Medium

### M1. Container DuckDB directory is world-writable (`chmod 777`)

`Dockerfile-cuda:53`, `Dockerfile-cuda-release` (equivalent line)

```dockerfile
RUN mkdir /.duckdb/ && chmod 777 /.duckdb/
```

Any compromised process in the container can plant a malicious DuckDB
extension under this path, which DuckDB will load on next use. The non-CUDA
release image uses `chmod 700`, which is the right baseline.

**Fix.** Replace with `chmod 700` and `chown` to the runtime user; create
the directory under the runtime user instead of root.

### M2. Cloud-metadata IPs not blocked in MCP / outbound HTTP allowlist

`crates/runtime/src/tools/mcp/catalog.rs` (`is_localhost`)

`is_localhost` covers `127.0.0.1`, `::1`, `localhost`, `0.0.0.0` but not
`169.254.169.254`, `fd00:ec2::254`, RFC1918, or ULA addresses. Any tool that
relies on this gate (MCP HTTP transport, others) can be steered at internal
services on a multi-tenant host.

**Fix.** Replace `is_localhost` with a "is private/internal" check covering
the IP families above; reuse the same helper in C2's fix.

### M3. Kafka SSL `endpoint_identification_algorithm=none` accepted

`crates/runtime/src/dataconnector/kafka.rs:124`,
`crates/runtime/src/dataconnector/debezium.rs:142,224`

`none` disables hostname verification on the Kafka client. The runtime warns
on invalid values and defaults to `https`, but accepts `none` silently when
the value is a string match.

**Fix.** Remove `none` from the accepted set, or require a separate
`kafka_tls_skip_verify` opt-in flag that logs at `warn!` on every connection.

### M4. Metrics endpoint binds to `0.0.0.0` by default in shipped CMDs

`Dockerfile-cuda:76` (and other Dockerfiles)

```dockerfile
CMD ["--http","0.0.0.0:8090","--metrics","0.0.0.0:9090","--flight","0.0.0.0:50051"]
```

The metrics server has no auth layer (separate from the API auth) and
exposes per-dataset and per-query telemetry. Default binding to `0.0.0.0`
means anyone on the pod / VPC network can scrape it.

**Fix.** Default `--metrics` to `127.0.0.1:9090` in the shipped CMD; require
an explicit override to expose. Document that the metrics server should sit
behind the same auth or be bound to a private interface.

### M5. `.dockerignore` is too narrow

`.dockerignore`

Only excludes `target/` and `test/`. The build context shipped to the daemon
includes `.git/`, `.github/`, `.cargo/`, local `.env*` files, and any
`*.pem`/`*.key` placed at the repo root.

**Fix.** Add `.git`, `.github`, `.env*`, `.cargo`, `*.pem`, `*.key`, `*.pfx`,
`*.p12`, `node_modules`, and editor caches.

### M6. Install scripts download release binaries without checksum verification

`install/install.sh`, `install/install-spiced.sh`

Tarballs are fetched from GitHub releases over HTTPS and executed without a
SHA256 verification step against a separately-fetched checksum file. The
TLS guarantee covers transport but not a compromised release artifact.

**Fix.** Publish (and sign) `checksums.txt` alongside each release; have the
installer download both, verify the artifact, and abort on mismatch.

### M7. Tooling installs use `curl | sh` without checksum verification

`.github/actions/setup-rust/action.yml:41`,
`.github/workflows/e2e_test_release_install_helm.yml`

Both rustup and Helm are piped from `curl` directly into `sh`/`bash`. Any
upstream compromise lands directly in CI runners (which hold release-signing
permissions in some workflows).

**Fix.** Pin the script by SHA256 (download, verify, then execute) or use
versioned action wrappers (`actions-rs/toolchain` / `azure/setup-helm`)
already pinned by SHA in this repo's other workflows.

### M8. CUDA dev/local Dockerfiles run as root

`Dockerfile-cuda` (no `USER` directive after the runtime stage),
`Dockerfile.local`

The hardened release Dockerfiles use `USER 65534:65534` (nobody). The CUDA
and local-dev variants do not, so a process compromise in those images runs
as root. These images are commonly used in production for GPU inference.

**Fix.** Add a non-root `USER` line to the runtime stage of `Dockerfile-cuda`
and `Dockerfile-cuda-release` (with `chown` of `/app` and `/.duckdb`).

### M9. CORS wildcard plus `Authorization` allow-header

`crates/runtime/src/http/routes.rs` (CORS layer construction)

When operators set `allowed_origins: ["*"]`, the layer maps to
`AllowOrigin::Any` while `allow_headers([AUTHORIZATION, ...])` is still in
effect. Browsers won't actually send credentialed requests with a wildcard
origin (the spec blocks it), but it relaxes the model and is easy to misuse
when paired with proxies that rewrite the origin.

**Fix.** Refuse to start when `allowed_origins` contains `*` and auth is
configured, or strip `AUTHORIZATION` from `allow_headers` in that mode.

### M10. Bearer tokens flow through `format!` strings in remote UDF builder

`crates/runtime-datafusion-udfs/src/user_functions/remote.rs` (auth header
construction near `:200`, `:248`)

The auth bearer is held as `Option<String>` and concatenated into header
values with `format!`. Any `Display`/`Debug` of the surrounding error or
config types risks landing the token in a log. The rest of the codebase
uses `SecretString` for credentials.

**Fix.** Store the token as `SecretString` and only call `expose_secret()`
when constructing the outbound header value, never inside an error path.

### M11. Kubernetes secrets store: confirm path-segment encoding is applied

`crates/runtime-secrets/src/stores/kubernetes.rs` (uses
`PATH_SEGMENT_ENCODE_SET`)

The encode set is defined but the call sites that build the API URL must
all use it. A missed call site would let `from: kubernetes:../...` escape
the configured namespace (subject to RBAC).

**Fix.** Audit every URL construction in this store; add a focused test that
feeds `..` and `/` into the secret name and asserts the request URL stays
within the namespace.

### M12. No HTTP security headers on the API

`crates/runtime/src/http/routes.rs` (response middleware)

No `X-Content-Type-Options`, `Strict-Transport-Security`, `X-Frame-Options`,
or `Content-Security-Policy` are emitted. Low impact for a JSON API, but
matters for any of the HTML-bearing routes (`/v1/iceberg`, the dev
swagger feature, error pages) and for sniffing-mitigation in browser
clients.

**Fix.** Add a `SetResponseHeaderLayer` stack with at least
`x-content-type-options: nosniff` and `strict-transport-security` when TLS
is enabled.

---

## Low

### L1. `ApiKeyAuth::http_verify` reads `X-API-Key` only on the first header

`crates/runtime-auth/src/api_key/mod.rs:60`

`headers.get("X-API-Key")` returns the first value. A misconfigured proxy
that appends a second header value (instead of replacing) could let a
client smuggle a valid key followed by an attacker-controlled value that
then flows into downstream identity checks. Low likelihood, easy to fix.

**Fix.** Use `headers.get_all` and reject when more than one value is
present, or canonicalize at the proxy layer with documented assumptions.

### L2. gRPC metadata key is lower-case-only

`crates/runtime-auth/src/api_key/mod.rs:105` (`metadata.get("x-api-key")`)

gRPC metadata keys are lower-cased by spec, but Tonic clients sometimes
send mixed case from user code. Compatibility-only.

### L3. `install/install-nightly.sh` reads `GITHUB_TOKEN` from the environment

`install/install-nightly.sh:151`

The token is consumed via env var, which is fine, but error messages echo
context that can end up in CI logs. Add a clear "do not run with `set -x`"
warning and redact the token from any error path.

### L4. Test-only `CapturingVerifier` accepts all certs

`crates/runtime/tests/tls_reload/mod.rs:154-172`

`#[cfg(test)]`-gated. Noted for completeness; ensure no `cfg` accident or
re-export ever exposes this builder to non-test crates.

### L5. `deny.toml` has no advisories block

`deny.toml`

`cargo deny check` runs licenses/sources/bans only. Add an `advisories`
section to fail builds on RUSTSEC entries; if any are unavoidable, lock them
in as explicit `ignore = ["RUSTSEC-…"]` with an expiry comment.

---

## What was checked and found clean

- **SQL injection.** All HTTP and Flight SQL paths run user input through
  DataFusion's logical plan + `ParamValues` rather than string-concatenated
  SQL. No `format!`-built queries on user input were found in
  `crates/runtime/src/http/`, `crates/runtime/src/flight/`, or the SQL tool.
- **Open redirect / CSRF.** No state-changing GET endpoints; no
  `Location`-from-input redirects.
- **Path traversal in HTTP.** No file-serving endpoint that takes a
  client-supplied filename. NFS/SMB connectors validate URLs through
  `url::Url`.
- **Unsafe deserialization.** Network input is JSON (serde) or
  Arrow/Protobuf (typed). No `bincode`/`pickle`/yaml-on-untrusted-input.
- **TLS server config.** `rustls` with hot-reload via `CertWatcher`;
  `with_no_client_auth()` is correct for a public API.
- **JWT.** GitHub app token uses `Algorithm::RS256` with no algorithm
  negotiation; no `alg: none` path.
- **Postgres replication TLS.** `prefer`/`require` modes log loudly and
  default is `verify-full`. Acceptable, with a doc note recommended.
- **Cluster mTLS.** `--allow-insecure-connections` is opt-in and named
  loudly enough to be flagged in review.
- **Secrets at rest.** Remote-store configs implement manual `Debug` to
  redact; runtime values use `SecretString` with zeroize on drop.
- **CI action pinning.** Workflows pin third-party actions by commit SHA,
  not by tag.

---

## Suggested remediation order

1. Constant-time API key compare (C1) — small change, broad blast radius.
2. Remote-UDF and MCP SSRF allowlist (C2 / M2) — share one helper.
3. MySQL `preferred` and Vault `tls_skip_verify` (H1, H2) — both are
   one-line config gates with high MITM impact.
4. Default credential chain identity check (H3).
5. Auth-failure rate limiting (H4).
6. Container hardening sweep (M1, M4, M5, M8).
7. Install/CI checksum verification (M6, M7).
8. Remaining medium/low items as part of a hardening pass.
