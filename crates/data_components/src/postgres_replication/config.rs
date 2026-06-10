/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Replication parameters derived from connector params + environment.

use std::path::PathBuf;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};

/// Parameters for a single dataset's replication stream.
///
/// Built by the connector from spicepod params; see
/// `connector-postgres::lib::replication_params_from_connector_params`.
#[derive(Clone)]
pub struct ReplicationParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    pub sslmode: SslMode,
    /// Optional path to a PEM-encoded CA certificate bundle. Only used when
    /// `sslmode` is `VerifyCa` or `VerifyFull`.
    pub sslrootcert: Option<PathBuf>,

    pub slot_name: String,
    pub publication_name: String,
    pub initial_snapshot: bool,
    pub temporary_slot: bool,
    pub status_interval: Duration,
    /// Rows per emitted snapshot batch during initial bootstrap.
    pub bootstrap_batch_size: usize,
}

impl std::fmt::Debug for ReplicationParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationParams")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("sslmode", &self.sslmode)
            .field("sslrootcert", &self.sslrootcert)
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("temporary_slot", &self.temporary_slot)
            .field("status_interval", &self.status_interval)
            .field("bootstrap_batch_size", &self.bootstrap_batch_size)
            .finish_non_exhaustive()
    }
}

/// Dataset-level `on_schema_change` policy, plumbed into the replication stream
/// so the source layer can reconcile pgoutput `Relation` messages against the
/// working schema. Mirrors the runtime's `OnSchemaChange` enum — this crate
/// cannot depend on the runtime, so the connector maps between the two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SchemaEvolutionPolicy {
    /// `on_schema_change: block` (or omitted): run the legacy validation
    /// verbatim — extra source columns are silently ignored and a dataset
    /// column missing from the relation is a hard schema-mismatch error.
    #[default]
    Block,
    /// `on_schema_change: fail`: detect-and-error — any source relation schema
    /// change stops the stream with a terminal actionable error.
    Fail,
    /// `on_schema_change: append_new_columns`.
    AppendNewColumns,
    /// `on_schema_change: sync_all_columns`.
    SyncAllColumns,
}

impl SchemaEvolutionPolicy {
    /// `true` for the policies that adopt widening changes at the source layer
    /// (`append_new_columns` / `sync_all_columns`). The source adopts the full
    /// widening set for both so wider batches reach the runtime apply loop,
    /// which enforces the per-policy evolution set (added-only vs full).
    #[must_use]
    pub fn adopts_changes(self) -> bool {
        matches!(self, Self::AppendNewColumns | Self::SyncAllColumns)
    }
}

impl std::fmt::Display for SchemaEvolutionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Block => write!(f, "block"),
            Self::Fail => write!(f, "fail"),
            Self::AppendNewColumns => write!(f, "append_new_columns"),
            Self::SyncAllColumns => write!(f, "sync_all_columns"),
        }
    }
}

/// SSL negotiation + certificate-verification mode. Matches the standard
/// libpq sslmode values so users can set `pg_sslmode` with their normal
/// Postgres vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    /// No TLS. Plaintext connection; rejected by TLS-enforced servers.
    Disable,
    /// Try TLS; fall back to plaintext if the server doesn't support it.
    /// No cert verification.
    Prefer,
    /// Require TLS. No cert verification (vulnerable to MITM — use for
    /// development only).
    Require,
    /// Require TLS and verify the server certificate chains to a trusted
    /// CA. Does NOT verify the hostname.
    VerifyCa,
    /// Require TLS, verify chain, and verify the hostname in the cert
    /// matches the server's hostname (recommended for production).
    VerifyFull,
}

impl SslMode {
    #[must_use]
    pub fn from_str_or_default(s: Option<&str>) -> Self {
        match s.map(str::to_ascii_lowercase).as_deref() {
            Some("disable") => Self::Disable,
            Some("require") => Self::Require,
            Some("verify-ca") => Self::VerifyCa,
            Some("verify-full") => Self::VerifyFull,
            _ => Self::Prefer,
        }
    }

    /// Whether this mode requires any TLS negotiation at all.
    #[must_use]
    pub fn requires_tls(self) -> bool {
        !matches!(self, Self::Disable)
    }

    /// Whether this mode verifies the server certificate chain.
    #[must_use]
    pub fn verifies_certificate(self) -> bool {
        matches!(self, Self::VerifyCa | Self::VerifyFull)
    }

    /// Whether this mode verifies the server hostname against the cert.
    #[must_use]
    pub fn verifies_hostname(self) -> bool {
        matches!(self, Self::VerifyFull)
    }
}

/// Postgres identifiers have a 63-byte cap (NAMEDATALEN - 1). We budget:
///
///   - 6 bytes `spice_`
///   - up to `SLOT_DATASET_PORTION_MAX` / `PUB_DATASET_PORTION_MAX` of
///     sanitized dataset name
///   - 1 byte `_`
///   - 6 bytes dataset-hash (slot only, to survive truncation collisions)
///   - 1 byte `_`
///   - fixed 8-byte instance hash (slot) OR 3 bytes `pub` (publication)
///
/// which keeps the final identifier under the limit.
const PG_IDENTIFIER_MAX_BYTES: usize = 63;
const SLOT_PREFIX: &str = "spice_";
const SLOT_HASH_LEN: usize = 8;
const DATASET_HASH_LEN: usize = 6;
/// Max sanitized-dataset bytes for a slot name: 63 − (6 + 1 + 6 + 1 + 8) = 41.
const SLOT_DATASET_PORTION_MAX: usize =
    PG_IDENTIFIER_MAX_BYTES - SLOT_PREFIX.len() - 1 - DATASET_HASH_LEN - 1 - SLOT_HASH_LEN;
/// Max sanitized-dataset bytes for a publication name: 63 − (6 + 1 + 6 + 1 + 3) = 46.
const PUB_DATASET_PORTION_MAX: usize =
    PG_IDENTIFIER_MAX_BYTES - SLOT_PREFIX.len() - 1 - DATASET_HASH_LEN - 1 - 3;

/// Build a default slot name:
/// `spice_{sanitized_dataset}_{dataset_suffix}_{instance_suffix}`.
///
/// `dataset_suffix` is a short hash of the *full* dataset name so that two
/// long dataset names that happen to share the same truncated sanitized prefix
/// still produce distinct default slot names.
///
/// `instance_suffix` is an 8-char blake3-ish hash (actually `twox-hash` xxh3
/// for zero-dep reuse) of `SPICE_INSTANCE_ID` falling back to the machine
/// hostname, so each replica gets a distinct, deterministic slot across
/// restarts.
///
/// The sanitized dataset portion is truncated to keep the final identifier
/// within Postgres' 63-byte limit.
#[must_use]
pub fn default_slot_name(dataset_name: &str) -> String {
    let instance = std::env::var("SPICE_INSTANCE_ID")
        .ok()
        .or_else(|| hostname::get().ok().and_then(|h| h.into_string().ok()))
        .unwrap_or_else(|| "unknown".to_string());
    let instance_hash = xxh3_short_hash(&instance);
    let dataset_hash = xxh3_short_hash_prefix(dataset_name, DATASET_HASH_LEN);
    let dataset = truncate_to_bytes(&sanitize(dataset_name), SLOT_DATASET_PORTION_MAX);
    format!("{SLOT_PREFIX}{dataset}_{dataset_hash}_{instance_hash}")
}

/// Default publication is shared across replicas:
/// `spice_{sanitized_dataset}_{dataset_suffix}_pub`.
///
/// The `dataset_suffix` disambiguates truncated dataset names for the same
/// reason as [`default_slot_name`].
#[must_use]
pub fn default_publication_name(dataset_name: &str) -> String {
    let dataset_hash = xxh3_short_hash_prefix(dataset_name, DATASET_HASH_LEN);
    let dataset = truncate_to_bytes(&sanitize(dataset_name), PUB_DATASET_PORTION_MAX);
    format!("{SLOT_PREFIX}{dataset}_{dataset_hash}_pub")
}

/// Truncate an ASCII identifier to at most `max_bytes` bytes. Our `sanitize`
/// output is pure ASCII so byte-truncation = char-truncation; safe.
fn truncate_to_bytes(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        s.to_string()
    } else {
        s[..max_bytes].to_string()
    }
}

/// Postgres identifiers must match `[a-z_][a-z0-9_]*` to avoid quoting.
fn sanitize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || !out.starts_with(|c: char| c == '_' || c.is_ascii_alphabetic()) {
        out.insert(0, 'd');
    }
    out
}

fn xxh3_short_hash(s: &str) -> String {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash3_64::with_seed(0);
    h.write(s.as_bytes());
    let v = h.finish();
    // 8 hex chars is enough to disambiguate replicas at human scale.
    // Truncating the low 32 bits is intentional — we only want 8 hex chars.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "intentional 32-bit truncation to produce an 8-hex-char identifier"
    )]
    {
        format!("{:08x}", v as u32)
    }
}

fn xxh3_short_hash_prefix(s: &str, len: usize) -> String {
    xxh3_short_hash(s).chars().take(len).collect()
}

// Environment-based hostname discovery only. We intentionally avoid reading
// `/etc/hostname` because that's blocking I/O, and this function is called on
// a Tokio runtime thread during connector initialization. Any Kubernetes or
// Docker deployment already sets `HOSTNAME`, and fallback callers see
// `"unknown"` in the unlikely case neither env var is set.
mod hostname {
    pub fn get() -> std::io::Result<std::ffi::OsString> {
        if let Ok(h) = std::env::var("HOSTNAME") {
            return Ok(h.into());
        }
        if let Ok(h) = std::env::var("COMPUTERNAME") {
            return Ok(h.into());
        }
        Err(std::io::Error::other("hostname unavailable"))
    }
}

impl ReplicationParams {
    /// Build a tokio-postgres config for setup queries (not replication).
    #[must_use]
    pub fn setup_pg_config(&self) -> tokio_postgres::Config {
        self.pg_config(&format!("spice-replication-setup/{}", self.slot_name))
    }

    /// Build a tokio-postgres config with a custom `application_name`.
    #[must_use]
    pub fn pg_config(&self, application_name: &str) -> tokio_postgres::Config {
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(self.password.expose_secret())
            .dbname(&self.database)
            .application_name(application_name);
        match self.sslmode {
            SslMode::Disable => cfg.ssl_mode(tokio_postgres::config::SslMode::Disable),
            SslMode::Prefer => cfg.ssl_mode(tokio_postgres::config::SslMode::Prefer),
            SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {
                cfg.ssl_mode(tokio_postgres::config::SslMode::Require)
            }
        };
        cfg
    }

    /// Build a native-tls `MakeTlsConnector` matching the configured sslmode.
    /// Returns `None` when TLS is disabled — callers should connect with `NoTls`
    /// in that case.
    ///
    /// - `Disable` → `None` (no TLS)
    /// - `Prefer` / `Require` → accept any certificate (no verification)
    /// - `VerifyCa` → verify chain against the configured `sslrootcert` (or
    ///   system roots if none provided); hostname verification DISABLED
    /// - `VerifyFull` → verify chain AND hostname (strictest)
    pub async fn native_tls_connector(
        &self,
    ) -> std::result::Result<Option<postgres_native_tls::MakeTlsConnector>, TlsConfigError> {
        if !self.sslmode.requires_tls() {
            return Ok(None);
        }

        let mut builder = native_tls::TlsConnector::builder();
        // The `danger_accept_invalid_*` calls below implement the standard libpq
        // sslmode contract (see https://www.postgresql.org/docs/current/libpq-ssl.html
        // Table 34.1). Each dangerous flag is gated on an explicit user choice of
        // `pg_sslmode`: `prefer`/`require` opt out of all certificate validation,
        // `verify-ca` opts out of hostname validation. We emit a runtime warning
        // in each non-verifying branch so operators see the security posture of
        // their deployment in the logs. Production-safe default is `verify-full`,
        // which takes the no-danger-flags path and is the native-tls default.
        if !self.sslmode.verifies_certificate() {
            // require/prefer: accept anything — encryption only, no trust anchor.
            tracing::warn!(
                sslmode = ?self.sslmode,
                "Postgres replication TLS is not verifying server certificates. \
                 Set pg_sslmode=verify-full for production deployments."
            );
            builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        } else if !self.sslmode.verifies_hostname() {
            // verify-ca: chain is verified but hostname is not.
            tracing::warn!(
                sslmode = ?self.sslmode,
                "Postgres replication TLS is verifying the certificate chain but \
                 not the server hostname. Set pg_sslmode=verify-full to enable \
                 full MITM protection."
            );
            builder.danger_accept_invalid_hostnames(true);
        }
        // verify-full: both chain and hostname checked (native-tls default).

        if let Some(ca_path) = &self.sslrootcert {
            // Async I/O — this method is called from Tokio runtime threads
            // during setup/bootstrap; std::fs::read would block the reactor.
            let pem_bytes = tokio::fs::read(ca_path)
                .await
                .map_err(|e| TlsConfigError::ReadCa {
                    path: ca_path.clone(),
                    source: e,
                })?;
            let certs = parse_pem_certificates(ca_path, &pem_bytes)?;
            if certs.is_empty() {
                return Err(TlsConfigError::EmptyCaBundle {
                    path: ca_path.clone(),
                });
            }
            for cert in certs {
                builder.add_root_certificate(cert);
            }
        }

        let connector = builder.build().map_err(TlsConfigError::BuildConnector)?;
        Ok(Some(postgres_native_tls::MakeTlsConnector::new(connector)))
    }
}

/// Errors raised while assembling TLS configuration from `ReplicationParams`.
#[derive(Debug)]
pub enum TlsConfigError {
    ReadCa {
        path: PathBuf,
        source: std::io::Error,
    },
    ParseCa {
        source: native_tls::Error,
    },
    /// `BEGIN CERTIFICATE` marker with no matching `END CERTIFICATE`.
    TruncatedPem {
        path: PathBuf,
    },
    /// `sslrootcert` supplied but file contained zero parseable certificates.
    EmptyCaBundle {
        path: PathBuf,
    },
    BuildConnector(native_tls::Error),
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfigError::ReadCa { path, source } => {
                write!(
                    f,
                    "failed to read sslrootcert at {}: {source}",
                    path.display()
                )
            }
            TlsConfigError::ParseCa { source } => {
                write!(f, "failed to parse sslrootcert PEM: {source}")
            }
            TlsConfigError::TruncatedPem { path } => write!(
                f,
                "sslrootcert at {} has a BEGIN CERTIFICATE block without a matching END marker",
                path.display()
            ),
            TlsConfigError::EmptyCaBundle { path } => write!(
                f,
                "sslrootcert at {} contains no parseable CA certificates",
                path.display()
            ),
            TlsConfigError::BuildConnector(source) => {
                write!(f, "failed to build native-tls connector: {source}")
            }
        }
    }
}

impl std::error::Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TlsConfigError::ReadCa { source, .. } => Some(source),
            TlsConfigError::ParseCa { source } | TlsConfigError::BuildConnector(source) => {
                Some(source)
            }
            TlsConfigError::TruncatedPem { .. } | TlsConfigError::EmptyCaBundle { .. } => None,
        }
    }
}

/// Split a PEM blob into individual `native_tls::Certificate`s. An unmatched
/// `BEGIN CERTIFICATE` (missing `END CERTIFICATE`) returns
/// `TlsConfigError::TruncatedPem` rather than silently returning whatever was
/// parsed so far — the alternative would leave operators thinking their CA
/// bundle is loaded when it isn't.
fn parse_pem_certificates(
    path: &std::path::Path,
    pem: &[u8],
) -> std::result::Result<Vec<native_tls::Certificate>, TlsConfigError> {
    let mut certs = Vec::new();
    let mut remaining = pem;
    while let Some(begin) = find_subslice(remaining, b"-----BEGIN CERTIFICATE-----") {
        let tail = &remaining[begin..];
        let Some(end_rel) = find_subslice(tail, b"-----END CERTIFICATE-----") else {
            return Err(TlsConfigError::TruncatedPem {
                path: path.to_path_buf(),
            });
        };
        let end = begin + end_rel + b"-----END CERTIFICATE-----".len();
        let block = &remaining[begin..end];
        let cert = native_tls::Certificate::from_pem(block)
            .map_err(|source| TlsConfigError::ParseCa { source })?;
        certs.push(cert);
        remaining = &remaining[end..];
    }
    Ok(certs)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_non_alnum() {
        assert_eq!(sanitize("public.users"), "public_users");
        assert_eq!(sanitize("my-dataset"), "my_dataset");
        assert_eq!(sanitize(""), "d");
        assert_eq!(sanitize("9leading"), "d9leading");
    }

    #[test]
    fn default_names_are_deterministic_within_a_process() {
        // Whatever the current env is, two calls should produce the same name.
        // We intentionally don't mutate SPICE_INSTANCE_ID here because Rust
        // tests run concurrently and other tests may also read it.
        let a1 = default_slot_name("users");
        let a2 = default_slot_name("users");
        assert_eq!(a1, a2);
        assert!(a1.starts_with("spice_users_"));
    }

    #[test]
    fn publication_default() {
        // Format: spice_{dataset}_{6-char hash}_pub
        let users = default_publication_name("users");
        assert!(users.starts_with("spice_users_"), "got {users}");
        assert!(users.ends_with("_pub"), "got {users}");
        let orders = default_publication_name("public.orders");
        assert!(orders.starts_with("spice_public_orders_"), "got {orders}");
        assert!(orders.ends_with("_pub"), "got {orders}");
    }

    #[test]
    fn slot_name_is_truncated_to_postgres_limit() {
        // 120-char dataset name → slot must still be ≤ 63 bytes.
        let long = "a".repeat(120);
        let slot = default_slot_name(&long);
        assert!(
            slot.len() <= PG_IDENTIFIER_MAX_BYTES,
            "slot `{slot}` exceeds {PG_IDENTIFIER_MAX_BYTES} bytes: {}",
            slot.len()
        );
        assert!(slot.starts_with(SLOT_PREFIX));
        // Must still end in the instance hash (8 hex chars after final `_`).
        let hash_part = slot.rsplit_once('_').expect("format has _").1;
        assert_eq!(hash_part.len(), SLOT_HASH_LEN);
    }

    #[test]
    fn publication_name_is_truncated_to_postgres_limit() {
        let long = "b".repeat(120);
        let pubname = default_publication_name(&long);
        assert!(
            pubname.len() <= PG_IDENTIFIER_MAX_BYTES,
            "publication `{pubname}` exceeds {PG_IDENTIFIER_MAX_BYTES} bytes"
        );
        assert!(pubname.starts_with(SLOT_PREFIX));
        assert!(pubname.ends_with("_pub"));
    }

    #[test]
    fn truncated_prefix_collisions_are_disambiguated() {
        // Two dataset names that share the first 60 characters must still
        // produce distinct default slot and publication names — the dataset
        // hash suffix guards against truncation collisions.
        let shared_prefix = "a".repeat(60);
        let a = format!("{shared_prefix}_alpha");
        let b = format!("{shared_prefix}_beta");
        assert_ne!(default_slot_name(&a), default_slot_name(&b));
        assert_ne!(default_publication_name(&a), default_publication_name(&b));
    }
}
