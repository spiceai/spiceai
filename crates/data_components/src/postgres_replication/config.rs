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

use std::time::Duration;

use pgwire_replication::{CaCertificate, PgOutputFormat};
use secrecy::{ExposeSecret, SecretString};

/// Interpret a user-supplied `pg_sslrootcert` value as either PEM content or a
/// path to a PEM file.
///
/// A CA bundle is just as often injected as a configuration value — an
/// orchestrator secret, an environment variable — as it is mounted as a file, so
/// both spellings must verify identically. The two are told apart by content, on
/// the PEM armor: anything containing a `BEGIN CERTIFICATE` block is PEM, and
/// everything else is a path. Detecting on the armor rather than on the shape of
/// the string (length, newlines, a leading `/`) keeps every value that is a path
/// today still a path.
///
/// Content arriving through a channel that does not survive real newlines (a
/// single-line environment variable, a JSON string pasted verbatim) carries them
/// as the two characters `\` and `n`; those are restored before parsing.
#[must_use]
pub fn ca_certificate_from_param(value: &str) -> CaCertificate {
    if value.contains("-----BEGIN CERTIFICATE-----") {
        return CaCertificate::Pem(value.replace("\\n", "\n").into_bytes());
    }
    CaCertificate::Path(value.into())
}

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
    /// Optional PEM-encoded CA certificate bundle, either inline or by path (see
    /// [`ca_certificate_from_param`]). Only used when `sslmode` is `VerifyCa` or
    /// `VerifyFull`.
    pub sslrootcert: Option<CaCertificate>,

    pub slot_name: String,
    pub publication_name: String,
    pub initial_snapshot: bool,
    /// Take the initial snapshot even when resuming from an existing slot.
    ///
    /// Set by the connector when the dataset's accelerator does not persist
    /// across restarts (in-memory engines, `mode: memory`, `mode: file_create`):
    /// the accelerator starts empty every boot, so a plain slot resume would
    /// leave it serving only rows touched after startup — silently missing
    /// all history. Snapshot-then-resume is correct for an empty accelerator:
    /// the WAL overlap from `confirmed_flush_lsn` replays idempotently via
    /// the PK upsert. `initial_snapshot: false` still disables all snapshots.
    pub snapshot_on_resume: bool,
    pub status_interval: Duration,
    /// Lag-based readiness threshold: the dataset is marked Ready once its
    /// replication lag (now minus the newest applied commit's source time)
    /// falls below this, so a snapshotting or backlog-draining dataset stays
    /// not-ready and never serves stale data. User param
    /// `pg_replication_ready_lag` (default 2s).
    pub ready_lag: Duration,
    /// Rows per emitted snapshot batch during initial bootstrap.
    pub bootstrap_batch_size: usize,
    /// `true` when the slot name was explicitly configured
    /// (`pg_replication_slot`). Explicitly-named slots are served by the
    /// shared multiplexer ([`super::shared`]): every dataset on the same
    /// connection naming the same slot shares one replication
    /// connection/decoder, with changes routed per table. Default
    /// (per-dataset generated) slot names keep the dedicated per-dataset
    /// stream.
    pub shared: bool,
    /// Capacity of each shared-slot member's bounded coalescing mailbox
    /// (envelopes).
    /// Only consulted on the shared path ([`super::shared`]); the per-dataset
    /// stream does not use it. Adjacent compatible transactions can share one
    /// envelope, so this bounds published envelope count rather than source
    /// transaction count. Defaults to
    /// [`super::shared::DEFAULT_MEMBER_CHANNEL_CAPACITY`].
    pub member_channel_capacity: usize,

    /// pgoutput column output format to request on the WAL stream. Internal —
    /// not a spicepod parameter: the connector always sets [`PgOutputFormat::Binary`]
    /// (binary decodes faster and avoids source-side text formatting). Exposed
    /// as a field only so tests can force [`PgOutputFormat::Text`] to exercise
    /// the text fallback and assert binary/text parity. Per-column the server
    /// still emits text for types lacking a binary send function, so the text
    /// decode path stays live regardless of this setting.
    pub pg_output_format: PgOutputFormat,
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
            .field("snapshot_on_resume", &self.snapshot_on_resume)
            .field("status_interval", &self.status_interval)
            .field("bootstrap_batch_size", &self.bootstrap_batch_size)
            .field("shared", &self.shared)
            .field("member_channel_capacity", &self.member_channel_capacity)
            .field("pg_output_format", &self.pg_output_format)
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

    /// Strict variant of [`Self::from_str_or_default`]: an absent value uses
    /// the `prefer` default, but an unrecognized value is rejected rather than
    /// silently downgraded to `prefer`. A typo'd `verify-full` quietly turning
    /// into `prefer` would disable certificate/hostname verification (a silent
    /// TLS/MITM downgrade), so the CDC parameter path validates loudly.
    pub fn from_str_strict(s: Option<&str>) -> std::result::Result<Self, String> {
        match s.map(str::trim) {
            None | Some("") => Ok(Self::Prefer),
            Some(raw) => match raw.to_ascii_lowercase().as_str() {
                "disable" => Ok(Self::Disable),
                "prefer" => Ok(Self::Prefer),
                "require" => Ok(Self::Require),
                "verify-ca" => Ok(Self::VerifyCa),
                "verify-full" => Ok(Self::VerifyFull),
                _ => Err(format!(
                    "must be one of disable, prefer, require, verify-ca, verify-full, got {raw:?}"
                )),
            },
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

/// Reserved by `PostgreSQL` for the conflict-detection replication slot
/// (`CONFLICT_DETECTION_SLOT` in `src/include/replication/slot.h`). Rejected
/// the same way as `ReplicationSlotValidateNameInternal(..., allow_reserved_name=false)`.
const CONFLICT_DETECTION_SLOT: &str = "pg_conflict_detection";

const SLOT_PREFIX: &str = "spice_";
const SLOT_HASH_LEN: usize = 8;
const DATASET_HASH_LEN: usize = 6;
/// Max sanitized-dataset bytes for a slot name: 63 − (6 + 1 + 6 + 1 + 8) = 41.
const SLOT_DATASET_PORTION_MAX: usize =
    PG_IDENTIFIER_MAX_BYTES - SLOT_PREFIX.len() - 1 - DATASET_HASH_LEN - 1 - SLOT_HASH_LEN;
/// Max sanitized-dataset bytes for a publication name: 63 − (6 + 1 + 6 + 1 + 3) = 46.
const PUB_DATASET_PORTION_MAX: usize =
    PG_IDENTIFIER_MAX_BYTES - SLOT_PREFIX.len() - 1 - DATASET_HASH_LEN - 1 - 3;

/// Validates a `PostgreSQL` replication slot name.
///
/// Mirrors `ReplicationSlotValidateNameInternal` in `PostgreSQL` `slot.c`:
/// names must match `[a-z0-9_]{1,NAMEDATALEN-1}` (`NAMEDATALEN` is 64, so at
/// most 63 bytes) and must not be the reserved conflict-detection slot
/// (`pg_conflict_detection`).
///
/// # Errors
///
/// Returns a short reason suitable for prefixing with the user-facing parameter
/// name, for example: parameter `pg_replication_slot` must be …
pub fn validate_replication_slot_name(name: &str) -> Result<(), String> {
    // Postgres uses `strlen` (byte length). Slot names are ASCII-only when
    // valid, so byte length equals char length for accepted names; reject
    // overlong UTF-8 by bytes the same way the server would.
    if name.is_empty() {
        return Err(format!(
            "must be 1 to {PG_IDENTIFIER_MAX_BYTES} bytes matching [a-z0-9_], got {name:?}"
        ));
    }
    if name.len() > PG_IDENTIFIER_MAX_BYTES {
        return Err(format!(
            "must be at most {PG_IDENTIFIER_MAX_BYTES} bytes, got {} bytes in {name:?}",
            name.len()
        ));
    }
    if let Some(invalid) = name
        .chars()
        .find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '_'))
    {
        return Err(format!(
            "must contain only lowercase letters, numbers, and underscores ([a-z0-9_]), \
             found invalid character {invalid:?} in {name:?}"
        ));
    }
    if name == CONFLICT_DETECTION_SLOT {
        return Err(format!(
            "must not use the reserved name {CONFLICT_DETECTION_SLOT:?} \
             (reserved by PostgreSQL for conflict detection)"
        ));
    }
    Ok(())
}

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
    slot_name_for(dataset_name, &resolve_instance_id())
}

/// Build the slot name for `dataset_name` scoped to `instance_id` (the value
/// that distinguishes one spiced instance from another sharing the same source
/// database). Factored out as a pure function — the instance identity is passed
/// in rather than read from the environment — so its determinism and
/// distinctness properties can be unit-tested without mutating process-global
/// env vars. For the resulting slot name:
///
///   - deterministic/stable (a strict guarantee): identical `(dataset_name,
///     instance_id)` always produces the identical name, so a restart of the
///     same instance resumes its existing replication slot rather than
///     orphaning one;
///   - distinct per instance (in practice, not a strict guarantee): two
///     instances (distinct `instance_id`) pointed at the same catalog get
///     different names, so they don't share one physical Postgres slot (which
///     permits a single consumer). The instance identity is folded into a short
///     8-hex hash, so a collision is possible but astronomically unlikely;
///   - distinct per dataset/catalog (same caveat): distinct `dataset_name`s get
///     different names, disambiguated by a 6-hex hash of the full name so that
///     names sharing a truncated prefix still differ.
fn slot_name_for(dataset_name: &str, instance_id: &str) -> String {
    let instance_hash = xxh3_short_hash(instance_id);
    let dataset_hash = xxh3_short_hash_prefix(dataset_name, DATASET_HASH_LEN);
    let dataset = truncate_to_bytes(&sanitize(dataset_name), SLOT_DATASET_PORTION_MAX);
    format!("{SLOT_PREFIX}{dataset}_{dataset_hash}_{instance_hash}")
}

/// Default publication name for a dataset with an explicitly-named
/// (shareable) replication slot: `{sanitized_slot}_pub`.
///
/// Derived from the slot rather than the dataset so that every dataset
/// sharing the slot lands on the *same* publication by default — the shared
/// stream opens one replication connection with one publication covering all
/// member tables.
#[must_use]
pub fn publication_name_for_slot(slot_name: &str) -> String {
    let base = sanitize(slot_name);
    let base = truncate_to_bytes(&base, PG_IDENTIFIER_MAX_BYTES - 4);
    format!("{base}_pub")
}

/// The identity distinguishing this spiced instance from another sharing the
/// same source database: `SPICE_INSTANCE_ID` if set, else the machine hostname,
/// else `"unknown"`. Stable across restarts of the same instance, which is what
/// keeps [`slot_name_for`] resuming the same replication slot on restart.
fn resolve_instance_id() -> String {
    std::env::var("SPICE_INSTANCE_ID")
        .ok()
        .or_else(|| hostname::get().ok().and_then(|h| h.into_string().ok()))
        .unwrap_or_else(|| "unknown".to_string())
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

/// Slot-name prefix for a CDC-accelerated catalog's single shared replication
/// slot. Distinct from the per-dataset `spice_` prefix so a catalog slot and a
/// same-named dataset slot can never collide, and so catalog slots stay
/// greppable.
const CATALOG_SLOT_PREFIX: &str = "spice_catalog_";

/// Max sanitized-catalog-name bytes in a catalog slot name:
/// 63 − (14 prefix + 1 separator + 6 hash) = 42.
const CATALOG_SLOT_NAME_PORTION_MAX: usize =
    PG_IDENTIFIER_MAX_BYTES - CATALOG_SLOT_PREFIX.len() - 1 - DATASET_HASH_LEN;

/// Build the shared replication-slot name for a CDC-accelerated catalog:
/// `spice_catalog_{sanitized_catalog}_{catalog_hash}`.
///
/// Unlike [`default_slot_name`], this is derived PURELY from the catalog name
/// with **no instance component**, which makes it:
///
///   - deterministic and stable -- the same catalog name always yields the same
///     slot name, so a restart (or a reschedule of the catalog onto a different
///     node) recomputes the identical name and *reuses* the existing replication
///     slot rather than orphaning it and re-snapshotting from scratch;
///   - independent of the Spice instance/host -- two nodes configured with the
///     same catalog resolve to the same slot name. Since `PostgreSQL` permits
///     only one consumer per slot, the catalog acceleration path fails loudly at
///     startup when the slot is already actively held by another consumer (see
///     `AcceleratedCatalogProvider::refresh`), rather than silently competing for
///     it. No slot identity is persisted by Spice: the name is a pure function of
///     the catalog name, and the durable state is the `PostgreSQL` slot itself.
///
/// `catalog_hash` is a short hash of the *full* catalog name so two long names
/// that share a truncated sanitized prefix still produce distinct slot names.
/// The sanitized portion is truncated to keep the identifier within Postgres'
/// 63-byte limit.
#[must_use]
pub fn catalog_slot_name(catalog_name: &str) -> String {
    let catalog_hash = xxh3_short_hash_prefix(catalog_name, DATASET_HASH_LEN);
    let catalog = truncate_to_bytes(&sanitize(catalog_name), CATALOG_SLOT_NAME_PORTION_MAX);
    format!("{CATALOG_SLOT_PREFIX}{catalog}_{catalog_hash}")
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

        if let Some(ca) = &self.sslrootcert {
            let source = ca.describe();
            let pem_bytes = match ca {
                // Async I/O — this method is called from Tokio runtime threads
                // during setup/bootstrap; std::fs::read would block the reactor.
                CaCertificate::Path(path) => {
                    std::borrow::Cow::Owned(tokio::fs::read(path).await.map_err(|e| {
                        TlsConfigError::ReadCa {
                            source_label: source.clone(),
                            source: e,
                        }
                    })?)
                }
                CaCertificate::Pem(pem) => std::borrow::Cow::Borrowed(pem.as_slice()),
            };
            let certs = parse_pem_certificates(&source, &pem_bytes)?;
            if certs.is_empty() {
                return Err(TlsConfigError::EmptyCaBundle {
                    source_label: source,
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
/// The `source_label` fields name where the CA came from — a path, or
/// `inline PEM content (N bytes)` — never the certificate itself, which is
/// kilobytes of base64 that would swamp a single-line log record.
#[derive(Debug)]
pub enum TlsConfigError {
    ReadCa {
        source_label: String,
        source: std::io::Error,
    },
    ParseCa {
        source_label: String,
        source: native_tls::Error,
    },
    /// `BEGIN CERTIFICATE` marker with no matching `END CERTIFICATE`.
    TruncatedPem {
        source_label: String,
    },
    /// `sslrootcert` supplied but it contained zero parseable certificates.
    EmptyCaBundle {
        source_label: String,
    },
    BuildConnector(native_tls::Error),
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfigError::ReadCa {
                source_label,
                source,
            } => {
                write!(
                    f,
                    "failed to read sslrootcert from {source_label}: {source}"
                )
            }
            TlsConfigError::ParseCa {
                source_label,
                source,
            } => {
                write!(
                    f,
                    "failed to parse sslrootcert PEM from {source_label}: {source}"
                )
            }
            TlsConfigError::TruncatedPem { source_label } => write!(
                f,
                "sslrootcert from {source_label} has a BEGIN CERTIFICATE block without a matching END marker"
            ),
            TlsConfigError::EmptyCaBundle { source_label } => write!(
                f,
                "sslrootcert from {source_label} contains no parseable CA certificates"
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
            TlsConfigError::ParseCa { source, .. } | TlsConfigError::BuildConnector(source) => {
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
    source_label: &str,
    pem: &[u8],
) -> std::result::Result<Vec<native_tls::Certificate>, TlsConfigError> {
    let mut certs = Vec::new();
    let mut remaining = pem;
    while let Some(begin) = find_subslice(remaining, b"-----BEGIN CERTIFICATE-----") {
        let tail = &remaining[begin..];
        let Some(end_rel) = find_subslice(tail, b"-----END CERTIFICATE-----") else {
            return Err(TlsConfigError::TruncatedPem {
                source_label: source_label.to_string(),
            });
        };
        let end = begin + end_rel + b"-----END CERTIFICATE-----".len();
        let block = &remaining[begin..end];
        let cert =
            native_tls::Certificate::from_pem(block).map_err(|source| TlsConfigError::ParseCa {
                source_label: source_label.to_string(),
                source,
            })?;
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

    /// Self-signed CA (`CN=Spice Replication Test CA`, `CA:TRUE`), expiring in
    /// 2126. Real DER so `native_tls` actually accepts it as a trust anchor —
    /// a placeholder string would make these tests pass for the wrong reason.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIC4DCCAcigAwIBAgIJAODHR+uzOPBvMA0GCSqGSIb3DQEBCwUAMCQxIjAgBgNV
BAMMGVNwaWNlIFJlcGxpY2F0aW9uIFRlc3QgQ0EwIBcNMjYwNzI4MDU0MDQ0WhgP
MjEyNjA3MDQwNTQwNDRaMCQxIjAgBgNVBAMMGVNwaWNlIFJlcGxpY2F0aW9uIFRl
c3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCzoou00DrTAevF
RZ6+PFmSBUhzZXsABQFztlPigZzJ1m8hnja66hnkWKyIid9DcitnjkWgtQZCVxm6
s05tM6QAy5lI2wlfWD7hQi+yIWKv2dcVuD/J4hWPjmG5a5VtRAInV0yBymkCRI6Z
68JYfvKh+Rku1y6H3dUfNm8dxCbo589L1U8ucJqlQv9Iy/X7Lze+pj2JFU/L1g3t
k/5ziVgJjdh3VetrHkU1YOiHRPFsqXOxXc2lpzUjd23QR3FfkZkVgLUfEvPWHRSf
xipaPFhllw9WUWEl6bVqAGO0btPO1OKKqBlIcizf2YO2+lFs/o0e7bApGzI3l5HP
VZr/e6ZLAgMBAAGjEzARMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQAD
ggEBACC1XMNpbA+172MQks9R7cqRY5I0HObJRX3dpIsOqrm3EUcHMt9kx7QrO1Af
gzAWC0ZNHppeU/cuq9ZKZQiFrSmr5fKtXzsxkvgLYRCFO+ZCKZl9k3z9j0AQbTPR
klJa4bo2SS6WbmoATimD6e0moT++neRIDx7MlijtWB8grfhuH7yFN9xoTRDgdYBU
KLeFNAIi+S5cVzUwjMiOQnmljphKSRoQnihpA/c6WAVAN3VqMdoPpfmR2pTi7rio
38busw0nt/y+JCVWzNDr/i5f3mvNi5SaHZ5PTOVnocyMUw+ysx5eQOrJwrirW9XD
TXTE85+Or9IUwDI9543jsyCvuQ8=
-----END CERTIFICATE-----
";

    /// `verify-full` params — the strictest mode, and the one that actually
    /// consults `sslrootcert`. A weaker mode would build a connector even with
    /// the CA ignored, which is exactly the vacuity this fix is about.
    fn verify_full_params(sslrootcert: Option<CaCertificate>) -> ReplicationParams {
        ReplicationParams {
            host: "localhost".to_string(),
            port: 5432,
            user: "u".to_string(),
            password: SecretString::from(String::new()),
            database: "db".to_string(),
            sslmode: SslMode::VerifyFull,
            sslrootcert,
            slot_name: "slot".to_string(),
            publication_name: "pub".to_string(),
            initial_snapshot: true,
            snapshot_on_resume: false,
            status_interval: Duration::from_secs(5),
            ready_lag: Duration::from_secs(2),
            bootstrap_batch_size: 1024,
            shared: false,
            member_channel_capacity: 16,
            pg_output_format: PgOutputFormat::Binary,
        }
    }

    #[test]
    fn a_value_carrying_pem_armor_is_inline_content_everything_else_is_a_path() {
        assert_eq!(
            ca_certificate_from_param(TEST_CA_PEM),
            CaCertificate::Pem(TEST_CA_PEM.as_bytes().to_vec())
        );

        // Every spelling that works as a path today must stay a path.
        for path in [
            "/etc/ssl/pg-ca.pem",
            "ca.pem",
            "./certs/ca.crt",
            "/var/run/secrets/ca-bundle",
            "C:\\certs\\ca.pem",
        ] {
            assert_eq!(
                ca_certificate_from_param(path),
                CaCertificate::Path(path.into()),
                "{path} must be treated as a filesystem path"
            );
        }
    }

    #[test]
    fn escaped_newlines_in_inline_pem_are_restored() {
        let single_line = TEST_CA_PEM.replace('\n', "\\n");
        assert!(!single_line.contains('\n'));

        let CaCertificate::Pem(pem) = ca_certificate_from_param(&single_line) else {
            panic!("armored content must be detected as inline PEM");
        };
        assert_eq!(pem, TEST_CA_PEM.as_bytes());
    }

    #[tokio::test]
    async fn native_tls_connector_trusts_an_inline_pem_ca() {
        let params = verify_full_params(Some(ca_certificate_from_param(TEST_CA_PEM)));

        let connector = params
            .native_tls_connector()
            .await
            .expect("inline PEM CA must be accepted");
        assert!(
            connector.is_some(),
            "verify-full must produce a TLS connector"
        );
    }

    #[tokio::test]
    async fn native_tls_connector_trusts_a_ca_read_from_a_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ca.pem");
        tokio::fs::write(&path, TEST_CA_PEM)
            .await
            .expect("write CA fixture");

        let param = path.to_str().expect("utf-8 path");
        let params = verify_full_params(Some(ca_certificate_from_param(param)));
        assert!(matches!(params.sslrootcert, Some(CaCertificate::Path(_))));

        let connector = params
            .native_tls_connector()
            .await
            .expect("CA path must keep working");
        assert!(
            connector.is_some(),
            "verify-full must produce a TLS connector"
        );
    }

    /// `MakeTlsConnector` is not `Debug`, so `expect_err` is unavailable.
    async fn tls_error(ca: CaCertificate, must_fail: &str) -> TlsConfigError {
        match verify_full_params(Some(ca)).native_tls_connector().await {
            Ok(_) => panic!("{must_fail}"),
            Err(e) => e,
        }
    }

    #[tokio::test]
    async fn a_ca_that_is_neither_readable_nor_pem_is_a_hard_error() {
        // Not armored, so it is a path — and it does not exist. Verification
        // must fail rather than fall back to no trust anchor.
        let missing = tls_error(
            ca_certificate_from_param("/nonexistent/spice-test-ca.pem"),
            "an unreadable CA path must not be ignored",
        )
        .await;
        assert!(matches!(missing, TlsConfigError::ReadCa { .. }));
        assert!(
            missing
                .to_string()
                .contains("/nonexistent/spice-test-ca.pem")
        );

        // Armored but not a certificate: detected as inline PEM, then rejected.
        let garbage = tls_error(
            CaCertificate::Pem(
                b"-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n".to_vec(),
            ),
            "unparseable inline PEM must not be ignored",
        )
        .await;
        assert!(matches!(garbage, TlsConfigError::ParseCa { .. }));

        // Truncated armor is caught rather than silently yielding zero anchors.
        let truncated = tls_error(
            CaCertificate::Pem(b"-----BEGIN CERTIFICATE-----\nMIIC\n".to_vec()),
            "truncated inline PEM must not be ignored",
        )
        .await;
        assert!(matches!(truncated, TlsConfigError::TruncatedPem { .. }));
    }

    #[test]
    fn errors_and_debug_output_never_echo_inline_certificate_content() {
        let ca = ca_certificate_from_param(TEST_CA_PEM);
        let body = "MIIC4DCCAcigAwIBAgIJAODHR";

        assert!(!ca.describe().contains(body));
        assert!(!format!("{ca:?}").contains(body));
        assert!(!format!("{:?}", verify_full_params(Some(ca.clone()))).contains(body));

        let label = ca.describe();
        for err in [
            TlsConfigError::TruncatedPem {
                source_label: label.clone(),
            },
            TlsConfigError::EmptyCaBundle {
                source_label: label,
            },
        ] {
            let rendered = err.to_string();
            assert!(!rendered.contains(body), "error leaked PEM content");
            assert!(rendered.contains("inline PEM content"));
            assert!(!rendered.contains('\n'), "error must stay single-line");
        }
    }

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
    fn slot_name_is_stable_across_restarts_of_the_same_instance() {
        // A restart of the same spiced instance (same catalog name, same
        // instance id) must resolve to the identical slot name, so CDC resumes
        // the existing replication slot instead of orphaning it and forcing a
        // fresh snapshot.
        let before_restart = slot_name_for("my_catalog", "instance-a");
        let after_restart = slot_name_for("my_catalog", "instance-a");
        assert_eq!(before_restart, after_restart);
    }

    #[test]
    fn slot_name_is_unique_per_instance_for_the_same_catalog() {
        // Two spiced instances pointed at the SAME catalog on the same database
        // must not collide on one physical replication slot -- Postgres permits
        // a single consumer per slot, so a collision would have one instance
        // steal the other's stream. Distinct instance ids => distinct slots.
        let instance_a = slot_name_for("my_catalog", "instance-a");
        let instance_b = slot_name_for("my_catalog", "instance-b");
        assert_ne!(instance_a, instance_b);
    }

    #[test]
    fn slot_name_is_unique_per_catalog_for_the_same_instance() {
        // One spiced instance accelerating two different catalogs must give each
        // its own slot.
        let catalog_one = slot_name_for("catalog_one", "instance-a");
        let catalog_two = slot_name_for("catalog_two", "instance-a");
        assert_ne!(catalog_one, catalog_two);
    }

    #[test]
    fn slot_name_stays_within_postgres_limit_for_any_instance_id() {
        // The instance id is hashed to a fixed 8 chars, so even a pathologically
        // long id can't push the slot name past Postgres' identifier limit.
        let long_instance = "i".repeat(300);
        let slot = slot_name_for("catalog", &long_instance);
        assert!(
            slot.len() <= PG_IDENTIFIER_MAX_BYTES,
            "slot `{slot}` exceeds {PG_IDENTIFIER_MAX_BYTES} bytes: {}",
            slot.len()
        );
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
    fn publication_name_for_slot_is_slot_derived() {
        assert_eq!(
            publication_name_for_slot("spice_spicehq_dev"),
            "spice_spicehq_dev_pub"
        );
        // Sanitized so odd slot names still produce a valid identifier.
        assert_eq!(publication_name_for_slot("my-slot"), "my_slot_pub");
        // 63-byte cap survives long slot names.
        let long = "p".repeat(120);
        assert!(publication_name_for_slot(&long).len() <= PG_IDENTIFIER_MAX_BYTES);
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

    #[test]
    fn catalog_slot_name_is_deterministic_and_instance_independent() {
        // A catalog slot name is a pure function of the catalog name -- it reads
        // no instance id / hostname / env at all -- so it is identical on every
        // call, which is exactly what lets a restart (or a reschedule onto a
        // different node) recompute the same name and reuse the existing slot.
        let a = catalog_slot_name("my_pg");
        let b = catalog_slot_name("my_pg");
        assert_eq!(a, b);
        assert!(a.starts_with("spice_catalog_my_pg_"), "got {a}");
    }

    #[test]
    fn catalog_slot_name_omits_the_instance_suffix() {
        // The whole point of PR-3: unlike `default_slot_name` (which folds in an
        // 8-hex instance hash so two instances get different slots), the catalog
        // slot name carries NO instance component, so it does not end in the
        // instance hash `default_slot_name` appends.
        let catalog = catalog_slot_name("orders");
        let dataset = default_slot_name("orders");
        // Different from the per-dataset slot, and using the dedicated
        // `spice_catalog_` prefix rather than the per-dataset `spice_` format.
        // (`spice_catalog_` does start with `spice_`, so a `!starts_with(SLOT_PREFIX)`
        // check would be wrong -- the distinguishing property is the catalog prefix.)
        assert_ne!(catalog, dataset);
        assert!(catalog.starts_with(CATALOG_SLOT_PREFIX), "got {catalog}");
        // The per-dataset slot ends in the 8-hex instance hash; the catalog slot
        // ends in a 6-hex catalog-name hash and has no instance component.
        let catalog_suffix = catalog.rsplit_once('_').expect("has a suffix").1;
        assert_eq!(catalog_suffix.len(), DATASET_HASH_LEN, "got {catalog}");
    }

    #[test]
    fn catalog_slot_name_is_unique_per_catalog() {
        assert_ne!(catalog_slot_name("one"), catalog_slot_name("two"));
        // Truncation-collision guard: two long names sharing a truncated prefix
        // still differ via the full-name hash.
        let shared = "a".repeat(60);
        assert_ne!(
            catalog_slot_name(&format!("{shared}_alpha")),
            catalog_slot_name(&format!("{shared}_beta"))
        );
    }

    #[test]
    fn catalog_slot_name_sanitizes_and_stays_within_postgres_limit() {
        // Special characters are sanitized to `_`, and even a pathologically long
        // catalog name stays within Postgres' 63-byte identifier limit.
        let sanitized = catalog_slot_name("my-catalog.name");
        assert!(
            !sanitized.contains('-') && !sanitized.contains('.'),
            "{sanitized}"
        );

        let long = catalog_slot_name(&"c".repeat(300));
        assert!(
            long.len() <= PG_IDENTIFIER_MAX_BYTES,
            "catalog slot `{long}` exceeds {PG_IDENTIFIER_MAX_BYTES} bytes: {}",
            long.len()
        );
        // The publication derived from it must also fit.
        assert!(publication_name_for_slot(&long).len() <= PG_IDENTIFIER_MAX_BYTES);
    }

    #[test]
    fn validate_replication_slot_name_accepts_valid_names() {
        for name in [
            "a",
            "spice_users",
            "slot_1",
            "9leading_digit_ok",
            "a_b_c_012",
            &"x".repeat(PG_IDENTIFIER_MAX_BYTES),
        ] {
            assert!(
                validate_replication_slot_name(name).is_ok(),
                "expected {name:?} to be valid"
            );
        }
    }

    // Mirrors ReplicationSlotValidateNameInternal in PostgreSQL slot.c
    // (empty / too long / invalid char / reserved name).
    #[test]
    fn validate_replication_slot_name_rejects_postgres_invalid() {
        assert!(
            validate_replication_slot_name("")
                .expect_err("empty")
                .contains("must be 1 to")
        );
        let too_long = "a".repeat(PG_IDENTIFIER_MAX_BYTES + 1);
        assert!(
            validate_replication_slot_name(&too_long)
                .expect_err("too long")
                .contains("must be at most")
        );
        let hyphen_err =
            validate_replication_slot_name("scp-onboarding-realtime-analytics-prod-us-east-1")
                .expect_err("hyphen");
        assert!(
            hyphen_err.contains("invalid character '-'"),
            "unexpected: {hyphen_err}"
        );
        for (name, needle) in [
            ("MySlot", "invalid character 'M'"),
            ("slot.name", "invalid character '.'"),
            ("slot/name", "invalid character '/'"),
            ("slot name", "invalid character ' '"),
            (CONFLICT_DETECTION_SLOT, "reserved name"),
        ] {
            let err = validate_replication_slot_name(name).expect_err(name);
            assert!(
                err.contains(needle),
                "for {name:?}: expected {needle:?} in {err}"
            );
        }
    }

    #[test]
    fn default_slot_names_pass_postgres_validation() {
        for dataset in ["users", "public.orders", "my-dataset", "9leading", ""] {
            let slot = default_slot_name(dataset);
            validate_replication_slot_name(&slot)
                .unwrap_or_else(|e| panic!("default slot `{slot}` for {dataset:?} invalid: {e}"));
        }
        let long = "a".repeat(120);
        let slot = default_slot_name(&long);
        validate_replication_slot_name(&slot)
            .unwrap_or_else(|e| panic!("truncated default slot `{slot}` invalid: {e}"));
    }
}
