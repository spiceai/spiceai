//! Configuration types for `PostgreSQL` replication connections.
//!
//! This module provides configuration structures for establishing replication
//! connections to `PostgreSQL`, including TLS settings and replication parameters.

use std::path::PathBuf;
use std::time::Duration;

use crate::lsn::Lsn;

/// SSL/TLS connection mode.
///
/// These modes match `PostgreSQL`'s `sslmode` connection parameter.
/// See [PostgreSQL SSL Support](https://www.postgresql.org/docs/current/libpq-ssl.html)
/// for detailed documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Never use TLS. Connection will fail if server requires TLS.
    #[default]
    Disable,

    /// Try TLS first, fall back to unencrypted if server doesn't support it.
    ///
    /// **Warning**: Vulnerable to downgrade attacks. Not recommended for production.
    Prefer,

    /// Require TLS but don't verify the server certificate.
    ///
    /// Protects against passive eavesdropping but not active MITM attacks.
    Require,

    /// Require TLS and verify the server certificate chain against trusted CAs.
    ///
    /// Does NOT verify that the certificate hostname matches the connection target.
    VerifyCa,

    /// Require TLS, verify certificate chain, AND verify hostname matches.
    ///
    /// **Recommended for production**. Provides full protection against MITM attacks.
    VerifyFull,
}

/// Column output format requested from pgoutput via `START_REPLICATION`.
///
/// pgoutput encodes each column with a per-column tag, so `Binary` is a
/// *request*: the server still emits text for any type lacking a binary send
/// function (and always uses the binary form for the fixed Begin/Commit
/// framing). A consumer must therefore be prepared to decode either form per
/// column regardless of this setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PgOutputFormat {
    /// Request text output (`START_REPLICATION` without the `binary` option).
    /// pgoutput's historical default and the widest-compatibility choice.
    #[default]
    Text,

    /// Request binary output (`binary 'true'`). Values for types with a binary
    /// send function arrive in their `send`/`recv` wire form; the rest still
    /// arrive as text.
    Binary,
}

impl SslMode {
    /// Returns `true` if this mode requires TLS (won't fall back to plain).
    #[inline]
    #[must_use]
    pub fn requires_tls(&self) -> bool {
        !matches!(self, SslMode::Disable | SslMode::Prefer)
    }

    /// Returns `true` if this mode verifies the certificate chain.
    #[inline]
    #[must_use]
    pub fn verifies_certificate(&self) -> bool {
        matches!(self, SslMode::VerifyCa | SslMode::VerifyFull)
    }

    /// Returns `true` if this mode verifies the server hostname.
    #[inline]
    #[must_use]
    pub fn verifies_hostname(&self) -> bool {
        matches!(self, SslMode::VerifyFull)
    }
}

/// Where the trusted CA certificate bundle comes from.
///
/// Deployments that mount the CA as a file use [`CaCertificate::Path`];
/// deployments that inject it as a configuration value (an orchestrator secret,
/// an environment variable) use [`CaCertificate::Pem`] and never touch the
/// filesystem.
#[derive(Clone, PartialEq, Eq)]
pub enum CaCertificate {
    /// Filesystem path to a PEM file containing trusted CA certificates.
    Path(PathBuf),
    /// PEM-encoded CA certificate content.
    Pem(Vec<u8>),
}

impl CaCertificate {
    /// A short label naming the source, safe to put in an error or log line.
    ///
    /// Never returns the certificate content itself: a PEM blob is kilobytes of
    /// base64 that would swamp a single-line log record.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            CaCertificate::Path(path) => path.display().to_string(),
            CaCertificate::Pem(pem) => format!("inline PEM content ({} bytes)", pem.len()),
        }
    }
}

/// Renders the source, not the certificate bytes — see [`CaCertificate::describe`].
impl std::fmt::Debug for CaCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CaCertificate::Path(path) => f.debug_tuple("Path").field(path).finish(),
            CaCertificate::Pem(pem) => write!(f, "Pem(<{} bytes>)", pem.len()),
        }
    }
}

/// TLS/SSL configuration for `PostgreSQL` connections.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// SSL mode controlling connection security level.
    pub mode: SslMode,

    /// Source of the trusted CA certificates.
    ///
    /// If `None` and verification is enabled (`VerifyCa`/`VerifyFull`),
    /// the Mozilla root certificates (webpki-roots) are used.
    pub ca: Option<CaCertificate>,

    /// Override SNI hostname sent during TLS handshake.
    ///
    /// Useful when:
    /// - Connecting via IP address but certificate has a DNS name
    /// - Using a load balancer with different internal/external names
    ///
    /// If `None`, the connection `host` is used for SNI.
    pub sni_hostname: Option<String>,

    /// Path to PEM file containing client certificate chain.
    ///
    /// Required for mutual TLS (mTLS) authentication.
    /// Must be paired with `client_key_pem_path`.
    pub client_cert_pem_path: Option<PathBuf>,

    /// Path to PEM file containing client private key.
    ///
    /// Required for mutual TLS (mTLS) authentication.
    /// Must be paired with `client_cert_pem_path`.
    /// Supports PKCS#8, PKCS#1 (RSA), and SEC1 (EC) formats.
    pub client_key_pem_path: Option<PathBuf>,
}

impl TlsConfig {
    /// Create a configuration with TLS disabled.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::TlsConfig;
    ///
    /// let tls = TlsConfig::disabled();
    /// assert!(!tls.mode.requires_tls());
    /// ```
    #[must_use]
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create a configuration requiring TLS without certificate verification.
    ///
    /// **Warning**: This mode is vulnerable to MITM attacks.
    /// Use `verify_ca()` or `verify_full()` for production.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::TlsConfig;
    ///
    /// let tls = TlsConfig::require();
    /// assert!(tls.mode.requires_tls());
    /// assert!(!tls.mode.verifies_certificate());
    /// ```
    #[must_use]
    pub fn require() -> Self {
        Self {
            mode: SslMode::Require,
            ..Default::default()
        }
    }

    /// Create a configuration with certificate chain verification.
    ///
    /// # Arguments
    /// * `ca` - Source of the CA certificates, or `None` for system roots
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::{CaCertificate, TlsConfig};
    ///
    /// // Using system/Mozilla roots
    /// let tls = TlsConfig::verify_ca(None);
    ///
    /// // Using a custom CA read from a file
    /// let tls = TlsConfig::verify_ca(Some(CaCertificate::Path("/path/to/ca.pem".into())));
    ///
    /// // Using a custom CA supplied as PEM content
    /// let tls = TlsConfig::verify_ca(Some(CaCertificate::Pem(ca_pem_bytes())));
    /// # fn ca_pem_bytes() -> Vec<u8> { Vec::new() }
    /// ```
    #[must_use]
    pub fn verify_ca(ca: Option<CaCertificate>) -> Self {
        Self {
            mode: SslMode::VerifyCa,
            ca,
            ..Default::default()
        }
    }

    /// Create a configuration with full verification (chain + hostname).
    ///
    /// **Recommended for production**.
    ///
    /// # Arguments
    /// * `ca` - Source of the CA certificates, or `None` for system roots
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::{CaCertificate, TlsConfig};
    ///
    /// let tls = TlsConfig::verify_full(Some(CaCertificate::Path("/etc/ssl/certs/ca.pem".into())));
    /// assert!(tls.mode.verifies_hostname());
    /// ```
    #[must_use]
    pub fn verify_full(ca: Option<CaCertificate>) -> Self {
        Self {
            mode: SslMode::VerifyFull,
            ca,
            ..Default::default()
        }
    }

    /// Set SNI hostname override.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::TlsConfig;
    ///
    /// let tls = TlsConfig::verify_full(None)
    ///     .with_sni_hostname("db.example.com");
    /// ```
    #[must_use]
    pub fn with_sni_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.sni_hostname = Some(hostname.into());
        self
    }

    /// Configure client certificate for mutual TLS.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::{CaCertificate, TlsConfig};
    ///
    /// let tls = TlsConfig::verify_full(Some(CaCertificate::Path("/ca.pem".into())))
    ///     .with_client_cert("/client.pem", "/client.key");
    /// ```
    #[must_use]
    pub fn with_client_cert(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert_pem_path = Some(cert_path.into());
        self.client_key_pem_path = Some(key_path.into());
        self
    }

    /// Returns `true` if mutual TLS (client certificate) is configured.
    #[inline]
    #[must_use]
    pub fn is_mtls(&self) -> bool {
        self.client_cert_pem_path.is_some() && self.client_key_pem_path.is_some()
    }
}

/// Configuration for `PostgreSQL` logical replication connections.
///
/// # Example
///
/// ```
/// use pgwire_replication::config::{CaCertificate, ReplicationConfig, TlsConfig, SslMode};
/// use pgwire_replication::lsn::Lsn;
/// use std::time::Duration;
///
/// let config = ReplicationConfig {
///     host: "db.example.com".into(),
///     port: 5432,
///     user: "replicator".into(),
///     password: "secret".into(),
///     database: "mydb".into(),
///     slot: "my_slot".into(),
///     publication: "my_publication".into(),
///     tls: TlsConfig::verify_full(Some(CaCertificate::Path("/path/to/ca.pem".into()))),
///     start_lsn: Lsn(0),  // Start from slot's confirmed position
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// `PostgreSQL` server hostname or IP address.
    pub host: String,

    /// `PostgreSQL` server port (default: 5432).
    pub port: u16,

    /// `PostgreSQL` username with replication privileges.
    ///
    /// The user must have the `REPLICATION` attribute or be a superuser.
    pub user: String,

    /// Password for authentication.
    pub password: String,

    /// Database name to connect to.
    pub database: String,

    /// TLS/SSL configuration.
    pub tls: TlsConfig,

    /// Name of the replication slot to use.
    ///
    /// The slot must already exist and be a logical replication slot
    /// using the `pgoutput` plugin.
    pub slot: String,

    /// Name of the publication to subscribe to.
    ///
    /// The publication must exist and include the tables you want to replicate.
    pub publication: String,

    /// LSN position to start replication from.
    ///
    /// - `Lsn(0)`: Start from slot's `confirmed_flush_lsn`
    /// - Specific LSN: Resume from that position (must be >= slot's `restart_lsn`)
    pub start_lsn: Lsn,

    /// Optional LSN to stop replication at.
    ///
    /// When set, replication will stop once a commit with `end_lsn >= stop_at_lsn`
    /// is received. Useful for:
    /// - Bounded replay (e.g., point-in-time recovery)
    /// - Testing with known data ranges
    ///
    /// If `None`, replication continues indefinitely (normal CDC mode).
    pub stop_at_lsn: Option<Lsn>,

    /// Interval for sending standby status updates to the server.
    ///
    /// Status updates inform `PostgreSQL` of the client's replay position,
    /// allowing the server to release WAL segments. Too infrequent updates
    /// may cause WAL accumulation; too frequent updates add overhead.
    ///
    /// This interval also bounds the server-side liveness window: while a
    /// consumer is backpressured (see [`Self::feedback_while_backpressured`])
    /// the worker proactively re-sends a standby status update on every
    /// `status_interval`, so this value should be comfortably below the
    /// server's `wal_sender_timeout` (default 60 s). A ratio of ~1/6 leaves
    /// margin under CPU pressure.
    ///
    /// Default: 5 seconds
    pub status_interval: Duration,

    /// Maximum time to wait for server messages before waking up.
    ///
    /// Silence is normal during logical replication. When this interval elapses
    /// with no incoming messages, the client will send a standby status update
    /// (feedback) and continue waiting.
    ///
    /// This effectively bounds how long the worker can stay blocked in a read
    /// while idle.
    ///
    /// Default: 10 seconds
    pub idle_wakeup_interval: Duration,

    /// Size of the bounded event buffer between replication worker and consumer.
    ///
    /// Larger buffers can smooth out processing latency spikes but use more memory.
    /// Each event is typically 100-1000 bytes depending on row size.
    ///
    /// Default: 8192 events
    pub buffer_events: usize,

    /// Keep sending standby status feedback while the consumer is backpressured.
    ///
    /// When `true` (default), a full event channel no longer parks the worker:
    /// it keeps emitting standby status updates on [`Self::status_interval`]
    /// while it waits for the consumer to drain, so `PostgreSQL` never sees a
    /// feedback gap longer than `status_interval` and will not terminate the
    /// walsender on `wal_sender_timeout`.
    ///
    /// When `false`, the worker reverts to hard backpressure: a full channel
    /// blocks the worker (including feedback) until the consumer drains. Use
    /// only if a consumer specifically relies on that coupling.
    pub feedback_while_backpressured: bool,

    /// Maximum accepted size (bytes) of a single backend message payload during
    /// streaming. A frame whose declared length exceeds this is rejected as a
    /// protocol error before any buffer is allocated for it, bounding the memory
    /// a malformed or malicious length field can request.
    ///
    /// The default is `PostgreSQL`'s ~1 GiB field-size ceiling, so legitimate
    /// large-row (TOAST) changes are never rejected. Lower it only if you know
    /// the replicated relations have no large values — the reader already grows
    /// its buffer incrementally from bytes actually received, so a high cap does
    /// not by itself cause a large allocation.
    ///
    /// Default: 1 GiB
    pub max_message_size: usize,

    /// pgoutput logical decoding protocol version requested in
    /// `START_REPLICATION` (`proto_version '<n>'`).
    ///
    /// `1` is the baseline every supported server speaks. Higher versions add
    /// features (2: in-progress transaction streaming; 3: two-phase commit;
    /// 4: parallel-apply streaming) whose extra message types the consumer
    /// must be prepared to handle — leave at `1` unless the consumer decodes
    /// them.
    ///
    /// Default: 1
    pub proto_version: u8,

    /// Column output format requested from pgoutput. See [`PgOutputFormat`].
    ///
    /// Default: [`PgOutputFormat::Text`]
    pub format: PgOutputFormat,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            database: "postgres".into(),
            tls: TlsConfig::default(),
            slot: "slot".into(),
            publication: "pub".into(),
            start_lsn: Lsn(0),
            stop_at_lsn: None,
            status_interval: Duration::from_secs(5),
            idle_wakeup_interval: Duration::from_secs(10),
            buffer_events: 8192,
            feedback_while_backpressured: true,
            max_message_size: crate::protocol::framing::MAX_MESSAGE_SIZE,
            proto_version: 1,
            format: PgOutputFormat::Text,
        }
    }
}

impl ReplicationConfig {
    /// Create a new configuration with required fields.
    ///
    /// Other fields use defaults and can be customized with builder methods.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::ReplicationConfig;
    ///
    /// let config = ReplicationConfig::new(
    ///     "db.example.com",
    ///     "replicator",
    ///     "secret",
    ///     "mydb",
    ///     "my_slot",
    ///     "my_pub",
    /// );
    /// ```
    pub fn new(
        host: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
        slot: impl Into<String>,
        publication: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            user: user.into(),
            password: password.into(),
            database: database.into(),
            slot: slot.into(),
            publication: publication.into(),
            ..Default::default()
        }
    }

    /// Returns `true` if `host` refers to a Unix domain socket directory.
    ///
    /// Following libpq convention, a host starting with `/` is treated as
    /// the directory containing the `PostgreSQL` Unix socket file.
    #[inline]
    #[must_use]
    pub fn is_unix_socket(&self) -> bool {
        self.host.starts_with('/')
    }

    /// Returns the full Unix socket path: `{host}/.s.PGSQL.{port}`.
    ///
    /// # Panics
    ///
    /// Panics if `host` does not start with `/` (i.e. `is_unix_socket()` is false).
    #[must_use]
    pub fn unix_socket_path(&self) -> std::path::PathBuf {
        assert!(
            self.is_unix_socket(),
            "unix_socket_path() called but host is not a socket directory: {:?}",
            self.host
        );
        std::path::Path::new(&self.host).join(format!(".s.PGSQL.{}", self.port))
    }

    /// Create a configuration for connecting via Unix domain socket.
    ///
    /// `socket_dir` is the directory containing the `PostgreSQL` socket file
    /// (e.g. `/var/run/postgresql`). The actual socket path will be
    /// `{socket_dir}/.s.PGSQL.{port}`.
    ///
    /// TLS is automatically disabled for Unix socket connections.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::ReplicationConfig;
    ///
    /// let config = ReplicationConfig::unix(
    ///     "/var/run/postgresql",
    ///     5432,
    ///     "replicator",
    ///     "secret",
    ///     "mydb",
    ///     "my_slot",
    ///     "my_pub",
    /// );
    /// assert!(config.is_unix_socket());
    /// ```
    pub fn unix(
        socket_dir: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
        slot: impl Into<String>,
        publication: impl Into<String>,
    ) -> Self {
        Self {
            host: socket_dir.into(),
            port,
            user: user.into(),
            password: password.into(),
            database: database.into(),
            tls: TlsConfig::disabled(),
            slot: slot.into(),
            publication: publication.into(),
            ..Default::default()
        }
    }

    /// Set the server port.
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set TLS configuration.
    #[must_use]
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Set the starting LSN.
    #[must_use]
    pub fn with_start_lsn(mut self, lsn: Lsn) -> Self {
        self.start_lsn = lsn;
        self
    }

    /// Set an optional stop LSN for bounded replay.
    #[must_use]
    pub fn with_stop_lsn(mut self, lsn: Lsn) -> Self {
        self.stop_at_lsn = Some(lsn);
        self
    }

    /// Set the status update interval.
    #[must_use]
    pub fn with_status_interval(mut self, interval: Duration) -> Self {
        self.status_interval = interval;
        self
    }

    /// Set the idle wakeup interval.
    #[must_use]
    pub fn with_wakeup_interval(mut self, timeout: Duration) -> Self {
        self.idle_wakeup_interval = timeout;
        self
    }

    /// Set the event buffer size.
    #[must_use]
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_events = size;
        self
    }

    /// Set whether standby status feedback keeps flowing while the consumer is
    /// backpressured. See [`Self::feedback_while_backpressured`].
    #[must_use]
    pub fn with_feedback_while_backpressured(mut self, enabled: bool) -> Self {
        self.feedback_while_backpressured = enabled;
        self
    }

    /// Set the maximum accepted backend-message payload size (bytes).
    ///
    /// See [`max_message_size`](Self::max_message_size). Defaults to ~1 GiB.
    #[must_use]
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Set the pgoutput protocol version. See [`proto_version`](Self::proto_version).
    #[must_use]
    pub fn with_proto_version(mut self, version: u8) -> Self {
        self.proto_version = version;
        self
    }

    /// Set the requested pgoutput column output format. See [`PgOutputFormat`].
    #[must_use]
    pub fn with_format(mut self, format: PgOutputFormat) -> Self {
        self.format = format;
        self
    }

    /// Returns the connection string for display (password masked).
    ///
    /// Useful for logging without exposing credentials.
    #[must_use]
    pub fn display_connection(&self) -> String {
        if self.is_unix_socket() {
            format!(
                "postgresql://{}:***@[{}]:{}/{}",
                self.user,
                self.unix_socket_path().display(),
                self.port,
                self.database
            )
        } else {
            format!(
                "postgresql://{}:***@{}:{}/{}",
                self.user, self.host, self.port, self.database
            )
        }
    }
}
