//! Configuration types for PostgreSQL replication connections.
//!
//! This module provides configuration structures for establishing replication
//! connections to PostgreSQL, including TLS settings and replication parameters.

use std::path::PathBuf;
use std::time::Duration;

use crate::lsn::Lsn;

/// SSL/TLS connection mode.
///
/// These modes match PostgreSQL's `sslmode` connection parameter.
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

impl SslMode {
    /// Returns `true` if this mode requires TLS (won't fall back to plain).
    #[inline]
    pub fn requires_tls(&self) -> bool {
        !matches!(self, SslMode::Disable | SslMode::Prefer)
    }

    /// Returns `true` if this mode verifies the certificate chain.
    #[inline]
    pub fn verifies_certificate(&self) -> bool {
        matches!(self, SslMode::VerifyCa | SslMode::VerifyFull)
    }

    /// Returns `true` if this mode verifies the server hostname.
    #[inline]
    pub fn verifies_hostname(&self) -> bool {
        matches!(self, SslMode::VerifyFull)
    }
}

/// TLS/SSL configuration for PostgreSQL connections.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// SSL mode controlling connection security level.
    pub mode: SslMode,

    /// Path to PEM file containing trusted CA certificates.
    ///
    /// If `None` and verification is enabled (`VerifyCa`/`VerifyFull`),
    /// the Mozilla root certificates (webpki-roots) are used.
    pub ca_pem_path: Option<PathBuf>,

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
    pub fn require() -> Self {
        Self {
            mode: SslMode::Require,
            ..Default::default()
        }
    }

    /// Create a configuration with certificate chain verification.
    ///
    /// # Arguments
    /// * `ca_path` - Path to CA certificate PEM file, or `None` for system roots
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::TlsConfig;
    ///
    /// // Using system/Mozilla roots
    /// let tls = TlsConfig::verify_ca(None);
    ///
    /// // Using custom CA
    /// let tls = TlsConfig::verify_ca(Some("/path/to/ca.pem".into()));
    /// ```
    pub fn verify_ca(ca_path: Option<PathBuf>) -> Self {
        Self {
            mode: SslMode::VerifyCa,
            ca_pem_path: ca_path,
            ..Default::default()
        }
    }

    /// Create a configuration with full verification (chain + hostname).
    ///
    /// **Recommended for production**.
    ///
    /// # Arguments
    /// * `ca_path` - Path to CA certificate PEM file, or `None` for system roots
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::TlsConfig;
    ///
    /// let tls = TlsConfig::verify_full(Some("/etc/ssl/certs/ca.pem".into()));
    /// assert!(tls.mode.verifies_hostname());
    /// ```
    pub fn verify_full(ca_path: Option<PathBuf>) -> Self {
        Self {
            mode: SslMode::VerifyFull,
            ca_pem_path: ca_path,
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
    pub fn with_sni_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.sni_hostname = Some(hostname.into());
        self
    }

    /// Configure client certificate for mutual TLS.
    ///
    /// # Example
    /// ```
    /// use pgwire_replication::config::TlsConfig;
    ///
    /// let tls = TlsConfig::verify_full(Some("/ca.pem".into()))
    ///     .with_client_cert("/client.pem", "/client.key");
    /// ```
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
    pub fn is_mtls(&self) -> bool {
        self.client_cert_pem_path.is_some() && self.client_key_pem_path.is_some()
    }
}

/// Configuration for PostgreSQL logical replication connections.
///
/// # Example
///
/// ```
/// use pgwire_replication::config::{ReplicationConfig, TlsConfig, SslMode};
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
///     tls: TlsConfig::verify_full(Some("/path/to/ca.pem".into())),
///     start_lsn: Lsn(0),  // Start from slot's confirmed position
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// PostgreSQL server hostname or IP address.
    pub host: String,

    /// PostgreSQL server port (default: 5432).
    pub port: u16,

    /// PostgreSQL username with replication privileges.
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
    /// - Specific LSN: Resume from that position (must be >= slot's restart_lsn)
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
    /// Status updates inform PostgreSQL of the client's replay position,
    /// allowing the server to release WAL segments. Too infrequent updates
    /// may cause WAL accumulation; too frequent updates add overhead.
    ///
    /// Default: 1 second (matches pg_recvlogical)
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
            status_interval: Duration::from_secs(10),
            idle_wakeup_interval: Duration::from_secs(10),
            buffer_events: 8192,
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
    /// the directory containing the PostgreSQL Unix socket file.
    #[inline]
    pub fn is_unix_socket(&self) -> bool {
        self.host.starts_with('/')
    }

    /// Returns the full Unix socket path: `{host}/.s.PGSQL.{port}`.
    ///
    /// # Panics
    ///
    /// Panics if `host` does not start with `/` (i.e. `is_unix_socket()` is false).
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
    /// `socket_dir` is the directory containing the PostgreSQL socket file
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
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set TLS configuration.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Set the starting LSN.
    pub fn with_start_lsn(mut self, lsn: Lsn) -> Self {
        self.start_lsn = lsn;
        self
    }

    /// Set an optional stop LSN for bounded replay.
    pub fn with_stop_lsn(mut self, lsn: Lsn) -> Self {
        self.stop_at_lsn = Some(lsn);
        self
    }

    /// Set the status update interval.
    pub fn with_status_interval(mut self, interval: Duration) -> Self {
        self.status_interval = interval;
        self
    }

    /// Set the idle wakeup interval.
    pub fn with_wakeup_interval(mut self, timeout: Duration) -> Self {
        self.idle_wakeup_interval = timeout;
        self
    }

    /// Set the event buffer size.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_events = size;
        self
    }

    /// Returns the connection string for display (password masked).
    ///
    /// Useful for logging without exposing credentials.
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
