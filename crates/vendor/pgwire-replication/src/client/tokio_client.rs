use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;

use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use std::sync::Arc;

#[cfg(not(feature = "tls-rustls"))]
use crate::config::SslMode;

use super::worker::{ReplicationEvent, ReplicationEventReceiver, SharedProgress, WorkerState};

/// PostgreSQL logical replication client.
///
/// This client spawns a background worker task that maintains the replication
/// connection and streams events to the consumer via a bounded channel.
///
/// # Example
///
/// ```no_run
/// use pgwire_replication::client::{ReplicationClient, ReplicationEvent};
/// use pgwire_replication::config::ReplicationConfig;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ReplicationConfig::new(
///         "localhost",
///         "postgres",
///         "password",
///         "mydb",
///         "my_slot",
///         "my_pub",
///     );
///
///     let mut client = ReplicationClient::connect(config).await?;
///
///     while let Some(ev) = client.recv().await? {
///         match ev {
///             ReplicationEvent::XLogData { data, wal_end, .. } => {
///                 process_change(&data);
///                 client.update_applied_lsn(wal_end);
///             }
///             ReplicationEvent::KeepAlive { .. } => {}
///             ReplicationEvent::StoppedAt { reached } => {
///                 println!("Reached stop LSN: {reached}");
///                 break;
///             }
///             _ => {}
///         }
///     }
///
///     Ok(())
/// }
///
/// fn process_change(_data: &bytes::Bytes) {
///     // user-defined
/// }
/// ```
pub struct ReplicationClient {
    rx: ReplicationEventReceiver,
    progress: Arc<SharedProgress>,
    stop_tx: watch::Sender<bool>,
    join: Option<JoinHandle<std::result::Result<(), PgWireError>>>,
}

impl ReplicationClient {
    /// Connect to PostgreSQL and start streaming replication events.
    ///
    /// This establishes a TCP connection (optionally upgrading to TLS),
    /// authenticates, and starts the replication stream. Events are buffered
    /// in a channel of size `config.buffer_events`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TCP connection fails
    /// - TLS handshake fails (when enabled)
    /// - Authentication fails
    /// - Replication slot doesn't exist
    /// - Publication doesn't exist
    /// - Unix socket does not exist (when host starts with `/`)
    /// - TLS requested with Unix socket connection
    pub async fn connect(cfg: ReplicationConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(cfg.buffer_events);

        // Progress is shared via atomics: cheap, monotonic, no async backpressure.
        let progress = Arc::new(SharedProgress::new(cfg.start_lsn));

        let (stop_tx, stop_rx) = watch::channel(false);

        let progress_for_worker = Arc::clone(&progress);
        let cfg_for_worker = cfg.clone();

        let join = tokio::spawn(async move {
            let mut worker = WorkerState::new(cfg_for_worker, progress_for_worker, stop_rx, tx);
            let res = run_worker(&mut worker, &cfg).await;
            if let Err(ref e) = res {
                tracing::error!("replication worker terminated with error: {e}");
            }
            res
        });

        Ok(Self {
            rx,
            progress,
            stop_tx,
            join: Some(join),
        })
    }

    /// Receive the next replication event.
    ///
    /// - `Ok(Some(event))` => received an event
    /// - `Ok(None)`        => replication ended normally (stop requested or stop_at_lsn reached)
    /// - `Err(e)`          => replication ended abnormally
    pub async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(e)) => Err(e),
            None => self.handle_worker_shutdown().await,
        }
    }

    async fn handle_worker_shutdown(&mut self) -> Result<Option<ReplicationEvent>> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Internal("replication worker already joined".into()))?;

        match join.await {
            Ok(Ok(())) => Ok(None),
            Ok(Err(e)) => Err(e),
            Err(join_err) => Err(PgWireError::Task(format!(
                "replication worker panicked: {join_err}"
            ))),
        }
    }

    /// Update the applied/durable LSN reported to the server.
    ///
    /// Semantics: call this only once you have durably persisted all events up to `lsn`.
    /// This update is monotonic and cheap; wire feedback is still governed by the worker’s
    /// `status_interval` and keepalive reply requests.
    #[inline]
    pub fn update_applied_lsn(&self, lsn: Lsn) {
        self.progress.update_applied(lsn);
    }

    /// Request the worker to stop gracefully.
    ///
    /// After calling this, [`recv()`](Self::recv) will return remaining buffered
    /// events, then `Ok(None)` once the worker exits cleanly.
    ///
    /// This sends a CopyDone message to the server to cleanly terminate
    /// the replication stream.
    #[inline]
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    pub fn is_running(&self) -> bool {
        self.join
            .as_ref()
            .map(|j| !j.is_finished())
            .unwrap_or(false)
    }

    /// Wait for the worker task to complete and return its result.
    ///
    /// This consumes the client. Use this for diagnostics or to ensure
    /// clean shutdown after calling [`stop()`](Self::stop).
    pub async fn join(mut self) -> Result<()> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Task("worker already joined".into()))?;

        match join.await {
            Ok(inner) => inner,
            Err(e) => Err(PgWireError::Task(format!("join error: {e}"))),
        }
    }

    /// Abort the worker task immediately.
    ///
    /// This is a hard cancel and does not send CopyDone.
    /// Prefer `stop()`/`shutdown()` for graceful termination.
    pub fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }

    /// Request a graceful stop and wait for the worker to exit.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.stop();

        // Drain events until the worker closes the channel.
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Ok(_ev) => {} //discard; caller can drain themselves if they need events
                Err(e) => return Err(e),
            }
        }

        self.join_mut().await
    }

    /// Wait for the worker task to complete and return its result.
    async fn join_mut(&mut self) -> Result<()> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Task("worker already joined".into()))?;

        match join.await {
            Ok(inner) => inner,
            Err(e) => Err(PgWireError::Task(format!("join error: {e}"))),
        }
    }
}

impl Drop for ReplicationClient {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);

        // We cannot .await here. Prefer to detach a join in the background
        // so the worker can exit cleanly without being aborted.
        if let Some(join) = self.join.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        let _ = join.await;
                    });
                }
                Err(_) => {
                    // No Tokio runtime available (dropping outside async context).
                    // Fall back to abort to avoid a potentially unbounded leaked task.
                    tracing::debug!(
                        "dropping ReplicationClient outside a Tokio runtime; aborting worker task"
                    );
                    join.abort();
                }
            }
        }
    }
}

async fn run_worker(worker: &mut WorkerState, cfg: &ReplicationConfig) -> Result<()> {
    #[cfg(unix)]
    if cfg.is_unix_socket() {
        if cfg.tls.mode.requires_tls() {
            return Err(PgWireError::Tls(
                "TLS is not supported over Unix domain sockets".into(),
            ));
        }

        let path = cfg.unix_socket_path();
        let mut stream = UnixStream::connect(&path).await.map_err(|e| {
            PgWireError::Io(std::sync::Arc::new(std::io::Error::new(
                e.kind(),
                format!("failed to connect to Unix socket {}: {e}", path.display()),
            )))
        })?;

        return worker.run_on_stream(&mut stream).await;
    }

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
    tcp.set_nodelay(true)?;

    #[cfg(feature = "tls-rustls")]
    {
        use crate::tls::rustls::{maybe_upgrade_to_tls, MaybeTlsStream};
        let upgraded = maybe_upgrade_to_tls(tcp, &cfg.tls, &cfg.host).await?;
        match upgraded {
            MaybeTlsStream::Plain(mut s) => worker.run_on_stream(&mut s).await,
            MaybeTlsStream::Tls(mut s) => worker.run_on_stream(s.as_mut()).await,
        }
    }

    #[cfg(not(feature = "tls-rustls"))]
    {
        if !matches!(cfg.tls.mode, SslMode::Disable) {
            return Err(PgWireError::Tls("tls-rustls feature not enabled".into()));
        }
        let mut s = tcp;
        worker.run_on_stream(&mut s).await
    }
}
