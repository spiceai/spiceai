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

/// `PostgreSQL` logical replication client.
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

/// Outcome of a non-blocking [`ReplicationClient::try_recv`].
#[derive(Debug)]
pub enum TryRecvEvent {
    /// An event was immediately available in the worker's buffer.
    Event(ReplicationEvent),
    /// Nothing is buffered right now. The caller should fall back to the
    /// awaiting [`ReplicationClient::recv`] to block until the next event.
    Empty,
    /// The worker has exited and its channel is drained. The caller should
    /// call [`ReplicationClient::recv`] to observe the terminal result
    /// (`Ok(None)` on a clean stop, `Err(_)` on failure).
    Closed,
}

impl ReplicationClient {
    /// Connect to `PostgreSQL` and start streaming replication events.
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
    #[expect(
        clippy::unused_async,
        reason = "public async constructor: callers await it, and it spawns the worker task"
    )]
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
    /// - `Ok(None)`        => replication ended normally (stop requested or `stop_at_lsn` reached)
    /// - `Err(e)`          => replication ended abnormally
    ///
    /// # Errors
    /// Returns the worker's terminating error if the replication stream failed
    /// (I/O, protocol, auth, or a worker panic surfaced as [`PgWireError::Task`]).
    pub async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(e)) => Err(e),
            None => self.handle_worker_shutdown().await,
        }
    }

    /// Receive the next replication event without awaiting.
    ///
    /// Unlike [`recv`](Self::recv), this never yields to the runtime — it
    /// returns immediately with whatever the worker has already buffered.
    /// Consumers drain with `try_recv` in a tight loop and fall back to the
    /// awaiting `recv` only when it reports [`TryRecvEvent::Empty`], avoiding a
    /// per-message timer on the hot path.
    ///
    /// - `Ok(TryRecvEvent::Event(ev))` => an event was buffered
    /// - `Ok(TryRecvEvent::Empty)`     => nothing buffered right now
    /// - `Ok(TryRecvEvent::Closed)`    => worker exited; call
    ///   [`recv`](Self::recv) for the terminal result
    /// - `Err(e)`                      => the worker reported an error event
    ///
    /// Like `recv`, this only reads an in-process channel and so is cancel-safe
    /// (there is nothing to await). It does not itself surface the worker's
    /// terminal `Result`: on a drained/closed channel it returns `Closed` and
    /// leaves the join handle intact for [`recv`](Self::recv) to reap.
    ///
    /// # Errors
    ///
    /// Returns the [`PgWireError`] the worker emitted if an error event is next
    /// in the buffer (the same errors [`recv`](Self::recv) surfaces).
    #[inline]
    pub fn try_recv(&mut self) -> Result<TryRecvEvent> {
        match self.rx.try_recv() {
            Ok(Ok(ev)) => Ok(TryRecvEvent::Event(ev)),
            Ok(Err(e)) => Err(e),
            Err(mpsc::error::TryRecvError::Empty) => Ok(TryRecvEvent::Empty),
            Err(mpsc::error::TryRecvError::Disconnected) => Ok(TryRecvEvent::Closed),
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
    /// This sends a `CopyDone` message to the server to cleanly terminate
    /// the replication stream.
    #[inline]
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.join.as_ref().is_some_and(|j| !j.is_finished())
    }

    /// Wait for the worker task to complete and return its result.
    ///
    /// This consumes the client. Use this for diagnostics or to ensure
    /// clean shutdown after calling [`stop()`](Self::stop).
    ///
    /// # Errors
    /// Returns the worker's terminating error, or [`PgWireError::Task`] if the
    /// worker was already joined or panicked.
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
    /// This is a hard cancel and does not send `CopyDone`.
    /// Prefer `stop()`/`shutdown()` for graceful termination.
    pub fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }

    /// Request a graceful stop and wait for the worker to exit.
    ///
    /// # Errors
    /// Returns the worker's terminating error if it ended abnormally, or
    /// [`PgWireError::Task`] if the worker was already joined or panicked.
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
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = join.await;
                });
            } else {
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

#[cfg(test)]
mod tests {
    use super::*;

    impl ReplicationClient {
        /// Build a client around a pre-populated channel and an inert worker
        /// join handle, for exercising the `rx`-backed methods (`try_recv`,
        /// `recv`) without a live Postgres connection.
        fn for_test(
            rx: ReplicationEventReceiver,
            join: JoinHandle<std::result::Result<(), PgWireError>>,
        ) -> Self {
            let (stop_tx, _stop_rx) = watch::channel(false);
            Self {
                rx,
                progress: Arc::new(SharedProgress::new(Lsn(0))),
                stop_tx,
                join: Some(join),
            }
        }
    }

    #[tokio::test]
    async fn try_recv_reports_event_empty_and_closed() {
        let (tx, rx) = mpsc::channel(4);
        let join = tokio::spawn(async { Ok(()) });
        let mut client = ReplicationClient::for_test(rx, join);

        // Nothing buffered yet.
        assert!(matches!(client.try_recv(), Ok(TryRecvEvent::Empty)));

        // A buffered event is drained without awaiting.
        tx.send(Ok(ReplicationEvent::KeepAlive {
            wal_end: Lsn(7),
            reply_requested: false,
            server_time_micros: 0,
        }))
        .await
        .expect("send event");
        match client.try_recv() {
            Ok(TryRecvEvent::Event(ReplicationEvent::KeepAlive { wal_end, .. })) => {
                assert_eq!(wal_end, Lsn(7));
            }
            other => panic!("expected a KeepAlive event, got {other:?}"),
        }

        // Drained again.
        assert!(matches!(client.try_recv(), Ok(TryRecvEvent::Empty)));

        // A worker error event surfaces as `Err`.
        tx.send(Err(PgWireError::Internal("boom".into())))
            .await
            .expect("send err");
        client
            .try_recv()
            .expect_err("a worker error event must surface as Err");

        // Dropping the sender closes the channel → `Closed`.
        drop(tx);
        assert!(matches!(client.try_recv(), Ok(TryRecvEvent::Closed)));
    }
}
