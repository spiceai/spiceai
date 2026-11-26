/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A TCP proxy that holds traffic during a partition and hides upstream closes
//! from the client. It holds up to [`HOLD_LIMIT`] per direction before it stops
//! reading, and does not model TCP retransmission or backpressure. It can also
//! stall every open connection permanently and cap the upstream byte rate.

use std::future::pending;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::script::ProxyFault;

/// Bytes held per direction during a partition before the proxy stops reading.
const HOLD_LIMIT: usize = 1 << 20;
const BUFFER_CAPACITY: usize = 64 << 10;
/// The longest slice of upstream bytes a rate-capped connection relays at once.
const THROTTLE_SLICE: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Default)]
struct Faults {
    partitioned: bool,
    /// Connections with a lower id are stalled permanently.
    stalled_below: u64,
    upstream_bytes_per_second: Option<NonZeroU64>,
}

/// A loopback listener that can black-hole the path to `upstream`.
pub struct PartitionProxy {
    address: SocketAddr,
    faults: watch::Sender<Faults>,
    accepted: Arc<AtomicU64>,
    task: JoinHandle<()>,
    #[cfg_attr(not(test), expect(dead_code, reason = "read by proxy tests"))]
    severed: Arc<AtomicUsize>,
}

impl PartitionProxy {
    pub async fn start(upstream: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let (faults, faults_rx) = watch::channel(Faults::default());
        let accepted = Arc::new(AtomicU64::new(0));
        let severed = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(accept_connections(
            listener,
            upstream,
            faults_rx,
            Arc::clone(&accepted),
            Arc::clone(&severed),
        ));
        Ok(Self {
            address,
            faults,
            accepted,
            task,
            severed,
        })
    }

    pub fn port(&self) -> u16 {
        self.address.port()
    }

    pub fn inject(&self, fault: ProxyFault) {
        let accepted = self.accepted.load(Ordering::Relaxed);
        self.faults.send_modify(|faults| match fault {
            ProxyFault::Partition => faults.partitioned = true,
            ProxyFault::Heal => faults.partitioned = false,
            ProxyFault::Stall => faults.stalled_below = accepted,
            ProxyFault::Throttle(rate) => faults.upstream_bytes_per_second = Some(rate),
            ProxyFault::Unthrottle => faults.upstream_bytes_per_second = None,
        });
    }

    pub fn stop(&self) {
        self.task.abort();
    }

    #[cfg(test)]
    fn severed_connections(&self) -> usize {
        self.severed.load(Ordering::Acquire)
    }
}

impl Drop for PartitionProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn accept_connections(
    listener: TcpListener,
    upstream: SocketAddr,
    faults: watch::Receiver<Faults>,
    accepted_count: Arc<AtomicU64>,
    severed: Arc<AtomicUsize>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                // Skip a failed accept; the pause avoids spinning on a
                // persistent error such as too many open files.
                let Ok((client, _)) = accepted else {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                };
                let id = accepted_count.fetch_add(1, Ordering::Relaxed);
                connections.spawn(Connection::new(
                    id,
                    client,
                    upstream,
                    faults.clone(),
                    Arc::clone(&severed),
                ).run());
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

struct Connection {
    id: u64,
    client: TcpStream,
    upstream_address: SocketAddr,
    upstream: Option<TcpStream>,
    faults: watch::Receiver<Faults>,
    from_client: Vec<u8>,
    from_client_offset: usize,
    from_upstream: Vec<u8>,
    from_upstream_offset: usize,
    client_state: SideState,
    upstream_state: SideState,
    severed: bool,
    severed_count: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SideState {
    Open,
    Eof,
    HalfClosed,
}

impl Connection {
    fn new(
        id: u64,
        client: TcpStream,
        upstream_address: SocketAddr,
        faults: watch::Receiver<Faults>,
        severed_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            id,
            client,
            upstream_address,
            upstream: None,
            faults,
            from_client: Vec::with_capacity(BUFFER_CAPACITY),
            from_client_offset: 0,
            from_upstream: Vec::with_capacity(BUFFER_CAPACITY),
            from_upstream_offset: 0,
            client_state: SideState::Open,
            upstream_state: SideState::Open,
            severed: false,
            severed_count,
        }
    }

    fn held(&self) -> bool {
        let faults = self.faults.borrow();
        faults.partitioned || self.id < faults.stalled_below
    }

    async fn run(mut self) {
        loop {
            if self.held() {
                self.read_partitioned().await;
                continue;
            }
            if self.upstream.is_none() {
                if !self.connect_upstream().await {
                    return;
                }
                continue;
            }
            if self.severed {
                if !self.wait_for_client_after_sever().await {
                    return;
                }
                continue;
            }
            if !self.forward_unpartitioned().await {
                return;
            }
        }
    }

    async fn connect_upstream(&mut self) -> bool {
        tokio::select! {
            biased;
            changed = self.faults.changed() => changed.is_ok(),
            connected = TcpStream::connect(self.upstream_address) => {
                let Ok(upstream) = connected else {
                    return false;
                };
                self.upstream = Some(upstream);
                true
            }
        }
    }

    async fn read_partitioned(&mut self) {
        while self.held() {
            tokio::select! {
                biased;
                changed = self.faults.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if !self.held() {
                        self.drain_upstream_at_heal();
                    }
                }
                read = self.client.read_buf(&mut self.from_client), if self.client_state == SideState::Open && self.from_client.len() < HOLD_LIMIT => {
                    match read {
                        Ok(0) | Err(_) => self.client_state = SideState::Eof,
                        Ok(_) => {}
                    }
                }
                // A close left unread here is found when the partition heals.
                read = read_connected(self.upstream.as_mut(), &mut self.from_upstream), if !self.severed && self.from_upstream.len() < HOLD_LIMIT => {
                    match read {
                        Ok(0) | Err(_) => self.mark_severed(),
                        Ok(_) => {}
                    }
                }
            }
        }
    }

    fn drain_upstream_at_heal(&mut self) {
        if self.severed {
            return;
        }
        let Some(upstream) = &self.upstream else {
            return;
        };
        // A close behind more than `HOLD_LIMIT` of data reaches the client.
        let severed = loop {
            if self.from_upstream.len() >= HOLD_LIMIT {
                break false;
            }
            match upstream.try_read_buf(&mut self.from_upstream) {
                Ok(0) => break true,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break false,
                Err(_) => break true,
            }
        };
        if severed {
            self.mark_severed();
        }
    }

    fn mark_severed(&mut self) {
        self.severed = true;
        self.from_upstream.clear();
        self.from_upstream_offset = 0;
        self.severed_count.fetch_add(1, Ordering::Release);
    }

    async fn wait_for_client_after_sever(&mut self) -> bool {
        if !self.from_client.is_empty() {
            reset(&self.client);
            return false;
        }
        if self.client_state != SideState::Open {
            return false;
        }
        tokio::select! {
            biased;
            changed = self.faults.changed() => changed.is_ok(),
            read = self.client.read_buf(&mut self.from_client) => match read {
                Ok(0) | Err(_) => false,
                Ok(_) => {
                    reset(&self.client);
                    false
                }
            }
        }
    }

    async fn forward_unpartitioned(&mut self) -> bool {
        if self.client_state == SideState::Eof && self.from_client.is_empty() {
            let Some(upstream) = &mut self.upstream else {
                return false;
            };
            if upstream.shutdown().await.is_err() {
                return false;
            }
            self.client_state = SideState::HalfClosed;
        }
        if self.upstream_state == SideState::Eof && self.from_upstream.is_empty() {
            if self.client.shutdown().await.is_err() {
                return false;
            }
            self.upstream_state = SideState::HalfClosed;
        }
        if self.client_state != SideState::Open && self.upstream_state != SideState::Open {
            return false;
        }
        let Some(upstream) = &self.upstream else {
            return false;
        };
        tokio::select! {
            biased;
            changed = self.faults.changed() => changed.is_ok(),
            ready = upstream.writable(), if !self.from_client.is_empty() => {
                if ready.is_err() || flush(upstream, &mut self.from_client, &mut self.from_client_offset, usize::MAX) {
                    reset(&self.client);
                    return false;
                }
                true
            }
            ready = self.client.writable(), if !self.from_upstream.is_empty() => {
                let rate = self.faults.borrow().upstream_bytes_per_second;
                let limit = rate.map_or(usize::MAX, |rate| {
                    let slice = u64::try_from(THROTTLE_SLICE.as_nanos()).unwrap_or(u64::MAX);
                    usize::try_from(rate.get().saturating_mul(slice) / 1_000_000_000)
                        .unwrap_or(usize::MAX)
                        .max(1)
                });
                let pending = self.from_upstream.len() - self.from_upstream_offset;
                if ready.is_err() || flush(&self.client, &mut self.from_upstream, &mut self.from_upstream_offset, limit) {
                    reset(upstream);
                    return false;
                }
                if let Some(rate) = rate {
                    let written = pending - (self.from_upstream.len() - self.from_upstream_offset);
                    let written = u64::try_from(written).unwrap_or(u64::MAX);
                    tokio::time::sleep(Duration::from_nanos(
                        written.saturating_mul(1_000_000_000) / rate.get(),
                    ))
                    .await;
                }
                true
            }
            ready = self.client.readable(), if self.client_state == SideState::Open && self.from_client.is_empty() => {
                if read_ready(ready, &self.client, &mut self.from_client, &mut self.client_state).is_err() {
                    reset(upstream);
                    return false;
                }
                true
            }
            ready = upstream.readable(), if self.upstream_state == SideState::Open && self.from_upstream.is_empty() => {
                if read_ready(ready, upstream, &mut self.from_upstream, &mut self.upstream_state).is_err() {
                    reset(&self.client);
                    return false;
                }
                true
            }
        }
    }
}

async fn read_connected(stream: Option<&mut TcpStream>, held: &mut Vec<u8>) -> io::Result<usize> {
    match stream {
        Some(stream) => stream.read_buf(held).await,
        None => pending().await,
    }
}

/// Writes up to `limit` held bytes; true if the stream failed.
fn flush(stream: &TcpStream, held: &mut Vec<u8>, offset: &mut usize, limit: usize) -> bool {
    let end = held.len().min(offset.saturating_add(limit));
    match stream.try_write(&held[*offset..end]) {
        Ok(0) => true,
        Ok(written) => {
            *offset += written;
            if *offset == held.len() {
                held.clear();
                *offset = 0;
            }
            false
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => false,
        Err(_) => true,
    }
}

fn read_ready(
    ready: io::Result<()>,
    stream: &TcpStream,
    held: &mut Vec<u8>,
    state: &mut SideState,
) -> io::Result<()> {
    ready?;
    match stream.try_read_buf(held) {
        Ok(0) => *state = SideState::Eof,
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn reset(stream: &TcpStream) {
    let _ = stream.set_zero_linger();
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;

    use super::*;

    const WAIT: Duration = Duration::from_secs(2);
    const STAYS_PENDING: Duration = Duration::from_millis(50);

    async fn wait_until(mut ready: impl FnMut() -> bool, message: &str) {
        timeout(WAIT, async {
            while !ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{message}"));
    }

    async fn listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        (listener, address)
    }

    async fn within<T>(future: impl Future<Output = T>) -> T {
        timeout(WAIT, future).await.expect("operation timed out")
    }

    #[tokio::test]
    async fn forwards_transparently() {
        let (listener, upstream) = listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept proxy");
            let mut request = [0_u8; 5];
            stream.read_exact(&mut request).await.expect("read request");
            assert_eq!(&request, b"hello");
            stream.write_all(b"world").await.expect("write response");
        });
        let proxy = PartitionProxy::start(upstream).await.expect("start proxy");
        let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect client");
        client
            .write_all(b"hello")
            .await
            .expect("write through proxy");
        let mut response = [0_u8; 5];
        within(client.read_exact(&mut response))
            .await
            .expect("read through proxy");
        assert_eq!(&response, b"world");
        within(server).await.expect("server task");
        proxy.stop();
    }

    #[tokio::test]
    async fn holds_bytes_until_heal_and_keeps_the_socket_working() {
        let (listener, upstream) = listener().await;
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (push_tx, push_rx) = oneshot::channel();
        let (pushed_tx, pushed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept proxy");
            accepted_tx.send(()).expect("report accepted connection");
            push_rx.await.expect("send during partition");
            stream.write_all(b"push").await.expect("write server push");
            pushed_tx.send(()).expect("report server push");
            let mut bytes = [0_u8; 4];
            for expected in [b"held", b"next"] {
                stream.read_exact(&mut bytes).await.expect("read request");
                assert_eq!(&bytes, expected);
                stream.write_all(&bytes).await.expect("echo request");
            }
        });
        let proxy = PartitionProxy::start(upstream).await.expect("start proxy");
        let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect client");
        within(accepted_rx).await.expect("upstream accepted proxy");

        proxy.inject(ProxyFault::Partition);
        push_tx.send(()).expect("request server push");
        within(pushed_rx).await.expect("server pushed bytes");
        client
            .write_all(b"held")
            .await
            .expect("write during partition");
        let mut response = [0_u8; 4];
        assert!(
            timeout(STAYS_PENDING, client.read_exact(&mut response))
                .await
                .is_err(),
            "partitioned response must stay pending"
        );
        proxy.inject(ProxyFault::Heal);
        within(client.read_exact(&mut response))
            .await
            .expect("server push after heal");
        assert_eq!(&response, b"push");
        within(client.read_exact(&mut response))
            .await
            .expect("held response after heal");
        assert_eq!(&response, b"held");

        client.write_all(b"next").await.expect("write after heal");
        within(client.read_exact(&mut response))
            .await
            .expect("response after heal");
        assert_eq!(&response, b"next");
        within(server).await.expect("server task");
        proxy.stop();
    }

    #[tokio::test]
    async fn loses_an_upstream_close_then_resets_a_client_that_writes() {
        let (listener, upstream) = listener().await;
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (close_tx, close_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept proxy");
            accepted_tx.send(()).expect("report accepted connection");
            close_rx.await.expect("close requested");
            stream
                .write_all(b"doomed")
                .await
                .expect("write bytes before close");
            drop(stream);
        });
        let proxy = PartitionProxy::start(upstream).await.expect("start proxy");
        let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect client");
        within(accepted_rx).await.expect("upstream accepted proxy");

        proxy.inject(ProxyFault::Partition);
        close_tx.send(()).expect("request upstream close");
        within(server).await.expect("server task");
        wait_until(
            || proxy.severed_connections() == 1,
            "proxy did not observe the partitioned upstream close",
        )
        .await;
        proxy.inject(ProxyFault::Heal);

        let mut byte = [0_u8; 1];
        assert!(
            timeout(STAYS_PENDING, client.read(&mut byte))
                .await
                .is_err(),
            "lost upstream close must produce neither bytes nor EOF"
        );
        client
            .write_all(b"x")
            .await
            .expect("write that triggers reset");
        let error = within(client.read(&mut byte))
            .await
            .expect_err("severed connection must reset after a client write");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        proxy.stop();
    }

    #[tokio::test]
    async fn delays_an_upstream_connection_made_during_the_partition() {
        let (listener, upstream) = listener().await;
        let (accepted_tx, mut accepted_rx) = mpsc::channel(1);
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept proxy");
            accepted_tx
                .send(())
                .await
                .expect("report accepted connection");
        });
        let proxy = PartitionProxy::start(upstream).await.expect("start proxy");
        proxy.inject(ProxyFault::Partition);
        let _client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect client during partition");
        assert!(
            timeout(STAYS_PENDING, accepted_rx.recv()).await.is_err(),
            "upstream must not receive a connection during the partition"
        );

        proxy.inject(ProxyFault::Heal);
        assert_eq!(within(accepted_rx.recv()).await, Some(()));
        within(server).await.expect("server task");
        proxy.stop();
    }

    #[tokio::test]
    async fn stalls_open_connections_and_relays_new_ones() {
        let (listener, upstream) = listener().await;
        let (accepted_tx, mut accepted_rx) = mpsc::channel(1);
        let (push_tx, push_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stalled, _) = listener.accept().await.expect("accept first connection");
            accepted_tx.send(()).await.expect("report first connection");
            push_rx.await.expect("push requested");
            stalled.write_all(b"lost").await.expect("write on the stalled connection");
            let (mut relayed, _) = listener.accept().await.expect("accept second connection");
            relayed.write_all(b"live").await.expect("write on the new connection");
            stalled
        });
        let proxy = PartitionProxy::start(upstream).await.expect("start proxy");
        let mut stalled = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect first client");
        within(accepted_rx.recv()).await.expect("upstream accepted proxy");

        proxy.inject(ProxyFault::Stall);
        push_tx.send(()).expect("request push");
        let mut relayed = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect second client");
        let mut response = [0_u8; 4];
        within(relayed.read_exact(&mut response))
            .await
            .expect("read on the new connection");
        assert_eq!(&response, b"live");
        assert!(
            timeout(STAYS_PENDING, stalled.read(&mut response))
                .await
                .is_err(),
            "a stalled connection must deliver nothing"
        );
        within(server).await.expect("server task");
        proxy.stop();
    }

    #[tokio::test]
    async fn caps_the_rate_of_upstream_bytes() {
        let (listener, upstream) = listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept proxy");
            stream.write_all(&[7_u8; 300]).await.expect("write response");
            stream
        });
        let proxy = PartitionProxy::start(upstream).await.expect("start proxy");
        proxy.inject(ProxyFault::Throttle(NonZeroU64::new(1000).expect("positive rate")));
        let mut client = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, proxy.port()))
            .await
            .expect("connect client");
        let started = tokio::time::Instant::now();
        let mut response = [0_u8; 300];
        within(client.read_exact(&mut response))
            .await
            .expect("read through proxy");
        // The last 100-byte slice arrives after the first two have each
        // waited 100 ms.
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "300 bytes at 1000 B/s arrived in {:?}",
            started.elapsed()
        );
        assert_eq!(response, [7_u8; 300]);
        within(server).await.expect("server task");
        proxy.stop();
    }
}
