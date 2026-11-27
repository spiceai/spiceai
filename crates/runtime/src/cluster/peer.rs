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

//! What a cluster node may do in a request to another node, by the identity its
//! certificate gives it ([`runtime_cluster::peer`]).
//!
//! On the scheduler's cluster port, a request that names an executor is from
//! that executor only when it comes from the node that registered the executor
//! ([`authorize_executor_claim`]), and an executor advertises only addresses its
//! certificate names.
//!
//! On an executor's Flight server, a request from the scheduler may write — the
//! scheduler forwards writes there — while a request from any other node of the
//! cluster is a peer executor's read ([`ClusterPeer`]).

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock, Weak},
    task::{Context, Poll},
};

use dashmap::DashMap;
use runtime_cluster::{
    ExecutorRegistry,
    peer::{PeerCertificate, PeerIdentity, SchedulerIdentities, address_host},
};
use runtime_request_context::{Extension, RequestContext};
use rustls::pki_types::CertificateDer;
use tonic::{
    Status,
    transport::server::{TcpConnectInfo, TlsConnectInfo},
};
use tower::{Layer, Service};

const DISTRIBUTED_QUERY_DOCS: &str = "https://spiceai.org/docs/features/distributed-query";

/// What a cluster node may do through an executor's Flight server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClusterPeerRole {
    /// The scheduler, which reads from the executor and forwards writes to it.
    Scheduler,
    /// Another executor, which reads from it: shuffle fetches and broadcast-join
    /// scans.
    Executor,
    /// Any node, over a cluster running without mTLS
    /// (`--allow-insecure-connections`), where no node is identified and every
    /// connection is as trusted as the network it comes from.
    Unauthenticated,
}

/// Per-connection cache of a peer's parsed certificate identity, keyed by the
/// address of the chain `Arc` tonic shares across every request on one TLS
/// connection. The value holds a [`Weak`] to that `Arc`: it keeps the allocation,
/// so the address cannot be reused by a later connection while the entry lives,
/// yet lets the certificates themselves be freed when the connection closes.
///
/// Only the identity is cached, never the role: the role is decided on each
/// request against the scheduler identities observed now, so a change to them (a
/// scheduler replaced at its address, a scheduler address forgotten) takes effect
/// on a connection that is already open.
type PeerIdentityCache = DashMap<usize, (Weak<Vec<CertificateDer<'static>>>, Option<PeerIdentity>)>;

/// Bound on cached connections. A cluster has few peers, so this is only a
/// safety bound: over it the entries of closed connections are dropped, and if
/// the cache is still full the identity is resolved without caching.
const PEER_IDENTITY_CACHE_CAP: usize = 4096;

/// The cluster node that sent a request to this executor's Flight server, kept
/// on the request's [`RequestContext`].
///
/// The role is decided from the node's certificate the first time a handler
/// asks, so requests that never ask (shuffle fetches) do not parse it. The
/// certificate's identity is parsed once per TLS connection; the role is decided
/// from it on each request.
#[derive(Clone)]
pub(crate) struct ClusterPeer {
    /// The certificate chain the peer presented on its TLS connection, shared by
    /// every request on that connection; `None` without a client certificate.
    peer_certs: Option<Arc<Vec<CertificateDer<'static>>>>,
    /// `None` without cluster mTLS.
    schedulers: Option<Arc<SchedulerIdentities>>,
    identities: Arc<PeerIdentityCache>,
    role: Arc<OnceLock<ClusterPeerRole>>,
}

impl ClusterPeer {
    pub(crate) fn role(&self) -> ClusterPeerRole {
        *self.role.get_or_init(|| self.resolve_role())
    }

    fn resolve_role(&self) -> ClusterPeerRole {
        let Some(schedulers) = self.schedulers.as_ref() else {
            return ClusterPeerRole::Unauthenticated;
        };
        match self.identity() {
            Some(identity) if schedulers.contains(&identity) => ClusterPeerRole::Scheduler,
            _ => ClusterPeerRole::Executor,
        }
    }

    /// The identity of the certificate the peer presented on this connection,
    /// parsed on the connection's first request that asks and cached for the
    /// rest.
    fn identity(&self) -> Option<PeerIdentity> {
        let chain = self.peer_certs.as_ref()?;
        let key = Arc::as_ptr(chain) as usize;
        // While `chain` is held its address is live, and an entry's `Weak` keeps
        // its own allocation, so an entry at this address is this connection's.
        if let Some(entry) = self.identities.get(&key) {
            return entry.1.clone();
        }
        let identity = chain.first().and_then(PeerIdentity::from_certificate);
        if self.identities.len() >= PEER_IDENTITY_CACHE_CAP {
            self.identities
                .retain(|_, (connection, _)| connection.strong_count() > 0);
        }
        if self.identities.len() < PEER_IDENTITY_CACHE_CAP {
            self.identities
                .insert(key, (Arc::downgrade(chain), identity.clone()));
        }
        identity
    }
}

impl Extension for ClusterPeer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Whether the request in `context` came from the scheduler to this executor's
/// Flight server. `false` for every request that did not arrive there, and for
/// every request without cluster mTLS, where the scheduler is not identified.
pub(crate) fn is_from_scheduler(context: &RequestContext) -> bool {
    cluster_peer_role(context) == Some(ClusterPeerRole::Scheduler)
}

/// The role of the cluster node that sent the request in `context` to this
/// executor's Flight server; `None` for a request that did not arrive there.
pub(crate) fn cluster_peer_role(context: &RequestContext) -> Option<ClusterPeerRole> {
    context.extension::<ClusterPeer>().map(|peer| peer.role())
}

/// Whether the request in `context` came to this executor's Flight server from
/// another node of the cluster than the scheduler, which may only read.
pub(crate) fn is_from_peer_executor(context: &RequestContext) -> bool {
    cluster_peer_role(context) == Some(ClusterPeerRole::Executor)
}

/// Records on each request to an executor's Flight server which cluster node
/// sent it. Must run inside the request-context layer, which puts the
/// [`RequestContext`] on the request, and before authorization.
///
/// Without cluster mTLS (`--allow-insecure-connections`) no node is identified:
/// every request is recorded as [`ClusterPeerRole::Unauthenticated`].
#[derive(Clone)]
pub(crate) struct ClusterPeerLayer {
    schedulers: Option<Arc<SchedulerIdentities>>,
    identities: Arc<PeerIdentityCache>,
}

impl ClusterPeerLayer {
    pub(crate) fn new(schedulers: Option<Arc<SchedulerIdentities>>) -> Self {
        Self {
            schedulers,
            identities: Arc::new(PeerIdentityCache::new()),
        }
    }
}

impl<S> Layer<S> for ClusterPeerLayer {
    type Service = ClusterPeerMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ClusterPeerMiddleware {
            inner,
            schedulers: self.schedulers.clone(),
            identities: Arc::clone(&self.identities),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClusterPeerMiddleware<S> {
    inner: S,
    schedulers: Option<Arc<SchedulerIdentities>>,
    identities: Arc<PeerIdentityCache>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for ClusterPeerMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        if let Some(context) = req.extensions().get::<Arc<RequestContext>>() {
            context.insert_extension(ClusterPeer {
                peer_certs: req
                    .extensions()
                    .get::<TlsConnectInfo<TcpConnectInfo>>()
                    .and_then(TlsConnectInfo::peer_certs),
                schedulers: self.schedulers.clone(),
                identities: Arc::clone(&self.identities),
                role: Arc::new(OnceLock::new()),
            });
        }
        Box::pin(async move { inner.call(req).await })
    }
}

/// An address an executor advertises, where other nodes connect to it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Advertised<'a> {
    /// A URL or `host:port`: its Flight endpoint, or the `host:port` of its
    /// Ballista registration.
    Address(&'a str),
}

impl Advertised<'_> {
    fn host(&self) -> Option<&str> {
        let Self::Address(address) = self;
        address_host(address)
    }

    /// The address as a refusal names it: `host:port`, without a URL scheme.
    fn display(&self) -> &str {
        let Self::Address(address) = self;
        address
            .split_once("://")
            .map_or(*address, |(_, rest)| rest)
            .trim_end_matches('/')
    }
}

/// Refuses a request that acts as executor `executor_id` unless it comes from the
/// node that registered the executor — the node whose certificate first claimed
/// that id. This is the identity binding that stops one cluster node acting as
/// another's executor.
///
/// It also refuses a request that advertises an address the node's certificate
/// does not name: other nodes reach this executor there and verify its
/// certificate against the host, so an address the certificate does not name is
/// unreachable (Enhancement spiceai/spiceai#16481).
///
/// `certificate` is the certificate the request's connection presented; `None`
/// without cluster mTLS, where no request is refused. `action` names what was
/// refused, for the identity message.
pub(crate) fn authorize_executor_claim(
    registry: &ExecutorRegistry,
    certificate: Option<&PeerCertificate>,
    executor_id: &str,
    advertised: Option<Advertised<'_>>,
    action: &str,
) -> Result<(), Status> {
    let identity = certificate.map(PeerCertificate::identity);
    if let Some(registered) = registry.registered_identity_conflict(executor_id, identity) {
        let message =
            registered_elsewhere_message(action, executor_id, identity, registered.as_ref());
        tracing::warn!("{message}");
        return Err(Status::permission_denied(message));
    }
    if let (Some(certificate), Some(advertised)) = (certificate, advertised) {
        let named = advertised
            .host()
            .is_some_and(|host| certificate.names_host(host));
        if !named {
            let message = runtime_cluster::peer::registration_address_mismatch_message(
                executor_id,
                certificate.identity().subject(),
                advertised.display(),
            );
            tracing::warn!("{message}");
            return Err(Status::permission_denied(message));
        }
    }
    Ok(())
}

fn describe(identity: Option<&PeerIdentity>) -> String {
    identity.map_or_else(
        || "a node without a cluster certificate".to_string(),
        |identity| format!("the cluster node with certificate {identity}"),
    )
}

/// Why a request acting as a registered executor from another node is refused.
pub(crate) fn registered_elsewhere_message(
    action: &str,
    executor_id: &str,
    from: Option<&PeerIdentity>,
    registered: Option<&PeerIdentity>,
) -> String {
    format!(
        "Refused to {action} for executor '{executor_id}' from {}, so that node gets no work as '{executor_id}': the executor is registered by {}, and a node acts only as the executor it registered. If two instances use SPICE_INSTANCE_ID '{executor_id}', give each its own. See: {DISTRIBUTED_QUERY_DOCS}",
        describe(from),
        describe(registered),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, DnType, KeyPair, SanType};

    fn certificate(common_name: &str, sans: &[&str]) -> PeerCertificate {
        PeerCertificate::from_chain(std::slice::from_ref(&certificate_der(common_name, sans)))
            .expect("peer certificate")
    }

    fn certificate_der(common_name: &str, sans: &[&str]) -> CertificateDer<'static> {
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        for san in sans {
            params.subject_alt_names.push(match san.parse() {
                Ok(ip) => SanType::IpAddress(ip),
                Err(_) => SanType::DnsName((*san).try_into().expect("DNS name")),
            });
        }
        let key = KeyPair::generate().expect("key");
        let cert = params.self_signed(&key).expect("certificate");
        cert.der().clone()
    }

    /// A request on the connection whose presented chain is `chain`.
    fn request_on(
        chain: &Arc<Vec<CertificateDer<'static>>>,
        schedulers: &Arc<SchedulerIdentities>,
        identities: &Arc<PeerIdentityCache>,
    ) -> ClusterPeer {
        ClusterPeer {
            peer_certs: Some(Arc::clone(chain)),
            schedulers: Some(Arc::clone(schedulers)),
            identities: Arc::clone(identities),
            role: Arc::new(OnceLock::new()),
        }
    }

    #[test]
    fn a_connection_role_follows_the_scheduler_identities_observed_now() {
        let schedulers = Arc::new(SchedulerIdentities::new());
        let identities = Arc::new(PeerIdentityCache::new());
        let address = vec!["https://scheduler:50052".to_string()];
        let scheduler_der = certificate_der("scheduler", &["scheduler"]);
        let scheduler = PeerIdentity::from_certificate(&scheduler_der).expect("identity");
        let connection = Arc::new(vec![scheduler_der]);

        // Before any scheduler is observed, the connection is a peer executor's.
        assert_eq!(
            request_on(&connection, &schedulers, &identities).role(),
            ClusterPeerRole::Executor
        );
        assert_eq!(identities.len(), 1, "identity cached for the connection");

        // Once this certificate is observed at the scheduler address, a later
        // request on the same connection is the scheduler's.
        schedulers.record(&address, vec![(address[0].clone(), scheduler.clone())]);
        assert_eq!(
            request_on(&connection, &schedulers, &identities).role(),
            ClusterPeerRole::Scheduler
        );

        // A different scheduler now answers at that address: the open connection
        // loses the scheduler role on its next request.
        schedulers.record(
            &address,
            vec![(address[0].clone(), identity("scheduler-2"))],
        );
        assert_eq!(
            request_on(&connection, &schedulers, &identities).role(),
            ClusterPeerRole::Executor
        );

        // And regains it when the original scheduler is observed again.
        schedulers.record(&address, vec![(address[0].clone(), scheduler)]);
        assert_eq!(
            request_on(&connection, &schedulers, &identities).role(),
            ClusterPeerRole::Scheduler
        );
        assert_eq!(
            identities.len(),
            1,
            "every request on one connection reuses its cached identity"
        );
    }

    #[test]
    fn closed_connections_leave_the_identity_cache_when_it_fills() {
        let schedulers = Arc::new(SchedulerIdentities::new());
        let identities = Arc::new(PeerIdentityCache::new());
        let der = certificate_der("executor", &["executor"]);

        // Fill the cache with connections that then close.
        for _ in 0..PEER_IDENTITY_CACHE_CAP {
            let closed = Arc::new(vec![der.clone()]);
            assert_eq!(
                request_on(&closed, &schedulers, &identities).role(),
                ClusterPeerRole::Executor
            );
        }
        assert_eq!(identities.len(), PEER_IDENTITY_CACHE_CAP);

        // The next connection drops the closed ones and is cached itself.
        let open = Arc::new(vec![der]);
        request_on(&open, &schedulers, &identities).role();
        assert_eq!(identities.len(), 1);
        assert!(identities.contains_key(&(Arc::as_ptr(&open) as usize)));
    }

    fn identity(common_name: &str) -> PeerIdentity {
        certificate(common_name, &["node.cluster.local"])
            .identity()
            .clone()
    }

    #[test]
    fn refusals_name_the_executor_the_nodes_and_the_fix() {
        assert_eq!(
            registered_elsewhere_message(
                "open the control stream",
                "executor-0",
                Some(&identity("executor-1")),
                Some(&identity("executor-0")),
            ),
            "Refused to open the control stream for executor 'executor-0' from the cluster node with certificate 'CN=executor-1', so that node gets no work as 'executor-0': the executor is registered by the cluster node with certificate 'CN=executor-0', and a node acts only as the executor it registered. If two instances use SPICE_INSTANCE_ID 'executor-0', give each its own. See: https://spiceai.org/docs/features/distributed-query"
        );
    }

    #[test]
    fn a_node_advertises_only_an_address_its_certificate_names() {
        // The node's certificate names its per-pod DNS name, not an arbitrary IP.
        let cert = certificate("executor-1", &["executor-1.cluster.local"]);
        assert!(
            Advertised::Address("https://executor-1.cluster.local:50052")
                .host()
                .is_some_and(|host| cert.names_host(host))
        );
        assert!(
            !Advertised::Address("https://10.0.0.7:50052")
                .host()
                .is_some_and(|host| cert.names_host(host))
        );
        // The refusal names the address as `host:port`, without the URL scheme.
        let advertised = Advertised::Address("https://10.0.0.7:50052");
        assert_eq!(advertised.display(), "10.0.0.7:50052");
        assert_eq!(Advertised::Address("[::1]:50052").display(), "[::1]:50052");
        assert_eq!(
            runtime_cluster::peer::registration_address_mismatch_message(
                "executor-1",
                cert.identity().subject(),
                advertised.display(),
            ),
            "Refused to register executor 'executor-1': it advertises '10.0.0.7:50052', which its cluster certificate (CN=executor-1) does not name, so other cluster nodes could not connect to it. Issue each node a certificate that names its `--node-advertise-address`. See: https://spiceai.org/docs/features/distributed-query"
        );
    }
}
