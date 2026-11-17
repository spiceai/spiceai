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
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};

use runtime_cluster::{
    ExecutorRegistry,
    peer::{PeerCertificate, PeerIdentity, SchedulerIdentities, address_host},
};
use runtime_request_context::{Extension, RequestContext};
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

/// The cluster node that sent a request to this executor's Flight server, kept
/// on the request's [`RequestContext`].
///
/// The role is decided from the node's certificate the first time a handler
/// asks, so requests that never ask (shuffle fetches) do not parse it.
#[derive(Clone)]
pub(crate) struct ClusterPeer {
    connect_info: Option<TlsConnectInfo<TcpConnectInfo>>,
    /// `None` without cluster mTLS.
    schedulers: Option<Arc<SchedulerIdentities>>,
    role: Arc<OnceLock<ClusterPeerRole>>,
}

impl ClusterPeer {
    pub(crate) fn role(&self) -> ClusterPeerRole {
        *self.role.get_or_init(|| {
            let Some(schedulers) = self.schedulers.as_ref() else {
                return ClusterPeerRole::Unauthenticated;
            };
            let certificate = self
                .connect_info
                .as_ref()
                .and_then(PeerCertificate::from_connect_info);
            match certificate {
                Some(certificate) if schedulers.contains(certificate.identity()) => {
                    ClusterPeerRole::Scheduler
                }
                _ => ClusterPeerRole::Executor,
            }
        })
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
}

impl ClusterPeerLayer {
    pub(crate) fn new(schedulers: Option<Arc<SchedulerIdentities>>) -> Self {
        Self { schedulers }
    }
}

impl<S> Layer<S> for ClusterPeerLayer {
    type Service = ClusterPeerMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ClusterPeerMiddleware {
            inner,
            schedulers: self.schedulers.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClusterPeerMiddleware<S> {
    inner: S,
    schedulers: Option<Arc<SchedulerIdentities>>,
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
                connect_info: req
                    .extensions()
                    .get::<TlsConnectInfo<TcpConnectInfo>>()
                    .cloned(),
                schedulers: self.schedulers.clone(),
                role: Arc::new(OnceLock::new()),
            });
        }
        Box::pin(async move { inner.call(req).await })
    }
}

/// An address an executor advertises, where other nodes connect to it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Advertised<'a> {
    /// A URL or `host:port`, such as its Flight endpoint.
    Address(&'a str),
    /// A bare host, such as the host of its Ballista registration.
    Host(&'a str),
}

impl Advertised<'_> {
    fn host(&self) -> Option<&str> {
        match self {
            Self::Address(address) => address_host(address),
            Self::Host(host) => Some(host),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Address(value) | Self::Host(value) => value,
        }
    }
}

/// Refuses a request that acts as executor `executor_id` unless it comes from the
/// node that registered the executor — the node whose certificate first claimed
/// that id. This is the identity binding that stops one cluster node acting as
/// another's executor.
///
/// When the request advertises an address the node's certificate does not name,
/// it is served but a warning is logged: the scheduler's TLS client will not
/// complete a handshake to a mismatched certificate there, so the address is
/// unreachable, but refusing it is gated behind an Enhancement because it would
/// reject a node whose certificate omits its advertise address (see the
/// `cluster-trust` report).
///
/// `certificate` is the certificate the request's connection presented; `None`
/// without cluster mTLS, where no request is refused. `action` names what was
/// refused, for the message.
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
            tracing::warn!(
                "{}",
                unnamed_address_message(
                    action,
                    executor_id,
                    certificate.identity(),
                    advertised.as_str(),
                )
            );
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

/// Warns that an executor advertises an address its certificate does not name,
/// so the scheduler cannot reach it there.
pub(crate) fn unnamed_address_message(
    action: &str,
    executor_id: &str,
    from: &PeerIdentity,
    advertised: &str,
) -> String {
    format!(
        "Executor '{executor_id}' ({action}) advertises address '{advertised}', which the certificate of the cluster node that sent it, {from}, is not valid for, so the scheduler cannot reach it there and sends it no work. Issue each node a certificate that names its `--node-advertise-address`. See: {DISTRIBUTED_QUERY_DOCS}",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, DnType, KeyPair, SanType};

    fn identity(common_name: &str) -> PeerIdentity {
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.subject_alt_names.push(SanType::DnsName(
            "node.cluster.local".try_into().expect("DNS name"),
        ));
        let key = KeyPair::generate().expect("key");
        let certificate = params.self_signed(&key).expect("certificate");
        PeerIdentity::from_certificate(certificate.der()).expect("identity")
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
        assert_eq!(
            unnamed_address_message(
                "allocate partitions",
                "executor-1",
                &identity("executor-1"),
                "https://10.0.0.7:50052",
            ),
            "Executor 'executor-1' (allocate partitions) advertises address 'https://10.0.0.7:50052', which the certificate of the cluster node that sent it, 'CN=executor-1', is not valid for, so the scheduler cannot reach it there and sends it no work. Issue each node a certificate that names its `--node-advertise-address`. See: https://spiceai.org/docs/features/distributed-query"
        );
    }
}
