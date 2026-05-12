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

//! Flight-side mTLS principal injection.
//!
//! Per-connection peer certificates flow into Flight requests via
//! [`tonic::transport::server::TlsConnectInfo`], which `tonic` puts on
//! every request's extensions when the `tls-connect-info` feature is
//! enabled. This layer reads that extension, builds the
//! [`runtime_auth::MtlsPrincipal`] / [`runtime_auth::ChannelIdentity`],
//! and — in [`IdentitySource::Channel`] mode — sets the principal on
//! the request's [`AuthRequestContext`] so the existing
//! [`runtime_auth::layer::flight::BasicAuthMiddleware`] short-circuits
//! without checking a bearer token.
//!
//! Must run **after** [`crate::flight::middleware::request_context`]'s
//! layer (which inserts the `Arc<dyn AuthRequestContext>` extension)
//! and **before** [`runtime_auth::layer::flight::BasicAuthLayer`] (so
//! the principal is in place when the auth layer's short-circuit
//! check runs).
//!
//! ## Performance note
//!
//! Unlike the HTTP path — which intercepts the rustls handshake at
//! accept-time and pre-computes per-connection identities (see
//! [`crate::http::mtls`]) — the Flight path receives the peer chain
//! per-request via tonic's extension mechanism. The leaf cert is
//! re-parsed and re-hashed on every RPC. This is a deliberate
//! trade-off: the call sites are cheap (one X.509 parse, one
//! SHA-256), tonic does not expose a per-connection middleware hook
//! that lets us hang state off the underlying `TlsStream`, and the
//! `Arc<Vec<CertificateDer>>` is at least shared across requests so
//! we don't re-allocate the chain bytes themselves. A future change
//! could memoise identities keyed by `Arc::as_ptr` of
//! `peer_certs()`'s Arc; the public surface here would be unchanged.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use runtime_auth::{
    AuthRequestContext, IdentitySource,
    mtls::{channel_identity_from_cert, principal_from_cert},
};
use tonic::transport::server::TlsConnectInfo;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct MtlsLayer {
    identity_source: IdentitySource,
}

impl MtlsLayer {
    #[must_use]
    pub fn new(identity_source: IdentitySource) -> Self {
        Self { identity_source }
    }
}

impl<S> Layer<S> for MtlsLayer {
    type Service = MtlsMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MtlsMiddleware {
            inner,
            identity_source: self.identity_source,
        }
    }
}

#[derive(Clone)]
pub struct MtlsMiddleware<S> {
    inner: S,
    identity_source: IdentitySource,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MtlsMiddleware<S>
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

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let identity_source = self.identity_source;

        // Pull the verified peer cert chain out of the per-connection
        // `TlsConnectInfo` extension that `tonic` populates when the
        // `tls-connect-info` feature is enabled. `Option<SocketAddr>`
        // is the inner connect-info shape for our `tls::incoming`
        // adaptor (TcpStream-backed). The returned `Arc<Vec<...>>` is
        // the per-connection chain that tonic re-clones into every
        // request, so cloning it is a refcount bump — but parsing the
        // leaf still happens per-request (see module docs).
        let chain_arc = req
            .extensions()
            .get::<TlsConnectInfo<SocketAddr>>()
            .and_then(TlsConnectInfo::peer_certs);

        if let Some(chain) = chain_arc.as_deref()
            && let Some(leaf) = chain.first()
        {
            let leaf_der: &[u8] = leaf.as_ref();
            // Always: ChannelIdentity for audit, regardless of mode.
            if let Some(channel) = channel_identity_from_cert(leaf_der) {
                req.extensions_mut().insert(channel);
            }

            // Channel mode only: promote to the request's auth principal.
            if matches!(identity_source, IdentitySource::Channel)
                && let Some(principal) = principal_from_cert(leaf_der)
            {
                let principal_arc: Arc<dyn runtime_auth::AuthPrincipal + Send + Sync> =
                    Arc::new(principal);
                if let Some(ctx) = req
                    .extensions()
                    .get::<Arc<dyn AuthRequestContext + Send + Sync>>()
                    && let Err(err) = ctx.set_auth_principal(Arc::clone(&principal_arc))
                {
                    tracing::warn!(%err, "Flight mTLS: failed to set auth principal on request context");
                }
                req.extensions_mut().insert(principal_arc);
            }
        }

        Box::pin(async move { inner.call(req).await })
    }
}
