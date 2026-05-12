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

//! Per-connection mTLS context for the HTTP server.
//!
//! The HTTP listener accepts a TCP stream, runs the rustls handshake,
//! and — only when the handshake succeeded — extracts the verified
//! peer cert chain (if any), pre-computes the
//! [`runtime_auth::ChannelIdentity`] and (in
//! [`IdentitySource::Channel`] mode) the
//! [`runtime_auth::MtlsPrincipal`] **once per connection**, and wraps
//! the connection's `Router` with an `Extension<Arc<PerConnTls>>`.
//!
//! From there a per-request middleware ([`mtls_request_layer`]) just
//! clones the pre-computed Arcs into the per-request extensions and
//! — in `Channel` mode — sets the principal on the request's
//! [`AuthRequestContext`] so the existing
//! [`runtime_auth::layer::http::AuthMiddleware`] short-circuits
//! without checking an API key / bearer token. No X.509 parsing or
//! SHA-256 hashing happens on the per-request hot path.
//!
//! The Flight half of the same wiring lives in
//! [`crate::flight::mtls`].

use std::sync::Arc;

use axum::{
    Extension,
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use runtime_auth::{
    AuthPrincipal, AuthRequestContext, ChannelIdentity, IdentitySource,
    mtls::{channel_identity_from_cert, principal_from_cert},
};
use rustls::pki_types::CertificateDer;

use crate::tls::ClientAuthEnforcement;

/// Per-connection mTLS context, populated once by the accept loop and
/// inherited by every HTTP request served on that connection.
///
/// Both [`Self::channel_identity`] and [`Self::principal`] are
/// **pre-computed at connection accept** so per-request middleware
/// does no X.509 parsing or hashing on the hot path. Cloning each
/// inner `Arc` is just a refcount bump.
///
/// [`Self::client_auth`] is plumbed in once per connection so the
/// route-layer gate can 401 non-probe requests that arrived without a
/// verified client cert when `client_auth: required` is configured
/// (the HTTP listener uses an `allow_unauthenticated` rustls verifier
/// to keep `/health` and `/v1/ready` reachable for Kubernetes
/// probes).
pub(crate) struct PerConnTls {
    /// Always populated when a verified peer cert was presented (any
    /// [`IdentitySource`]). `None` for plain-HTTPS-no-client-cert.
    pub channel_identity: Option<Arc<ChannelIdentity>>,
    /// Populated only when [`IdentitySource::Channel`] is configured
    /// AND a verified peer cert was presented; otherwise `None`.
    /// When `Some`, `mtls_request_layer` short-circuits the
    /// downstream auth layer by setting this principal on the
    /// request's [`AuthRequestContext`].
    pub principal: Option<Arc<dyn AuthPrincipal + Send + Sync>>,
    /// Whether the route-layer gate should reject requests with no
    /// verified peer cert. Copied in once per connection from
    /// [`crate::tls::TlsConfig::client_auth`].
    pub client_auth: ClientAuthEnforcement,
}

impl PerConnTls {
    /// Build a `PerConnTls` from the verified peer cert chain (the
    /// `peer_certificates()` snapshot off the rustls connection), the
    /// configured [`IdentitySource`], and the configured
    /// [`ClientAuthEnforcement`]. Pre-computes the audit
    /// `ChannelIdentity` always; pre-computes the auth principal
    /// only when in `Channel` mode.
    ///
    /// Always returns `Some` when [`ClientAuthEnforcement::Required`]
    /// so the per-request gate observes the enforcement value even
    /// for connections with no peer cert (which the HTTP listener now
    /// admits on purpose so probes work without mounting a client
    /// cert).
    pub fn build(
        peer_chain: Option<&[CertificateDer<'_>]>,
        identity_source: IdentitySource,
        client_auth: ClientAuthEnforcement,
    ) -> Option<Self> {
        let leaf = peer_chain.and_then(<[CertificateDer<'_>]>::first);
        let (channel_identity, principal) = match leaf {
            Some(leaf) => {
                let leaf_der: &[u8] = leaf.as_ref();
                let channel = channel_identity_from_cert(leaf_der).map(Arc::new);
                let principal: Option<Arc<dyn AuthPrincipal + Send + Sync>> =
                    if matches!(identity_source, IdentitySource::Channel) {
                        principal_from_cert(leaf_der).map(|p| Arc::new(p) as _)
                    } else {
                        None
                    };
                (channel, principal)
            }
            None => (None, None),
        };
        if channel_identity.is_none()
            && principal.is_none()
            && matches!(client_auth, ClientAuthEnforcement::Disabled)
        {
            return None;
        }
        Some(Self {
            channel_identity,
            principal,
            client_auth,
        })
    }
}

/// Per-request gate that 401s when `client_auth: required` is
/// configured AND the connection presented no verified client cert.
/// Wired onto the *authenticated* HTTP router only; `/health` and
/// `/v1/ready` live on the unauthenticated router and bypass this
/// gate by construction so Kubernetes liveness/readiness probes work
/// over TLS without needing to mount a client certificate. Connections
/// that did present a valid cert pass through unchanged.
pub(crate) async fn require_channel_identity(req: Request<Body>, next: Next) -> Response {
    if let Some(per_conn) = req.extensions().get::<Arc<PerConnTls>>()
        && matches!(per_conn.client_auth, ClientAuthEnforcement::Required)
        && per_conn.channel_identity.is_none()
    {
        return (StatusCode::UNAUTHORIZED, "client certificate required").into_response();
    }
    next.run(req).await
}

/// Per-request middleware that:
///
/// 1. Always inserts a [`runtime_auth::ChannelIdentity`] extension
///    when one was pre-computed for this connection (regardless of
///    [`IdentitySource`]) so audit consumers can see which cert was
///    on the wire.
/// 2. When [`IdentitySource::Channel`] was configured at startup
///    AND a peer cert was presented, additionally promotes the
///    pre-computed [`runtime_auth::MtlsPrincipal`] to the request's
///    auth principal.
///
/// Must run **after** the request-context middleware (which inserts
/// the `Arc<dyn AuthRequestContext>` extension) and **before** the
/// auth layer (so the principal is in place when its short-circuit
/// check runs).
pub(crate) async fn mtls_request_layer(req: Request<Body>, next: Next) -> Response {
    let (mut parts, body) = req.into_parts();

    if let Some(per_conn) = parts.extensions.get::<Arc<PerConnTls>>().cloned() {
        if let Some(channel) = per_conn.channel_identity.as_ref() {
            // ChannelIdentity is `Clone`; cheap (small struct, owned strings).
            parts.extensions.insert(ChannelIdentity::clone(channel));
        }

        if let Some(principal) = per_conn.principal.as_ref() {
            let principal = Arc::clone(principal);
            if let Some(ctx) = parts
                .extensions
                .get::<Arc<dyn AuthRequestContext + Send + Sync>>()
                && let Err(err) = ctx.set_auth_principal(Arc::clone(&principal))
            {
                tracing::warn!(%err, "mTLS: failed to set auth principal on request context");
            }
            parts.extensions.insert(principal);
        }
    }

    let req = Request::from_parts(parts, body);
    next.run(req).await
}

/// Wrap a per-connection [`axum::Router`] with the [`PerConnTls`]
/// extension when there is something to install. Convenience helper
/// used by the TLS accept loop so the extension type stays an
/// implementation detail of this module.
pub(crate) fn install_per_conn_extension(
    router: axum::Router,
    per_conn: Option<PerConnTls>,
) -> axum::Router {
    match per_conn {
        Some(p) => router.layer(Extension(Arc::new(p))),
        None => router,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_returns_none_for_plain_https_in_anonymous_mode() {
        // No peer cert at all, with enforcement disabled: nothing to
        // install — build() short-circuits to None.
        assert!(
            PerConnTls::build(
                None,
                IdentitySource::Anonymous,
                ClientAuthEnforcement::Disabled
            )
            .is_none()
        );
        // Empty chain.
        assert!(
            PerConnTls::build(
                Some(&[]),
                IdentitySource::Channel,
                ClientAuthEnforcement::Disabled
            )
            .is_none()
        );
    }

    #[test]
    fn build_returns_some_when_required_even_without_cert() {
        // With `client_auth: required`, the per-conn context must be
        // installed even when no peer cert was presented so the
        // route-layer gate can observe the enforcement value and 401
        // non-probe requests.
        let per_conn = PerConnTls::build(
            None,
            IdentitySource::Anonymous,
            ClientAuthEnforcement::Required,
        )
        .expect("per-conn context installed under Required");
        assert!(per_conn.channel_identity.is_none());
        assert!(per_conn.principal.is_none());
        assert!(matches!(
            per_conn.client_auth,
            ClientAuthEnforcement::Required
        ));
    }
}
