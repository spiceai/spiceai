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

//! Requests without credentials, on a runtime without `runtime.auth`.
//!
//! Such a request gets one of two levels of access, decided by the listener it arrives on.
//! A listener bound only to loopback serves it as the local operator ([`LocalPrincipal`], full
//! access), because only processes on this machine can reach it. Any other listener serves it
//! read-only: it carries no principal, and every write check refuses a request without one. A
//! request on a loopback listener that came through a proxy (it carries `Forwarded` or
//! `X-Forwarded-For`) is a network caller, so it is served read-only too.

use std::{
    borrow::Cow,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use http::HeaderMap;

use crate::{AuthPrincipal, AuthPrincipalRef, AuthRequestContext};

/// Why a request without credentials is served read-only: the clause every refusal it gets
/// shares.
pub const READ_ONLY_BECAUSE: &str = "because it reached Spice over the network or through a proxy and `runtime.auth` is not configured";

/// How a caller without credentials gets write access: the end of every refusal it gets.
pub const HOW_TO_WRITE: &str = "Configure `runtime.auth` with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`) and send it with the request. See https://spiceai.org/docs/api/auth";

/// The local operator's stable id ([`AuthPrincipal::stable_id`]): the scope of what it owns,
/// such as its async query jobs.
pub const LOCAL_OPERATOR_ID: &str = "local:operator";

/// Whether `principal` is the local operator ([`LocalPrincipal`]).
#[must_use]
pub fn is_local_operator(principal: &dyn AuthPrincipal) -> bool {
    principal.stable_id().as_deref() == Some(LOCAL_OPERATOR_ID)
}

/// The local operator: a request without credentials on a listener bound only to loopback, on
/// a runtime without `runtime.auth`. Only processes on this machine reach such a listener, so
/// it has full access, as `spice run` always had.
pub struct LocalPrincipal;

impl AuthPrincipal for LocalPrincipal {
    fn username(&self) -> &'static str {
        "local"
    }

    fn groups(&self) -> &[&str] {
        &["read_write"]
    }

    /// The operator's own identity, never the anonymous one, so what the operator owns (the
    /// tool steps a model took for it, its async query jobs) is not given to a caller on
    /// another listener.
    fn stable_id(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(LOCAL_OPERATOR_ID))
    }

    /// The operator reads every dataset, as a caller without credentials on another listener
    /// does, so they share the results cache, and a response says `Results-Cache-Scope: shared`
    /// as it did before the operator had a principal.
    fn results_cache_is_shared(&self) -> bool {
        true
    }
}

/// How a listener serves a request that carries no credentials, on a runtime without
/// `runtime.auth`. Decided once per listener, from its bind address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnonymousAccess {
    /// Bound only to loopback: the request is the local operator, with full access.
    LocalOperator,
    /// Bound beyond loopback: the request is read-only.
    ReadOnly,
}

impl AnonymousAccess {
    /// The access of a listener bound to `bind_address`.
    #[must_use]
    pub fn of_listener(bind_address: &SocketAddr) -> Self {
        if util::net::is_loopback_addr(bind_address) {
            Self::LocalOperator
        } else {
            Self::ReadOnly
        }
    }

    /// The principal for a request on this listener that carries no credentials and these
    /// `headers` (or gRPC metadata, which travels as headers): the local operator on a
    /// loopback-only listener, unless the request came through a proxy; otherwise none.
    #[must_use]
    pub fn principal_for(self, headers: &HeaderMap) -> Option<AuthPrincipalRef> {
        match self {
            Self::LocalOperator if !is_forwarded(headers) => Some(Arc::new(LocalPrincipal)),
            Self::LocalOperator | Self::ReadOnly => None,
        }
    }

    /// Gives `context`, a request on this listener with these `headers`, the principal
    /// [`Self::principal_for`] decides, unless it already has one (a client certificate).
    pub fn apply(self, headers: &HeaderMap, context: &dyn AuthRequestContext) {
        if context.auth_principal().is_some() {
            return;
        }
        if let Some(principal) = self.principal_for(headers) {
            // Only another layer setting a principal first makes this fail, and that
            // principal stands.
            let _ = context.set_auth_principal(principal);
        }
    }

    /// Whether the startup line for this listener is a warning.
    #[must_use]
    pub fn startup_line_warns(self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    /// The line `listener` (`HTTP`, `Flight`), bound to `bind_address`, logs at startup to say
    /// how it serves requests without credentials.
    #[must_use]
    pub fn startup_line(self, listener: &str, bind_address: &SocketAddr) -> String {
        match self {
            Self::LocalOperator => format!(
                "Spice Runtime {listener} on {bind_address} serves requests without credentials as the local operator, with full access, because it listens only on loopback and `runtime.auth` is not configured. A request that came through a proxy (it carries `Forwarded` or `X-Forwarded-For`) is served read-only. Configure `runtime.auth` before listening on other addresses. See https://spiceai.org/docs/api/auth"
            ),
            Self::ReadOnly => {
                let unspecified = match bind_address.ip() {
                    IpAddr::V4(ip) if ip.is_unspecified() => " (0.0.0.0 is not a loopback address)",
                    IpAddr::V6(ip) if ip.is_unspecified() => " (:: is not a loopback address)",
                    IpAddr::V4(_) | IpAddr::V6(_) => "",
                };
                format!(
                    "Spice Runtime {listener} on {bind_address} serves requests without credentials read-only, because it listens beyond loopback{unspecified} and `runtime.auth` is not configured: any caller who can reach it can query every dataset, but cannot write data, call read-write tools, or read other callers' task history. To allow writes, configure `runtime.auth` with an API key ending in `:rw`. See https://spiceai.org/docs/api/auth"
                )
            }
        }
    }
}

/// Whether a request came through a proxy: it carries `Forwarded` or `X-Forwarded-For`, as a
/// header or as gRPC metadata.
#[must_use]
pub fn is_forwarded(headers: &HeaderMap) -> bool {
    headers.contains_key(http::header::FORWARDED) || headers.contains_key("x-forwarded-for")
}

/// The refusal of a route that changes data, for a request without credentials.
#[must_use]
pub fn write_route_refusal() -> String {
    format!(
        "This route changes data, and the request has no credentials, which this runtime serves read-only {READ_ONLY_BECAUSE}. {HOW_TO_WRITE}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, http::HeaderValue::from_static(value));
        }
        headers
    }

    fn addr(addr: &str) -> SocketAddr {
        addr.parse().expect("a socket address")
    }

    /// The listener decides: loopback-only is the local operator, anything else read-only.
    #[test]
    fn a_listener_decides_from_its_bind_address() {
        for bound in ["127.0.0.1:8090", "[::1]:50051", "127.0.0.2:8090"] {
            assert_eq!(
                AnonymousAccess::of_listener(&addr(bound)),
                AnonymousAccess::LocalOperator,
                "{bound}"
            );
        }
        for bound in ["0.0.0.0:8090", "[::]:8090", "10.0.0.5:8090"] {
            assert_eq!(
                AnonymousAccess::of_listener(&addr(bound)),
                AnonymousAccess::ReadOnly,
                "{bound}"
            );
        }
    }

    /// The local operator has write access and its own identity; a proxied request on the
    /// same listener, and any request on another listener, has no principal.
    #[test]
    fn only_a_direct_request_on_a_loopback_listener_is_the_local_operator() {
        let local = AnonymousAccess::LocalOperator
            .principal_for(&headers(&[]))
            .expect("a direct loopback request is the local operator");
        assert_eq!(local.username(), "local");
        assert!(crate::principal_has_write_access(&local));
        assert_eq!(local.stable_id().as_deref(), Some(LOCAL_OPERATOR_ID));
        assert!(is_local_operator(local.as_ref()));
        assert!(!is_local_operator(&ReadOnlyCertificate));
        assert!(local.results_cache_is_shared());

        for forwarded in [
            headers(&[("forwarded", "for=192.0.2.60;proto=http")]),
            headers(&[("x-forwarded-for", "192.0.2.60")]),
            headers(&[("X-Forwarded-For", "")]),
        ] {
            assert!(
                AnonymousAccess::LocalOperator
                    .principal_for(&forwarded)
                    .is_none(),
                "a proxied request is read-only: {forwarded:?}"
            );
        }
        assert!(
            AnonymousAccess::ReadOnly
                .principal_for(&headers(&[]))
                .is_none()
        );
    }

    /// A request context that holds one principal, as a request's does.
    #[derive(Default)]
    struct Context(std::sync::OnceLock<AuthPrincipalRef>);

    impl AuthRequestContext for Context {
        fn set_auth_principal(
            &self,
            auth_principal: AuthPrincipalRef,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.0
                .set(auth_principal)
                .map_err(|_| "a principal is already set".into())
        }

        fn auth_principal(&self) -> Option<&AuthPrincipalRef> {
            self.0.get()
        }
    }

    /// A verified client certificate's principal, with read access only.
    struct ReadOnlyCertificate;

    impl AuthPrincipal for ReadOnlyCertificate {
        fn username(&self) -> &'static str {
            "client.example"
        }

        fn groups(&self) -> &[&str] {
            &["read"]
        }
    }

    /// A request that already has a principal (a client certificate) keeps it on every
    /// listener; one without gets the listener's decision.
    #[test]
    fn apply_keeps_a_certificate_and_otherwise_decides_by_listener() {
        let certified = Context::default();
        certified
            .set_auth_principal(Arc::new(ReadOnlyCertificate))
            .expect("an empty context takes a principal");
        AnonymousAccess::LocalOperator.apply(&headers(&[]), &certified);
        let kept = certified.auth_principal().expect("the certificate stays");
        assert_eq!(kept.username(), "client.example");
        assert!(!crate::principal_has_write_access(kept));

        let on_loopback = Context::default();
        AnonymousAccess::LocalOperator.apply(&headers(&[]), &on_loopback);
        assert_eq!(
            on_loopback.auth_principal().map(|p| p.username()),
            Some("local")
        );

        let beyond_loopback = Context::default();
        AnonymousAccess::ReadOnly.apply(&headers(&[]), &beyond_loopback);
        assert!(beyond_loopback.auth_principal().is_none());

        let proxied = Context::default();
        AnonymousAccess::LocalOperator
            .apply(&headers(&[("x-forwarded-for", "192.0.2.60")]), &proxied);
        assert!(proxied.auth_principal().is_none());
    }

    #[test]
    fn startup_lines() {
        assert_eq!(
            AnonymousAccess::LocalOperator.startup_line("HTTP", &addr("127.0.0.1:8090")),
            "Spice Runtime HTTP on 127.0.0.1:8090 serves requests without credentials as the local operator, with full access, because it listens only on loopback and `runtime.auth` is not configured. A request that came through a proxy (it carries `Forwarded` or `X-Forwarded-For`) is served read-only. Configure `runtime.auth` before listening on other addresses. See https://spiceai.org/docs/api/auth"
        );
        assert_eq!(
            AnonymousAccess::ReadOnly.startup_line("HTTP", &addr("0.0.0.0:8090")),
            "Spice Runtime HTTP on 0.0.0.0:8090 serves requests without credentials read-only, because it listens beyond loopback (0.0.0.0 is not a loopback address) and `runtime.auth` is not configured: any caller who can reach it can query every dataset, but cannot write data, call read-write tools, or read other callers' task history. To allow writes, configure `runtime.auth` with an API key ending in `:rw`. See https://spiceai.org/docs/api/auth"
        );
        assert_eq!(
            AnonymousAccess::ReadOnly.startup_line("Flight", &addr("[::]:50051")),
            "Spice Runtime Flight on [::]:50051 serves requests without credentials read-only, because it listens beyond loopback (:: is not a loopback address) and `runtime.auth` is not configured: any caller who can reach it can query every dataset, but cannot write data, call read-write tools, or read other callers' task history. To allow writes, configure `runtime.auth` with an API key ending in `:rw`. See https://spiceai.org/docs/api/auth"
        );
        assert_eq!(
            AnonymousAccess::ReadOnly.startup_line("HTTP", &addr("10.0.0.5:8090")),
            "Spice Runtime HTTP on 10.0.0.5:8090 serves requests without credentials read-only, because it listens beyond loopback and `runtime.auth` is not configured: any caller who can reach it can query every dataset, but cannot write data, call read-write tools, or read other callers' task history. To allow writes, configure `runtime.auth` with an API key ending in `:rw`. See https://spiceai.org/docs/api/auth"
        );
    }

    #[test]
    fn write_route_refusal_text() {
        assert_eq!(
            write_route_refusal(),
            "This route changes data, and the request has no credentials, which this runtime serves read-only because it reached Spice over the network or through a proxy and `runtime.auth` is not configured. Configure `runtime.auth` with a read-write API key (a `runtime.auth.api-key.keys` entry ending in `:rw`) and send it with the request. See https://spiceai.org/docs/api/auth"
        );
    }
}
