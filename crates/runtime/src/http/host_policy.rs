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

//! The `Host` and `Origin` a request must carry for the HTTP listener to answer it.
//!
//! A runtime that does not require authentication (no `runtime.auth`, and
//! `runtime.tls.client_auth_mode` not `required`) answers only requests addressed
//! to it by an IP address, a local name (`localhost`, `*.localhost`) or a name in
//! `runtime.allowed_hosts`, and only browser requests from a local origin or from
//! `runtime.cors.allowed_origins` when CORS is enabled. This is the standard defense
//! against DNS rebinding and cross-site requests. A runtime that requires
//! authentication relies on it, and checks `Host` only when `runtime.allowed_hosts`
//! is set.
//!
//! Both policies are built once when the listener starts, so the per-request check
//! compares the request's headers against pre-normalized entries without allocating.

use std::{net::IpAddr, sync::Arc};

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderValue, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use runtime_request_context::{Extension, RequestContext};
use spicepod::component::runtime::CorsConfig;

const ALLOWED_HOSTS_DOCS: &str =
    "https://spiceai.org/docs/reference/spicepod/runtime#allowed_hosts";
const CORS_DOCS: &str = "https://spiceai.org/docs/reference/spicepod/runtime#cors";

/// The `Host` and `Origin` policies of one HTTP listener.
#[derive(Debug, Clone)]
pub(crate) struct RequestOriginPolicy {
    hosts: HostPolicy,
    origins: OriginPolicy,
    requires_auth: bool,
    /// The port a `Host` without one names: 443 over TLS, else 80.
    default_port: u16,
}

#[derive(Debug, Clone)]
enum HostPolicy {
    /// Any `Host`.
    Any,
    /// IP addresses, local names, and these entries.
    Allow(Arc<[HostEntry]>),
}

/// One `runtime.allowed_hosts` entry, normalized: lowercase, no trailing dot, no
/// IPv6 brackets.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HostEntry {
    host: String,
    /// When set, the request's port (or its default port) must equal it.
    port: Option<u16>,
    /// A leading-dot entry (`.corp.example`): the name and every name under it.
    subdomains: bool,
}

#[derive(Debug, Clone)]
enum OriginPolicy {
    /// Any `Origin`, or none checked.
    Any,
    /// `http` and `https` origins on a local name, any port.
    Local,
    /// `runtime.cors.allowed_origins` as written.
    List(Arc<[WebOrigin]>),
}

/// An RFC 6454 origin: `null`, or a scheme, host and effective port.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WebOrigin {
    Null,
    Tuple {
        scheme: String,
        host: String,
        port: Option<u16>,
    },
}

impl RequestOriginPolicy {
    /// The policies for a listener.
    ///
    /// `requires_auth`: the runtime authenticates every caller (`runtime.auth`
    /// is configured, or client certificates are required). `allowed_hosts`:
    /// `runtime.allowed_hosts`, already resolved against the deprecated
    /// `runtime.mcp.allowed_hosts`. `tls`: the listener serves HTTPS.
    pub(crate) fn new(
        requires_auth: bool,
        allowed_hosts: Option<&[String]>,
        cors: &CorsConfig,
        tls: bool,
    ) -> Self {
        let hosts = match allowed_hosts {
            Some(entries) if entries.iter().any(|entry| entry.trim() == "*") => HostPolicy::Any,
            None if requires_auth => HostPolicy::Any,
            entries => HostPolicy::Allow(
                entries
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|entry| HostEntry::parse(entry))
                    .collect(),
            ),
        };
        let origins = if requires_auth {
            OriginPolicy::Any
        } else if cors.enabled {
            if cors
                .allowed_origins
                .iter()
                .any(|origin| origin.trim() == "*")
            {
                OriginPolicy::Any
            } else {
                OriginPolicy::List(
                    cors.allowed_origins
                        .iter()
                        .filter_map(|origin| WebOrigin::parse(origin))
                        .collect(),
                )
            }
        } else {
            OriginPolicy::Local
        };
        Self {
            hosts,
            origins,
            requires_auth,
            default_port: if tls { 443 } else { 80 },
        }
    }

    /// The refusal for a request, or `None` when the listener answers it.
    /// `host` is the `Host` header, or else the HTTP/2 `:authority`.
    fn refusal(&self, host: Option<&[u8]>, origin: Option<&HeaderValue>) -> Option<String> {
        if let (HostPolicy::Allow(entries), Some(host)) = (&self.hosts, host)
            && !host_allowed(entries, host, self.default_port)
        {
            let host = String::from_utf8_lossy(host);
            return Some(host_refusal_message(
                split_authority(&host).0,
                self.requires_auth,
            ));
        }
        if let Some(origin) = origin
            && !self.origin_allowed(origin)
        {
            return Some(origin_refusal_message(&String::from_utf8_lossy(
                origin.as_bytes(),
            )));
        }
        None
    }

    fn origin_allowed(&self, origin: &HeaderValue) -> bool {
        let parsed = || origin.to_str().ok().and_then(WebOrigin::parse);
        match &self.origins {
            OriginPolicy::Any => true,
            OriginPolicy::Local => matches!(
                parsed(),
                Some(WebOrigin::Tuple { scheme, host, .. })
                    if (scheme == "http" || scheme == "https") && util::net::is_loopback_host(&host)
            ),
            OriginPolicy::List(allowed) => parsed().is_some_and(|origin| allowed.contains(&origin)),
        }
    }
}

/// Marks a request the policy refused, for the request metrics: as for a
/// request authentication refused, its client dimensions come from headers a
/// caller chose, so the metrics record only the runtime-defined labels.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestOriginRefused;

impl Extension for RequestOriginRefused {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Answers `403` to a request whose `Host` or `Origin` the listener does not
/// accept, before authentication, routing or a handler runs.
pub(crate) async fn check_request_origin(
    State(policy): State<Arc<RequestOriginPolicy>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // HTTP/2 carries the host in `:authority`, which hyper keeps on the URI.
    let host = req
        .headers()
        .get(header::HOST)
        .map(HeaderValue::as_bytes)
        .or_else(|| {
            req.uri()
                .authority()
                .map(|authority| authority.as_str().as_bytes())
        });
    if let Some(message) = policy.refusal(host, req.headers().get(header::ORIGIN)) {
        if let Some(context) = req.extensions().get::<Arc<RequestContext>>() {
            context.insert_extension(RequestOriginRefused);
        }
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": message })),
        )
            .into_response();
    }
    next.run(req).await
}

/// The 403 for a `Host` the listener does not answer to.
fn host_refusal_message(host: &str, requires_auth: bool) -> String {
    if requires_auth {
        format!(
            "Host '{host}' is not allowed: this runtime answers only to IP addresses, localhost and the names in `runtime.allowed_hosts`. Add '{host}' to `runtime.allowed_hosts`. See {ALLOWED_HOSTS_DOCS}"
        )
    } else {
        format!(
            "Host '{host}' is not allowed: this runtime requires no authentication, so it answers only to IP addresses, localhost and the names in `runtime.allowed_hosts`. Add '{host}' to `runtime.allowed_hosts`, or configure `runtime.auth`. See {ALLOWED_HOSTS_DOCS}"
        )
    }
}

/// The 403 for an `Origin` the listener does not accept.
fn origin_refusal_message(origin: &str) -> String {
    format!(
        "Origin '{origin}' is not allowed: this runtime requires no authentication, so it accepts browser requests only from local origins, or from `runtime.cors.allowed_origins` when `runtime.cors.enabled` is true. Add the origin there, or configure `runtime.auth`. See {CORS_DOCS}"
    )
}

/// The startup line of a listener that requires no authentication.
#[must_use]
pub fn no_authentication_message(address: &str) -> String {
    format!(
        "HTTP on {address} requires no authentication, so it answers only to IP addresses and localhost names. Add host names with `runtime.allowed_hosts`, or configure `runtime.auth`. See {ALLOWED_HOSTS_DOCS}"
    )
}

fn host_allowed(entries: &[HostEntry], host: &[u8], default_port: u16) -> bool {
    let Ok(authority) = std::str::from_utf8(host) else {
        return false;
    };
    let (name, port) = split_authority(authority);
    let name = name.strip_suffix('.').unwrap_or(name);
    if name.parse::<IpAddr>().is_ok() || util::net::is_loopback_host(name) {
        return true;
    }
    let port = port.unwrap_or(default_port);
    entries.iter().any(|entry| entry.matches(name, port))
}

/// Splits `host[:port]` or `[ipv6][:port]` into the host, without brackets, and
/// the port. A port that is not a number is left on the host, which then matches
/// nothing.
fn split_authority(authority: &str) -> (&str, Option<u16>) {
    if let Some(rest) = authority.strip_prefix('[') {
        return match rest.split_once(']') {
            Some((host, tail)) => (
                host,
                tail.strip_prefix(':').and_then(|port| port.parse().ok()),
            ),
            None => (authority, None),
        };
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => match port.parse() {
            Ok(port) => (host, Some(port)),
            Err(_) => (authority, None),
        },
        _ => (authority, None),
    }
}

impl HostEntry {
    fn parse(entry: &str) -> Option<Self> {
        let entry = entry.trim();
        if entry.is_empty() {
            return None;
        }
        let (subdomains, entry) = match entry.strip_prefix('.') {
            Some(rest) => (true, rest),
            None => (false, entry),
        };
        let (host, port) = if entry.contains("://") {
            let uri = http::Uri::try_from(entry).ok()?;
            let authority = uri.authority()?;
            (authority.host().to_string(), authority.port_u16())
        } else {
            let (host, port) = split_authority(entry);
            (host.to_string(), port)
        };
        let host = host.trim_matches(['[', ']']);
        let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
        (!host.is_empty()).then_some(Self {
            host,
            port,
            subdomains,
        })
    }

    /// Whether `name` (no trailing dot) on `port` matches this entry.
    fn matches(&self, name: &str, port: u16) -> bool {
        let name_matches = name.eq_ignore_ascii_case(&self.host)
            || (self.subdomains
                && name.len() > self.host.len()
                && name
                    .get(name.len() - self.host.len()..)
                    .is_some_and(|suffix| suffix.eq_ignore_ascii_case(&self.host))
                && name.as_bytes().get(name.len() - self.host.len() - 1) == Some(&b'.'));
        name_matches && self.port.is_none_or(|entry_port| entry_port == port)
    }
}

impl WebOrigin {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("null") {
            return Some(Self::Null);
        }
        let uri = http::Uri::try_from(value).ok()?;
        let scheme = uri.scheme_str()?.to_ascii_lowercase();
        let authority = uri.authority()?;
        let port = authority.port_u16().or(match scheme.as_str() {
            "http" | "ws" => Some(80),
            "https" | "wss" => Some(443),
            _ => None,
        });
        let host = authority
            .host()
            .trim_matches(['[', ']'])
            .to_ascii_lowercase();
        Some(Self::Tuple { scheme, host, port })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cors(enabled: bool, origins: &[&str]) -> CorsConfig {
        CorsConfig {
            enabled,
            allowed_origins: origins.iter().map(ToString::to_string).collect(),
        }
    }

    fn policy(requires_auth: bool, hosts: Option<&[&str]>) -> RequestOriginPolicy {
        let hosts: Option<Vec<String>> =
            hosts.map(|hosts| hosts.iter().map(ToString::to_string).collect());
        RequestOriginPolicy::new(requires_auth, hosts.as_deref(), &cors(false, &["*"]), false)
    }

    fn host_refused(policy: &RequestOriginPolicy, host: &str) -> bool {
        policy.refusal(Some(host.as_bytes()), None).is_some()
    }

    #[test]
    fn splits_host_and_port() {
        let cases = [
            ("localhost", ("localhost", None)),
            ("localhost:8090", ("localhost", Some(8090))),
            ("[::1]", ("::1", None)),
            ("[::1]:8090", ("::1", Some(8090))),
            ("::1", ("::1", None)),
            ("example.com:http", ("example.com:http", None)),
            ("[::1", ("[::1", None)),
        ];
        for (authority, expected) in cases {
            assert_eq!(split_authority(authority), expected, "{authority}");
        }
    }

    #[test]
    fn normalizes_allowed_host_entries() {
        let entry = |host: &str, port: Option<u16>, subdomains: bool| HostEntry {
            host: host.to_string(),
            port,
            subdomains,
        };
        let cases = [
            ("spice.internal", Some(entry("spice.internal", None, false))),
            (
                "Spice.Internal.",
                Some(entry("spice.internal", None, false)),
            ),
            (
                "spice.internal:8443",
                Some(entry("spice.internal", Some(8443), false)),
            ),
            (
                "https://spice.internal",
                Some(entry("spice.internal", None, false)),
            ),
            (
                "https://spice.internal:8443/",
                Some(entry("spice.internal", Some(8443), false)),
            ),
            (".corp.example", Some(entry("corp.example", None, true))),
            ("[fd00::1]:8090", Some(entry("fd00::1", Some(8090), false))),
            ("  ", None),
        ];
        for (raw, expected) in cases {
            assert_eq!(HostEntry::parse(raw), expected, "{raw}");
        }
    }

    #[test]
    fn a_runtime_without_authentication_answers_local_names_ips_and_listed_hosts() {
        let unlisted = policy(false, None);
        for host in [
            "localhost",
            "localhost:8090",
            "LOCALHOST.",
            "app.localhost:8090",
            "127.0.0.1:8090",
            "127.9.9.9",
            "[::1]:8090",
            "0.0.0.0:8090",
            "10.0.0.7:8090",
            "[fd00::1]",
        ] {
            assert!(!host_refused(&unlisted, host), "{host} must be answered");
        }
        for host in [
            "app.example.com:8090",
            "localhost.example.com",
            "notlocalhost",
            "spice.internal",
        ] {
            assert!(host_refused(&unlisted, host), "{host} must be refused");
        }

        let listed = policy(
            false,
            Some(&["spice.internal", ".corp.example", "api.example:8443"]),
        );
        for host in [
            "spice.internal",
            "SPICE.internal:8090",
            "corp.example",
            "a.b.corp.example:8090",
            "api.example:8443",
        ] {
            assert!(!host_refused(&listed, host), "{host} must be answered");
        }
        for host in [
            "xcorp.example",
            "api.example:8090",
            "api.example",
            "other.internal",
        ] {
            assert!(host_refused(&listed, host), "{host} must be refused");
        }
    }

    #[test]
    fn an_authenticated_runtime_checks_hosts_only_when_listed() {
        assert!(!host_refused(&policy(true, None), "app.example.com"));
        let listed = policy(true, Some(&["spice.internal"]));
        assert!(!host_refused(&listed, "spice.internal:8090"));
        assert!(!host_refused(&listed, "10.0.0.7:8090"));
        assert!(host_refused(&listed, "app.example.com"));
        assert!(!host_refused(
            &policy(false, Some(&["*"])),
            "app.example.com"
        ));
    }

    #[test]
    fn origins_are_local_or_the_enabled_cors_list() {
        let origin_refused = |policy: &RequestOriginPolicy, origin: &str| {
            policy
                .refusal(
                    None,
                    Some(&HeaderValue::from_str(origin).expect("valid header")),
                )
                .is_some()
        };
        let local = RequestOriginPolicy::new(false, None, &cors(false, &["*"]), false);
        for origin in [
            "http://localhost:3000",
            "https://app.localhost",
            "http://127.0.0.1:8090",
            "http://[::1]:5173",
        ] {
            assert!(!origin_refused(&local, origin), "{origin} must be accepted");
        }
        for origin in [
            "https://app.example.com",
            "null",
            "http://10.0.0.7:8090",
            "not an origin",
        ] {
            assert!(origin_refused(&local, origin), "{origin} must be refused");
        }

        let listed = RequestOriginPolicy::new(
            false,
            None,
            &cors(true, &["https://app.example.com"]),
            false,
        );
        assert!(!origin_refused(&listed, "https://app.example.com"));
        assert!(!origin_refused(&listed, "https://app.example.com:443"));
        assert!(origin_refused(&listed, "https://app.example.com:8443"));
        assert!(origin_refused(&listed, "http://localhost:3000"));

        let any = RequestOriginPolicy::new(false, None, &cors(true, &["*"]), false);
        assert!(!origin_refused(&any, "https://app.example.com"));
        let authenticated = RequestOriginPolicy::new(true, None, &cors(false, &["*"]), false);
        assert!(!origin_refused(&authenticated, "https://app.example.com"));
    }

    #[test]
    fn refusals_name_the_setting_to_change() {
        let refusal = policy(false, None)
            .refusal(
                Some(b"app.example.com:8090"),
                Some(&HeaderValue::from_static("http://localhost:3000")),
            )
            .expect("refused");
        assert_eq!(
            refusal,
            "Host 'app.example.com' is not allowed: this runtime requires no authentication, so it answers only to IP addresses, localhost and the names in `runtime.allowed_hosts`. Add 'app.example.com' to `runtime.allowed_hosts`, or configure `runtime.auth`. See https://spiceai.org/docs/reference/spicepod/runtime#allowed_hosts"
        );
        let refusal = policy(false, None)
            .refusal(
                Some(b"localhost:8090"),
                Some(&HeaderValue::from_static("https://app.example.com")),
            )
            .expect("refused");
        assert_eq!(
            refusal,
            "Origin 'https://app.example.com' is not allowed: this runtime requires no authentication, so it accepts browser requests only from local origins, or from `runtime.cors.allowed_origins` when `runtime.cors.enabled` is true. Add the origin there, or configure `runtime.auth`. See https://spiceai.org/docs/reference/spicepod/runtime#cors"
        );
        assert_eq!(
            no_authentication_message("0.0.0.0:8090"),
            "HTTP on 0.0.0.0:8090 requires no authentication, so it answers only to IP addresses and localhost names. Add host names with `runtime.allowed_hosts`, or configure `runtime.auth`. See https://spiceai.org/docs/reference/spicepod/runtime#allowed_hosts"
        );
    }
}
