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

//! Server-side request forgery (SSRF) protection for HTTP requests whose destination a query
//! chooses.
//!
//! One classifier decides which addresses and host names are internal. URL tables apply it to
//! the URL a query names, to every address that URL's host resolves to, and to every redirect;
//! the HTTPS data connector applies it to every redirect and to every address a redirect's new
//! host resolves to.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};

use object_store_spawn::DestinationGuard;
pub use object_store_spawn::{
    DestinationRefused, REDIRECT_CHAIN_REFUSED, redirect_continues_off_origin,
};
use url::Url;

/// Environment variable that, when set to a truthy value (`1`/`true`/`yes`/`on`),
/// disables the SSRF guard on `http`/`https` URL tables, on redirects to another
/// host from `http`/`https` datasets in file formats such as Parquet or CSV, and
/// on storage endpoints and `file://` locations supplied by an Iceberg REST
/// catalog, allowing them to target private, loopback, and link-local addresses
/// or the local filesystem.
/// Off by default — intended only for operators who knowingly query an internal
/// HTTP object store or a local Iceberg warehouse.
pub const ALLOW_PRIVATE_HOSTS_ENV: &str = "SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS";

/// Whether the SSRF guard is disabled via [`ALLOW_PRIVATE_HOSTS_ENV`].
#[must_use]
pub fn allow_private_hosts() -> bool {
    std::env::var(ALLOW_PRIVATE_HOSTS_ENV).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Returns `true` if `ip` is in a range that must not be reachable through a
/// user-supplied URL: loopback, private/RFC1918, link-local (incl. the
/// `169.254.169.254` cloud-metadata endpoint), unique-local, CGNAT,
/// unspecified, multicast, and other reserved ranges.
#[must_use]
pub fn is_internal_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_internal_ipv4(v4),
        // Unwrap IPv4-mapped/-compatible addresses (e.g. `::ffff:169.254.169.254`)
        // and re-check them as IPv4 so they can't bypass the v4 rules.
        IpAddr::V6(v6) => match v6.to_ipv4() {
            Some(v4) => is_internal_ipv4(v4),
            None => is_internal_ipv6(v6),
        },
    }
}

fn is_internal_ipv4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    // Only the four oldest, universally-stable classifiers are used as methods;
    // the rest are explicit prefix checks so behavior is obvious and portable.
    ip.is_loopback()            // 127.0.0.0/8
        || ip.is_private()      // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()   // 169.254.0.0/16 (incl. 169.254.169.254 metadata)
        || ip.is_unspecified()  // 0.0.0.0
        || o[0] == 0                                 // 0.0.0.0/8 "this network"
        || (o[0] == 100 && (o[1] & 0xc0) == 64)      // 100.64.0.0/10 CGNAT
        || (o[0] == 198 && (o[1] & 0xfe) == 18)      // 198.18.0.0/15 benchmarking
        || (o[0] == 192 && o[1] == 0 && o[2] == 2)   // 192.0.2.0/24 TEST-NET-1
        || (o[0] == 198 && o[1] == 51 && o[2] == 100) // 198.51.100.0/24 TEST-NET-2
        || (o[0] == 203 && o[1] == 0 && o[2] == 113) // 203.0.113.0/24 TEST-NET-3
        || o[0] >= 224 // 224.0.0.0/4 multicast, 240.0.0.0/4 reserved, 255.255.255.255 broadcast
}

fn is_internal_ipv6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback()                  // ::1
        || ip.is_unspecified()        // ::
        || (first & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (first & 0xffc0) == 0xfe80 // fe80::/10 link local
        || (first & 0xff00) == 0xff00 // ff00::/8 multicast
}

/// Returns `true` for hostnames that never legitimately point at a remote
/// endpoint and must be blocked without needing DNS resolution: `localhost`
/// (and `*.localhost`, RFC 6761) and the well-known cloud-metadata names.
#[must_use]
pub fn is_blocked_internal_hostname(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower == "localhost"
        || lower.ends_with(".localhost")
        || lower == "metadata.google.internal"
        || lower == "metadata.goog"
}

/// Whether a redirect to `url` must be refused because its host is an internal IP literal or
/// a well-known internal name.
///
/// This check needs no DNS, so it can run inside a synchronous redirect policy. A host name it
/// lets through can still resolve to an internal address; URL tables also check every resolved
/// address (see [`url_table_destination_guard`]).
#[must_use]
pub fn redirect_target_is_internal(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => is_internal_ip(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => is_internal_ip(IpAddr::V6(ip)),
        Some(url::Host::Domain(domain)) => is_blocked_internal_hostname(domain),
        None => false,
    }
}

/// Scheme prefix under which `http`/`https` URL tables register their object stores.
///
/// A URL table's store refuses internal destinations; a dataset's store for the same host does
/// not. Registering the two under different schemes keeps them in separate registry entries, so
/// neither is ever served in place of the other.
pub const URL_TABLE_SCHEME_PREFIX: &str = "spice-url-table+";

/// The address a URL table whose store is registered under `url` actually reads, or `None`
/// when `url` is not a URL table's store URL.
#[must_use]
pub fn url_table_target(url: &Url) -> Option<Url> {
    let target = url.as_str().strip_prefix(URL_TABLE_SCHEME_PREFIX)?;
    let target = Url::parse(target).ok()?;
    matches!(target.scheme(), "http" | "https").then_some(target)
}

/// What a URL table's HTTP store refuses: any internal resolved address, and any redirect to
/// an internal IP literal or a well-known internal name.
///
/// A proxy from `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` is resolved by the same guard, so a proxy
/// on a private address is refused unless `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true` is set. The
/// guard does not see the address a proxy resolves the URL to; the URL's own pre-flight check
/// and the redirect check still apply.
#[must_use]
pub fn url_table_destination_guard() -> DestinationGuard {
    DestinationGuard::new(
        is_internal_ip,
        redirect_target_is_internal,
        format!(
            "URL tables cannot read from private, loopback, or link-local addresses, including a proxy host that resolves to one. If this runtime intentionally queries an internal HTTP endpoint or uses a private proxy, set {ALLOW_PRIVATE_HOSTS_ENV}=true"
        ),
    )
}

/// What the HTTPS data connector refuses for a host it reaches only through a redirect: any
/// internal address that host resolves to.
#[must_use]
pub fn redirect_destination_guard() -> DestinationGuard {
    DestinationGuard::new(
        is_internal_ip,
        redirect_target_is_internal,
        "the HTTPS data connector does not follow a redirect to a private, loopback, or link-local address",
    )
}

/// `host` as [`RedirectHostResolver`] keys it: `example.com.` and `example.com` name the same host.
#[must_use]
pub fn redirect_host_key(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

/// Resolves the hosts a redirect introduced through a [`DestinationGuard`], so a redirect cannot
/// reach an internal address by name. Every other host, such as the request's own or a proxy's,
/// resolves unchecked.
#[derive(Clone, Debug)]
pub struct RedirectHostResolver {
    guard: DestinationGuard,
    redirect_hosts: Arc<Mutex<HashSet<String>>>,
}

impl RedirectHostResolver {
    #[must_use]
    pub fn new(guard: DestinationGuard) -> Self {
        Self {
            guard,
            redirect_hosts: Arc::default(),
        }
    }

    /// Checks every later lookup of `host` with the guard.
    pub fn check_host(&self, host: &str) {
        self.redirect_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(redirect_host_key(host));
    }
}

impl reqwest::dns::Resolve for RedirectHostResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        let checked = self
            .redirect_hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&redirect_host_key(&host));
        let guard = self.guard.clone();
        Box::pin(async move {
            let addresses: Vec<SocketAddr> = if checked {
                guard.resolve(&host).await?
            } else {
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect()
            };
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Why [`guard_redirects`] refuses a redirect.
const OFF_HOST_REDIRECT_REFUSED: &str = "a redirect off the request's host to a private, loopback, or link-local address is not followed";

/// Why [`guard_redirects`] refuses a redirect under [`RedirectScope::RequestOrigin`].
const OFF_ORIGIN_REDIRECT_REFUSED: &str = "a request that carries a custom credential header follows a redirect only to its own scheme, host, and port. Configure the endpoint URL as the redirect target directly.";

/// Which redirects [`guard_redirects`] follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedirectScope {
    /// A redirect within the request's own host, or to another host that is not private,
    /// loopback, or link-local.
    PublicHosts,
    /// Only a redirect to the request's own scheme, host, and port, for a request that carries a
    /// credential in a header reqwest does not remove when a redirect leaves that origin.
    RequestOrigin,
    /// [`RedirectScope::PublicHosts`] for a request that carries credentials: a redirect chain
    /// that has left the request's host and port does not continue on the host it moved to.
    PublicHostsWithCredentials,
}

/// What [`guard_redirects`] does with one redirect.
#[derive(Debug, PartialEq, Eq)]
enum RedirectCheck {
    /// The redirect stays on the request's own host, which is not checked.
    OwnHost,
    /// The redirect leaves the request's scheme, host, or port under
    /// [`RedirectScope::RequestOrigin`].
    OffOrigin,
    /// The redirect continues a chain on a host it moved to under
    /// [`RedirectScope::PublicHostsWithCredentials`].
    ContinuesOffOrigin,
    /// The redirect leaves for an internal IP literal or a well-known internal name.
    Internal,
    /// The redirect leaves for another host, whose addresses are checked when it resolves.
    OtherHost,
}

/// Classifies a redirect to `target` from a request whose URLs so far are `previous`, the
/// request's own URL first.
fn check_redirect(scope: RedirectScope, previous: &[Url], target: &Url) -> RedirectCheck {
    if scope == RedirectScope::RequestOrigin
        && previous
            .first()
            .is_none_or(|own| own.origin() != target.origin())
    {
        return RedirectCheck::OffOrigin;
    }
    if scope == RedirectScope::PublicHostsWithCredentials
        && redirect_continues_off_origin(previous, target)
    {
        return RedirectCheck::ContinuesOffOrigin;
    }
    let own_host = previous
        .first()
        .and_then(Url::host_str)
        .map(redirect_host_key);
    if own_host.is_some() && own_host == target.host_str().map(redirect_host_key) {
        RedirectCheck::OwnHost
    } else if redirect_target_is_internal(target) {
        RedirectCheck::Internal
    } else {
        RedirectCheck::OtherHost
    }
}

/// Makes `builder` follow at most `max_redirects` redirects within `scope`, and refuse one that
/// leaves the request's own host for a private, loopback, or link-local destination: an internal
/// IP literal, a well-known internal name, or a host that resolves to an internal address. A
/// redirect within the request's own host is followed, whatever that host resolves to.
pub fn guard_redirects(
    builder: reqwest::ClientBuilder,
    max_redirects: usize,
    scope: RedirectScope,
) -> reqwest::ClientBuilder {
    let resolver = RedirectHostResolver::new(DestinationGuard::new(
        is_internal_ip,
        redirect_target_is_internal,
        OFF_HOST_REDIRECT_REFUSED,
    ));
    let redirect_hosts = resolver.clone();
    builder
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= max_redirects {
                return attempt.error("too many redirects");
            }
            match check_redirect(scope, attempt.previous(), attempt.url()) {
                RedirectCheck::OwnHost => attempt.follow(),
                RedirectCheck::OffOrigin => {
                    let refused = format!(
                        "Refused to follow the redirect to {}: {OFF_ORIGIN_REDIRECT_REFUSED}",
                        attempt.url()
                    );
                    attempt.error(refused)
                }
                RedirectCheck::ContinuesOffOrigin => {
                    let refused = format!(
                        "Refused to follow the redirect to {}: {REDIRECT_CHAIN_REFUSED}",
                        attempt.url()
                    );
                    attempt.error(refused)
                }
                RedirectCheck::Internal => {
                    let refused = format!(
                        "Refused to connect to {}: {OFF_HOST_REDIRECT_REFUSED}",
                        attempt.url()
                    );
                    attempt.error(refused)
                }
                RedirectCheck::OtherHost => {
                    // The policy cannot resolve DNS, so the resolver checks the addresses of
                    // each new host a redirect names.
                    if let Some(url::Host::Domain(host)) = attempt.url().host() {
                        redirect_hosts.check_host(host);
                    }
                    attempt.follow()
                }
            }
        }))
        .dns_resolver(Arc::new(resolver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_is_internal_ip_blocks_dangerous_ranges() {
        // Must be blocked (SSRF targets).
        for ip in [
            "127.0.0.1",
            "127.1.2.3",
            "169.254.169.254", // AWS/GCP/Azure IMDS
            "10.0.0.5",
            "192.168.1.1",
            "172.16.0.1",
            "172.31.255.255",
            "100.64.0.1", // CGNAT
            "0.0.0.0",
            "198.18.0.1", // benchmarking
            "240.0.0.1",  // reserved
            "255.255.255.255",
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(is_internal_ip(addr), "{ip} should be treated as internal");
        }

        // IPv6 internal ranges.
        for ip in [
            "::1",                    // loopback
            "::",                     // unspecified
            "fe80::1",                // link local
            "fc00::1",                // unique local
            "fd12:3456::1",           // unique local
            "::ffff:169.254.169.254", // v4-mapped metadata
            "::ffff:127.0.0.1",       // v4-mapped loopback
            "ff02::1",                // multicast
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(is_internal_ip(addr), "{ip} should be treated as internal");
        }

        // Must be allowed (legitimate public addresses).
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",        // example.com
            "2606:4700:4700::1111", // Cloudflare DNS
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(!is_internal_ip(addr), "{ip} should be treated as public");
        }
    }

    #[test]
    fn test_is_blocked_internal_hostname() {
        // Well-known internal names blocked without DNS (case-insensitive).
        for host in [
            "localhost",
            "LOCALHOST",
            "service.localhost",
            "metadata.google.internal",
            "metadata.goog",
        ] {
            assert!(
                is_blocked_internal_hostname(host),
                "{host} should be blocked"
            );
        }

        // Legitimate public hostnames must pass (DNS resolution handles these).
        for host in [
            "example.com",
            "localhost.example.com", // not the `.localhost` TLD
            "metadata.google.com",   // not the metadata sentinel
        ] {
            assert!(
                !is_blocked_internal_hostname(host),
                "{host} should not be blocked by literal match"
            );
        }
    }

    #[test]
    fn redirect_target_is_internal_classifies_each_host_form() {
        for url in [
            "http://169.254.169.254/latest/meta-data/",
            "http://172.30.0.2:18077/secret.parquet",
            "https://[::1]/data",
            "http://[::ffff:10.0.0.1]/data",
            "http://localhost:8080/data",
            "http://metadata.google.internal/computeMetadata/v1/",
        ] {
            let url = Url::parse(url).expect("valid URL");
            assert!(redirect_target_is_internal(&url), "{url} must be refused");
        }
        for url in [
            "https://8.8.8.8/data.parquet",
            "https://example.com/data.parquet",
        ] {
            let url = Url::parse(url).expect("valid URL");
            assert!(!redirect_target_is_internal(&url), "{url} must be allowed");
        }
    }

    #[test]
    fn url_table_target_round_trips_only_http_urls() {
        let store_url =
            Url::parse("spice-url-table+https://example.com:8443/a/b.csv#client_timeout=5s")
                .expect("valid URL");
        assert_eq!(
            url_table_target(&store_url).map(String::from),
            Some("https://example.com:8443/a/b.csv#client_timeout=5s".to_string())
        );

        for url in [
            "https://example.com/a.csv",
            "spice-url-table+s3://bucket/a.csv",
            "spice-url-table+file:///etc/passwd",
        ] {
            let url = Url::parse(url).expect("valid URL");
            assert_eq!(
                url_table_target(&url),
                None,
                "{url} is not a URL table store"
            );
        }
    }

    #[test]
    fn check_redirect_refuses_only_an_internal_target_off_the_request_host() {
        let previous = [Url::parse("http://graphql.example.com/graphql").expect("valid URL")];
        for target in [
            "http://127.0.0.1:18791/graphql",
            "http://10.0.0.1/graphql",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]/graphql",
            "http://localhost/graphql",
            "http://metadata.google.internal/computeMetadata/v1/",
        ] {
            let target = Url::parse(target).expect("valid URL");
            assert_eq!(
                check_redirect(RedirectScope::PublicHosts, &previous, &target),
                RedirectCheck::Internal,
                "{target} must be refused"
            );
        }
        for target in [
            "https://93.184.216.34/graphql",
            "https://api.example.org/graphql",
        ] {
            let target = Url::parse(target).expect("valid URL");
            assert_eq!(
                check_redirect(RedirectScope::PublicHosts, &previous, &target),
                RedirectCheck::OtherHost,
                "{target} must be followed"
            );
        }

        let previous = [Url::parse("http://127.0.0.1:4000/graphql").expect("valid URL")];
        let target = Url::parse("http://127.0.0.1:4000/graphql/").expect("valid URL");
        assert_eq!(
            check_redirect(RedirectScope::PublicHosts, &previous, &target),
            RedirectCheck::OwnHost,
            "a redirect within the request's own host is followed"
        );
    }

    #[test]
    fn check_redirect_in_the_request_origin_scope_refuses_every_target_off_the_origin() {
        let previous = [Url::parse("http://api.example.com/graphql").expect("valid URL")];
        for target in [
            "https://93.184.216.34/graphql",
            "https://api.example.org/graphql",
        ] {
            let target = Url::parse(target).expect("valid URL");
            assert_eq!(
                check_redirect(RedirectScope::PublicHosts, &previous, &target),
                RedirectCheck::OtherHost,
                "{target} must be followed in the public-hosts scope"
            );
            assert_eq!(
                check_redirect(RedirectScope::RequestOrigin, &previous, &target),
                RedirectCheck::OffOrigin,
                "{target} must be refused in the request-origin scope"
            );
        }
        for target in [
            "https://api.example.com/graphql",
            "http://api.example.com:8080/graphql",
        ] {
            let target = Url::parse(target).expect("valid URL");
            assert_eq!(
                check_redirect(RedirectScope::RequestOrigin, &previous, &target),
                RedirectCheck::OffOrigin,
                "{target} changes the scheme or port, so it must be refused"
            );
        }

        let target = Url::parse("http://api.example.com:80/graphql/").expect("valid URL");
        assert_eq!(
            check_redirect(RedirectScope::RequestOrigin, &previous, &target),
            RedirectCheck::OwnHost,
            "a redirect within the request's origin is followed"
        );
    }

    #[test]
    fn check_redirect_with_credentials_refuses_a_chain_that_continues_on_another_host() {
        let url = |value: &str| Url::parse(value).expect("valid URL");
        let endpoint = url("https://api.example.com/graphql");
        let moved = [endpoint.clone(), url("https://cdn.example.net/a")];
        let next = url("https://cdn.example.net/b");
        assert_eq!(
            check_redirect(RedirectScope::PublicHostsWithCredentials, &moved, &next),
            RedirectCheck::ContinuesOffOrigin
        );
        assert_eq!(
            check_redirect(RedirectScope::PublicHosts, &moved, &next),
            RedirectCheck::OtherHost,
            "a request without credentials follows the chain"
        );
        for (previous, next) in [
            (&[endpoint][..], "https://cdn.example.net/a"),
            (&moved[..], "https://edge.example.org/x"),
        ] {
            assert_eq!(
                check_redirect(
                    RedirectScope::PublicHostsWithCredentials,
                    previous,
                    &url(next)
                ),
                RedirectCheck::OtherHost,
                "{next} must be followed"
            );
        }
        assert_eq!(
            check_redirect(
                RedirectScope::PublicHostsWithCredentials,
                &moved,
                &url("https://api.example.com/back")
            ),
            RedirectCheck::OwnHost
        );
    }
}
