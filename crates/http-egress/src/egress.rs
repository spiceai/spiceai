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

//! One client's view of the hosts it reaches: what its resolver checks, and which redirects it
//! follows.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    future::Future,
    iter,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use snafu::Snafu;
use url::Url;

use crate::guard::{BoxError, CheckedHosts, DestinationGuard, DestinationRefused, lookup};

/// Redirects followed before a request fails, the depth of `reqwest`'s default policy.
const DEFAULT_MAX_REDIRECTS: usize = 10;

type LookupFuture = Pin<Box<dyn Future<Output = Result<Vec<SocketAddr>, BoxError>> + Send>>;
type Lookup = Arc<dyn Fn(String) -> LookupFuture + Send + Sync>;

/// How far a client trusts the addresses a host name resolves to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostTrust {
    /// Introduced by a redirect that changed the origin: every answer is checked.
    Checked,
    /// Reached directly, and its first answer had a public address: only public addresses are
    /// used from then on, so the name cannot be rebound to an internal address.
    Public,
    /// Reached directly, and every address in its first answer was internal: the operator
    /// configured an internal host, and its answers are not checked.
    Internal,
}

/// One HTTP client's egress state, shared by its resolver and its redirect decisions.
///
/// Clone it to share the state; build one per client, since what it learns about a host is
/// specific to the destination that client was configured for.
#[derive(Clone)]
pub struct Egress {
    inner: Arc<Inner>,
}

struct Inner {
    guard: Option<DestinationGuard>,
    max_redirects: usize,
    hosts: Mutex<HashMap<String, HostTrust>>,
    lookup: Lookup,
}

impl Default for Egress {
    /// No destination checks, and the default redirect limit: the redirect policy alone
    /// (no downgrade, credentials stay on their origin).
    fn default() -> Self {
        Self::new(None, DEFAULT_MAX_REDIRECTS)
    }
}

impl Egress {
    /// The egress of a client that reaches one endpoint the operator configured and never
    /// follows it to another origin (see [`crate::origin_bound`]). The endpoint may be on a
    /// loopback or private address, which stays allowed; a host that first resolved to a
    /// public address is reached only at public addresses from then on. `reason` completes
    /// the error for a redirect to another port of an endpoint host that resolved to an
    /// internal address.
    #[must_use]
    pub fn for_configured_endpoint(reason: &str) -> Self {
        // Every origin change is refused by the redirect policy, so the guard judges no
        // redirect target of its own: its addresses pin the endpoint's host.
        let guard =
            DestinationGuard::new(crate::is_internal_ip, |_: &Url| false, reason).redirects_only();
        Self::new(Some(guard), DEFAULT_MAX_REDIRECTS)
    }
}

impl fmt::Debug for Egress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Egress")
            .field("guard", &self.inner.guard)
            .field("max_redirects", &self.inner.max_redirects)
            .finish_non_exhaustive()
    }
}

/// A redirect hop [`Egress::redirect`] allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub struct Hop {
    left_origin: bool,
}

impl Hop {
    /// Whether the chain, including this hop, has left the origin of the request that started
    /// it. Once it has, the request carries no credential and no body for the rest of the
    /// chain, even back on the first origin: a server on another origin chose that `Location`.
    #[must_use]
    pub fn left_origin(self) -> bool {
        self.left_origin
    }
}

/// A redirect the egress policy does not follow. Worded as the cause clause of the error the
/// caller builds around it, which names the resource and the fix.
#[derive(Clone, Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RedirectRefused {
    #[snafu(display("the server redirected more than {max} times"))]
    TooMany { max: usize },

    #[snafu(display(
        "the server redirected to a `{scheme}` URL, and only `http` and `https` redirects are \
         followed"
    ))]
    Scheme { scheme: String },

    #[snafu(display(
        "the server redirected from HTTPS to plaintext HTTP at {target}, and a redirect never \
         downgrades the connection"
    ))]
    Downgrade { target: String },

    #[snafu(display("{source}"))]
    Destination { source: DestinationRefused },

    #[snafu(display(
        "the server redirected to another origin, {target}, and this request carries a \
         credential that is sent only to {origin}"
    ))]
    CrossOrigin { origin: String, target: String },

    #[snafu(display(
        "the redirect chain left {origin} and continued on {target}, and a request that carries \
         credentials does not follow a redirect chain that continues on another host"
    ))]
    ContinuesOffOrigin { origin: String, target: String },

    #[snafu(display(
        "the server answered {status} to resend the request body to another origin, {target}, \
         and a request body is never sent to an origin other than the one it was meant for"
    ))]
    BodyAcrossOrigins { status: u16, target: String },

    #[snafu(display("the server answered {status} with a `Location` that is not a valid URL"))]
    InvalidLocation { status: u16 },

    #[snafu(display(
        "the server answered {status} to resend a request body that cannot be resent"
    ))]
    BodyNotReplayable { status: u16 },
}

/// The part of `url` an error may show: scheme, host and a non-default port, never a path,
/// query or user information, which can carry a signed token.
pub(crate) fn origin_label(url: &Url) -> String {
    url.origin().ascii_serialization()
}

/// Whether a redirect to `next`, after the URLs in `chain` (the request's own URL first), stays on
/// the host and port of the previous URL when that URL is not on the request's own host and port.
/// A client that sends credentials refuses such a redirect, so its credentials stay on the
/// request's origin across a redirect chain ([`crate::OnOriginChange::FollowWithCredentials`]).
#[must_use]
pub fn redirect_continues_off_origin(chain: &[Url], next: &Url) -> bool {
    let same_host_and_port = |a: &Url, b: &Url| {
        a.host_str() == b.host_str() && a.port_or_known_default() == b.port_or_known_default()
    };
    match (chain.first(), chain.last()) {
        (Some(first), Some(previous)) => {
            same_host_and_port(previous, next) && !same_host_and_port(first, previous)
        }
        _ => false,
    }
}

/// `host` as the host map keys it: `example.com.` and `example.com` name the same host.
fn host_key(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

impl Egress {
    /// `guard` decides which destinations are internal; `None` checks no address and keeps
    /// only the redirect policy. A chain longer than `max_redirects` redirects is refused.
    #[must_use]
    pub fn new(guard: Option<DestinationGuard>, max_redirects: usize) -> Self {
        Self::with_lookup(
            guard,
            max_redirects,
            Arc::new(|host: String| Box::pin(async move { lookup(&host).await }) as LookupFuture),
        )
    }

    fn with_lookup(guard: Option<DestinationGuard>, max_redirects: usize, lookup: Lookup) -> Self {
        Self {
            inner: Arc::new(Inner {
                guard,
                max_redirects,
                hosts: Mutex::new(HashMap::new()),
                lookup,
            }),
        }
    }

    fn hosts(&self) -> MutexGuard<'_, HashMap<String, HostTrust>> {
        self.inner
            .hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The addresses `name` may be reached at.
    ///
    /// # Errors
    ///
    /// Returns the lookup error, or a [`DestinationRefused`] when the guard refuses an address
    /// `name` resolves to.
    pub async fn resolve(&self, name: &str) -> Result<Vec<SocketAddr>, BoxError> {
        let host = name.to_ascii_lowercase();
        let addresses = (self.inner.lookup)(host.clone()).await?;
        let Some(guard) = self.inner.guard.as_ref() else {
            return Ok(addresses);
        };
        let blocked = addresses
            .iter()
            .any(|address| guard.blocks_address(address.ip()));
        if guard.checked_hosts == CheckedHosts::All {
            return if blocked {
                Err(Box::new(guard.refused(host)))
            } else {
                Ok(addresses)
            };
        }

        // The first answer for a host this client reaches directly decides how far the name is
        // trusted; a host a redirect introduced is already `Checked`. Only an answer that is
        // internal throughout marks an operator-chosen internal host: a name that answers with
        // any public address is a public host, reached only at its public addresses from then
        // on, so a mixed answer cannot steer the connection to an internal one.
        let all_blocked = addresses
            .iter()
            .all(|address| guard.blocks_address(address.ip()));
        let first = if all_blocked {
            HostTrust::Internal
        } else {
            HostTrust::Public
        };
        let trust = *self.hosts().entry(host_key(&host)).or_insert(first);
        match trust {
            HostTrust::Checked if blocked => Err(Box::new(guard.refused(host))),
            HostTrust::Internal | HostTrust::Checked => Ok(addresses),
            HostTrust::Public => {
                let public: Vec<SocketAddr> = addresses
                    .into_iter()
                    .filter(|address| !guard.blocks_address(address.ip()))
                    .collect();
                if public.is_empty() {
                    Err(Box::new(DestinationRefused::rebound(&host)))
                } else {
                    Ok(public)
                }
            }
        }
    }

    /// Whether a redirect to `next` is followed, after the URLs in `chain` (the request's own
    /// URL first) were requested.
    ///
    /// # Errors
    ///
    /// Returns why the redirect is refused: too many hops, a scheme other than `http` or
    /// `https`, a downgrade from HTTPS to HTTP, or an origin change onto a destination the
    /// guard refuses.
    pub fn redirect(&self, chain: &[Url], next: &Url) -> Result<Hop, RedirectRefused> {
        let max = self.inner.max_redirects;
        snafu::ensure!(chain.len() <= max, TooManySnafu { max });
        snafu::ensure!(
            matches!(next.scheme(), "http" | "https"),
            SchemeSnafu {
                scheme: next.scheme()
            }
        );
        snafu::ensure!(
            next.scheme() != "http" || chain.iter().all(|url| url.scheme() != "https"),
            DowngradeSnafu {
                target: origin_label(next)
            }
        );

        let (Some(first), Some(previous)) = (chain.first(), chain.last()) else {
            return Ok(Hop { left_origin: false });
        };
        let origin = first.origin();
        let left_origin = chain
            .iter()
            .chain(iter::once(next))
            .any(|url| url.origin() != origin);
        if previous.origin() == next.origin() {
            return Ok(Hop { left_origin });
        }

        // The origin changes: a different scheme, host, or port is a new destination, checked
        // as one even when the host name is the same.
        if let Some(guard) = self.inner.guard.as_ref() {
            if guard.blocks_redirect(next) {
                return Err(RedirectRefused::Destination {
                    source: guard.refused(next.as_str()),
                });
            }
            if guard.checked_hosts == CheckedHosts::AwayFromOrigin
                && let Some(url::Host::Domain(domain)) = next.host()
            {
                match self.hosts().entry(host_key(domain)) {
                    // A redirect may not open another port or scheme on a host the operator
                    // configured as internal.
                    Entry::Occupied(entry) if *entry.get() == HostTrust::Internal => {
                        return Err(RedirectRefused::Destination {
                            source: guard.refused(next.as_str()),
                        });
                    }
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) => {
                        entry.insert(HostTrust::Checked);
                    }
                }
            }
        }
        Ok(Hop { left_origin })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        net::{IpAddr, Ipv4Addr},
        sync::Mutex,
    };

    use super::*;

    const PUBLIC: IpAddr = IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34));
    const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    fn url(value: &str) -> Url {
        Url::parse(value).expect("test URL parses")
    }

    fn loopback_guard() -> DestinationGuard {
        DestinationGuard::new(
            |address| address.is_loopback(),
            |url| url.host_str() == Some("127.0.0.1"),
            "loopback is blocked",
        )
    }

    /// An egress whose lookups answer from `answers`, in order, so a test can change what a
    /// name resolves to between two lookups the way a rebinding DNS server does.
    fn scripted(guard: DestinationGuard, answers: Vec<IpAddr>) -> Egress {
        let answers = Arc::new(Mutex::new(VecDeque::from(answers)));
        Egress::with_lookup(
            Some(guard),
            DEFAULT_MAX_REDIRECTS,
            Arc::new(move |_host: String| {
                let next = answers
                    .lock()
                    .expect("the scripted answers are not poisoned")
                    .pop_front()
                    .expect("the test scripted enough answers");
                Box::pin(async move { Ok(vec![SocketAddr::new(next, 0)]) }) as LookupFuture
            }),
        )
    }

    async fn resolve_error(egress: &Egress, name: &str) -> String {
        egress
            .resolve(name)
            .await
            .expect_err("the answer is refused")
            .to_string()
    }

    /// A configured host that first answered with a public address cannot be rebound to an
    /// internal one: the second answer is refused, with no redirect involved.
    #[tokio::test]
    async fn a_public_host_cannot_be_rebound_to_an_internal_address() {
        let egress = scripted(loopback_guard().redirects_only(), vec![PUBLIC, LOOPBACK]);

        let first = egress.resolve("data.example").await.expect("public answer");
        assert_eq!(first, vec![SocketAddr::new(PUBLIC, 0)]);

        let message = resolve_error(&egress, "data.example").await;
        assert_eq!(
            message,
            "Refused to connect to data.example: this host first resolved to a public address \
             and now resolves only to private, loopback, or link-local addresses, and a host is \
             not allowed to move into an internal network while it is in use. If the host moved \
             on purpose, reload the dataset or restart the runtime"
        );
    }

    /// A name that answers with a public and an internal address at once is a public host: it
    /// is reached only at its public address, now and on every later answer, so a mixed answer
    /// cannot steer the connection inward.
    #[tokio::test]
    async fn a_mixed_answer_reaches_only_the_public_address() {
        let answers = Arc::new(Mutex::new(VecDeque::from([
            vec![PUBLIC, LOOPBACK],
            vec![LOOPBACK, PUBLIC],
            vec![LOOPBACK],
        ])));
        let egress = Egress::with_lookup(
            Some(loopback_guard().redirects_only()),
            DEFAULT_MAX_REDIRECTS,
            Arc::new(move |_host: String| {
                let next = answers
                    .lock()
                    .expect("the scripted answers are not poisoned")
                    .pop_front()
                    .expect("the test scripted enough answers");
                Box::pin(
                    async move { Ok(next.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect()) },
                ) as LookupFuture
            }),
        );

        for _ in 0..2 {
            assert_eq!(
                egress.resolve("mixed.example").await.expect("public part"),
                vec![SocketAddr::new(PUBLIC, 0)]
            );
        }
        let message = resolve_error(&egress, "mixed.example").await;
        assert!(
            message.contains("first resolved to a public address"),
            "{message}"
        );
    }

    /// A configured host that first answered with an internal address is one the operator
    /// chose, and stays reachable.
    #[tokio::test]
    async fn a_host_configured_as_internal_stays_reachable() {
        let egress = scripted(loopback_guard().redirects_only(), vec![LOOPBACK, LOOPBACK]);
        for _ in 0..2 {
            assert_eq!(
                egress.resolve("minio.internal").await.expect("allowed"),
                vec![SocketAddr::new(LOOPBACK, 0)]
            );
        }
    }

    /// The finding this guards against: a redirect to the configured host name on another
    /// port. Once the origin changes, the name is checked, so a rebinding answer for the new
    /// connection is refused.
    #[tokio::test]
    async fn a_same_name_redirect_to_another_port_cannot_reach_an_internal_address() {
        let egress = scripted(loopback_guard().redirects_only(), vec![PUBLIC, LOOPBACK]);
        egress.resolve("data.example").await.expect("public answer");

        let hop = egress
            .redirect(
                &[url("http://data.example/a")],
                &url("http://data.example:6379/"),
            )
            .expect("an origin change onto a public host is followed");
        assert!(hop.left_origin());

        let message = resolve_error(&egress, "data.example").await;
        assert!(
            message.starts_with("Refused to connect to data.example:"),
            "{message}"
        );
    }

    /// A redirect may not open another port or scheme on a host the operator configured as
    /// internal, while a redirect within the configured origin is followed.
    #[tokio::test]
    async fn an_origin_change_onto_an_internal_configured_host_is_refused() {
        let egress = scripted(loopback_guard().redirects_only(), vec![LOOPBACK]);
        egress.resolve("api.internal").await.expect("internal host");

        let within = egress
            .redirect(
                &[url("http://api.internal:8080/a")],
                &url("http://api.internal:8080/b"),
            )
            .expect("a redirect within the configured origin is followed");
        assert!(!within.left_origin());

        let message = egress
            .redirect(
                &[url("http://api.internal:8080/a")],
                &url("http://api.internal:6379/"),
            )
            .expect_err("another port on the internal host is refused")
            .to_string();
        assert_eq!(
            message,
            "Refused to connect to http://api.internal:6379/: loopback is blocked"
        );
    }

    /// An IP literal is never resolved, so the URL check refuses an origin change onto an
    /// internal one, including another port of the configured address.
    #[test]
    fn an_origin_change_onto_an_internal_literal_is_refused() {
        let egress = Egress::new(Some(loopback_guard().redirects_only()), 10);
        let within = egress
            .redirect(
                &[url("http://127.0.0.1:8080/a")],
                &url("http://127.0.0.1:8080/b"),
            )
            .expect("the configured origin is trusted");
        assert!(!within.left_origin());
        let message = egress
            .redirect(
                &[url("http://127.0.0.1:8080/a")],
                &url("http://127.0.0.1:9000/b"),
            )
            .expect_err("another port is another origin")
            .to_string();
        assert_eq!(
            message,
            "Refused to connect to http://127.0.0.1:9000/b: loopback is blocked"
        );
    }

    #[test]
    fn a_downgrade_is_refused_anywhere_in_the_chain() {
        let egress = Egress::default();
        let message = egress
            .redirect(&[url("https://a.example/x")], &url("http://a.example/x"))
            .expect_err("https to http is refused")
            .to_string();
        assert_eq!(
            message,
            "the server redirected from HTTPS to plaintext HTTP at http://a.example, and a \
             redirect never downgrades the connection"
        );
        egress
            .redirect(
                &[url("http://a.example/x"), url("https://a.example/x")],
                &url("http://b.example/"),
            )
            .expect_err("a chain that was ever HTTPS stays HTTPS");
        let upgrade = egress
            .redirect(&[url("http://a.example/x")], &url("https://a.example/x"))
            .expect("an upgrade is followed");
        assert!(upgrade.left_origin(), "a scheme change is an origin change");
    }

    #[test]
    fn leaving_the_origin_is_remembered_for_the_rest_of_the_chain() {
        let egress = Egress::default();
        let back = egress
            .redirect(
                &[url("https://a.example/x"), url("https://b.example/y")],
                &url("https://a.example/z"),
            )
            .expect("followed");
        assert!(
            back.left_origin(),
            "a server on another origin chose this hop"
        );
        let implicit_port = egress
            .redirect(
                &[url("https://a.example/x")],
                &url("https://a.example:443/y"),
            )
            .expect("followed");
        assert!(!implicit_port.left_origin(), "443 is https's own port");
    }

    #[test]
    fn the_chain_is_bounded_and_only_http_is_followed() {
        let egress = Egress::new(None, 2);
        let chain = [url("https://a.example/1"), url("https://a.example/2")];
        let second = egress
            .redirect(&chain, &url("https://a.example/3"))
            .expect("two redirects are allowed");
        assert!(!second.left_origin());
        let three = [
            url("https://a.example/1"),
            url("https://a.example/2"),
            url("https://a.example/3"),
        ];
        assert_eq!(
            egress
                .redirect(&three, &url("https://a.example/4"))
                .expect_err("a third is not")
                .to_string(),
            "the server redirected more than 2 times"
        );
        assert_eq!(
            egress
                .redirect(&chain[..1], &url("file:///etc/passwd"))
                .expect_err("file is refused")
                .to_string(),
            "the server redirected to a `file` URL, and only `http` and `https` redirects are \
             followed"
        );
    }

    fn chain(urls: &[&str]) -> Vec<Url> {
        urls.iter()
            .map(|url| Url::parse(url).expect("valid URL"))
            .collect()
    }

    #[test]
    fn a_redirect_chain_may_not_continue_on_a_host_it_moved_to() {
        let endpoint = "https://api.example.com/data";
        for (previous, next) in [
            (
                vec![endpoint, "https://cdn.example.net/a"],
                "https://cdn.example.net/b",
            ),
            (
                vec![endpoint, "https://cdn.example.net/a"],
                "http://cdn.example.net:443/b",
            ),
            (
                vec![endpoint, "https://api.example.com:8443/a"],
                "https://api.example.com:8443/b",
            ),
            (
                vec![
                    endpoint,
                    "https://cdn.example.net/a",
                    "https://edge.example.org/x",
                ],
                "https://edge.example.org/y",
            ),
        ] {
            let next = Url::parse(next).expect("valid URL");
            assert!(
                redirect_continues_off_origin(&chain(&previous), &next),
                "{previous:?} -> {next} must be refused"
            );
        }
        for (previous, next) in [
            (vec![endpoint], "https://cdn.example.net/a"),
            (vec![endpoint], "https://api.example.com/other"),
            (
                vec![endpoint, "https://api.example.com/x"],
                "https://api.example.com:443/y",
            ),
            (
                vec![endpoint, "https://api.example.com/x"],
                "https://cdn.example.net/a",
            ),
            (
                vec![endpoint, "https://cdn.example.net/a"],
                "https://edge.example.org/x",
            ),
            (
                vec![endpoint, "https://cdn.example.net/a"],
                "https://api.example.com/back",
            ),
        ] {
            let next = Url::parse(next).expect("valid URL");
            assert!(
                !redirect_continues_off_origin(&chain(&previous), &next),
                "{previous:?} -> {next} must be followed"
            );
        }
    }
}
