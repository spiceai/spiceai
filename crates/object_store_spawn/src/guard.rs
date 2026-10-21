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

//! Destination checks for [`SpawnedReqwestConnector`](crate::SpawnedReqwestConnector).
//!
//! `object_store` builds its `reqwest` client privately and follows up to ten redirects to any
//! host. A guarded connector builds the client itself instead, from the same
//! [`ClientOptions`], and adds two checks:
//!
//! - a redirect policy that refuses any hop whose URL the guard blocks, and
//! - a DNS resolver that refuses a host when any address it resolves to is blocked, so a
//!   hostname cannot be used to reach an address the redirect check would have refused.
//!
//! Hosts given as IP literals are not resolved, so callers check the first URL themselves;
//! redirect targets are checked by the policy.
//!
//! An HTTP proxy is pinned to one resolved address when the client is built. The proxy hop
//! connects to that address, so the resolver does not see the proxy's hostname, while a direct
//! connection to the same hostname — a `NO_PROXY` exclusion, or a destination whose scheme does
//! not use that proxy — is still checked. An `https://` proxy keeps its hostname because its
//! certificate is checked against that name, and the resolver checks the name like any other host.

use std::{
    error::Error as StdError,
    fmt,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use object_store::{
    ClientConfigKey, ClientOptions,
    client::{HttpError, HttpErrorKind},
};
use reqwest::{
    NoProxy, Proxy,
    dns::{Addrs, Name, Resolve, Resolving},
    redirect,
};
use snafu::Snafu;
use url::Url;

/// Redirects followed before the request fails; the same limit `object_store`'s client uses.
const MAX_REDIRECTS: usize = 10;

/// Which destinations a configured proxy applies to. Matches `reqwest`'s `Proxy::http`,
/// `Proxy::https`, and `Proxy::all`.
#[derive(Clone, Copy)]
enum ProxyScope {
    Http,
    Https,
    All,
}

type AddressCheck = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;
type UrlCheck = Arc<dyn Fn(&Url) -> bool + Send + Sync>;

/// Which destinations a guarded connector refuses to reach.
#[derive(Clone)]
pub struct DestinationGuard {
    blocks_address: AddressCheck,
    blocks_redirect: UrlCheck,
    reason: Arc<str>,
}

impl DestinationGuard {
    /// `blocks_address` is asked about every address a hostname resolves to, and
    /// `blocks_redirect` about every redirect target. `reason` completes the error a refused
    /// request returns: "Refused to connect to {target}: {reason}".
    #[must_use]
    pub fn new(
        blocks_address: impl Fn(IpAddr) -> bool + Send + Sync + 'static,
        blocks_redirect: impl Fn(&Url) -> bool + Send + Sync + 'static,
        reason: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            blocks_address: Arc::new(blocks_address),
            blocks_redirect: Arc::new(blocks_redirect),
            reason: reason.into(),
        }
    }

    fn refused(&self, target: impl Into<String>) -> DestinationRefused {
        DestinationRefused {
            target: target.into(),
            reason: Arc::clone(&self.reason),
        }
    }
}

impl fmt::Debug for DestinationGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DestinationGuard")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// A request refused by a [`DestinationGuard`].
#[derive(Debug, Clone, Snafu)]
#[snafu(display("Refused to connect to {target}: {reason}"))]
pub struct DestinationRefused {
    target: String,
    reason: Arc<str>,
}

/// Returns the refusal behind `error`, if a guard caused it.
///
/// `reqwest` reports a refused DNS lookup as a connect error, which `object_store` retries
/// with backoff; the caller re-wraps the refusal so the request fails at once instead.
pub(crate) fn refusal(error: &HttpError) -> Option<HttpError> {
    let mut source: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = source {
        if let Some(refused) = current.downcast_ref::<DestinationRefused>() {
            return Some(HttpError::new(HttpErrorKind::Unknown, refused.clone()));
        }
        source = current.source();
    }
    None
}

/// Builds the `reqwest` client `object_store` would build from `options`, plus the checks of
/// `guard`.
///
/// Custom root certificates (`ClientOptions::with_root_certificate`) are not readable from
/// `ClientOptions` and are not applied, and addresses are not shuffled
/// (`ClientConfigKey::RandomizeAddresses`); callers that rely on either must not use a guard.
pub(crate) fn build_client(
    options: &ClientOptions,
    guard: &DestinationGuard,
) -> object_store::Result<reqwest::Client> {
    let value = |key: ClientConfigKey| options.get_config_value(&key);
    let mut builder = reqwest::Client::builder();

    if let Some(user_agent) = value(ClientConfigKey::UserAgent) {
        builder = builder.user_agent(user_agent);
    }
    if let Some(headers) = options.get_default_headers() {
        builder = builder.default_headers(headers.clone());
    }

    builder = install_proxies(builder, options)?;

    if let Some(timeout) = duration(value(ClientConfigKey::Timeout))? {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = duration(value(ClientConfigKey::ConnectTimeout))? {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = duration(value(ClientConfigKey::PoolIdleTimeout))? {
        builder = builder.pool_idle_timeout(timeout);
    }
    if let Some(max) = number(value(ClientConfigKey::PoolMaxIdlePerHost))? {
        builder = builder.pool_max_idle_per_host(max);
    }
    if let Some(interval) = duration(value(ClientConfigKey::Http2KeepAliveInterval))? {
        builder = builder.http2_keep_alive_interval(interval);
    }
    if let Some(timeout) = duration(value(ClientConfigKey::Http2KeepAliveTimeout))? {
        builder = builder.http2_keep_alive_timeout(timeout);
    }
    if flag(value(ClientConfigKey::Http2KeepAliveWhileIdle))? {
        builder = builder.http2_keep_alive_while_idle(true);
    }
    if let Some(size) = number(value(ClientConfigKey::Http2MaxFrameSize))? {
        let size = u32::try_from(size).map_err(generic)?;
        builder = builder.http2_max_frame_size(Some(size));
    }
    if flag(value(ClientConfigKey::Http1Only))? {
        builder = builder.http1_only();
    }
    if flag(value(ClientConfigKey::Http2Only))? {
        builder = builder.http2_prior_knowledge();
    }
    if flag(value(ClientConfigKey::AllowInvalidCertificates))? {
        builder = builder.danger_accept_invalid_certs(true);
    }

    let redirect_guard = guard.clone();
    builder
        // Compression changes `Content-Length`, which `object_store` reads as the object size.
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .redirect(redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= MAX_REDIRECTS {
                return attempt.error("too many redirects");
            }
            if (redirect_guard.blocks_redirect)(attempt.url()) {
                let refused = redirect_guard.refused(attempt.url().as_str());
                return attempt.error(refused);
            }
            attempt.follow()
        }))
        .dns_resolver(Arc::new(GuardedResolver {
            guard: guard.clone(),
        }))
        .https_only(!flag(value(ClientConfigKey::AllowHttp))?)
        .build()
        .map_err(generic)
}

/// Resolves with the system resolver and refuses a host if any of its addresses is blocked,
/// so a name that resolves to both a public and a blocked address cannot be used either.
struct GuardedResolver {
    guard: DestinationGuard,
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_ascii_lowercase();
        let guard = self.guard.clone();
        Box::pin(async move {
            let addresses: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if addresses
                .iter()
                .any(|address| (guard.blocks_address)(address.ip()))
            {
                return Err(Box::new(guard.refused(host)) as Box<dyn StdError + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

/// Installs the proxies `reqwest` would use, with each HTTP proxy pinned to a resolved address.
///
/// An explicit `proxy_url` replaces environment proxies, because handing `reqwest` any proxy
/// turns off its own environment lookup. With no explicit URL, the same variables and precedence
/// as `hyper-util`'s environment matcher are read here: `ALL_PROXY` fills in for a missing
/// `HTTP_PROXY` or `HTTPS_PROXY`, and `REQUEST_METHOD` suppresses them the way that matcher does
/// for CGI.
fn install_proxies(
    mut builder: reqwest::ClientBuilder,
    options: &ClientOptions,
) -> object_store::Result<reqwest::ClientBuilder> {
    let value = |key: ClientConfigKey| options.get_config_value(&key);
    if let Some(proxy_url) = value(ClientConfigKey::ProxyUrl) {
        if let Some(certificate) = value(ClientConfigKey::ProxyCaCertificate) {
            let certificate =
                reqwest::Certificate::from_pem(certificate.as_bytes()).map_err(generic)?;
            builder = builder.add_root_certificate(certificate);
        }
        let url = parse_proxy_url(&proxy_url).ok_or_else(|| invalid("proxy_url", &proxy_url))?;
        builder = builder.proxy(scoped_proxy(
            ProxyScope::All,
            &url,
            value(ClientConfigKey::ProxyExcludes).as_deref(),
        )?);
        return Ok(builder);
    }

    if std::env::var_os("REQUEST_METHOD").is_some() {
        return Ok(builder);
    }

    let all = first_env(&["ALL_PROXY", "all_proxy"]);
    let http = first_env(&["HTTP_PROXY", "http_proxy"]);
    let https = first_env(&["HTTPS_PROXY", "https_proxy"]);
    let no_proxy = first_env(&["NO_PROXY", "no_proxy"]);
    let excludes = nonempty(no_proxy.trim());

    if let Some(raw) = nonempty(http.trim()).or(nonempty(all.trim()))
        && let Some(proxy) = env_proxy(ProxyScope::Http, raw, excludes)?
    {
        builder = builder.proxy(proxy);
    }
    if let Some(raw) = nonempty(https.trim()).or(nonempty(all.trim()))
        && let Some(proxy) = env_proxy(ProxyScope::Https, raw, excludes)?
    {
        builder = builder.proxy(proxy);
    }
    Ok(builder)
}

fn env_proxy(
    scope: ProxyScope,
    raw: &str,
    excludes: Option<&str>,
) -> object_store::Result<Option<Proxy>> {
    let Some(url) = parse_proxy_url(raw) else {
        return Ok(None);
    };
    // This client is built without the `socks` feature. Leaving another scheme to `reqwest`'s
    // own environment handling would require not calling `ClientBuilder::proxy` at all, which
    // also drops the HTTP proxies installed above. Skip the scheme instead.
    if !matches!(url.scheme(), "http" | "https") {
        return Ok(None);
    }
    scoped_proxy(scope, &url, excludes).map(Some)
}

fn scoped_proxy(
    scope: ProxyScope,
    url: &Url,
    excludes: Option<&str>,
) -> object_store::Result<Proxy> {
    let pinned = pin_http_proxy(url);
    let mut proxy = match scope {
        ProxyScope::Http => Proxy::http(pinned.as_str()),
        ProxyScope::Https => Proxy::https(pinned.as_str()),
        ProxyScope::All => Proxy::all(pinned.as_str()),
    }
    .map_err(generic)?;
    if let Some(excludes) = excludes {
        proxy = proxy.no_proxy(NoProxy::from_string(excludes));
    }
    Ok(proxy)
}

/// Rewrites an `http://` proxy to the first address it resolves to.
///
/// The guarded resolver then never exempts that hostname: a direct connection to it is checked,
/// and the proxy hop uses the address. Resolution happens once, while the client is built.
/// `connect` is synchronous, so the lookup blocks the caller for that one query. An `https://`
/// proxy is left unchanged so certificate verification still sees its hostname.
fn pin_http_proxy(url: &Url) -> Url {
    if url.scheme() != "http" {
        return url.clone();
    }
    if matches!(url.host(), Some(url::Host::Ipv4(_) | url::Host::Ipv6(_))) {
        return url.clone();
    }
    let Some(host) = url.host_str() else {
        return url.clone();
    };
    let port = url.port_or_known_default().unwrap_or(80);
    let Ok(mut addresses) = (host, port).to_socket_addrs() else {
        return url.clone();
    };
    let Some(address) = addresses.next() else {
        return url.clone();
    };
    let mut pinned = url.clone();
    if pinned.set_ip_host(address.ip()).is_err() || pinned.set_port(Some(address.port())).is_err() {
        return url.clone();
    }
    pinned
}

fn parse_proxy_url(proxy: &str) -> Option<Url> {
    let proxy = proxy.trim();
    if proxy.is_empty() {
        return None;
    }
    Url::parse(proxy)
        .or_else(|_| Url::parse(&format!("http://{proxy}")))
        .ok()
}

fn first_env(names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .unwrap_or_default()
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn generic(source: impl StdError + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "HTTP",
        source: Box::new(source),
    }
}

fn invalid(key: &str, value: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "HTTP",
        source: format!("failed to parse \"{value}\" for {key}").into(),
    }
}

fn duration(value: Option<String>) -> object_store::Result<Option<Duration>> {
    value
        .map(|v| humantime::parse_duration(&v).map_err(|_| invalid("a duration", &v)))
        .transpose()
}

fn number(value: Option<String>) -> object_store::Result<Option<usize>> {
    value
        .map(|v| v.parse::<usize>().map_err(|_| invalid("a number", &v)))
        .transpose()
}

/// Parses a boolean the way `object_store` does.
fn flag(value: Option<String>) -> object_store::Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" | "y" => Ok(true),
        "0" | "false" | "off" | "no" | "n" => Ok(false),
        _ => Err(invalid("a boolean", &value)),
    }
}
