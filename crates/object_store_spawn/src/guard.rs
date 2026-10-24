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
//! host. A guarded connector builds a minimal client itself and adds two checks:
//!
//! - a redirect policy that refuses any hop whose URL the guard blocks, and
//! - a DNS resolver that refuses a host when any address it resolves to is blocked, so a
//!   hostname cannot be used to reach an address the redirect check would have refused.
//!
//! Hosts given as IP literals are not resolved, so callers check the first URL themselves;
//! redirect targets are checked by the policy.
//!
//! The client applies only `allow_http`, `timeout`, `connect_timeout`, `http1_only`,
//! `user_agent`, and default headers. Building fails if any other client option differs from
//! [`ClientOptions::new`], so an option is never silently dropped. Two exceptions pass unapplied:
//! root certificates, which cannot be read from [`ClientOptions`], and the default
//! `randomize_addresses`, since the guarded resolver returns addresses in system order.
//!
//! Proxies come from `reqwest`'s own `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`NO_PROXY` lookup.
//! A proxy hostname goes through the guarded resolver like any other host, so a proxy on a
//! blocked address is refused with the guard's reason. The guard cannot see the address a proxy
//! resolves the destination to; the caller's first-URL check and the redirect policy still apply.

use std::{error::Error as StdError, fmt, net::IpAddr, net::SocketAddr, sync::Arc};

use object_store::{
    ClientConfigKey, ClientOptions,
    client::{HttpError, HttpErrorKind},
};
use reqwest::{
    dns::{Addrs, Name, Resolve, Resolving},
    redirect,
};
use snafu::Snafu;
use url::Url;

/// Redirects followed before the request fails; the same limit `object_store`'s client uses.
const MAX_REDIRECTS: usize = 10;

/// Client options [`build_client`] does not apply; each must keep its default.
const UNAPPLIED: &[ClientConfigKey] = &[
    ClientConfigKey::AllowInvalidCertificates,
    ClientConfigKey::Http2KeepAliveInterval,
    ClientConfigKey::Http2KeepAliveTimeout,
    ClientConfigKey::Http2KeepAliveWhileIdle,
    ClientConfigKey::Http2MaxFrameSize,
    ClientConfigKey::Http2Only,
    ClientConfigKey::PoolIdleTimeout,
    ClientConfigKey::PoolMaxIdlePerHost,
    ClientConfigKey::ProxyUrl,
    ClientConfigKey::ProxyCaCertificate,
    ClientConfigKey::ProxyExcludes,
    ClientConfigKey::RandomizeAddresses,
];

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

/// Builds a `reqwest` client from `options` with the checks of `guard`.
pub(crate) fn build_client(
    options: &ClientOptions,
    guard: &DestinationGuard,
) -> object_store::Result<reqwest::Client> {
    let defaults = ClientOptions::new();
    let value = |key: ClientConfigKey| options.get_config_value(&key);
    if let Some(key) = UNAPPLIED
        .iter()
        .find(|key| options.get_config_value(key) != defaults.get_config_value(key))
    {
        return Err(generic(format!(
            "the guarded HTTP client does not apply `{}`; it only applies `allow_http`, \
             `timeout`, `connect_timeout`, `http1_only`, `user_agent`, and default headers",
            key.as_ref()
        )));
    }

    let redirect_guard = guard.clone();
    let mut builder = reqwest::Client::builder()
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
        .https_only(!flag(value(ClientConfigKey::AllowHttp))?);

    if let Some(user_agent) = value(ClientConfigKey::UserAgent) {
        builder = builder.user_agent(user_agent);
    }
    if let Some(headers) = options.get_default_headers() {
        builder = builder.default_headers(headers.clone());
    }
    if let Some(timeout) = value(ClientConfigKey::Timeout) {
        builder = builder.timeout(humantime::parse_duration(&timeout).map_err(generic)?);
    }
    if let Some(timeout) = value(ClientConfigKey::ConnectTimeout) {
        builder = builder.connect_timeout(humantime::parse_duration(&timeout).map_err(generic)?);
    }
    if flag(value(ClientConfigKey::Http1Only))? {
        builder = builder.http1_only();
    }
    builder.build().map_err(generic)
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

fn generic(source: impl Into<Box<dyn StdError + Send + Sync>>) -> object_store::Error {
    object_store::Error::Generic {
        store: "HTTP",
        source: source.into(),
    }
}

/// Parses a boolean the way `object_store` does.
fn flag(value: Option<String>) -> object_store::Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" | "y" => Ok(true),
        "0" | "false" | "off" | "no" | "n" => Ok(false),
        _ => Err(generic(format!("failed to parse \"{value}\" as a boolean"))),
    }
}
