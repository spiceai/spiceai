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
//! host. A guarded connector builds a minimal client itself and applies the shared egress policy
//! of [`http_egress`] to it:
//!
//! - a redirect policy that refuses a downgrade from HTTPS to HTTP, and any hop that changes the
//!   origin onto a destination the guard blocks, and
//! - a DNS resolver that refuses a host when any address it resolves to is blocked, so a
//!   hostname cannot be used to reach an address the redirect check would have refused.
//!
//! Hosts given as IP literals are not resolved, so callers check the first URL themselves;
//! redirect targets are checked by the policy. A guard made with
//! [`DestinationGuard::redirects_only`] trusts the store's own host as its first answer leaves
//! it: an internal first answer is a host the operator configured, and a public one is checked
//! from then on. A redirect that changes the port or scheme on that host is a new destination.
//!
//! `reqwest` removes `Authorization` and the other standard credential headers when a redirect
//! changes the host or port, which covers the only credential these stores send (`http_auth`'s
//! `Authorization: Basic`); with downgrades refused, a scheme change is an upgrade.
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

use std::{error::Error as StdError, sync::Arc};

pub use http_egress::{DestinationGuard, DestinationRefused};
use http_egress::{Egress, RedirectRefused};
use object_store::{
    ClientConfigKey, ClientOptions,
    client::{HttpError, HttpErrorKind},
};
use reqwest::{
    dns::{Addrs, Name, Resolve, Resolving},
    redirect,
};

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

/// Returns the refusal behind `error`, if a guard caused it.
///
/// `reqwest` reports a refused DNS lookup as a connect error, and a refused redirect as a
/// redirect error, which `object_store` may retry with backoff; the caller re-wraps the refusal
/// so the request fails at once instead.
pub(crate) fn refusal(error: &HttpError) -> Option<HttpError> {
    let mut source: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = source {
        if let Some(refused) = current.downcast_ref::<DestinationRefused>() {
            return Some(HttpError::new(HttpErrorKind::Unknown, refused.clone()));
        }
        if let Some(refused) = current.downcast_ref::<RedirectRefused>() {
            return Some(match refused {
                RedirectRefused::Destination { source } => {
                    HttpError::new(HttpErrorKind::Unknown, source.clone())
                }
                other => HttpError::new(HttpErrorKind::Unknown, other.clone()),
            });
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

    let egress = Egress::new(Some(guard.clone()), MAX_REDIRECTS);
    let policy_egress = egress.clone();
    let mut builder = reqwest::Client::builder()
        // Compression changes `Content-Length`, which `object_store` reads as the object size.
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .redirect(redirect::Policy::custom(move |attempt| match policy_egress
            .redirect(attempt.previous(), attempt.url())
        {
            Ok(_) => attempt.follow(),
            Err(refused) => attempt.error(refused),
        }))
        .dns_resolver(Arc::new(GuardedResolver { egress }))
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

/// Resolves every host through the client's [`Egress`].
struct GuardedResolver {
    egress: Egress,
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let egress = self.egress.clone();
        let name = name.as_str().to_string();
        Box::pin(async move {
            let addresses = egress.resolve(&name).await?;
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
