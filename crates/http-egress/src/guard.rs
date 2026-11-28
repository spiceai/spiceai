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

//! Which destinations a client refuses to reach.

use std::{
    error::Error as StdError,
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use snafu::Snafu;
use url::Url;

pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;

type AddressCheck = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;
type UrlCheck = Arc<dyn Fn(&Url) -> bool + Send + Sync>;

/// Which hosts a [`DestinationGuard`] checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckedHosts {
    /// Every host, including the one the request names: the destination is the caller's choice.
    All,
    /// Every host a redirect introduced. A host the client reaches directly is checked once its
    /// first answer was public; see [`crate::Egress`].
    AwayFromOrigin,
}

/// Which destinations a client refuses to reach.
#[derive(Clone)]
pub struct DestinationGuard {
    blocks_address: AddressCheck,
    blocks_redirect: UrlCheck,
    reason: Arc<str>,
    pub(crate) checked_hosts: CheckedHosts,
}

impl DestinationGuard {
    /// `blocks_address` is asked about every address a checked host resolves to, and
    /// `blocks_redirect` about every redirect target that changes the origin. `reason`
    /// completes the error a refused request returns: "Refused to connect to {target}:
    /// {reason}".
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
            checked_hosts: CheckedHosts::All,
        }
    }

    /// Trusts the hosts a client reaches directly (the configured origin, and a proxy) and
    /// checks the rest, for a destination the operator chose rather than a query.
    ///
    /// A directly reached host is pinned to its first answer: when every address in it is
    /// internal the operator configured an internal host, and the host stays allowed; when any
    /// is public, the host is reached only at public addresses from then on, so the name cannot
    /// be rebound to an internal address. A redirect that changes the origin (scheme, host or
    /// port) is checked like a redirect to another host, and one onto a host that answered
    /// with an internal address is refused.
    #[must_use]
    pub fn redirects_only(mut self) -> Self {
        self.checked_hosts = CheckedHosts::AwayFromOrigin;
        self
    }

    /// Whether `address` is a destination this guard refuses.
    #[must_use]
    pub fn blocks_address(&self, address: IpAddr) -> bool {
        (self.blocks_address)(address)
    }

    /// Whether a redirect to `url` is refused before any DNS lookup.
    #[must_use]
    pub fn blocks_redirect(&self, url: &Url) -> bool {
        (self.blocks_redirect)(url)
    }

    /// Resolves `host` with the system resolver and refuses it if any address it resolves to is
    /// blocked, so a name that resolves to both a public and a blocked address is refused too.
    ///
    /// # Errors
    ///
    /// Returns the lookup error, or a [`DestinationRefused`] when an address is blocked.
    pub async fn resolve(&self, host: &str) -> Result<Vec<SocketAddr>, BoxError> {
        let host = host.to_ascii_lowercase();
        let addresses = lookup(&host).await?;
        if addresses
            .iter()
            .any(|address| self.blocks_address(address.ip()))
        {
            return Err(Box::new(self.refused(host)));
        }
        Ok(addresses)
    }

    /// The error a request refused by this guard returns.
    #[must_use]
    pub fn refused(&self, target: impl Into<String>) -> DestinationRefused {
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
            .field("checked_hosts", &self.checked_hosts)
            .finish_non_exhaustive()
    }
}

/// A request refused because of where it would go.
#[derive(Debug, Clone, Snafu)]
#[snafu(display("Refused to connect to {target}: {reason}"))]
pub struct DestinationRefused {
    target: String,
    reason: Arc<str>,
}

impl DestinationRefused {
    /// A name that first resolved to a public address now resolves to an internal one.
    pub(crate) fn rebound(host: &str) -> Self {
        Self {
            target: host.to_string(),
            reason: Arc::from(
                "this host first resolved to a public address and now resolves only to \
                 private, loopback, or link-local addresses, and a host is not allowed to move \
                 into an internal network while it is in use. If the host moved on purpose, \
                 reload the dataset or restart the runtime",
            ),
        }
    }
}

/// Every address the system resolver returns for `host`.
pub(crate) async fn lookup(host: &str) -> Result<Vec<SocketAddr>, BoxError> {
    Ok(tokio::net::lookup_host((host, 0)).await?.collect())
}
