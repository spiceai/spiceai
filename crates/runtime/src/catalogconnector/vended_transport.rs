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

//! HTTP transport for catalog-vended Iceberg storage.
//!
//! [`GuardedStorage`](super::GuardedStorage) refuses a vended endpoint whose
//! host is a literal internal address. A domain can still resolve to one, and
//! a public endpoint can redirect to one. `OpenDAL` sends every storage request
//! through one process-wide transport, so this transport is installed ahead of
//! `OpenDAL`'s own default. A request to a vended host uses a `reqwest` client
//! whose resolver refuses internal answers and whose redirect policy refuses
//! internal literal addresses. Every other request, including credential
//! requests in the same operation, uses `OpenDAL`'s default client unchanged.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use http::{Request, Response};
use iceberg::io::{FileRead, FileWrite};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Result};
use opendal_http_transport_reqwest::ReqwestTransport;
use runtime_object_store::ssrf::{
    ALLOW_PRIVATE_HOSTS_ENV, is_blocked_internal_hostname, is_internal_ip,
};

const DNS_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(5);

tokio::task_local! {
    static VENDED_HOSTS: VendedHosts;
}

/// Hosts a catalog vended for one storage location, and the dataset or
/// catalog that location belongs to.
#[derive(Clone, Debug)]
pub(super) struct VendedHosts {
    component: String,
    hosts: Arc<[String]>,
}

impl VendedHosts {
    /// `None` when `hosts` is empty, so the operation is not guarded.
    pub(super) fn new(component: &str, hosts: Vec<String>) -> Option<Self> {
        (!hosts.is_empty()).then(|| Self {
            component: component.to_string(),
            hosts: hosts.into(),
        })
    }

    fn guards(&self, uri: &http::Uri) -> bool {
        uri.host().is_some_and(|host| {
            let host = normalized_host(host);
            self.hosts.iter().any(|guarded| *guarded == host)
        })
    }
}

/// Lowercased, without a trailing dot, so it compares with a URL host.
pub(super) fn normalized_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

/// Runs `fut` with `hosts` recorded, so requests to them are guarded.
pub(super) async fn scope<T>(hosts: Option<VendedHosts>, fut: impl Future<Output = T>) -> T {
    match hosts {
        Some(hosts) => VENDED_HOSTS.scope(hosts, fut).await,
        None => fut.await,
    }
}

/// [`FileRead`] that keeps the guard around range reads. The open happens in
/// [`Storage::reader`](iceberg::io::Storage::reader); later ranges are
/// separate requests.
pub(super) struct ScopedFileRead {
    pub(super) inner: Box<dyn FileRead>,
    pub(super) hosts: Option<VendedHosts>,
}

#[async_trait::async_trait]
impl FileRead for ScopedFileRead {
    async fn read(&self, range: std::ops::Range<u64>) -> iceberg::Result<bytes::Bytes> {
        scope(self.hosts.clone(), self.inner.read(range)).await
    }
}

/// [`FileWrite`] that keeps the guard around each upload request.
pub(super) struct ScopedFileWrite {
    pub(super) inner: Box<dyn FileWrite>,
    pub(super) hosts: Option<VendedHosts>,
}

#[async_trait::async_trait]
impl FileWrite for ScopedFileWrite {
    async fn write(&mut self, bs: bytes::Bytes) -> iceberg::Result<()> {
        scope(self.hosts.clone(), self.inner.write(bs)).await
    }

    async fn close(&mut self) -> iceberg::Result<()> {
        scope(self.hosts.clone(), self.inner.close()).await
    }
}

/// A connection or redirect target that the guarded client does not dial.
#[derive(Debug)]
struct Refused {
    target: String,
    unresolved: bool,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.unresolved {
            write!(
                f,
                "{:?} could not be resolved to a public address",
                self.target
            )
        } else {
            write!(
                f,
                "{:?} is or resolves to a private, loopback, or link-local address",
                self.target
            )
        }
    }
}

impl std::error::Error for Refused {}

/// User-facing error for a storage request the guarded client refused.
#[must_use]
pub(super) fn vended_storage_refused_message(component: &str, refused: &impl ToString) -> String {
    format!(
        "The Iceberg catalog for {component} directs storage requests to a host that is not \
         allowed: {}, so the request is not sent and the table cannot be read. If this endpoint \
         is trusted, set the `s3_endpoint` parameter to it, or set `{ALLOW_PRIVATE_HOSTS_ENV}=true`. \
         See: https://spiceai.org/docs/components/data-connectors/iceberg",
        refused.to_string()
    )
}

/// Refuses a redirect to an internal literal address. A domain target is
/// checked by [`GuardedResolver`] when the client connects to it.
fn redirect_policy(attempt: reqwest::redirect::Attempt<'_>) -> reqwest::redirect::Action {
    let internal = match attempt.url().host() {
        Some(url::Host::Ipv4(ip)) => is_internal_ip(ip.into()),
        Some(url::Host::Ipv6(ip)) => is_internal_ip(ip.into()),
        Some(url::Host::Domain(_)) => false,
        None => true,
    };
    if internal {
        let target = attempt.url().to_string();
        return attempt.error(Refused {
            target,
            unresolved: false,
        });
    }
    // `reqwest`'s default policy allows ten redirects, then errors.
    if attempt.previous().len() >= 10 {
        return attempt.error("too many redirects");
    }
    attempt.follow()
}

/// Resolves a host and returns its addresses only when every one is public.
/// The client connects to exactly the addresses checked here.
struct GuardedResolver;

impl reqwest::dns::Resolve for GuardedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = normalized_host(name.as_str());
        Box::pin(async move {
            let addrs = public_addrs(host).await?;
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

async fn public_addrs(host: String) -> std::result::Result<Vec<SocketAddr>, Refused> {
    if is_blocked_internal_hostname(&host) {
        return Err(Refused {
            target: host,
            unresolved: false,
        });
    }
    #[cfg(test)]
    if let Some((addrs, treat_as_public)) = resolver_override(&host) {
        let addrs = addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
        return if treat_as_public {
            Ok(addrs)
        } else {
            classify(host, addrs)
        };
    }
    let addrs = match tokio::time::timeout(
        DNS_RESOLUTION_TIMEOUT,
        tokio::net::lookup_host((host.as_str(), 0)),
    )
    .await
    {
        Ok(Ok(addrs)) => addrs.collect(),
        Ok(Err(_)) | Err(_) => Vec::new(),
    };
    classify(host, addrs)
}

fn classify(host: String, addrs: Vec<SocketAddr>) -> std::result::Result<Vec<SocketAddr>, Refused> {
    if addrs.is_empty() {
        return Err(Refused {
            target: host,
            unresolved: true,
        });
    }
    if addrs.iter().any(|addr| is_internal_ip(addr.ip())) {
        return Err(Refused {
            target: host,
            unresolved: false,
        });
    }
    Ok(addrs)
}

/// A test answer for [`GuardedResolver`]: addresses, and whether they are
/// treated as public so a test can point a public name at a local fixture.
#[cfg(test)]
type ResolverAnswer = (Vec<std::net::IpAddr>, bool);

/// Test answers per host, used in order; the last one repeats. Production
/// builds always use the system resolver.
#[cfg(test)]
type ResolverOverrides =
    std::collections::HashMap<String, std::collections::VecDeque<ResolverAnswer>>;

#[cfg(test)]
static RESOLVER_OVERRIDES: LazyLock<parking_lot::Mutex<ResolverOverrides>> =
    LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
fn resolver_override(host: &str) -> Option<ResolverAnswer> {
    let mut overrides = RESOLVER_OVERRIDES.lock();
    let answers = overrides.get_mut(host)?;
    if answers.len() > 1 {
        answers.pop_front()
    } else {
        answers.front().cloned()
    }
}

#[cfg(test)]
pub(super) fn set_resolver_override(
    host: &str,
    addrs: Vec<std::net::IpAddr>,
    treat_as_public: bool,
) {
    set_resolver_answers(host, vec![(addrs, treat_as_public)]);
}

#[cfg(test)]
pub(super) fn set_resolver_answers(host: &str, answers: Vec<ResolverAnswer>) {
    RESOLVER_OVERRIDES
        .lock()
        .insert(normalized_host(host), answers.into());
}

#[cfg(test)]
pub(super) fn clear_resolver_override(host: &str) {
    RESOLVER_OVERRIDES.lock().remove(&normalized_host(host));
}

static GUARDED: LazyLock<std::result::Result<ReqwestTransport, String>> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(redirect_policy))
        .dns_resolver(GuardedResolver)
        .build()
        .map(ReqwestTransport::new)
        .map_err(|err| err.to_string())
});

fn refusal(err: &Error) -> Option<&Refused> {
    let mut source = std::error::Error::source(err);
    while let Some(err) = source {
        if let Some(refused) = err.downcast_ref::<Refused>() {
            return Some(refused);
        }
        source = err.source();
    }
    None
}

struct VendedStorageTransport;

impl HttpTransport for VendedStorageTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let component = VENDED_HOSTS
            .try_with(|hosts| hosts.guards(req.uri()).then(|| hosts.component.clone()))
            .ok()
            .flatten();
        let Some(component) = component else {
            return ReqwestTransport::default().fetch(req).await;
        };
        let transport = GUARDED.as_ref().map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to build the storage HTTP client for {component}: {err}"),
            )
        })?;
        transport
            .fetch(req)
            .await
            .map_err(|err| match refusal(&err) {
                // Not temporary, so `OpenDAL`'s retry layer does not send it again.
                Some(refused) => Error::new(
                    ErrorKind::Unexpected,
                    vended_storage_refused_message(&component, refused),
                ),
                None => err,
            })
    }
}

// SAFETY: runs before `main`, ahead of OpenDAL's default-priority constructor
// (`priority` 500; `early` is 101). `HttpTransporter::install_default` keeps
// the first transport, so this occupies the process-wide client. The
// constructor only stores the transport; the reqwest clients are built on the
// first request.
#[ctor::ctor(unsafe, priority = early)]
fn install_vended_storage_transport() {
    HttpTransporter::install_default(VendedStorageTransport);
}
