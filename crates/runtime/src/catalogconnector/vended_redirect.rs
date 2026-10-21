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

//! Redirect policy for catalog-vended Iceberg storage.
//!
//! `OpenDAL`'s default `reqwest` transport follows redirects inside `Client::send`.
//! [`GuardedStorage`](super::GuardedStorage) checks the endpoint before that
//! call, so a public catalog endpoint can answer `302 Location` with an
//! internal URL and the client dials it. This transport is installed before
//! `OpenDAL`'s own default and refuses that hop when the request belongs to a
//! catalog-vended endpoint. Operator-configured endpoints leave the task-local
//! unset, so their redirects are unchanged.

use std::error::Error as StdError;
use std::future::Future;
use std::mem;
use std::sync::LazyLock;

use futures::TryStreamExt;
use http::Request;
use http::Response;
use iceberg::io::{FileRead, FileWrite};
use opendal::Buffer;
use opendal::Error;
use opendal::ErrorKind;
use opendal::HttpBody;
use opendal::HttpTransport;
use opendal::HttpTransporter;
use opendal::Result;
use opendal::raw::parse_content_encoding;
use opendal::raw::parse_content_length;
use runtime_datafusion::url_table::{is_blocked_internal_hostname, is_internal_ip};

tokio::task_local! {
    static VENDED_REDIRECT_COMPONENT: String;
}

/// Runs `fut` with the catalog component recorded, so a redirect the storage
/// client follows can be refused. `None` leaves operator redirects unchanged.
pub(super) async fn scope<T>(component: Option<String>, fut: impl Future<Output = T>) -> T {
    match component {
        Some(component) => VENDED_REDIRECT_COMPONENT.scope(component, fut).await,
        None => fut.await,
    }
}

/// [`FileRead`] that keeps the redirect guard around range reads. The open
/// happens in [`Storage::reader`](iceberg::io::Storage::reader); later ranges
/// are separate requests.
pub(super) struct ScopedFileRead {
    pub(super) inner: Box<dyn FileRead>,
    pub(super) component: Option<String>,
}

#[async_trait::async_trait]
impl FileRead for ScopedFileRead {
    async fn read(&self, range: std::ops::Range<u64>) -> iceberg::Result<bytes::Bytes> {
        scope(self.component.clone(), self.inner.read(range)).await
    }
}

/// [`FileWrite`] that keeps the redirect guard around each upload request.
pub(super) struct ScopedFileWrite {
    pub(super) inner: Box<dyn FileWrite>,
    pub(super) component: Option<String>,
}

#[async_trait::async_trait]
impl FileWrite for ScopedFileWrite {
    async fn write(&mut self, bs: bytes::Bytes) -> iceberg::Result<()> {
        let component = self.component.clone();
        scope(component, self.inner.write(bs)).await
    }

    async fn close(&mut self) -> iceberg::Result<()> {
        let component = self.component.clone();
        scope(component, self.inner.close()).await
    }
}

/// User-facing refusal for a redirect whose target is internal.
#[must_use]
pub(super) fn vended_redirect_refused_message(component: &str, target: &str) -> String {
    format!(
        "The Iceberg catalog for {component} redirected storage to {target:?}, a private, \
         loopback, or link-local address, so the redirect is not followed and the table cannot \
         be read. If this endpoint is trusted, set the `s3_endpoint` parameter to it, or set \
         `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true`. \
         See: https://spiceai.org/docs/components/data-connectors/iceberg"
    )
}

fn redirect_target_is_refused(target: &reqwest::Url) -> bool {
    if target.scheme() != "http" && target.scheme() != "https" {
        return true;
    }
    match target.host() {
        Some(url::Host::Ipv4(ip)) => is_internal_ip(ip.into()),
        Some(url::Host::Ipv6(ip)) => is_internal_ip(ip.into()),
        Some(url::Host::Domain(domain)) => is_blocked_internal_hostname(domain),
        None => true,
    }
}

fn redirect_policy(attempt: reqwest::redirect::Attempt<'_>) -> reqwest::redirect::Action {
    if let Ok(component) = VENDED_REDIRECT_COMPONENT.try_with(Clone::clone)
        && redirect_target_is_refused(attempt.url())
    {
        let message = vended_redirect_refused_message(&component, attempt.url().as_str());
        return attempt.error(message);
    }
    // `reqwest`'s default policy allows ten redirects, then errors.
    if attempt.previous().len() >= 10 {
        return attempt.error("too many redirects");
    }
    attempt.follow()
}

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(redirect_policy))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
});

/// Test stand-in that sends a vended host to a local fixture. The URL the
/// guard classified stays unchanged; only this process's storage client uses
/// the replacement authority. Production builds have no override.
#[cfg(test)]
static CONNECT_OVERRIDE: LazyLock<parking_lot::Mutex<std::collections::HashMap<String, String>>> =
    LazyLock::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

/// Points storage connections for `host` at `authority` (`host:port`).
#[cfg(test)]
pub(super) fn set_vended_redirect_connect_override(host: &str, authority: &str) {
    CONNECT_OVERRIDE
        .lock()
        .insert(host.to_ascii_lowercase(), authority.to_string());
}

/// Removes the connect override installed for `host`.
#[cfg(test)]
pub(super) fn clear_vended_redirect_connect_override(host: &str) {
    CONNECT_OVERRIDE.lock().remove(&host.to_ascii_lowercase());
}

fn rewrite_uri(uri: http::Uri) -> http::Uri {
    #[cfg(test)]
    {
        let Some(host) = uri.host() else {
            return uri;
        };
        let authority = {
            let overrides = CONNECT_OVERRIDE.lock();
            overrides.get(&host.to_ascii_lowercase()).cloned()
        };
        if let Some(authority) = authority {
            let path = uri
                .path_and_query()
                .map(|path| path.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            let scheme = uri.scheme_str().unwrap_or("http");
            if let Ok(rewritten) = format!("{scheme}://{authority}{path}").parse() {
                return rewritten;
            }
        }
    }
    uri
}

struct VendedRedirectTransport;

impl HttpTransport for VendedRedirectTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        fetch_with_redirect_policy(req).await
    }
}

async fn fetch_with_redirect_policy(req: Request<Buffer>) -> Result<Response<HttpBody>> {
    let uri = rewrite_uri(req.uri().clone());
    let is_head = req.method() == http::Method::HEAD;
    let (parts, body) = req.into_parts();

    let url = reqwest::Url::parse(&uri.to_string()).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "request url is invalid")
            .with_operation("reqwest::fetch")
            .with_context("url", uri.to_string())
            .set_source(err)
    })?;

    let mut req_builder = CLIENT.request(parts.method, url).headers(parts.headers);
    req_builder = req_builder.version(parts.version);
    if !body.is_empty() {
        req_builder = req_builder.body(reqwest::Body::wrap(HttpBufferBody(body)));
    }

    let mut resp = req_builder.send().await.map_err(|err| {
        // A refused redirect carries the user-facing message as its source.
        // `reqwest`'s own display is only "error following redirect".
        let message = if err.is_redirect() {
            StdError::source(&err).map_or_else(
                || "error following redirect".to_string(),
                ToString::to_string,
            )
        } else {
            "send http request".to_string()
        };
        Error::new(ErrorKind::Unexpected, message)
            .with_operation("reqwest::send")
            .with_context("url", uri.to_string())
            .with_temporary(is_temporary_error(&err))
            .set_source(err)
    })?;

    let content_length = if is_head || parse_content_encoding(resp.headers())?.is_some() {
        None
    } else {
        parse_content_length(resp.headers())?
    };

    let mut hr = Response::builder()
        .status(resp.status())
        .version(resp.version())
        .extension(uri.clone());
    let headers = hr.headers_mut().ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "HTTP response headers could not be built",
        )
    })?;
    mem::swap(headers, resp.headers_mut());

    let bs = HttpBody::new(
        resp.bytes_stream()
            .try_filter(|chunk| std::future::ready(!chunk.is_empty()))
            .map_ok(Buffer::from)
            .map_err(move |err| {
                Error::new(ErrorKind::Unexpected, "read data from http response")
                    .with_operation("reqwest::fetch")
                    .with_context("url", uri.to_string())
                    .with_temporary(is_temporary_error(&err))
                    .set_source(err)
            }),
        content_length,
    );

    hr.body(bs).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "HTTP response could not be built").set_source(err)
    })
}

#[inline]
fn is_temporary_error(err: &reqwest::Error) -> bool {
    // A redirect the policy refused is the answer, not a blip to retry.
    if err.is_redirect() {
        return false;
    }
    err.is_request() || err.is_body() || err.is_decode()
}

struct HttpBufferBody(Buffer);

impl http_body::Body for HttpBufferBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>>
    {
        match self.0.next() {
            Some(chunk) => std::task::Poll::Ready(Some(Ok(http_body::Frame::data(chunk)))),
            None => std::task::Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }
}

// SAFETY: runs before `main`, ahead of OpenDAL's default-priority constructor
// (`priority` 500; `early` is 101). `HttpTransporter::install_default` keeps
// the first transport, so this occupies the process-wide client. The
// constructor only stores the transport; the reqwest client is built on the
// first request.
#[ctor::ctor(unsafe, priority = early)]
fn install_vended_redirect_transport() {
    HttpTransporter::install_default(VendedRedirectTransport);
}
