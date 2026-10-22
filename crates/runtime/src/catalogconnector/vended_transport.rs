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
//! catalog-vended endpoint. A redirect to a hostname is resolved first and
//! refused when any address is internal or the lookup does not return a public
//! address. Operator-configured endpoints leave the task-local unset, so their
//! redirects are unchanged.

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
    if let Ok(component) = VENDED_REDIRECT_COMPONENT.try_with(Clone::clone) {
        if redirect_target_is_refused(attempt.url()) {
            let message = vended_redirect_refused_message(&component, attempt.url().as_str());
            return attempt.error(message);
        }
        // Return every guarded hop. `fetch_with_redirect_policy` applies the
        // method and body rules and resolves a domain before the next dial.
        // Following a public address inside `reqwest` and stopping on a later
        // domain would rebuild that request from the method and body saved
        // before the first hop, so a 303 that dropped an upload would be undone.
        return attempt.stop();
    }
    // `reqwest`'s default policy allows ten redirects, then errors.
    if attempt.previous().len() >= 10 {
        return attempt.error("too many redirects");
    }
    attempt.follow()
}

/// `None` when `target` may be dialed. A domain is resolved first; an internal
/// answer or a failed lookup is refused. Literal addresses are classified by
/// [`redirect_target_is_refused`].
async fn vended_redirect_host_refusal(component: &str, target: &reqwest::Url) -> Option<String> {
    if redirect_target_is_refused(target) {
        return Some(vended_redirect_refused_message(component, target.as_str()));
    }
    let Some(url::Host::Domain(domain)) = target.host() else {
        return None;
    };
    match super::resolve_vended_host(domain.to_string()).await {
        Ok(addrs) if addrs.iter().any(|ip| is_internal_ip(*ip)) => {
            Some(vended_redirect_refused_message(component, target.as_str()))
        }
        Ok(addrs) if !addrs.is_empty() => None,
        Ok(_) | Err(_) => Some(vended_redirect_unresolved_message(
            component,
            target.as_str(),
        )),
    }
}

fn vended_redirect_unresolved_message(component: &str, target: &str) -> String {
    format!(
        "The Iceberg catalog for {component} redirected storage to {target:?}, and its host \
         could not be resolved to a public address, so the redirect is not followed and the \
         table cannot be read. If this endpoint is trusted, set the `s3_endpoint` parameter to \
         it, or set `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true`. \
         See: https://spiceai.org/docs/components/data-connectors/iceberg"
    )
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
                .map_or_else(|| "/".to_string(), |path| path.as_str().to_string());
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

async fn fetch_with_redirect_policy(mut req: Request<Buffer>) -> Result<Response<HttpBody>> {
    for hop in 0..10 {
        let request_uri = req.uri().clone();
        let method = req.method().clone();
        let headers = req.headers().clone();
        let version = req.version();
        let body = req.body().clone();
        let response = send_once(req).await?;
        if !response.status().is_redirection() {
            return Ok(response);
        }
        let Some(component) = VENDED_REDIRECT_COMPONENT.try_with(Clone::clone).ok() else {
            return Ok(response);
        };
        let target = redirect_location(&request_uri, &response)?;
        if let Some(message) = vended_redirect_host_refusal(&component, &target).await {
            return Err(Error::new(ErrorKind::Unexpected, message).with_temporary(false));
        }
        if hop == 9 {
            break;
        }
        req = request_for_redirect(
            response.status(),
            &method,
            &headers,
            version,
            &body,
            &request_uri,
            &target,
        )?;
    }
    Err(Error::new(ErrorKind::Unexpected, "too many redirects"))
}

fn redirect_location(
    request_uri: &http::Uri,
    response: &Response<HttpBody>,
) -> Result<reqwest::Url> {
    let location = response
        .headers()
        .get(http::header::LOCATION)
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "redirect has no Location header"))?
        .to_str()
        .map_err(|err| {
            Error::new(ErrorKind::Unexpected, "redirect Location is not valid text").set_source(err)
        })?;
    let base = reqwest::Url::parse(&request_uri.to_string()).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "request url is invalid").set_source(err)
    })?;
    base.join(location).map_err(|err| {
        Error::new(
            ErrorKind::Unexpected,
            "redirect Location is not a valid URL",
        )
        .set_source(err)
    })
}

fn request_for_redirect(
    status: http::StatusCode,
    method: &http::Method,
    headers: &http::HeaderMap,
    version: http::Version,
    body: &Buffer,
    request_uri: &http::Uri,
    target: &reqwest::Url,
) -> Result<Request<Buffer>> {
    // `reqwest` follows operator redirects through `tower-http` 0.6.
    // 301 and 302 change only POST into GET and drop that body.
    // 303 changes every method except HEAD into GET and always drops the body.
    // 307 and 308 keep the method and the body. HEAD stays HEAD on all of these.
    let drop_body = match status {
        http::StatusCode::MOVED_PERMANENTLY | http::StatusCode::FOUND => {
            method == http::Method::POST
        }
        http::StatusCode::SEE_OTHER => true,
        _ => false,
    };
    let method = if drop_body && method != http::Method::HEAD {
        http::Method::GET
    } else {
        method.clone()
    };
    let body = if drop_body {
        Buffer::new()
    } else {
        body.clone()
    };
    let mut headers = headers.clone();
    // Scheme, host, and effective port. A new port, or https changing to http,
    // is a different origin and must not receive the first hop's credentials.
    if !same_redirect_origin(request_uri, target) {
        headers.remove(http::header::AUTHORIZATION);
        headers.remove(http::header::COOKIE);
        headers.remove(http::header::PROXY_AUTHORIZATION);
    }
    headers.remove(http::header::HOST);
    if drop_body {
        headers.remove(http::header::CONTENT_TYPE);
        headers.remove(http::header::CONTENT_LENGTH);
        headers.remove(http::header::CONTENT_ENCODING);
        headers.remove(http::header::TRANSFER_ENCODING);
    } else if body.is_empty() {
        headers.remove(http::header::CONTENT_LENGTH);
    }
    let uri = target.as_str().parse::<http::Uri>().map_err(|err| {
        Error::new(ErrorKind::Unexpected, "redirect target is not a valid URI").set_source(err)
    })?;
    let mut builder = Request::builder().method(method).uri(uri).version(version);
    if let Some(map) = builder.headers_mut() {
        *map = headers;
    }
    builder.body(body).map_err(|err| {
        Error::new(ErrorKind::Unexpected, "redirect request could not be built").set_source(err)
    })
}

fn same_redirect_origin(request_uri: &http::Uri, target: &reqwest::Url) -> bool {
    let (Some(left_scheme), Some(left_host), Some(right_host)) = (
        request_uri.scheme_str(),
        request_uri.host(),
        target.host_str(),
    ) else {
        return false;
    };
    if !left_scheme.eq_ignore_ascii_case(target.scheme())
        || !left_host.eq_ignore_ascii_case(right_host)
    {
        return false;
    }
    match (
        effective_port(left_scheme, request_uri.port_u16()),
        target.port_or_known_default(),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn effective_port(scheme: &str, explicit: Option<u16>) -> Option<u16> {
    explicit.or_else(|| match scheme.to_ascii_lowercase().as_str() {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    })
}

async fn send_once(req: Request<Buffer>) -> Result<Response<HttpBody>> {
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

#[cfg(test)]
mod tests {
    use super::request_for_redirect;

    use opendal::Buffer;

    fn signed_headers() -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("credential"),
        );
        headers.insert(
            http::header::COOKIE,
            http::HeaderValue::from_static("session=fixture"),
        );
        headers.insert(
            http::header::PROXY_AUTHORIZATION,
            http::HeaderValue::from_static("proxy-credential"),
        );
        headers.insert(
            http::header::HOST,
            http::HeaderValue::from_static("bucket.s3.amazonaws.com"),
        );
        headers
    }

    fn redirected(from: &str, to: &str) -> http::HeaderMap {
        let request_uri = from.parse().expect("request uri");
        let target = reqwest::Url::parse(to).expect("target url");
        let request = request_for_redirect(
            http::StatusCode::FOUND,
            &http::Method::GET,
            &signed_headers(),
            http::Version::HTTP_11,
            &Buffer::new(),
            &request_uri,
            &target,
        )
        .expect("build the redirected request");
        request.headers().clone()
    }

    fn assert_credentials_kept(headers: &http::HeaderMap) {
        assert_eq!(
            headers
                .get(http::header::AUTHORIZATION)
                .expect("authorization")
                .to_str()
                .expect("authorization is text"),
            "credential"
        );
        assert_eq!(
            headers
                .get(http::header::COOKIE)
                .expect("cookie")
                .to_str()
                .expect("cookie is text"),
            "session=fixture"
        );
        assert_eq!(
            headers
                .get(http::header::PROXY_AUTHORIZATION)
                .expect("proxy authorization")
                .to_str()
                .expect("proxy authorization is text"),
            "proxy-credential"
        );
        assert!(
            headers.get(http::header::HOST).is_none(),
            "Host is rebuilt for the next hop"
        );
    }

    fn assert_credentials_dropped(headers: &http::HeaderMap) {
        assert!(
            headers.get(http::header::AUTHORIZATION).is_none(),
            "authorization crossed origins: {headers:?}"
        );
        assert!(
            headers.get(http::header::COOKIE).is_none(),
            "cookie crossed origins: {headers:?}"
        );
        assert!(
            headers.get(http::header::PROXY_AUTHORIZATION).is_none(),
            "proxy authorization crossed origins: {headers:?}"
        );
    }

    #[test]
    fn redirect_keeps_sensitive_headers_only_for_the_same_origin() {
        assert_credentials_kept(&redirected(
            "http://bucket.s3.amazonaws.com/key",
            "http://bucket.s3.amazonaws.com/other",
        ));
        assert_credentials_kept(&redirected(
            "http://bucket.s3.amazonaws.com/key",
            "http://bucket.s3.amazonaws.com:80/other",
        ));
        assert_credentials_kept(&redirected(
            "https://bucket.s3.amazonaws.com:443/key",
            "https://BUCKET.s3.amazonaws.com/other",
        ));
        assert_credentials_dropped(&redirected(
            "http://bucket.s3.amazonaws.com/key",
            "http://bucket.s3.amazonaws.com:8443/other",
        ));
        assert_credentials_dropped(&redirected(
            "https://bucket.s3.amazonaws.com/key",
            "http://bucket.s3.amazonaws.com/other",
        ));
        assert_credentials_dropped(&redirected(
            "http://bucket.s3.amazonaws.com/key",
            "http://other.example/other",
        ));
    }

    /// Removes this test's connect and DNS overrides when the test ends.
    struct HeadRedirectOverrides {
        host: String,
    }

    impl HeadRedirectOverrides {
        fn install(host: &str, authority: &str) -> Self {
            super::set_vended_redirect_connect_override(host, authority);
            super::super::set_vended_endpoint_resolver_override(
                host,
                Ok(vec![std::net::Ipv4Addr::new(8, 8, 8, 8).into()]),
            );
            Self {
                host: host.to_ascii_lowercase(),
            }
        }
    }

    impl Drop for HeadRedirectOverrides {
        fn drop(&mut self) {
            super::clear_vended_redirect_connect_override(&self.host);
            super::super::clear_vended_endpoint_resolver_override(&self.host);
        }
    }

    #[tokio::test]
    async fn catalog_vended_head_redirect_stays_head() {
        let host = "head-redirect.vended.test";
        for (status, reason) in [
            (301_u16, "Moved Permanently"),
            (302, "Found"),
            (303, "See Other"),
            (307, "Temporary Redirect"),
            (308, "Permanent Redirect"),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the redirector");
            let addr = listener.local_addr().expect("read the redirector address");
            let other_port = addr.port().wrapping_add(1).max(1);
            let location = format!("http://{host}:{other_port}/object");
            let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let recorded = std::sync::Arc::clone(&requests);
            let redirect_to = location.clone();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let recorded = std::sync::Arc::clone(&recorded);
                    let redirect_to = redirect_to.clone();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut buf = vec![0_u8; 8192];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let head = String::from_utf8_lossy(&buf[..n]);
                        let line = head.lines().next().unwrap_or_default().to_string();
                        let is_head = line.starts_with("HEAD ");
                        let path = line.split_whitespace().nth(1).unwrap_or_default();
                        let response = if path == "/object" {
                            if is_head {
                                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                    .to_string()
                            } else {
                                "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                    .to_string()
                            }
                        } else {
                            format!(
                                "HTTP/1.1 {status} {reason}\r\nLocation: {redirect_to}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            )
                        };
                        recorded.lock().push(line);
                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                }
            });
            let _overrides = HeadRedirectOverrides::install(host, &addr.to_string());
            let request = http::Request::builder()
                .method(http::Method::HEAD)
                .uri(format!("http://{host}:{}/start", addr.port()))
                .body(opendal::Buffer::new())
                .expect("build the HEAD request");
            let response = super::scope(
                Some("dataset 't'".to_string()),
                super::fetch_with_redirect_policy(request),
            )
            .await
            .unwrap_or_else(|err| panic!("status {status} was not followed: {err}"));
            let recorded = requests.lock().clone();
            assert_eq!(
                response.status(),
                http::StatusCode::OK,
                "status {status} was not followed as HEAD: {recorded:?}"
            );
            assert_eq!(
                recorded,
                vec![
                    "HEAD /start HTTP/1.1".to_string(),
                    "HEAD /object HTTP/1.1".to_string(),
                ],
                "status {status}"
            );
        }
    }

    /// Read one `HTTP` request, including a body that arrives after the headers.
    ///
    /// One `read` can return only the header block. These fixtures record that
    /// text and assert on the body, so this waits until the last parsable
    /// `Content-Length` bytes have arrived after the header terminator, or
    /// until the peer closes the connection or two seconds pass. No
    /// `Content-Length` means the body is empty once the headers are complete.
    async fn read_http_request<R>(stream: &mut R) -> String
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 8192];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if http_message_is_complete(&buf) {
                break;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let n = match tokio::time::timeout(remaining, stream.read(&mut chunk)).await {
                Ok(Ok(n)) if n > 0 => n,
                _ => break,
            };
            buf.extend_from_slice(&chunk[..n]);
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// True once the header block is present and its declared body has arrived.
    fn http_message_is_complete(buf: &[u8]) -> bool {
        let Some(header_end) = buf.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let Some(length) = declared_body_length(&buf[..header_end]) else {
            return true;
        };
        buf.len() >= header_end.saturating_add(4).saturating_add(length)
    }

    /// The last `Content-Length` value that parses. An unparsable length does
    /// not replace an earlier one.
    fn declared_body_length(headers: &[u8]) -> Option<usize> {
        let header_text = std::str::from_utf8(headers).ok()?;
        let mut length = None;
        for line in header_text.split("\r\n") {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if name.eq_ignore_ascii_case("content-length")
                && let Ok(parsed) = value.trim().parse::<usize>()
            {
                length = Some(parsed);
            }
        }
        length
    }

    /// One chunk per `read`, then end of file. The upload tests deliver the
    /// headers and the body as separate chunks.
    struct ScriptedRead {
        chunks: Vec<&'static [u8]>,
        next: usize,
    }

    impl tokio::io::AsyncRead for ScriptedRead {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.next >= self.chunks.len() {
                return std::task::Poll::Ready(Ok(()));
            }
            let chunk = self.chunks[self.next];
            self.next += 1;
            let n = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..n]);
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn split_http_upload_is_recorded_whole() {
        for (label, chunks) in [
            (
                "headers then body",
                vec![
                    &b"PUT /object HTTP/1.1\r\nContent-Length: 15\r\n\r\n"[..],
                    &b"uploaded-object"[..],
                ],
            ),
            (
                "the last content-length",
                vec![
                    &b"PUT /object HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 15\r\n\r\n"[..],
                    &b"uploaded-object"[..],
                ],
            ),
        ] {
            let mut stream = ScriptedRead { chunks, next: 0 };
            let recorded = read_http_request(&mut stream).await;
            assert!(
                recorded.contains("uploaded-object"),
                "{label}: a body that arrives after the headers was dropped: {recorded:?}"
            );
        }
    }

    #[tokio::test]
    async fn catalog_vended_put_redirect_keeps_the_upload() {
        let host = "put-redirect.vended.test";
        for (status, reason, method, expect_method, expect_body) in [
            (301_u16, "Moved Permanently", "PUT", "PUT", true),
            (302, "Found", "PUT", "PUT", true),
            (302, "Found", "DELETE", "DELETE", true),
            (303, "See Other", "PUT", "GET", false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind the redirector");
            let addr = listener.local_addr().expect("read the redirector address");
            let other_port = addr.port().wrapping_add(1).max(1);
            let location = format!("http://{host}:{other_port}/object");
            let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
            let recorded = std::sync::Arc::clone(&requests);
            let redirect_to = location.clone();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let recorded = std::sync::Arc::clone(&recorded);
                    let redirect_to = redirect_to.clone();
                    tokio::spawn(async move {
                        use tokio::io::AsyncWriteExt;
                        let head = read_http_request(&mut stream).await;
                        let line = head.lines().next().unwrap_or_default();
                        let path = line.split_whitespace().nth(1).unwrap_or_default();
                        let response = if path == "/object" {
                            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                                .to_string()
                        } else {
                            format!(
                                "HTTP/1.1 {status} {reason}\r\nLocation: {redirect_to}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            )
                        };
                        recorded.lock().push(head);
                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                }
            });
            let _overrides = HeadRedirectOverrides::install(host, &addr.to_string());
            let payload = "uploaded-object";
            let request = http::Request::builder()
                .method(method)
                .uri(format!("http://{host}:{}/start", addr.port()))
                .header(http::header::CONTENT_LENGTH, payload.len())
                .body(opendal::Buffer::from(payload))
                .expect("build the upload request");
            let response = super::scope(
                Some("dataset 't'".to_string()),
                super::fetch_with_redirect_policy(request),
            )
            .await
            .unwrap_or_else(|err| panic!("status {status} {method} was not followed: {err}"));
            let recorded = requests.lock().clone();
            assert_eq!(
                response.status(),
                http::StatusCode::OK,
                "status {status} {method}: {recorded:?}"
            );
            assert_eq!(recorded.len(), 2, "status {status} {method}: {recorded:?}");
            assert!(
                recorded[1].starts_with(&format!("{expect_method} /object ")),
                "status {status} {method} followed as {expect_method}: {recorded:?}"
            );
            let second_has_body = recorded[1].contains("uploaded-object");
            assert_eq!(
                second_has_body, expect_body,
                "status {status} {method} body: {recorded:?}"
            );
        }
    }

    /// Clears the connect and DNS overrides for the mixed-redirect test.
    struct MixedRedirectOverrides {
        hosts: Vec<String>,
        resolved: String,
    }

    impl MixedRedirectOverrides {
        /// `1.2.3.4` is not in the internal-address set. The listener is
        /// loopback; `send_once` rewrites that literal host onto it.
        fn prepare() -> Self {
            let resolved = "mixed-target.vended.test";
            super::super::set_vended_endpoint_resolver_override(
                resolved,
                Ok(vec![std::net::Ipv4Addr::new(1, 1, 1, 1).into()]),
            );
            Self {
                hosts: Vec::new(),
                resolved: resolved.to_string(),
            }
        }

        fn point_at(&mut self, authority: &str) {
            let hosts = [
                "mixed-redirect.vended.test",
                "1.2.3.4",
                "mixed-target.vended.test",
            ];
            for host in hosts {
                super::set_vended_redirect_connect_override(host, authority);
            }
            self.hosts = hosts.into_iter().map(str::to_ascii_lowercase).collect();
        }
    }

    impl Drop for MixedRedirectOverrides {
        fn drop(&mut self) {
            for host in &self.hosts {
                super::clear_vended_redirect_connect_override(host);
            }
            super::super::clear_vended_endpoint_resolver_override(&self.resolved);
        }
    }

    #[tokio::test]
    async fn catalog_vended_mixed_redirect_keeps_the_rewritten_request() {
        let mut overrides = MixedRedirectOverrides::prepare();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the redirector");
        let addr = listener.local_addr().expect("read the redirector address");
        let port = addr.port();
        // The Location names the public literal. `send_once` rewrites it to
        // this listener, so the test does not bind that address.
        let middle = format!("http://1.2.3.4:{port}/middle");
        let object = format!("http://mixed-target.vended.test:{port}/object");
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorded = std::sync::Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let recorded = std::sync::Arc::clone(&recorded);
                let middle = middle.clone();
                let object = object.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let head = read_http_request(&mut stream).await;
                    let line = head.lines().next().unwrap_or_default();
                    let path = line.split_whitespace().nth(1).unwrap_or_default();
                    let response = if path == "/middle" {
                        format!(
                            "HTTP/1.1 307 Temporary Redirect\r\nLocation: {object}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                    } else if path == "/object" {
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    } else {
                        format!(
                            "HTTP/1.1 303 See Other\r\nLocation: {middle}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                    };
                    recorded.lock().push(head);
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        overrides.point_at(&addr.to_string());
        let _overrides = overrides;
        let payload = "uploaded-object";
        let request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(format!(
                "http://mixed-redirect.vended.test:{}/start",
                addr.port()
            ))
            .header(http::header::CONTENT_LENGTH, payload.len())
            .body(opendal::Buffer::from(payload))
            .expect("build the upload request");
        let response = super::scope(
            Some("dataset 't'".to_string()),
            super::fetch_with_redirect_policy(request),
        )
        .await
        .unwrap_or_else(|err| panic!("the mixed redirect was not followed: {err}"));
        let recorded = requests.lock().clone();
        assert_eq!(response.status(), http::StatusCode::OK, "{recorded:?}");
        assert_eq!(recorded.len(), 3, "{recorded:?}");
        assert!(
            recorded[0].starts_with("PUT /start "),
            "first hop: {recorded:?}"
        );
        assert!(
            recorded[0].contains("uploaded-object"),
            "upload left the first hop: {recorded:?}"
        );
        assert!(
            recorded[1].starts_with("GET /middle "),
            "303 should be GET: {recorded:?}"
        );
        assert!(
            !recorded[1].contains("uploaded-object"),
            "303 should drop the body: {recorded:?}"
        );
        assert!(
            recorded[2].starts_with("GET /object "),
            "307 should keep the GET from the 303: {recorded:?}"
        );
        assert!(
            !recorded[2].contains("uploaded-object"),
            "the dropped upload was restored: {recorded:?}"
        );
    }
}
