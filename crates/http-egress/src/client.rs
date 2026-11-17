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

//! The egress policy for `reqwest` clients.

use std::sync::Arc;

use bytes::Bytes;
use http::{
    HeaderMap, Method, StatusCode,
    header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, LOCATION, TRANSFER_ENCODING},
};
use reqwest::{Client, ClientBuilder, Request, Response, redirect::Policy};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::egress::{
    BodyAcrossOriginsSnafu, BodyNotReplayableSnafu, Egress, InvalidLocationSnafu, RedirectRefused,
    origin_label,
};

/// Headers that describe a request body, dropped with it.
const BODY_HEADERS: [http::HeaderName; 4] = [
    CONTENT_TYPE,
    CONTENT_LENGTH,
    CONTENT_ENCODING,
    TRANSFER_ENCODING,
];

/// A `reqwest` resolver that resolves through an [`Egress`].
#[derive(Clone, Debug)]
pub struct Resolver {
    egress: Egress,
}

impl Resolver {
    #[must_use]
    pub fn new(egress: Egress) -> Self {
        Self { egress }
    }
}

impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let egress = self.egress.clone();
        let name = name.as_str().to_string();
        Box::pin(async move {
            let addresses = egress.resolve(&name).await?;
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Configures `builder` for [`send`]: `reqwest` follows no redirect itself, and every host is
/// resolved through `egress`.
#[must_use]
pub fn configure(builder: ClientBuilder, egress: &Egress) -> ClientBuilder {
    builder
        .redirect(Policy::none())
        .dns_resolver(Arc::new(Resolver::new(egress.clone())))
}

/// What a client built with [`redirect_policy`] does when a redirect leaves the request's
/// origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnOriginChange {
    /// Follow it. Only for a client whose credentials are all ones `reqwest` removes on an
    /// origin change (`Authorization`, `Cookie`, `Proxy-Authorization`): a custom header and a
    /// `307`/`308` body are carried to the new origin. Use [`send`] for anything else.
    Follow,
    /// Refuse it, for a request whose credential travels in its body or a custom header and
    /// whose server has no reason to send it elsewhere, such as an OAuth token request.
    Refuse,
}

/// A redirect policy that applies `egress` to a client that lets `reqwest` follow redirects.
#[must_use]
pub fn redirect_policy(egress: Egress, on_origin_change: OnOriginChange) -> Policy {
    Policy::custom(move |attempt| {
        let decision = egress.redirect(attempt.previous(), attempt.url());
        let leaves = matches!(decision, Ok(hop) if hop.left_origin());
        match decision {
            Err(refused) => attempt.error(refused),
            Ok(_) if leaves && on_origin_change == OnOriginChange::Refuse => {
                let origin = attempt
                    .previous()
                    .first()
                    .map_or_else(String::new, origin_label);
                let target = origin_label(attempt.url());
                attempt.error(RedirectRefused::CrossOrigin { origin, target })
            }
            Ok(_) => attempt.follow(),
        }
    })
}

/// Why [`send`] did not return a response.
#[derive(Debug, Snafu)]
pub enum SendError {
    /// The request itself failed; `source` keeps `reqwest`'s classification (connect, timeout,
    /// a guard's refusal while resolving).
    #[snafu(display("{source}"))]
    Http { source: reqwest::Error },

    /// The egress policy refused a redirect.
    #[snafu(display("{source}"))]
    Redirect { source: RedirectRefused },
}

impl SendError {
    /// The `reqwest` error, when the request failed rather than a redirect being refused.
    #[must_use]
    pub fn as_http(&self) -> Option<&reqwest::Error> {
        match self {
            Self::Http { source } => Some(source),
            Self::Redirect { .. } => None,
        }
    }
}

/// A request body as [`send`] tracks it across redirects.
enum Body {
    None,
    Buffered(Bytes),
    /// A streaming body, which cannot be sent a second time.
    Streaming,
}

/// Sends `request` with `client`, following redirects under `egress`.
///
/// `client` must be built with [`configure`] and the same `egress`, so `reqwest` hands every
/// redirect back here. While the chain stays on the request's origin, each hop carries the
/// request's own headers. Once it leaves that origin, a hop carries none of them and no body:
/// any header name can carry a credential (an OAuth token under a custom name, an API key in
/// `http_headers`), so none is judged safe to forward. A `303`, or a `301`/`302` answering a
/// `POST`, becomes a `GET` as `reqwest` would make it, and a `307`/`308` that would resend a
/// body to another origin is refused. Headers the client sets on every request
/// (`ClientBuilder::default_headers`, the user agent) are sent on every hop, so a credential
/// belongs on the request, never on the client.
///
/// # Errors
///
/// Returns [`SendError::Http`] when a request fails and [`SendError::Redirect`] when the
/// egress policy refuses a redirect.
pub async fn send(
    client: &Client,
    request: Request,
    egress: &Egress,
) -> Result<Response, SendError> {
    let headers = request.headers().clone();
    let timeout = request.timeout().copied();
    let version = request.version();
    let mut method = request.method().clone();
    let mut body = match request.body() {
        None => Body::None,
        Some(body) => body.as_bytes().map_or(Body::Streaming, |bytes| {
            Body::Buffered(Bytes::copy_from_slice(bytes))
        }),
    };
    let mut chain = vec![request.url().clone()];
    let mut current = request;

    loop {
        let response = client.execute(current).await.context(HttpSnafu)?;
        let status = response.status();
        if !matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            return Ok(response);
        }
        // A redirect status without a `Location` is an answer, not a redirect.
        let Some(location) = response.headers().get(LOCATION) else {
            return Ok(response);
        };
        let next = location
            .to_str()
            .ok()
            .zip(chain.last())
            .and_then(|(location, previous)| previous.join(location).ok())
            .context(InvalidLocationSnafu {
                status: status.as_u16(),
            })
            .context(RedirectSnafu)?;
        let hop = egress.redirect(&chain, &next).context(RedirectSnafu)?;

        let resends_body = matches!(
            status,
            StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
        );
        if !resends_body
            && ((status == StatusCode::SEE_OTHER && method != Method::HEAD)
                || method == Method::POST)
        {
            method = Method::GET;
            body = Body::None;
        }

        let mut next_headers = if hop.left_origin() {
            if !matches!(body, Body::None) {
                return BodyAcrossOriginsSnafu {
                    status: status.as_u16(),
                    target: origin_label(&next),
                }
                .fail()
                .context(RedirectSnafu);
            }
            HeaderMap::new()
        } else {
            headers.clone()
        };

        let mut next_request = Request::new(method.clone(), next.clone());
        match &body {
            Body::None => {
                for name in &BODY_HEADERS {
                    next_headers.remove(name);
                }
            }
            Body::Buffered(bytes) => *next_request.body_mut() = Some(bytes.clone().into()),
            Body::Streaming => {
                return BodyNotReplayableSnafu {
                    status: status.as_u16(),
                }
                .fail()
                .context(RedirectSnafu);
            }
        }
        *next_request.headers_mut() = next_headers;
        *next_request.timeout_mut() = timeout;
        *next_request.version_mut() = version;

        chain.push(next);
        current = next_request;
    }
}
