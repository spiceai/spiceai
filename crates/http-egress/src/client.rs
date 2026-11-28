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
    BodyAcrossOriginsSnafu, BodyNotReplayableSnafu, Egress, Hop, InvalidLocationSnafu,
    RedirectRefused, origin_label, redirect_continues_off_origin,
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
pub fn configure(builder: ClientBuilder, egress: &Egress) -> ClientBuilder {
    builder
        .redirect(Policy::none())
        .dns_resolver(Arc::new(Resolver::new(egress.clone())))
}

/// What a client does when a redirect leaves the request's origin, whether `reqwest` follows
/// its redirects ([`redirect_policy`]) or [`send`] does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnOriginChange {
    /// Follow it, for a request that carries no credential. Under [`redirect_policy`], a custom
    /// header and a `307`/`308` body are carried to the new origin. Under [`send`], the new
    /// origin receives none of the request's headers and no body.
    Follow,
    /// Follow it, but refuse a later redirect that stays on the host and port the chain moved
    /// to ([`redirect_continues_off_origin`]), for a request whose credentials are all ones
    /// `reqwest` removes when a redirect changes the host or port (`Authorization`, `Cookie`,
    /// `Proxy-Authorization`): `reqwest` rebuilds every hop from the request's own headers and
    /// removes them only when that hop changes the host or port, so it sends them again on such
    /// a redirect. [`send`] carries no header off the origin, and refuses the same redirect, so
    /// a request with credentials follows the same chains whichever way it is sent.
    FollowWithCredentials,
    /// Refuse it, for a request whose credential travels in its body or a custom header and
    /// whose server has no reason to send it elsewhere, such as an OAuth token request.
    Refuse,
}

/// Whether a redirect to `next` is followed after the URLs in `chain` (the request's own URL
/// first) were requested: `egress` decides, and then `on_origin_change` refuses a hop that
/// leaves the request's origin ([`OnOriginChange::Refuse`]) or that continues a chain on the
/// host it moved to ([`OnOriginChange::FollowWithCredentials`]).
///
/// [`redirect_policy`] and [`send`] apply this decision; a client built on another `reqwest`
/// version applies it in its own redirect policy.
///
/// # Errors
///
/// Returns why the redirect is refused.
pub fn follow_redirect(
    egress: &Egress,
    chain: &[url::Url],
    next: &url::Url,
    on_origin_change: OnOriginChange,
) -> Result<Hop, RedirectRefused> {
    let hop = egress.redirect(chain, next)?;
    let origin = || chain.first().map_or_else(String::new, origin_label);
    match on_origin_change {
        OnOriginChange::Refuse if hop.left_origin() => Err(RedirectRefused::CrossOrigin {
            origin: origin(),
            target: origin_label(next),
        }),
        OnOriginChange::FollowWithCredentials if redirect_continues_off_origin(chain, next) => {
            Err(RedirectRefused::ContinuesOffOrigin {
                origin: origin(),
                target: origin_label(next),
            })
        }
        OnOriginChange::Follow | OnOriginChange::FollowWithCredentials | OnOriginChange::Refuse => {
            Ok(hop)
        }
    }
}

/// A redirect policy that applies `egress` to a client that lets `reqwest` follow redirects.
#[must_use]
pub fn redirect_policy(egress: Egress, on_origin_change: OnOriginChange) -> Policy {
    Policy::custom(move |attempt| {
        match follow_redirect(&egress, attempt.previous(), attempt.url(), on_origin_change) {
            Ok(_) => attempt.follow(),
            Err(refused) => attempt.error(refused),
        }
    })
}

/// Configures `builder` for a client whose requests carry credentials or a body that belong
/// to the origin each request is sent to, such as a token request or a model request: under
/// [`Egress::for_configured_endpoint`], `reqwest` follows a redirect only within that origin,
/// refuses one that changes the scheme, host or port or downgrades HTTPS to HTTP, and
/// resolves every host through the egress, so the configured endpoint's host is pinned to the
/// class of its first DNS answer.
///
/// A refused redirect fails the request with a [`RedirectRefused`] that
/// [`refused_redirect`] finds in the error's source chain.
pub fn origin_bound(builder: ClientBuilder, reason: &str) -> ClientBuilder {
    let egress = Egress::for_configured_endpoint(reason);
    builder
        .redirect(redirect_policy(egress.clone(), OnOriginChange::Refuse))
        .dns_resolver(Arc::new(Resolver::new(egress)))
}

/// The redirect an egress policy refused, when that is why `error` happened.
#[must_use]
pub fn refused_redirect<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a RedirectRefused> {
    std::iter::successors(Some(error), |e| e.source())
        .find_map(|e| e.downcast_ref::<RedirectRefused>())
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

/// Sends `request` with `client`, following redirects under `egress` and refusing those that
/// `on_origin_change` refuses ([`follow_redirect`]).
///
/// `client` must be built with [`configure`] and the same `egress`, so `reqwest` hands every
/// redirect back here. While the chain stays on the request's origin, each hop carries the
/// request's own headers. Once it leaves that origin, a hop carries none of them and no body:
/// any header name can carry a credential (an OAuth token under a custom name, an API key in
/// `http_headers`), so none is judged safe to forward. A `303`, or a `301`/`302` answering a
/// `POST`, becomes a `GET` as `reqwest` would make it, and drops the body and the headers that
/// describe it (`Content-Type`, `Content-Length`, `Content-Encoding`, `Transfer-Encoding`) on
/// that hop and every later one. A `307`/`308` keeps the method, the body and those headers, as
/// `reqwest` does, and one that would resend a body to another origin is refused. Headers the
/// client sets on every request (`ClientBuilder::default_headers`, the user agent) are sent on
/// every hop, so a credential belongs on the request, never on the client.
///
/// # Errors
///
/// Returns [`SendError::Http`] when a request fails and [`SendError::Redirect`] when the
/// egress policy refuses a redirect.
pub async fn send(
    client: &Client,
    request: Request,
    egress: &Egress,
    on_origin_change: OnOriginChange,
) -> Result<Response, SendError> {
    let mut headers = request.headers().clone();
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
        let hop =
            follow_redirect(egress, &chain, &next, on_origin_change).context(RedirectSnafu)?;

        // As `reqwest` follows redirects: a `303`, or a `301`/`302` answering a `POST`, drops the
        // body and the headers that describe it. They are removed from `headers`, which every
        // later hop starts from, so they stay dropped. A request sent without a body keeps any
        // `Content-Type` it set until a redirect drops it.
        let drops_body = status == StatusCode::SEE_OTHER
            || (matches!(status, StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND)
                && method == Method::POST);
        if drops_body {
            if method != Method::HEAD {
                method = Method::GET;
            }
            body = Body::None;
            for name in &BODY_HEADERS {
                headers.remove(name);
            }
        }

        let next_headers = if hop.left_origin() {
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
            Body::None => {}
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

#[cfg(test)]
mod tests {
    use url::Url;

    use super::{OnOriginChange, follow_redirect};
    use crate::{DestinationGuard, Egress, is_internal_ip, redirect_target_is_internal};

    /// The egress a data connector's client follows its redirects under.
    fn connector_egress() -> Egress {
        Egress::new(
            Some(
                DestinationGuard::new(
                    is_internal_ip,
                    redirect_target_is_internal,
                    "internal destinations are refused",
                )
                .redirects_only(),
            ),
            10,
        )
    }

    fn cross_origin(target_origin: &str) -> String {
        format!(
            "the server redirected to another origin, {target_origin}, and this request carries a \
             credential that is sent only to http://api.example.com"
        )
    }

    #[test]
    fn refusing_an_origin_change_refuses_every_target_off_the_request_origin() {
        let egress = connector_egress();
        let chain = [Url::parse("http://api.example.com/graphql").expect("valid URL")];
        for (target, target_origin) in [
            ("https://93.184.216.34/graphql", "https://93.184.216.34"),
            ("https://api.example.org/graphql", "https://api.example.org"),
        ] {
            let target = Url::parse(target).expect("valid URL");
            let hop = follow_redirect(&egress, &chain, &target, OnOriginChange::Follow)
                .unwrap_or_else(|refused| panic!("{target} must be followed: {refused}"));
            assert!(hop.left_origin(), "{target} leaves the request's origin");
            let refused = follow_redirect(&egress, &chain, &target, OnOriginChange::Refuse)
                .expect_err("an origin change must be refused");
            assert_eq!(refused.to_string(), cross_origin(target_origin), "{target}");
        }
        for (target, target_origin) in [
            ("https://api.example.com/graphql", "https://api.example.com"),
            (
                "http://api.example.com:8080/graphql",
                "http://api.example.com:8080",
            ),
        ] {
            let target = Url::parse(target).expect("valid URL");
            let refused = follow_redirect(&egress, &chain, &target, OnOriginChange::Refuse)
                .expect_err("a scheme or port change must be refused");
            assert_eq!(refused.to_string(), cross_origin(target_origin), "{target}");
        }

        let target = Url::parse("http://api.example.com:80/graphql/").expect("valid URL");
        let hop = follow_redirect(&egress, &chain, &target, OnOriginChange::Refuse)
            .expect("a redirect within the request's origin is followed");
        assert!(!hop.left_origin());
    }

    /// A request with credentials follows a redirect that leaves its origin, and a chain that
    /// moves on to yet another host, but not a redirect that stays on the host and port the
    /// chain moved to: `reqwest` would send the credentials it stripped there again.
    #[test]
    fn following_with_credentials_refuses_a_chain_that_continues_on_the_host_it_moved_to() {
        let url = |value: &str| Url::parse(value).expect("valid URL");
        let egress = connector_egress();
        let endpoint = url("https://api.example.com/graphql");
        let moved = [endpoint.clone(), url("https://cdn.example.net/a")];
        let next = url("https://cdn.example.net/b");

        let refused = follow_redirect(
            &egress,
            &moved,
            &next,
            OnOriginChange::FollowWithCredentials,
        )
        .expect_err("the chain must not continue on the host it moved to");
        assert_eq!(
            refused.to_string(),
            "the redirect chain left https://api.example.com and continued on \
             https://cdn.example.net, and a request that carries credentials does not follow a \
             redirect chain that continues on another host"
        );
        let hop = follow_redirect(&egress, &moved, &next, OnOriginChange::Follow)
            .expect("a request without credentials follows the chain");
        assert!(hop.left_origin());

        let request = [endpoint];
        for (chain, next, left_origin) in [
            (&request[..], "https://cdn.example.net/a", true),
            (&moved[..], "https://edge.example.org/x", true),
            (&moved[..], "https://cdn.example.net:8443/b", true),
            (&moved[..], "https://api.example.com/back", true),
            (&request[..], "https://api.example.com/v2/graphql", false),
        ] {
            let hop = follow_redirect(
                &egress,
                chain,
                &url(next),
                OnOriginChange::FollowWithCredentials,
            )
            .unwrap_or_else(|refused| panic!("{next} must be followed: {refused}"));
            assert_eq!(hop.left_origin(), left_origin, "{next}");
        }
    }

    /// An origin change onto an internal destination is refused for that reason, whichever
    /// way the client treats an origin change.
    #[test]
    fn an_internal_target_is_refused_as_internal_under_either_choice() {
        let chain = [Url::parse("http://api.example.com/graphql").expect("valid URL")];
        let target = Url::parse("http://169.254.169.254/latest/meta-data/").expect("valid URL");
        for on_origin_change in [
            OnOriginChange::Follow,
            OnOriginChange::FollowWithCredentials,
            OnOriginChange::Refuse,
        ] {
            let refused = follow_redirect(&connector_egress(), &chain, &target, on_origin_change)
                .expect_err("an internal target must be refused");
            assert_eq!(
                refused.to_string(),
                "Refused to connect to http://169.254.169.254/latest/meta-data/: internal \
                 destinations are refused",
                "{on_origin_change:?}"
            );
        }
    }
}
