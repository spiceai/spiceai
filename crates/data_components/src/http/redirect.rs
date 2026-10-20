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

//! Redirect handling for HTTP clients that attach credentials to their requests.
//!
//! On a redirect to another origin, `reqwest` removes only `Authorization`, `Cookie`,
//! `Cookie2`, `Proxy-Authorization` and `WWW-Authenticate`. Every other header the caller
//! set rides along — a custom API-key header, an `OAuth2` access token sent under a
//! configured header name such as `X-Shopify-Access-Token`, or any `http_headers` value —
//! and a 307 or 308 also replays the method and body. Headers cannot be edited from a
//! `reqwest` redirect policy, so [`RedirectGuard::policy`] follows a redirect only while it
//! stays on the origin of the hop before it, and stops at one that leaves it. The 3xx then
//! comes back to the caller, which can re-issue it without the credentials (see
//! [`cross_origin_redirect_target`]) or report it.

use reqwest::{Response, StatusCode, Url, header::LOCATION, redirect::Policy};

/// The redirect at which a request through [`RedirectGuard::policy`] fails: it follows at
/// most `MAX_REDIRECTS - 1`.
pub const MAX_REDIRECTS: usize = 5;

/// Which redirect targets may be requested at all, and the redirect policy built from it.
///
/// The same guard is handed to the client (as [`RedirectGuard::policy`]) and to whatever
/// re-issues the cross-origin redirects that policy stops at, so a target is checked
/// whether `reqwest` or the caller requests it. The caller checks it again rather than
/// trusting that a 3xx it receives went through this policy: `reqwest` does not consult a
/// policy for a request whose body it cannot clone, and the client may not have been
/// built with this one.
#[derive(Clone, Copy)]
pub struct RedirectGuard {
    is_blocked_target: fn(&Url) -> bool,
}

impl std::fmt::Debug for RedirectGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedirectGuard").finish_non_exhaustive()
    }
}

impl RedirectGuard {
    /// A guard that refuses every redirect target `is_blocked_target` returns `true` for.
    #[must_use]
    pub const fn new(is_blocked_target: fn(&Url) -> bool) -> Self {
        Self { is_blocked_target }
    }

    /// A guard that refuses no target, for a client whose redirects are bounded only by
    /// [`MAX_REDIRECTS`] and by staying on the origin.
    #[must_use]
    pub const fn same_origin_only() -> Self {
        fn never_blocked(_: &Url) -> bool {
            false
        }
        Self::new(never_blocked)
    }

    /// Whether `target` may be requested.
    #[must_use]
    pub fn allows(&self, target: &Url) -> bool {
        !(self.is_blocked_target)(target)
    }

    /// A redirect policy that follows redirects that stay on the origin of the hop before
    /// them, fails on a blocked target or at the [`MAX_REDIRECTS`]th redirect, and stops — returning the
    /// 3xx to the caller — at a redirect that leaves the origin.
    ///
    /// A blocked target fails before the origin is compared, so a cross-origin redirect
    /// the policy stops at has already passed the guard.
    #[must_use]
    pub fn policy(self) -> Policy {
        Policy::custom(move |attempt| {
            // `previous()` holds the original URL and every target followed so far, so it
            // has N entries at the Nth redirect: the request fails at its
            // `MAX_REDIRECTS`th.
            if attempt.previous().len() >= MAX_REDIRECTS {
                return attempt.error("too many redirects");
            }
            if !self.allows(attempt.url()) {
                return attempt.error("redirect to a private address is not allowed");
            }
            let leaves_origin = attempt
                .previous()
                .last()
                .is_none_or(|previous| previous.origin() != attempt.url().origin());
            if leaves_origin {
                attempt.stop()
            } else {
                attempt.follow()
            }
        })
    }
}

/// The target of `response` when it is a redirect to an origin other than the one that
/// answered it — which is a redirect [`RedirectGuard::policy`] did not follow.
///
/// `url::Url` defines origin as scheme, host and effective port, so `https://host` and
/// `https://host:443` share one; a scheme without a host (`file:`, `data:`) has an opaque
/// origin that never equals another, so it is always reported as cross-origin.
#[must_use]
pub fn cross_origin_redirect_target(response: &Response) -> Option<Url> {
    if !matches!(
        response.status(),
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    ) {
        return None;
    }
    let location = response.headers().get(LOCATION)?.to_str().ok()?;
    let target = response.url().join(location).ok()?;
    (target.origin() != response.url().origin()).then_some(target)
}

/// Whether a redirect with `status` replays the request's method and body (RFC 9110
/// §15.4.8 and §15.4.9), rather than turning it into a `GET` without a body.
#[must_use]
pub fn preserves_method_and_body(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
    )
}
