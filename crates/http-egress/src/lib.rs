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

//! One egress policy for outbound HTTP clients.
//!
//! A client that reaches a destination the operator configured (a dataset URL, an OAuth token
//! endpoint, an object store) can be sent somewhere else by the server it talks to: a redirect
//! names another origin, or the same host name later resolves to another address. This crate
//! decides, in one place, where such a client may go and what it may carry there.
//!
//! - [`DestinationGuard`] says which addresses and redirect targets are internal, and why a
//!   request to one is refused.
//! - [`Egress`] is one client's view of the hosts it reaches. Its resolver checks every host a
//!   redirect introduced. A host the client reaches directly (the configured host, or a proxy)
//!   is pinned to the class of its first DNS answer, so a name that first resolved to a public
//!   address cannot later resolve to an internal one. Its redirect decision refuses a downgrade
//!   from HTTPS to HTTP, treats a change of scheme, host or port as a change of origin, refuses
//!   an origin change onto an internal destination, and reports whether the chain has left the
//!   origin the request was made for.
//! - [`send`] follows redirects itself. Once a chain leaves the request's origin, the request
//!   carries none of the headers it set and no body, not only the five standard credential
//!   headers `reqwest` strips, and a `307`/`308` that would resend a body there is refused.
//!   [`redirect_policy`] applies the same decision to a client that lets `reqwest` follow
//!   redirects, for clients whose only credentials are the ones `reqwest` strips, or that must
//!   not follow an origin change at all. A request with credentials refuses a redirect chain
//!   that continues on the host it moved to ([`OnOriginChange::FollowWithCredentials`]),
//!   because `reqwest` sends the credentials it stripped again on such a hop.

mod client;
mod egress;
mod guard;
mod internal;

pub use client::{
    OnOriginChange, Resolver, SendError, configure, follow_redirect, origin_bound, redirect_policy,
    refused_redirect, send,
};
pub use egress::{Egress, Hop, RedirectRefused, redirect_continues_off_origin};
pub use guard::{DestinationGuard, DestinationRefused};
pub use internal::{is_blocked_internal_hostname, is_internal_ip, redirect_target_is_internal};
