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

//! Cache namespace for per-user (and per-system) cache scoping.
//!
//! Every cache surface in the runtime — SQL results, search, plan cache,
//! HTTP-connector caching accelerator — mixes the request's [`CacheNamespace`]
//! into its hash input. Two requests with different namespaces hash to
//! different cache keys for otherwise-identical inputs, which is what makes
//! cached results safe to serve under user-to-machine authentication.
//!
//! Scope is automatic and not user-configurable:
//! - No authenticated principal (or an anonymous principal) → [`CacheNamespace::Public`].
//! - Internal/system traffic (refresh tasks, cluster RPCs, SWR revalidation,
//!   health checks) → [`CacheNamespace::System`].
//! - Authenticated principal → [`CacheNamespace::Principal`] keyed on the
//!   principal's stable opaque id.
//!
//! See `plans/per-user-cache-option-1-namespace.md` and issue #10680 for the
//! full design.

use std::hash::Hash;
use std::sync::Arc;

/// The cache scope a request is executing under.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheNamespace {
    /// No authenticated principal; cached entries may be shared across all
    /// unauthenticated callers. This is the default for unauthenticated
    /// deployments and matches today's pre-isolation behavior.
    Public,
    /// Cached entries are scoped to a single authenticated principal.
    /// The inner string is a stable opaque identifier produced by the
    /// principal's `stable_id()`. It is **not** the bearer token or API
    /// key value; rotating the credential without changing the underlying
    /// identity must not invalidate the cache.
    Principal(Arc<str>),
    /// Internal / system traffic: refresh tasks, cluster RPCs, SWR
    /// background revalidation, health probes. Entries written under
    /// `System` are not visible to user requests.
    System,
}

impl CacheNamespace {
    /// A short, low-cardinality string suitable for telemetry dimensions.
    /// **Must never** include the principal id, which is high-cardinality
    /// and (depending on auth method) sensitive.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            CacheNamespace::Public => "public",
            CacheNamespace::Principal(_) => "principal",
            CacheNamespace::System => "system",
        }
    }

    /// A short, lower-case label suitable for the `Results-Cache-Scope` /
    /// `Search-Results-Cache-Scope` HTTP response headers. Uses HTTP/CDN
    /// vocabulary (`shared` / `user` / `system`) rather than the internal
    /// enum names so it matches operator expectations from `Cache-Control:
    /// private` and similar.
    #[must_use]
    pub fn as_header_value(&self) -> &'static str {
        match self {
            CacheNamespace::Public => "shared",
            CacheNamespace::Principal(_) => "user",
            CacheNamespace::System => "system",
        }
    }

    /// Inputs to be folded into a cache key hash so two distinct namespaces
    /// produce distinct cache keys for otherwise-identical payloads.
    ///
    /// Returns `(tag, id_bytes)` where `tag` is the namespace discriminant
    /// (kept stable across releases so cached entries survive upgrades):
    /// - `0` → [`CacheNamespace::Public`] (no id)
    /// - `1` → [`CacheNamespace::Principal`] (`id_bytes` is the principal's
    ///   stable opaque id)
    /// - `2` → [`CacheNamespace::System`] (no id)
    ///
    /// Pair with `cache::key::CacheKey::as_raw_key_in_namespace`.
    #[must_use]
    pub fn hash_inputs(&self) -> (u8, &[u8]) {
        match self {
            CacheNamespace::Public => (0, &[]),
            CacheNamespace::Principal(id) => (1, id.as_bytes()),
            CacheNamespace::System => (2, &[]),
        }
    }

    /// Stable string identifier suitable for storage inside the caching
    /// accelerator's `__spice_cache_namespace` column. Comparing rows by
    /// this value is what enforces cross-principal isolation in the
    /// accelerator.
    ///
    /// Format:
    /// - [`CacheNamespace::Public`] → `"public"`
    /// - [`CacheNamespace::System`] → `"system"`
    /// - [`CacheNamespace::Principal`] → the principal's opaque id
    ///   verbatim (e.g. `"apikey:0123456789abcdef"`)
    ///
    /// The values are stable across releases; persisted cache rows must
    /// continue to compare equal after upgrades.
    #[must_use]
    pub fn storage_id(&self) -> &str {
        match self {
            CacheNamespace::Public => "public",
            CacheNamespace::Principal(id) => id.as_ref(),
            CacheNamespace::System => "system",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_is_low_cardinality_and_excludes_id() {
        assert_eq!(CacheNamespace::Public.kind(), "public");
        assert_eq!(CacheNamespace::System.kind(), "system");
        let with_id = CacheNamespace::Principal(Arc::from("apikey:0123456789abcdef"));
        assert_eq!(with_id.kind(), "principal");
        // The id must not appear anywhere in the dimension value.
        assert!(!with_id.kind().contains("0123456789abcdef"));
    }

    #[test]
    fn header_value_uses_http_vocabulary() {
        assert_eq!(CacheNamespace::Public.as_header_value(), "shared");
        assert_eq!(
            CacheNamespace::Principal(Arc::from("apikey:abc")).as_header_value(),
            "user"
        );
        assert_eq!(CacheNamespace::System.as_header_value(), "system");
    }

    #[test]
    fn equal_principal_ids_are_equal_namespaces() {
        let a = CacheNamespace::Principal(Arc::from("u2m:sub:42"));
        let b = CacheNamespace::Principal(Arc::from("u2m:sub:42"));
        assert_eq!(a, b);
    }

    #[test]
    fn different_principal_ids_are_distinct() {
        let a = CacheNamespace::Principal(Arc::from("u2m:sub:42"));
        let b = CacheNamespace::Principal(Arc::from("u2m:sub:43"));
        assert_ne!(a, b);
    }

    #[test]
    fn hash_inputs_distinct_per_variant() {
        let pub_ns = CacheNamespace::Public;
        let sys_ns = CacheNamespace::System;
        let principal_ns = CacheNamespace::Principal(Arc::from("apikey:abc"));
        let pub_inputs = pub_ns.hash_inputs();
        let sys_inputs = sys_ns.hash_inputs();
        let p_inputs = principal_ns.hash_inputs();

        assert_eq!(pub_inputs, (0, &[][..]));
        assert_eq!(sys_inputs, (2, &[][..]));
        assert_eq!(p_inputs.0, 1);
        assert_eq!(p_inputs.1, b"apikey:abc");
        // Tags must be distinct so empty-id Public and System never collide.
        assert_ne!(pub_inputs.0, sys_inputs.0);
    }

    #[test]
    fn storage_id_is_stable_per_variant() {
        assert_eq!(CacheNamespace::Public.storage_id(), "public");
        assert_eq!(CacheNamespace::System.storage_id(), "system");
        assert_eq!(
            CacheNamespace::Principal(Arc::from("apikey:abc")).storage_id(),
            "apikey:abc",
        );
        // public and system are distinct so an unauthenticated user and a
        // background system task cannot collide on cached rows.
        assert_ne!(
            CacheNamespace::Public.storage_id(),
            CacheNamespace::System.storage_id(),
        );
    }
}
