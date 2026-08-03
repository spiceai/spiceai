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

//! Secrets delivered by the Spice Cloud control plane with a deployment.
//!
//! This is a **built-in** store, not one a spicepod declares. That is the whole
//! point: the same spicepod resolves `${ secrets:openai_key }` whether it runs
//! as a managed app or on a self-hosted instance, with no `secrets:` section
//! that would be meaningless on the other. A spicepod that had to name a
//! `cloud` store would not be portable.
//!
//! # Precedence
//!
//! Registered at the **lowest** precedence, so a user-declared store — `env`, a
//! Vault, a keyring — holding the same key still wins. Delivery adds a source;
//! it does not take the local override away.
//!
//! # Lifetime
//!
//! Values live in memory behind an `RwLock` and are replaced wholesale on each
//! deployment, so a redeploy swaps the set atomically without re-registering the
//! store or leaving a half-updated view visible to a concurrent lookup.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use secrecy::SecretString;
use zeroize::Zeroizing;

use crate::{AnyErrorResult, SecretStore};

/// Registered name of the delivered-secrets store.
///
/// Reachable as `${ cloud:KEY }` for diagnosis, but the path that matters is the
/// unqualified `${ secrets:KEY }` walk, which reaches it last.
pub const CLOUD_DELIVERED_STORE: &str = "cloud";

/// Secrets delivered with a deployment, held in memory.
#[derive(Default)]
pub struct CloudDeliveredSecretStore {
    /// Guarded by a `parking_lot` lock held only for the map clone/read — never
    /// across an `.await`, which is why a sync lock is correct in an async
    /// store.
    values: RwLock<Arc<BTreeMap<String, Zeroizing<Vec<u8>>>>>,
}

impl CloudDeliveredSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the delivered set. Called on every deployment that carries
    /// secrets, including one that carries none — an app whose secrets were all
    /// removed must stop resolving them.
    pub fn replace(&self, values: BTreeMap<String, Zeroizing<Vec<u8>>>) {
        *self.values.write() = Arc::new(values);
    }

    /// The delivered secret names, sorted. Safe to log and to report in status;
    /// the values are never exposed this way.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.values.read().keys().cloned().collect()
    }

    /// Number of delivered secrets currently held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.read().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.read().is_empty()
    }
}

#[async_trait]
impl SecretStore for CloudDeliveredSecretStore {
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        // Clone the Arc, then drop the guard: the UTF-8 conversion below must
        // not happen under the lock.
        let values = Arc::clone(&*self.values.read());
        let Some(value) = values.get(key) else {
            return Ok(None);
        };
        // A delivered value is arbitrary bytes on the wire, but a
        // `${ secrets:… }` substitution is textual. Report a non-UTF-8 value as
        // an error naming the key rather than lossily mangling it into a
        // credential that silently will not authenticate.
        let text = std::str::from_utf8(value).map_err(|_| {
            format!(
                "Delivered secret `{key}` is not valid UTF-8, so it cannot be substituted into a \
                 parameter. Store it as text in the Spice Cloud portal."
            )
        })?;
        Ok(Some(SecretString::from(text.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret as _;

    fn values(entries: &[(&str, &[u8])]) -> BTreeMap<String, Zeroizing<Vec<u8>>> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), Zeroizing::new(v.to_vec())))
            .collect()
    }

    #[tokio::test]
    async fn returns_a_delivered_value_and_none_for_an_absent_key() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("openai_key", b"sk-1")]));

        let found = store.get_secret("openai_key").await.expect("lookup");
        assert_eq!(found.expect("present").expose_secret(), "sk-1");
        assert!(
            store.get_secret("absent").await.expect("lookup").is_none(),
            "an absent key is None, not an error — the walk continues to other stores"
        );
    }

    #[tokio::test]
    async fn replace_swaps_the_whole_set() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("a", b"1"), ("b", b"2")]));
        assert_eq!(store.names(), vec!["a", "b"]);

        // A redeploy that dropped `b` must stop resolving it, not merge.
        store.replace(values(&[("a", b"9")]));
        assert_eq!(store.names(), vec!["a"]);
        assert_eq!(
            store
                .get_secret("a")
                .await
                .expect("lookup")
                .expect("present")
                .expose_secret(),
            "9"
        );
        assert!(store.get_secret("b").await.expect("lookup").is_none());
    }

    #[tokio::test]
    async fn an_empty_delivery_clears_everything() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("a", b"1")]));
        store.replace(BTreeMap::new());
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.get_secret("a").await.expect("lookup").is_none());
    }

    #[tokio::test]
    async fn a_non_utf8_value_errors_naming_the_key_not_the_value() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("binary_key", &[0xff, 0xfe])]));

        let err = store
            .get_secret("binary_key")
            .await
            .expect_err("a non-UTF-8 value cannot be substituted");
        let message = err.to_string();
        assert!(message.contains("binary_key"), "{message}");
        assert!(
            !message.contains('\u{fffd}'),
            "no lossy replacement: {message}"
        );
    }

    #[test]
    fn starts_empty() {
        let store = CloudDeliveredSecretStore::new();
        assert!(store.is_empty());
        assert!(store.names().is_empty());
    }
}
