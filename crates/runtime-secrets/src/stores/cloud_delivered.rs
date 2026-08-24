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
//! Values live in memory behind an `RwLock` and every write swaps the whole set
//! at once, so a delivery is visible to a concurrent lookup either entirely or
//! not at all, without re-registering the store.
//!
//! A deployment does not replace the set wholesale — see
//! [`CloudDeliveredSecretStore::install_new`]: a component resolves its secrets
//! as it loads, so what a running process can change is what nothing has
//! resolved yet. The set is replaced wholesale by the start that reads the local
//! cache, which is where a rotation lands.

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

/// What a delivery changed, and what it could not.
///
/// Both lists are names, sorted; a delivered value never appears here.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeliveryUpdate {
    /// Names this store did not hold and now resolves.
    pub installed: Vec<String>,
    /// Names whose delivered value is not the one in effect — rotated, or
    /// withdrawn by a delivery that no longer carries them. They keep resolving
    /// to the value the components that hold it resolved.
    pub pending: Vec<String>,
}

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

    /// Whether `values` is exactly the set already held — same names, same
    /// bytes.
    ///
    /// Answers whether a delivery changes anything without handing the caller a
    /// value to compare itself, which is what lets a redelivery of the current
    /// secrets be told apart from a rotation.
    #[must_use]
    pub fn holds(&self, values: &BTreeMap<String, Zeroizing<Vec<u8>>>) -> bool {
        **self.values.read() == *values
    }

    /// Install the delivered values this store does not hold yet, and report
    /// the ones it holds that the delivery does not agree with.
    ///
    /// A name the store has never held resolves to nothing today, so installing
    /// it cannot change what any component already resolved — the components a
    /// deployment adds can use it as they load. A name it does hold is the
    /// opposite: a component resolves `${ secrets:… }` once, while it loads, so
    /// a rotated or withdrawn value only reaches the components holding the old
    /// one by loading them again. Installing it would leave the components
    /// loaded before a deployment authenticating with one value and the ones
    /// loaded after it with another, so the value in effect is kept and the name
    /// is reported instead.
    ///
    /// Names only, never values, so the answer is safe to log and to report.
    pub fn install_new(&self, delivered: &BTreeMap<String, Zeroizing<Vec<u8>>>) -> DeliveryUpdate {
        // Held for the whole read-modify-write: a concurrent replace between
        // the read and the write would be lost.
        let mut values = self.values.write();

        let mut update = DeliveryUpdate::default();
        let mut merged = (**values).clone();
        for (name, value) in delivered {
            match merged.get(name) {
                Some(held) if held == value => {}
                Some(_) => update.pending.push(name.clone()),
                None => {
                    merged.insert(name.clone(), value.clone());
                    update.installed.push(name.clone());
                }
            }
        }
        // A name the delivery drops is still resolving here, and a component
        // that resolved it keeps running on it: withdrawing it takes a start.
        update.pending.extend(
            merged
                .keys()
                .filter(|name| !delivered.contains_key(*name))
                .cloned(),
        );
        update.pending.sort();

        *values = Arc::new(merged);
        update
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

    /// A name nothing has resolved here can be installed while the process
    /// runs; one that is already resolving cannot, because the components
    /// holding it only re-resolve when they load again.
    #[tokio::test]
    async fn install_new_adds_what_is_unknown_and_keeps_what_is_in_use() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("held", b"1")]));

        let update = store.install_new(&values(&[("held", b"2"), ("added", b"3")]));
        assert_eq!(update.installed, vec!["added"]);
        assert_eq!(update.pending, vec!["held"]);
        assert_eq!(
            store
                .get_secret("held")
                .await
                .expect("lookup")
                .expect("present")
                .expose_secret(),
            "1",
            "the value the loaded components resolved stays in effect"
        );
        assert_eq!(
            store
                .get_secret("added")
                .await
                .expect("lookup")
                .expect("present")
                .expose_secret(),
            "3",
            "a value nothing has resolved yet is usable straight away"
        );
    }

    /// Withdrawing a secret is a change to what a loaded component resolved,
    /// exactly like rotating one, so it waits the same way.
    #[test]
    fn install_new_reports_a_withdrawn_secret_and_keeps_resolving_it() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("dropped", b"1"), ("kept", b"2")]));

        let update = store.install_new(&values(&[("kept", b"2")]));
        assert!(update.installed.is_empty());
        assert_eq!(update.pending, vec!["dropped"]);
        assert_eq!(store.names(), vec!["dropped", "kept"]);
    }

    /// A redelivery of the values in effect changes nothing and reports
    /// nothing — that is what tells a deployment worth applying from a repeat.
    #[test]
    fn install_new_is_a_no_op_for_the_values_already_held() {
        let store = CloudDeliveredSecretStore::new();
        let delivered = values(&[("a", b"1"), ("b", b"2")]);
        store.replace(delivered.clone());

        let update = store.install_new(&delivered);
        assert_eq!(update, DeliveryUpdate::default());
        assert!(store.holds(&delivered));
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

    /// A caller deciding whether a delivery changes anything has to see a
    /// rotated value, not just a changed name — the names are identical across
    /// a rotation, which is the case that matters.
    #[test]
    fn holds_distinguishes_a_redelivery_from_a_rotation() {
        let store = CloudDeliveredSecretStore::new();
        store.replace(values(&[("a", b"1"), ("b", b"2")]));

        assert!(store.holds(&values(&[("a", b"1"), ("b", b"2")])));
        assert!(
            !store.holds(&values(&[("a", b"9"), ("b", b"2")])),
            "a rotated value is not the set already held"
        );
        assert!(
            !store.holds(&values(&[("a", b"1")])),
            "a removed secret is not the set already held"
        );
        assert!(
            !store.holds(&values(&[("a", b"1"), ("b", b"2"), ("c", b"3")])),
            "an added secret is not the set already held"
        );
        assert!(!store.holds(&BTreeMap::new()));
        assert!(
            CloudDeliveredSecretStore::new().holds(&BTreeMap::new()),
            "an empty delivery to an empty store changes nothing"
        );
    }

    #[test]
    fn starts_empty() {
        let store = CloudDeliveredSecretStore::new();
        assert!(store.is_empty());
        assert!(store.names().is_empty());
    }
}
