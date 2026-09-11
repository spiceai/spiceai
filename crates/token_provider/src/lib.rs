/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use tokio::sync::watch;

pub mod gcp_service_account_token;
pub mod github_app_token;
pub mod registry;

// Ensure the aws-lc-rs crypto provider is installed for jsonwebtoken before the first JWT
// operation. This is required because Cargo feature unification can activate both `aws_lc_rs`
// and `rust_crypto` features simultaneously (e.g. via a transitive dep like octocrab), causing
// jsonwebtoken's auto-detection to panic at runtime. Calling install_default() here makes it a
// compile error if the `aws_lc_rs` feature is ever missing, and a safe no-op if already set.
static JWT_CRYPTO_INIT: OnceLock<()> = OnceLock::new();

pub(crate) fn ensure_jwt_crypto_provider() {
    JWT_CRYPTO_INIT.get_or_init(|| {
        // install_default() returns Err only if a provider is already installed by another caller
        // (e.g. a test harness). That is not an error — any installed provider is acceptable here.
        // The only real failure mode (aws_lc_rs feature missing) is a compile error: the symbol
        // jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER would not exist.
        let _ = jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER.install_default();
    });
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get token. {source}"))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait TokenProvider: Send + Sync + Debug {
    fn get_token(&self) -> String;

    /// Returns a hash representing the configuration of this token provider.
    /// Token providers with the same configuration should return the same hash.
    ///
    /// This is used instead of implementing Hash directly on the trait object, as Hash is not dyn-compatible.
    fn dyn_hash(&self) -> String;

    /// Returns a `watch::Receiver` of new tokens, if the provider supports refresh.
    ///
    /// The default implementation gives no updates.
    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        None
    }
}

pub struct StaticTokenProvider {
    token: Arc<SecretString>,
}

impl std::fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl StaticTokenProvider {
    #[must_use]
    pub fn new(token: SecretString) -> Self {
        Self {
            token: Arc::new(token),
        }
    }
}

impl TokenProvider for StaticTokenProvider {
    fn get_token(&self) -> String {
        self.token.expose_secret().to_string()
    }

    fn dyn_hash(&self) -> String {
        // Domain-separated SHA-256 of the secret, so this stable identity is
        // one-way: it can serve as a registry/dedup key (and is safe to log)
        // without ever exposing the underlying token. (Previously a
        // `DefaultHasher` of the raw secret: non-cryptographic, and — being
        // seeded by a per-process `RandomState` — not even stable across runs.)
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut hasher = Sha256::new();
        hasher.update(b"spice/static-token-provider/v1\0");
        hasher.update(self.token.expose_secret().as_bytes());
        let digest = hasher.finalize();

        let mut out = String::with_capacity(4 + digest.len() * 2);
        out.push_str("stp-");
        for byte in digest {
            out.push(char::from(HEX[(byte >> 4) as usize]));
            out.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{SecretString, StaticTokenProvider, TokenProvider};

    #[test]
    fn dyn_hash_is_stable_one_way_and_distinct() {
        let secret = "super-secret-token-value";
        let a = StaticTokenProvider::new(SecretString::from(secret));
        let a2 = StaticTokenProvider::new(SecretString::from(secret));
        let b = StaticTokenProvider::new(SecretString::from("a-different-token"));

        // Stable for the same token, distinct for different tokens.
        assert_eq!(a.dyn_hash(), a2.dyn_hash());
        assert_ne!(a.dyn_hash(), b.dyn_hash());

        // One-way: the identity must never embed the raw secret, and is a
        // fixed-length, prefixed, hex digest.
        let h = a.dyn_hash();
        assert!(!h.contains(secret), "dyn_hash must not contain the secret");
        assert!(h.starts_with("stp-"));
        assert_eq!(h.len(), 4 + 64); // "stp-" + 32-byte SHA-256 in hex
    }
}
