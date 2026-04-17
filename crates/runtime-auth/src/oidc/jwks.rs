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

use std::sync::{Arc, RwLock};
use std::time::Duration;

use jsonwebtoken::jwk::{Jwk, JwkSet};

use crate::error::Error;

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const MAX_STARTUP_RETRIES: u32 = 3;
const REQUEST_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Caches JWKS keys fetched from an OIDC provider and refreshes them periodically.
///
/// Uses `std::sync::RwLock` (not tokio) so that the synchronous auth trait methods
/// (`HttpAuth::http_verify`, etc.) can read keys without an async runtime.
/// Keys are stored behind an `Arc` so readers only clone the `Arc`, not the full key set.
pub struct JwksCache {
    keys: RwLock<Arc<Vec<Jwk>>>,
    jwks_url: String,
    client: reqwest::Client,
}

/// The result of OIDC provider discovery.
pub struct DiscoveryResult {
    /// The canonical issuer identifier from the discovery document's `issuer` field.
    /// Per the OIDC Discovery spec (Section 4.3), this MUST be identical to the
    /// `iss` claim in tokens issued by this provider. Use this value — not the
    /// user-configured `issuer_url` — for JWT `iss` validation.
    pub issuer: String,
}

impl JwksCache {
    /// Create a new `JwksCache` by performing OIDC discovery and fetching the initial key set.
    ///
    /// 1. Fetches `{issuer_url}/.well-known/openid-configuration`
    /// 2. Extracts `issuer` and `jwks_uri` from the discovery document
    /// 3. Fetches the JWK set
    ///
    /// Returns the cache and a [`DiscoveryResult`] containing the canonical issuer.
    ///
    /// # Errors
    ///
    /// Returns an error if OIDC discovery or the initial JWKS key fetch fails.
    pub async fn new(issuer_url: &str) -> Result<(Self, DiscoveryResult), Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(REQUEST_CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| Error::JwksDiscoveryFailed(format!("Failed to build HTTP client: {e}")))?;

        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            issuer_url.trim_end_matches('/')
        );

        let (jwks_url, discovery) = Self::discover(&client, &discovery_url).await?;
        let keys = Self::fetch_keys_with_retry(&client, &jwks_url).await?;

        Ok((
            Self {
                keys: RwLock::new(Arc::new(keys)),
                jwks_url,
                client,
            },
            discovery,
        ))
    }

    /// Returns a cheap `Arc` clone of the currently cached JWK keys.
    pub fn get_keys(&self) -> Arc<Vec<Jwk>> {
        Arc::clone(
            &self
                .keys
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Re-fetches the JWKS from the provider and replaces the cached keys.
    async fn refresh(&self) -> Result<(), Error> {
        let keys = Self::fetch_keys(&self.client, &self.jwks_url).await?;
        let mut guard = self
            .keys
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Arc::new(keys);
        Ok(())
    }

    /// Spawns a background task that periodically refreshes the JWKS keys.
    pub fn start_refresh_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(DEFAULT_REFRESH_INTERVAL);
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                if let Err(e) = self.refresh().await {
                    tracing::warn!("Failed to refresh JWKS keys: {e}");
                }
            }
        })
    }

    /// Fetches the OIDC discovery document and extracts `issuer` and `jwks_uri`.
    ///
    /// Returns `(jwks_uri, DiscoveryResult)`.
    async fn discover(
        client: &reqwest::Client,
        discovery_url: &str,
    ) -> Result<(String, DiscoveryResult), Error> {
        let resp = client.get(discovery_url).send().await.map_err(|e| {
            Error::JwksDiscoveryFailed(format!(
                "Failed to fetch OIDC discovery document from {discovery_url}: {e}"
            ))
        })?;

        if !resp.status().is_success() {
            return Err(Error::JwksDiscoveryFailed(format!(
                "OIDC discovery endpoint {discovery_url} returned status {}",
                resp.status()
            )));
        }

        let config: serde_json::Value = resp.json().await.map_err(|e| {
            Error::JwksDiscoveryFailed(format!("Failed to parse OIDC discovery document: {e}"))
        })?;

        let jwks_uri = config["jwks_uri"]
            .as_str()
            .map(ToString::to_string)
            .ok_or_else(|| {
                Error::JwksDiscoveryFailed(
                    "OIDC discovery document missing 'jwks_uri' field".into(),
                )
            })?;

        let issuer = config["issuer"]
            .as_str()
            .map(ToString::to_string)
            .ok_or_else(|| {
                Error::JwksDiscoveryFailed("OIDC discovery document missing 'issuer' field".into())
            })?;

        Ok((jwks_uri, DiscoveryResult { issuer }))
    }

    async fn fetch_keys(client: &reqwest::Client, jwks_url: &str) -> Result<Vec<Jwk>, Error> {
        let resp = client.get(jwks_url).send().await.map_err(|e| {
            Error::JwksRefreshFailed(format!("Failed to fetch JWKS from {jwks_url}: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(Error::JwksRefreshFailed(format!(
                "JWKS endpoint {jwks_url} returned status {}",
                resp.status()
            )));
        }

        let jwk_set: JwkSet = resp
            .json()
            .await
            .map_err(|e| Error::JwksRefreshFailed(format!("Failed to parse JWKS response: {e}")))?;

        Ok(jwk_set.keys)
    }

    async fn fetch_keys_with_retry(
        client: &reqwest::Client,
        jwks_url: &str,
    ) -> Result<Vec<Jwk>, Error> {
        let mut last_err = None;
        for attempt in 0..MAX_STARTUP_RETRIES {
            match Self::fetch_keys(client, jwks_url).await {
                Ok(keys) => return Ok(keys),
                Err(e) => {
                    tracing::warn!(
                        attempt = attempt + 1,
                        max_retries = MAX_STARTUP_RETRIES,
                        "Failed to fetch initial JWKS keys, retrying: {e}"
                    );
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                }
            }
        }
        Err(last_err
            .unwrap_or_else(|| Error::JwksRefreshFailed("No JWKS fetch attempts made".into())))
    }
}
