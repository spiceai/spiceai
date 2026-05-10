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

use std::sync::Arc;

use app::spicepod::component::runtime::ApiKey;
use axum::http;

use crate::{AuthVerdict, FlightBasicAuth, GrpcAuth, HttpAuth, error::Error};

pub struct ApiKeyAuth {
    api_keys: Vec<ApiKey>,
}

impl ApiKeyAuth {
    #[must_use]
    pub fn new(api_keys: Vec<ApiKey>) -> Self {
        let mut filtered_keys = Vec::with_capacity(api_keys.len());
        let mut dropped_empty_keys = 0usize;

        for api_key in api_keys {
            if api_key.is_empty() {
                dropped_empty_keys += 1;
            } else {
                filtered_keys.push(api_key);
            }
        }

        if dropped_empty_keys > 0 {
            tracing::warn!(
                dropped_empty_keys,
                "Ignoring empty API key values to prevent insecure authentication bypass"
            );
        }

        Self {
            api_keys: filtered_keys,
        }
    }

    /// Look up `presented` against every configured API key without
    /// short-circuiting.
    ///
    /// `ApiKey`'s `PartialEq<str>` already performs a constant-time byte
    /// comparison via `subtle::ct_eq`, so an attacker cannot recover key
    /// bytes from per-comparison timing. Iterating with `find` / `any`
    /// would, however, leak the *position* of the matching key in the
    /// configured list (the first key matches in 1 comparison, the last
    /// in N). This routine compares against every key on every call so
    /// total verification time depends only on the number of configured
    /// keys, not which one matched (or whether any did).
    fn lookup(&self, presented: &str) -> Option<ApiKey> {
        let mut matched: Option<ApiKey> = None;
        for key in &self.api_keys {
            if key == presented {
                // Don't `break`: keep iterating so timing is independent
                // of which key matched. Later matches will not overwrite
                // the first because configured keys are de-duplicated by
                // the operator and `==` here is constant-time.
                if matched.is_none() {
                    matched = Some(key.clone());
                }
            }
        }
        matched
    }
}

impl HttpAuth for ApiKeyAuth {
    /// Checks the `X-API-Key` header for a valid API key
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error> {
        let api_key = request
            .headers
            .get("X-API-Key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();

        if api_key.is_empty() {
            return Ok(AuthVerdict::Deny);
        }

        match self.lookup(api_key) {
            Some(api_key) => Ok(AuthVerdict::Allow(Arc::new(api_key))),
            None => Ok(AuthVerdict::Deny),
        }
    }
}

impl FlightBasicAuth for ApiKeyAuth {
    fn validate(&self, _username: &str, password: &str) -> Result<String, Error> {
        if password.is_empty() {
            return Err(Error::InvalidCredentials);
        }

        if self.lookup(password).is_some() {
            Ok(password.to_string())
        } else {
            Err(Error::InvalidCredentials)
        }
    }

    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error> {
        if bearer_token.is_empty() {
            return Ok(AuthVerdict::Deny);
        }

        match self.lookup(bearer_token) {
            Some(api_key) => Ok(AuthVerdict::Allow(Arc::new(api_key))),
            None => Ok(AuthVerdict::Deny),
        }
    }
}

impl GrpcAuth for ApiKeyAuth {
    fn grpc_verify(&self, req: &tonic::Request<()>) -> Result<AuthVerdict, Error> {
        let metadata = req.metadata();
        let Some(api_key) = metadata.get("x-api-key") else {
            return Ok(AuthVerdict::Deny);
        };
        let Ok(api_key) = api_key.to_str() else {
            return Ok(AuthVerdict::Deny);
        };

        if api_key.is_empty() {
            return Ok(AuthVerdict::Deny);
        }

        match self.lookup(api_key) {
            Some(api_key) => Ok(AuthVerdict::Allow(Arc::new(api_key))),
            None => Ok(AuthVerdict::Deny),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::request::Builder;

    fn create_request_parts(api_key: Option<&str>) -> http::request::Parts {
        let mut builder = Builder::new().uri("https://example.com");

        if let Some(key) = api_key {
            builder = builder.header("X-API-Key", key);
        }

        let request = builder.body(()).expect("Failed to build request");
        request.into_parts().0
    }

    #[test]
    fn test_valid_api_key() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let parts = create_request_parts(Some("valid-key"));

        let result = auth.http_verify(&parts);
        assert!(matches!(result, Ok(AuthVerdict::Allow(_))));
    }

    #[test]
    fn test_invalid_api_key() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let parts = create_request_parts(Some("invalid-key"));

        let result = auth.http_verify(&parts);
        assert!(matches!(result, Ok(AuthVerdict::Deny)));
    }

    #[test]
    fn test_missing_api_key() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let parts = create_request_parts(None);

        let result = auth.http_verify(&parts);
        assert!(matches!(result, Ok(AuthVerdict::Deny)));
    }

    #[test]
    fn test_multiple_valid_keys() {
        let auth = ApiKeyAuth::new(vec![
            ApiKey::parse_str("key1"),
            ApiKey::parse_str("key2"),
            ApiKey::parse_str("key3"),
        ]);

        let parts = create_request_parts(Some("key2"));
        let result = auth.http_verify(&parts);
        assert!(matches!(result, Ok(AuthVerdict::Allow(_))));
    }

    #[test]
    fn test_empty_configured_key_is_ignored() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str(""), ApiKey::parse_str("valid-key")]);
        let empty_key_parts = create_request_parts(Some(""));
        let empty_key_result = auth.http_verify(&empty_key_parts);
        assert!(matches!(empty_key_result, Ok(AuthVerdict::Deny)));

        let valid_key_parts = create_request_parts(Some("valid-key"));
        let valid_key_result = auth.http_verify(&valid_key_parts);
        assert!(matches!(valid_key_result, Ok(AuthVerdict::Allow(_))));
    }

    #[test]
    fn test_lookup_matches_regardless_of_position() {
        let auth = ApiKeyAuth::new(vec![
            ApiKey::parse_str("first"),
            ApiKey::parse_str("middle"),
            ApiKey::parse_str("last"),
        ]);
        // Match at every position — covers the no-short-circuit behavior.
        assert!(auth.lookup("first").is_some());
        assert!(auth.lookup("middle").is_some());
        assert!(auth.lookup("last").is_some());
        assert!(auth.lookup("missing").is_none());
    }

    #[test]
    fn test_flight_basic_auth_rejects_empty_password() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        assert!(matches!(
            auth.validate("ignored", ""),
            Err(crate::error::Error::InvalidCredentials)
        ));
        auth.validate("ignored", "valid-key")
            .expect("valid key should be accepted");
        assert!(matches!(
            auth.validate("ignored", "wrong"),
            Err(crate::error::Error::InvalidCredentials)
        ));
    }
}
