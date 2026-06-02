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
    fn lookup_with_comparison_observer(
        &self,
        presented: &str,
        mut on_compare: impl FnMut(),
    ) -> Option<ApiKey> {
        let mut matched: Option<&ApiKey> = None;
        for key in &self.api_keys {
            on_compare();
            if key == presented {
                // Don't `break`: keep iterating so timing is independent
                // of which key matched. Preserve the first match if the
                // same key was configured more than once.
                if matched.is_none() {
                    matched = Some(key);
                }
            }
        }
        matched.cloned()
    }

    fn lookup(&self, presented: &str) -> Option<ApiKey> {
        self.lookup_with_comparison_observer(presented, || {})
    }
}

impl HttpAuth for ApiKeyAuth {
    /// Checks the `X-API-Key` header or `Authorization: Bearer` header for a valid API key
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error> {
        let api_key = request
            .headers
            .get("X-API-Key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();

        if !api_key.is_empty() {
            return match self.lookup(api_key) {
                Some(api_key) => Ok(AuthVerdict::Allow(Arc::new(api_key))),
                None => Ok(AuthVerdict::Deny),
            };
        }

        let bearer = request
            .headers
            .get("Authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .and_then(|(scheme, token)| scheme.eq_ignore_ascii_case("Bearer").then_some(token))
            .unwrap_or_default();

        if bearer.is_empty() {
            return Ok(AuthVerdict::Deny);
        }

        match self.lookup(bearer) {
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
    fn test_lookup_compares_every_key_regardless_of_position() {
        let auth = ApiKeyAuth::new(vec![
            ApiKey::parse_str("first"),
            ApiKey::parse_str("middle"),
            ApiKey::parse_str("last"),
        ]);

        for presented in ["first", "middle", "last", "missing"] {
            let mut comparison_count = 0;
            let matched = auth.lookup_with_comparison_observer(presented, || {
                comparison_count += 1;
            });

            assert_eq!(comparison_count, 3);
            assert_eq!(matched.is_some(), presented != "missing");
        }
    }

    fn create_bearer_request_parts(authorization: &str) -> http::request::Parts {
        let request = Builder::new()
            .uri("https://example.com")
            .header("Authorization", authorization)
            .body(())
            .expect("Failed to build request");
        request.into_parts().0
    }

    #[test]
    fn test_valid_bearer_token() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let parts = create_bearer_request_parts("Bearer valid-key");
        assert!(matches!(
            auth.http_verify(&parts),
            Ok(AuthVerdict::Allow(_))
        ));
    }

    #[test]
    fn test_invalid_bearer_token() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let parts = create_bearer_request_parts("Bearer wrong-key");
        assert!(matches!(auth.http_verify(&parts), Ok(AuthVerdict::Deny)));
    }

    #[test]
    fn test_bearer_scheme_case_insensitive() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        for header in ["bearer valid-key", "BEARER valid-key", "BeArEr valid-key"] {
            let parts = create_bearer_request_parts(header);
            assert!(
                matches!(auth.http_verify(&parts), Ok(AuthVerdict::Allow(_))),
                "scheme casing '{header}' should be accepted"
            );
        }
    }

    #[test]
    fn test_unknown_auth_scheme_denied() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let parts = create_bearer_request_parts("Basic valid-key");
        assert!(matches!(auth.http_verify(&parts), Ok(AuthVerdict::Deny)));
    }

    #[test]
    fn test_x_api_key_takes_precedence_over_bearer() {
        let auth = ApiKeyAuth::new(vec![ApiKey::parse_str("valid-key")]);
        let request = Builder::new()
            .uri("https://example.com")
            .header("X-API-Key", "valid-key")
            .header("Authorization", "Bearer wrong-key")
            .body(())
            .expect("Failed to build request");
        let parts = request.into_parts().0;
        assert!(matches!(
            auth.http_verify(&parts),
            Ok(AuthVerdict::Allow(_))
        ));
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
