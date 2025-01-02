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

use std::sync::Arc;

use app::spicepod::component::runtime::ApiKey;
use axum::http;

use crate::{error::Error, AuthVerdict, FlightBasicAuth, GrpcAuth, HttpAuth};

pub struct ApiKeyAuth {
    api_keys: Vec<ApiKey>,
}

impl ApiKeyAuth {
    #[must_use]
    pub fn new(api_keys: Vec<ApiKey>) -> Self {
        Self { api_keys }
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

        if let Some(api_key) = self.api_keys.iter().find(|key| *key == api_key) {
            Ok(AuthVerdict::Allow(Arc::new(api_key.clone())))
        } else {
            Ok(AuthVerdict::Deny)
        }
    }
}

impl FlightBasicAuth for ApiKeyAuth {
    fn validate(&self, _username: &str, password: &str) -> Result<String, Error> {
        if self.api_keys.iter().any(|key| key == password) {
            Ok(password.to_string())
        } else {
            Err(Error::InvalidCredentials)
        }
    }

    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error> {
        if let Some(api_key) = self.api_keys.iter().find(|key| *key == bearer_token) {
            Ok(AuthVerdict::Allow(Arc::new(api_key.clone())))
        } else {
            Ok(AuthVerdict::Deny)
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

        if let Some(api_key) = self.api_keys.iter().find(|key| *key == api_key) {
            Ok(AuthVerdict::Allow(Arc::new(api_key.clone())))
        } else {
            Ok(AuthVerdict::Deny)
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
}
