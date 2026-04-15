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

//! Composite authentication that dispatches to API key and/or OIDC providers.
//!
//! For HTTP: dispatches by header (`X-API-Key` -> API key, `Authorization: Bearer` -> OIDC).
//! For Flight/gRPC: tries API key first (cheap string comparison), then OIDC (crypto).

use std::sync::Arc;

use axum::http;

use crate::api_key::ApiKeyAuth;
use crate::error::Error;
use crate::oidc::OidcAuth;
use crate::{AuthVerdict, FlightBasicAuth, GrpcAuth, HttpAuth};

pub struct CompositeAuth {
    api_key_auth: Option<Arc<ApiKeyAuth>>,
    oidc_auth: Option<Arc<OidcAuth>>,
}

impl CompositeAuth {
    #[must_use]
    pub fn new(api_key_auth: Option<Arc<ApiKeyAuth>>, oidc_auth: Option<Arc<OidcAuth>>) -> Self {
        Self {
            api_key_auth,
            oidc_auth,
        }
    }
}

impl HttpAuth for CompositeAuth {
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error> {
        // Dispatch by header: X-API-Key -> API key auth, Authorization: Bearer -> OIDC
        if request.headers.contains_key("X-API-Key")
            && let Some(api_key_auth) = &self.api_key_auth
        {
            return api_key_auth.http_verify(request);
        }

        let is_bearer = request
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("Bearer "));

        if is_bearer && let Some(oidc) = &self.oidc_auth {
            return oidc.http_verify(request);
        }

        Ok(AuthVerdict::Deny)
    }
}

impl FlightBasicAuth for CompositeAuth {
    fn validate(&self, username: &str, password: &str) -> Result<String, Error> {
        // Try API key first (cheap), then OIDC (crypto)
        if let Some(api_key_auth) = &self.api_key_auth
            && let Ok(token) = api_key_auth.validate(username, password)
        {
            return Ok(token);
        }
        if let Some(oidc) = &self.oidc_auth {
            return oidc.validate(username, password);
        }
        Err(Error::InvalidCredentials)
    }

    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error> {
        // Try API key first (cheap), then OIDC (crypto)
        if let Some(api_key_auth) = &self.api_key_auth
            && let Ok(AuthVerdict::Allow(principal)) = api_key_auth.is_valid(bearer_token)
        {
            return Ok(AuthVerdict::Allow(principal));
        }
        if let Some(oidc) = &self.oidc_auth {
            return oidc.is_valid(bearer_token);
        }
        Ok(AuthVerdict::Deny)
    }
}

impl GrpcAuth for CompositeAuth {
    fn grpc_verify(&self, req: &tonic::Request<()>) -> Result<AuthVerdict, Error> {
        // Try API key first (uses x-api-key metadata), then OIDC (uses authorization metadata)
        if let Some(api_key_auth) = &self.api_key_auth
            && let Ok(AuthVerdict::Allow(principal)) = api_key_auth.grpc_verify(req)
        {
            return Ok(AuthVerdict::Allow(principal));
        }
        if let Some(oidc) = &self.oidc_auth {
            return oidc.grpc_verify(req);
        }
        Ok(AuthVerdict::Deny)
    }
}
