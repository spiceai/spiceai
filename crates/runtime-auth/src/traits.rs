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

use crate::error::Error;
use app::spicepod::component::runtime::ApiKey;
use axum::http;

pub type AuthPrincipalRef = Arc<dyn AuthPrincipal + Sync + Send>;

pub trait AuthPrincipal {
    fn username(&self) -> &str; // The username as presented during auth
    fn groups(&self) -> &[&str]; // Group memberships
}
pub trait AuthRequestContext {
    /// Sets the current authentication principal for the request context.
    ///
    /// # Errors
    ///
    /// This function returns an error if the principal cannot be set.
    fn set_auth_principal(
        &self,
        auth_principal: AuthPrincipalRef,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Retrieves the principal information associated with the request.
    ///
    /// Returns `None` if authentication is not set up or has not yet been completed.
    fn auth_principal(&self) -> Option<&AuthPrincipalRef>;
}

pub enum AuthVerdict {
    Allow(AuthPrincipalRef),
    Deny,
}

impl AuthPrincipal for ApiKey {
    fn username(&self) -> &'static str {
        "api_key_auth"
    }

    fn groups(&self) -> &[&str] {
        match self {
            ApiKey::ReadOnly { .. } => &["read"],
            ApiKey::ReadWrite { .. } => &["read_write"],
        }
    }
}

pub trait HttpAuth {
    /// Receive the entire HTTP request object and return a verdict on whether to allow/deny it
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn http_verify(&self, request: &http::request::Parts) -> Result<AuthVerdict, Error>;
}

/// A trait for validating Flight basic auth requests.
///
/// This is inspired by the arrow-go Flight basic auth implementation.
/// <https://github.com/apache/arrow-go/blob/55f8b2075e2bee544b2bb4120966297a5a7d4e43/arrow/flight/server_auth.go#L146>
pub trait FlightBasicAuth {
    /// Receive the username/password for Flight basic auth and return the token that should be used on each subsequent request.
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the username/password.
    fn validate(&self, username: &str, password: &str) -> Result<String, Error>;

    /// Receive a bearer token and return a verdict on whether it is valid.
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the bearer token.
    fn is_valid(&self, bearer_token: &str) -> Result<AuthVerdict, Error>;
}

pub trait GrpcAuth {
    // Receive the entire gRPC request object and return a verdict
    ///
    /// # Errors
    ///
    /// This function will return an error if the validator can't validate the request.
    fn grpc_verify(&self, req: &tonic::Request<()>) -> Result<AuthVerdict, Error>;
}
