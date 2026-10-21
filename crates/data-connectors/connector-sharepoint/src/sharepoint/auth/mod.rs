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

//! Authentication flows for the SharePoint connector.
//!
//! The [`SharepointAuth`] enum dispatches between OAuth2/OIDC flows exposed
//! by the vendored `graph-rs-sdk` (`graph_oauth`) and a hand-rolled SAML 2.0
//! Bearer Assertion Grant (RFC 7522) in [`saml`]. All flows ultimately produce
//! a [`graph_rs_sdk::GraphClient`] that the connector and downstream
//! [`crate::sharepoint::object_store::SharepointObjectStore`] share.
//!
//! Flows supported in v1:
//!
//! | Flow                   | Variant                    | Notes |
//! |------------------------|----------------------------|-------|
//! | Raw bearer token       | [`BearerToken`]            | passthrough; caller minted elsewhere |
//! | Client credentials     | [`ClientCredentials`]      | service/daemon — secret |
//! | Authorization code     | [`AuthCode`]               | caller has already completed redirect dance |
//! | Refresh token          | [`RefreshToken`]           | renewal from a prior grant |
//! | Device code            | [`DeviceCode`]             | caller has already obtained a device code |
//! | SAML bearer            | [`SamlBearer`]             | RFC 7522 assertion → Azure AD token |
//!
//! Flows not supported in v1 (explicit follow-ups):
//! - Interactive OAuth2 flows that require browser redirects driven *by* the
//!   connector (SP-initiated auth code without a pre-obtained code, full
//!   OIDC with interactive consent).
//! - WS-Trust / ADFS direct SAML token acquisition.
//! - Spice-minted SAML assertions (requires XMLDSig signing stack).

#![expect(
    clippy::doc_markdown,
    reason = "prose-frequent identifiers (SharePoint, OAuth2, XMLDSig) are clearer without backticks"
)]

pub mod saml;

use std::sync::Arc;

use graph_rs_sdk::{
    GraphClient,
    identity::{ConfidentialClientApplication, PublicClientApplication},
};
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};

/// Default OAuth2 scope for SharePoint/OneDrive via Microsoft Graph.
pub const DEFAULT_SCOPE: &str = "https://graph.microsoft.com/.default";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("SAML bearer flow failed: {source}"))]
    SamlBearer { source: saml::Error },

    #[snafu(display("Invalid redirect_uri '{uri}' for flow '{flow}': {source}"))]
    InvalidRedirectUri {
        flow: String,
        uri: String,
        source: url::ParseError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SharePoint auth flows — each variant captures the minimum parameters needed
/// to produce a working [`GraphClient`].
pub enum SharepointAuth {
    /// Raw bearer token passthrough — useful for short-lived testing or when
    /// an upstream auth broker mints tokens.
    BearerToken(SecretString),

    /// OAuth2 client credentials grant with a shared secret. Primary flow
    /// for daemon/service workloads.
    ClientCredentials {
        tenant_id: String,
        client_id: String,
        client_secret: SecretString,
        scope: Option<String>,
    },

    /// OAuth2 authorization code flow. Requires the caller to have already
    /// completed the user-agent redirect and captured the `auth_code`.
    AuthCode {
        tenant_id: String,
        client_id: String,
        client_secret: SecretString,
        auth_code: SecretString,
        redirect_uri: String,
        scope: Option<String>,
    },

    /// OAuth2 refresh token flow — exchanges an existing refresh token for a
    /// new access token.
    RefreshToken {
        tenant_id: String,
        client_id: String,
        client_secret: SecretString,
        refresh_token: SecretString,
        scope: Option<String>,
    },

    /// OAuth2 device code flow — the caller has already initiated a device
    /// code flow (via the Graph device code endpoint) and captured the
    /// `device_code` value to pass in.
    DeviceCode {
        tenant_id: String,
        client_id: String,
        device_code: SecretString,
        scope: Option<String>,
    },

    /// SAML 2.0 Bearer Assertion Grant (RFC 7522) — exchanges a pre-acquired
    /// SAML assertion for an Azure AD access token.
    SamlBearer(saml::SamlBearerConfig),
}

impl std::fmt::Debug for SharepointAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't leak secrets in {:?} logging.
        let tag = match self {
            Self::BearerToken(_) => "BearerToken",
            Self::ClientCredentials { .. } => "ClientCredentials",
            Self::AuthCode { .. } => "AuthCode",
            Self::RefreshToken { .. } => "RefreshToken",
            Self::DeviceCode { .. } => "DeviceCode",
            Self::SamlBearer(_) => "SamlBearer",
        };
        f.debug_struct("SharepointAuth")
            .field("flow", &tag)
            .finish()
    }
}

impl SharepointAuth {
    /// Produce a configured [`GraphClient`]. For token-exchange flows (SAML,
    /// refresh), this may involve an HTTP round-trip before returning.
    ///
    /// The SDK's builder pattern requires calling the terminal credential
    /// method (e.g. `with_client_secret`) *first* — it consumes the top-level
    /// application builder and returns a specialized credential builder on
    /// which `with_tenant`, `with_scope`, and `build` live.
    pub async fn build_graph_client(&self) -> Result<Arc<GraphClient>> {
        let client = match self {
            SharepointAuth::BearerToken(token) => GraphClient::new(token.expose_secret()),

            SharepointAuth::ClientCredentials {
                tenant_id,
                client_id,
                client_secret,
                scope,
            } => {
                let mut cred = ConfidentialClientApplication::builder(client_id.as_str())
                    .with_client_secret(client_secret.expose_secret());
                cred.with_tenant(tenant_id)
                    .with_scope([scope.as_deref().unwrap_or(DEFAULT_SCOPE)]);
                GraphClient::from(&cred.build())
            }

            SharepointAuth::AuthCode {
                tenant_id,
                client_id,
                client_secret,
                auth_code,
                redirect_uri,
                scope,
            } => {
                let redirect = parse_redirect_uri("auth_code", redirect_uri)?;
                let mut cred = ConfidentialClientApplication::builder(client_id.as_str())
                    .with_auth_code(auth_code.expose_secret());
                cred.with_client_secret(client_secret.expose_secret())
                    .with_tenant(tenant_id)
                    .with_redirect_uri(redirect)
                    .with_scope([scope.as_deref().unwrap_or(DEFAULT_SCOPE)]);
                GraphClient::from(&cred.build())
            }

            SharepointAuth::RefreshToken {
                tenant_id,
                client_id,
                client_secret,
                refresh_token,
                scope,
            } => {
                // The SDK exposes refresh-token exchange on
                // `AuthorizationCodeCredentialBuilder`. We start from
                // `with_auth_code("")` to reach that builder, then immediately
                // override with the refresh token — the empty auth code is
                // unused once a refresh token is present.
                let mut cred =
                    ConfidentialClientApplication::builder(client_id.as_str()).with_auth_code("");
                cred.with_refresh_token(refresh_token.expose_secret())
                    .with_client_secret(client_secret.expose_secret())
                    .with_tenant(tenant_id)
                    .with_scope([scope.as_deref().unwrap_or(DEFAULT_SCOPE)]);
                GraphClient::from(&cred.build())
            }

            SharepointAuth::DeviceCode {
                tenant_id,
                client_id,
                device_code,
                scope,
            } => {
                let mut cred = PublicClientApplication::builder(client_id.as_str())
                    .with_device_code(device_code.expose_secret());
                cred.with_tenant(tenant_id)
                    .with_scope([scope.as_deref().unwrap_or(DEFAULT_SCOPE)]);
                GraphClient::from(&cred.build())
            }

            SharepointAuth::SamlBearer(config) => {
                let flow = saml::SamlBearerFlow::new(config.clone());
                let token = flow.acquire_token().await.context(SamlBearerSnafu)?;
                GraphClient::new(token.access_token.expose_secret())
            }
        };
        Ok(Arc::new(client))
    }
}

fn parse_redirect_uri(flow: &str, uri: &str) -> Result<url::Url> {
    uri.parse::<url::Url>()
        .with_context(|_| InvalidRedirectUriSnafu {
            flow: flow.to_string(),
            uri: uri.to_string(),
        })
}
