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

//! A key service that signs JWTs with a private key it holds and never reveals, such as Azure
//! Key Vault or AWS KMS. The GitHub App token provider uses one in place of a PEM key.

use std::fmt;

use async_trait::async_trait;

/// The error a [`JwtSigner`] reports.
pub type SignError = Box<dyn std::error::Error + Send + Sync>;

/// A key service that holds an RSA private key and signs with it, so the key never leaves the
/// service.
#[async_trait]
pub trait JwtSigner: Send + Sync + fmt::Debug {
    /// The RS256 signature, RSASSA-PKCS1-v1_5 with SHA-256, of the message whose SHA-256 digest
    /// is `digest`.
    async fn sign_sha256_digest(&self, digest: &[u8; 32]) -> Result<Vec<u8>, SignError>;

    /// The key as its key service names it, for messages. Never key material.
    fn key_id(&self) -> &str;
}
