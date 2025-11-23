/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::error::{Error, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use pkcs8::{DecodePrivateKey, EncodePrivateKey};
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use spki::EncodePublicKey;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum AuthConfig {
    Password {
        username: String,
        password: String,
    },
    Jwt {
        username: String,
        private_key: String,
    },
}

impl AuthConfig {
    pub fn from_params(params: &HashMap<String, String>) -> Result<Self> {
        if let Some(private_key) = params.get("private_key") {
            let username = params
                .get("username")
                .ok_or_else(|| Error::InvalidArgument {
                    message: "username is required for JWT auth".to_string(),
                })?
                .clone();

            Ok(Self::Jwt {
                username,
                private_key: private_key.clone(),
            })
        } else if let Some(password) = params.get("password") {
            let username = params
                .get("username")
                .ok_or_else(|| Error::InvalidArgument {
                    message: "username is required for password auth".to_string(),
                })?
                .clone();

            Ok(Self::Password {
                username: username.clone(),
                password: password.clone(),
            })
        } else {
            Err(Error::InvalidArgument {
                message: "Either password or private_key must be provided".to_string(),
            })
        }
    }

    pub fn username(&self) -> &str {
        match self {
            Self::Password { username, .. } | Self::Jwt { username, .. } => username,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

/// Generate JWT token for Snowflake authentication using RSA private key.
pub fn generate_jwt_token(username: &str, account: &str, private_key_pem: &str) -> Result<String> {
    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem).map_err(|e| {
        Error::AuthenticationFailed {
            message: format!("Failed to parse private key: {}", e),
        }
    })?;

    let public_key_der = private_key.to_public_key().to_public_key_der().map_err(|e| {
        Error::AuthenticationFailed {
            message: format!("Failed to generate public key DER: {}", e),
        }
    })?;

    let mut hasher = Sha256::new();
    hasher.update(public_key_der.as_bytes());
    let public_key_fp = format!("SHA256:{}", base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hasher.finalize()));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::AuthenticationFailed {
            message: format!("Failed to get current time: {}", e),
        })?
        .as_secs();

    let qualified_username = format!("{}.{}", account.to_uppercase(), username.to_uppercase());

    let claims = JwtClaims {
        iss: format!("{}.{}", qualified_username, public_key_fp),
        sub: qualified_username,
        iat: now,
        exp: now + 3600, // 1 hour expiration
    };

    let private_key_der = private_key.to_pkcs8_der().map_err(|e| {
        Error::AuthenticationFailed {
            message: format!("Failed to encode private key to DER: {}", e),
        }
    })?;

    let encoding_key = EncodingKey::from_rsa_der(private_key_der.as_bytes());
    
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("JWT".to_string());

    encode(&header, &claims, &encoding_key).map_err(|e| Error::AuthenticationFailed {
        message: format!("Failed to encode JWT: {}", e),
    })
}
