/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow_flight::HandshakeRequest;
use base64::{prelude::BASE64_STANDARD, Engine};
use runtime_auth::{AuthVerdict, FlightBasicAuth};
use tonic::{metadata::MetadataMap, Status};

/// The handshake request for Flight basic auth is a base64 encoded token that is calculated like:
///
/// ```rust,no_run
/// let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
/// let val = format!("Basic {val}");
/// ```
pub(crate) fn validate_basic_auth_handshake(
    handshake_request: &HandshakeRequest,
    basic_auth: Option<&Arc<dyn FlightBasicAuth + Send + Sync>>,
) -> Result<(), Status> {
    if let Some(basic_auth) = basic_auth {
        let token = handshake_request.payload.to_vec();
        let Ok(token_str) = String::from_utf8(token) else {
            return Err(Status::permission_denied("Invalid handshake request"));
        };
        let auth_header_split = token_str.splitn(2, ' ').collect::<Vec<&str>>();
        if auth_header_split.len() != 2 || auth_header_split[0] != "Basic" {
            return Err(Status::permission_denied("Invalid handshake request"));
        }
        let Ok(decoded_auth) = BASE64_STANDARD.decode(auth_header_split[1]) else {
            return Err(Status::permission_denied("Invalid handshake request"));
        };
        let Ok(decoded_auth_str) = String::from_utf8(decoded_auth) else {
            return Err(Status::permission_denied("Invalid handshake request"));
        };
        return validate_basic_auth_token(&decoded_auth_str, basic_auth);
    }
    Ok(())
}

/// The request for Flight basic auth is a base64 encoded token that is calculated like:
///
/// ```rust,no_run
/// let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
/// let val = format!("Bearer {val}");
/// ```
pub(crate) fn validate_basic_auth_request(
    metadata: &MetadataMap,
    basic_auth: Option<&Arc<dyn FlightBasicAuth + Send + Sync>>,
) -> Result<(), Status> {
    if let Some(basic_auth) = basic_auth {
        // The username and password are base64 encoded together in the Authorization header prefixed by "Bearer "
        let Some(auth_header) = metadata.get("authorization") else {
            return Err(Status::permission_denied("Missing authorization header"));
        };
        let Ok(auth_header_str) = auth_header.to_str() else {
            return Err(Status::permission_denied("Invalid authorization header"));
        };
        let auth_header_split = auth_header_str.splitn(2, ' ').collect::<Vec<&str>>();
        if auth_header_split.len() != 2 || auth_header_split[0] != "Bearer" {
            return Err(Status::permission_denied("Invalid authorization header"));
        }
        let Ok(decoded_auth) = BASE64_STANDARD.decode(auth_header_split[1]) else {
            return Err(Status::permission_denied("Invalid authorization header"));
        };
        let Ok(decoded_auth_str) = String::from_utf8(decoded_auth) else {
            return Err(Status::permission_denied("Invalid authorization header"));
        };
        return validate_basic_auth_token(&decoded_auth_str, basic_auth);
    }
    Ok(())
}

fn validate_basic_auth_token(
    token: &str,
    basic_auth: &Arc<dyn FlightBasicAuth + Send + Sync>,
) -> Result<(), Status> {
    let Some(colon_index) = token.find(':') else {
        return Err(Status::permission_denied("Invalid credentials"));
    };
    let (username, password) = token.split_at(colon_index);
    match basic_auth.flight_basic(username, password) {
        Ok(AuthVerdict::Allow) => Ok(()),
        Ok(AuthVerdict::Deny) => Err(Status::permission_denied("Invalid credentials")),
        Err(e) => Err(Status::internal(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_flight::HandshakeRequest;
    use base64::{prelude::BASE64_STANDARD, Engine};
    use runtime_auth::{AuthVerdict, Error, FlightBasicAuth};
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::*;

    struct MockAuth {
        allow_user: String,
        allow_pass: String,
    }

    impl FlightBasicAuth for MockAuth {
        fn flight_basic(&self, username: &str, password: &str) -> Result<AuthVerdict, Error> {
            if username == self.allow_user && password == self.allow_pass {
                Ok(AuthVerdict::Allow)
            } else {
                Ok(AuthVerdict::Deny)
            }
        }
    }

    #[test]
    fn test_validate_basic_auth_handshake() {
        let auth = Arc::new(MockAuth {
            allow_user: "test_user".to_string(),
            allow_pass: "test_pass".to_string(),
        });

        // Test valid credentials
        let token = format!("test_user:test_pass");
        let encoded = BASE64_STANDARD.encode(token);
        let handshake = HandshakeRequest {
            payload: format!("Basic {encoded}").into_bytes(),
            ..Default::default()
        };
        assert!(validate_basic_auth_handshake(&handshake, Some(&auth)).is_ok());

        // Test invalid credentials
        let token = format!("wrong_user:wrong_pass");
        let encoded = BASE64_STANDARD.encode(token);
        let handshake = HandshakeRequest {
            payload: format!("Basic {encoded}").into_bytes(),
            ..Default::default()
        };
        assert!(validate_basic_auth_handshake(&handshake, Some(&auth)).is_err());

        // Test invalid format (missing Basic prefix)
        let token = format!("test_user:test_pass");
        let encoded = BASE64_STANDARD.encode(token);
        let handshake = HandshakeRequest {
            payload: encoded.into_bytes(),
            ..Default::default()
        };
        assert!(validate_basic_auth_handshake(&handshake, Some(&auth)).is_err());

        // Test invalid base64
        let handshake = HandshakeRequest {
            payload: "Basic invalid_base64".into_bytes(),
            ..Default::default()
        };
        assert!(validate_basic_auth_handshake(&handshake, Some(&auth)).is_err());

        // Test no auth required
        let handshake = HandshakeRequest::default();
        assert!(validate_basic_auth_handshake(&handshake, None).is_ok());
    }

    #[test]
    fn test_validate_basic_auth_request() {
        let auth = Arc::new(MockAuth {
            allow_user: "test_user".to_string(),
            allow_pass: "test_pass".to_string(),
        });

        // Test valid credentials
        let mut metadata = MetadataMap::new();
        let token = format!("test_user:test_pass");
        let encoded = BASE64_STANDARD.encode(token);
        let auth_header = format!("Bearer {encoded}");
        metadata.insert(
            "authorization",
            MetadataValue::try_from(&auth_header).expect("Valid metadata value"),
        );
        assert!(validate_basic_auth_request(&metadata, Some(&auth)).is_ok());

        // Test invalid credentials
        let mut metadata = MetadataMap::new();
        let token = format!("wrong_user:wrong_pass");
        let encoded = BASE64_STANDARD.encode(token);
        let auth_header = format!("Bearer {encoded}");
        metadata.insert(
            "authorization",
            MetadataValue::try_from(&auth_header).expect("Valid metadata value"),
        );
        assert!(validate_basic_auth_request(&metadata, Some(&auth)).is_err());

        // Test missing authorization header
        let metadata = MetadataMap::new();
        assert!(validate_basic_auth_request(&metadata, Some(&auth)).is_err());

        // Test invalid format (missing Bearer prefix)
        let mut metadata = MetadataMap::new();
        let token = format!("test_user:test_pass");
        let encoded = BASE64_STANDARD.encode(token);
        metadata.insert(
            "authorization",
            MetadataValue::try_from(&encoded).expect("Valid metadata value"),
        );
        assert!(validate_basic_auth_request(&metadata, Some(&auth)).is_err());

        // Test invalid base64
        let mut metadata = MetadataMap::new();
        metadata.insert(
            "authorization",
            MetadataValue::try_from("Bearer invalid_base64").expect("Valid metadata value"),
        );
        assert!(validate_basic_auth_request(&metadata, Some(&auth)).is_err());

        // Test no auth required
        let metadata = MetadataMap::new();
        assert!(validate_basic_auth_request(&metadata, None).is_ok());
    }
}
