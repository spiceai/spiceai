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

//! Basic authentication for HTTP(S) object stores.
//!
//! The object store registry builds a store from a URL alone. A connector stores
//! its user name and password here with [`register`] and adds the returned token
//! to the URL fragment as `basic_auth=<token>`, so no URL carries the password.
//! A token is random and applies only to the origin (scheme, host and port) it
//! was stored for.

use std::collections::HashMap;
use std::sync::{LazyLock, PoisonError, RwLock};

use base64::Engine;
use http::HeaderValue;
use secrecy::{ExposeSecret, SecretString};
use url::Url;

/// The URL fragment key that carries a token from [`register`].
pub const FRAGMENT_KEY: &str = "basic_auth";

struct StoredCredential {
    token: String,
    password: SecretString,
}

/// One entry per origin and user name. Storing another password for the same
/// pair, as when a secret is rotated, replaces the entry and its token.
static CREDENTIALS: LazyLock<RwLock<HashMap<(String, String), StoredCredential>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Stores the credentials for `url`'s origin and returns the token to put in
/// the URL fragment under [`FRAGMENT_KEY`]. The same credentials return the same
/// token.
#[must_use]
pub fn register(url: &Url, username: &str, password: SecretString) -> String {
    let key = (url.origin().ascii_serialization(), username.to_string());
    let mut credentials = CREDENTIALS.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(stored) = credentials.get(&key)
        && stored.password.expose_secret() == password.expose_secret()
    {
        return stored.token.clone();
    }
    let token = format!("{:032x}", rand::random::<u128>());
    credentials.insert(
        key,
        StoredCredential {
            token: token.clone(),
            password,
        },
    );
    token
}

/// The `Authorization` header for a request to `url`, when `token` was stored
/// for `url`'s origin.
pub(crate) fn authorization(url: &Url, token: &str) -> Option<HeaderValue> {
    let origin = url.origin().ascii_serialization();
    let credentials = CREDENTIALS.read().unwrap_or_else(PoisonError::into_inner);
    let ((_, username), stored) = credentials
        .iter()
        .find(|((stored_origin, _), stored)| *stored_origin == origin && stored.token == token)?;
    let encoded = base64::engine::general_purpose::STANDARD
        .encode(format!("{username}:{}", stored.password.expose_secret()));
    let mut value = HeaderValue::from_str(&format!("Basic {encoded}")).ok()?;
    value.set_sensitive(true);
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("valid URL")
    }

    fn header(url_str: &str, token: &str) -> Option<String> {
        authorization(&url(url_str), token).map(|value| {
            assert!(value.is_sensitive(), "the header must be marked sensitive");
            value.to_str().expect("ASCII header").to_string()
        })
    }

    #[test]
    fn token_applies_only_to_its_origin() {
        let token = register(
            &url("http://127.0.0.1:18496/data/items.csv"),
            "origin_user",
            SecretString::from("origin-pass"),
        );
        let expected = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("origin_user:origin-pass")
        );

        assert_eq!(
            header("http://127.0.0.1:18496/other/path.csv", &token),
            Some(expected)
        );
        assert_eq!(header("http://127.0.0.1:18497/items.csv", &token), None);
        assert_eq!(header("http://localhost:18496/items.csv", &token), None);
        assert_eq!(header("https://127.0.0.1:18496/items.csv", &token), None);
        assert_eq!(
            header("http://127.0.0.1:18496/items.csv", "0"),
            None,
            "an unknown token resolves nothing"
        );
    }

    #[test]
    fn same_credentials_share_a_token_and_a_rotated_password_replaces_it() {
        let origin = url("https://files.example.com/a.csv");
        let first = register(&origin, "rotating_user", SecretString::from("old"));
        assert_eq!(
            register(&origin, "rotating_user", SecretString::from("old")),
            first
        );
        assert_eq!(first.len(), 32);

        let rotated = register(&origin, "rotating_user", SecretString::from("new"));
        assert_ne!(rotated, first);
        assert_eq!(header("https://files.example.com/a.csv", &first), None);
        assert_eq!(
            header("https://files.example.com/a.csv", &rotated),
            Some(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("rotating_user:new")
            ))
        );
    }
}
