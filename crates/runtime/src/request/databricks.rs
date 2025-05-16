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

use axum_extra::headers::{Error as HeaderError, Header};
use http::HeaderValue;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct DatabricksAuth(pub HashMap<String, SecretString>);

impl Header for DatabricksAuth {
    fn name() -> &'static http::HeaderName {
        static NAME: http::HeaderName = http::HeaderName::from_static("spice-databricks-auth");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeaderError>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let mut auth_map = HashMap::new();
        for value in values {
            if let Ok(s) = value.to_str() {
                // Split each header value by comma for multiple values in a single header
                s.split(',')
                    .map(str::trim)
                    .filter_map(|part| part.split_once(':'))
                    .for_each(|(client_id, access_token)| {
                        auth_map.insert(
                            client_id.trim().to_string(),
                            SecretString::from(access_token.trim()),
                        );
                    });
            }
        }
        Ok(DatabricksAuth(auth_map))
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let joined = self
            .0
            .iter()
            .map(|(client_id, token)| format!("{}:{}", client_id, token.expose_secret()))
            .collect::<Vec<_>>()
            .join(", ");

        if let Ok(value) = HeaderValue::from_str(&joined) {
            values.extend(std::iter::once(value));
        }
    }
}

#[derive(Clone, Debug)]
pub struct DatabricksContextExtension {
    tokens: Arc<HashMap<String, SecretString>>,
}

impl Default for DatabricksContextExtension {
    fn default() -> Self {
        Self {
            tokens: Arc::new(HashMap::new()),
        }
    }
}

impl DatabricksContextExtension {
    #[must_use]
    pub fn from_headers(headers: DatabricksAuth) -> Self {
        Self {
            tokens: Arc::new(headers.0),
        }
    }

    #[must_use]
    pub fn get_token(&self, client_id: &str) -> Option<SecretString> {
        self.tokens.get(client_id).cloned()
    }
}
