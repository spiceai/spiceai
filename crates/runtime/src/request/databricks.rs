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

use http::HeaderMap;
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct DatabricksAuthExtension {
    tokens: Arc<HashMap<String, SecretString>>,
}

impl Default for DatabricksAuthExtension {
    fn default() -> Self {
        Self {
            tokens: Arc::new(HashMap::new()),
        }
    }
}

impl DatabricksAuthExtension {
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let databricks_headers = headers.get_all("Spice-Databricks-Auth");
        let values = databricks_headers.iter();

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

        if auth_map.is_empty() {
            None
        } else {
            Some(Self {
                tokens: Arc::new(auth_map),
            })
        }
    }

    #[must_use]
    pub fn get_token(&self, client_id: &str) -> Option<SecretString> {
        self.tokens.get(client_id).cloned()
    }
}
