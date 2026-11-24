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

use app::App;
use async_trait::async_trait;
use http::HeaderMap;
use runtime_request_context::Extension;
use secrecy::SecretString;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use crate::datafusion::DataFusion;

const SPICE_DATABRICKS_HEADER: &str = "spice-databricks-auth";

#[derive(Clone)]
pub struct DatabricksAuthExtension {
    app: Option<Arc<App>>,
    df: Option<Arc<DataFusion>>,
    tokens: Arc<HashMap<String, SecretString>>,
}

impl Default for DatabricksAuthExtension {
    fn default() -> Self {
        Self {
            app: None,
            df: None,
            tokens: Arc::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Extension for DatabricksAuthExtension {
    async fn load(&self) {
        self.load_u2m_components().await;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl DatabricksAuthExtension {
    #[must_use]
    pub fn new(
        app: Option<Arc<App>>,
        df: Option<Arc<DataFusion>>,
        tokens: Arc<HashMap<String, SecretString>>,
    ) -> Self {
        Self { app, df, tokens }
    }

    /// Whether `headers` carry Databricks user tokens, which
    /// [`Self::from_headers`] reads.
    #[must_use]
    pub fn in_headers(headers: &HeaderMap) -> bool {
        headers.contains_key(SPICE_DATABRICKS_HEADER)
    }

    #[must_use]
    pub fn from_headers(
        app: &Option<Arc<App>>,
        df: &Option<Arc<DataFusion>>,
        headers: &HeaderMap,
    ) -> Option<Self> {
        let databricks_headers = headers.get_all(SPICE_DATABRICKS_HEADER);
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
                app: app.as_ref().map(Arc::clone),
                df: df.as_ref().map(Arc::clone),
                tokens: Arc::new(auth_map),
            })
        }
    }

    #[must_use]
    pub fn get_token(&self, client_id: &str) -> Option<SecretString> {
        self.tokens.get(client_id).cloned()
    }

    /// Loads the deferred datasets and catalogs that the request's user tokens
    /// authenticate. Each is matched on the client ID it resolved, which a Spicepod may
    /// give as a secret reference.
    pub async fn load_u2m_components(&self) {
        let Some(df) = self.df.clone() else {
            return;
        };
        let client_ids = self.tokens.keys().cloned().collect::<Vec<_>>();

        let mut loads: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();
        for table in df.deferred_datasets_for(&client_ids).await {
            if df.table_exists(&table) {
                continue;
            }
            let df = Arc::clone(&df);
            loads.push(Box::pin(async move {
                if let Err(err) = df.load_deferred_dataset(table.clone()).await {
                    tracing::warn!("Failed to load dataset {table}: {err}");
                }
            }));
        }
        for name in df.deferred_catalogs_for(&client_ids).await {
            let df = Arc::clone(&df);
            loads.push(Box::pin(async move {
                match df.load_deferred_catalog(name.as_str()).await {
                    Ok(()) => {}
                    Err(err @ crate::datafusion::Error::InvalidDeferredCatalog { .. }) => {
                        tracing::error!("Failed to load catalog {name}: {err}");
                        runtime_metrics::catalogs::LOAD_ERROR.add(1, &[]);
                    }
                    Err(err) => tracing::warn!("Failed to load catalog {name}: {err}"),
                }
            }));
        }
        futures::future::join_all(loads).await;
    }
}
