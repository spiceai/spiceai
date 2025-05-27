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

use app::App;
use datafusion::sql::TableReference;
use http::HeaderMap;
use secrecy::SecretString;
use spicepod::{
    component::{catalog::Catalog, dataset::Dataset},
    param::ParamValue,
};
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

impl DatabricksAuthExtension {
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

    pub async fn load_u2m_components(&self) {
        if let (Some(app), Some(df)) = (self.app.clone(), self.df.clone()) {
            let client_ids = self.tokens.keys().cloned().collect::<Vec<_>>();
            let databricks_u2m_datasets: Vec<Dataset> = app
                .datasets
                .iter()
                .filter_map(|dataset| {
                    let params = dataset.params.as_ref()?;
                    let Some(ParamValue::String(client_id)) =
                        params.data.get("databricks_client_id")
                    else {
                        return None;
                    };

                    if !client_ids.contains(client_id) {
                        return None;
                    }

                    if df.table_exists(TableReference::from(&dataset.name)) {
                        return None;
                    }

                    Some(dataset.clone())
                })
                .collect();

            let dataset_futures = databricks_u2m_datasets.into_iter().map(|ds| {
                let df = Arc::clone(&df);
                let tr = TableReference::from(ds.name.clone());
                Box::pin(async move {
                    if let Err(err) = df.load_deferred_dataset(tr.clone()).await {
                        tracing::warn!("Failed to load dataset {}: {}", ds.name, err);
                    }
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            });

            let databricks_u2m_catalogs: Vec<Catalog> = app
                .catalogs
                .iter()
                .filter_map(|catalog| {
                    let params = catalog.params.as_ref()?;
                    let Some(ParamValue::String(client_id)) =
                        params.data.get("databricks_client_id")
                    else {
                        return None;
                    };

                    if !client_ids.contains(client_id) {
                        return None;
                    }

                    Some(catalog.clone())
                })
                .collect();

            let catalog_futures = databricks_u2m_catalogs.into_iter().map(|catalog| {
                let df = Arc::clone(&df);
                let name = catalog.name.clone();
                Box::pin(async move {
                    if let Err(err) = df.load_deferred_catalog(name.as_str()).await {
                        tracing::warn!("Failed to load catalog {}: {}", name, err);
                    }
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            });

            let all_futures: Vec<_> = dataset_futures.chain(catalog_futures).collect();
            futures::future::join_all(all_futures).await;
        }
    }
}
