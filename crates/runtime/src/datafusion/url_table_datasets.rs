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

//! The datasets object-store URL tables borrow credentials from: the Spicepod's datasets on
//! `s3`, `abfs(s)` and `gs`, as written, whatever their load state.

use std::{
    error::Error as StdError,
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use runtime_datafusion::url_table::{DatasetLocation, DatasetStoreScope, UrlTableDatasets};
use runtime_object_store::url_table::{ObjectLocation, StoreFamily};
use url::Url;

use crate::{
    Runtime,
    component::dataset::builder::DatasetBuilder,
    dataconnector::{
        create_new_connector,
        parameters::{ConnectorParamsBuilder, RuntimeConnectorContext},
    },
};

/// Parameters a dataset may name its Azure storage account in, when its `from` does not.
const AZURE_ACCOUNT_PARAMS: &[&str] = &["abfs_account", "azure_storage_account_name"];

/// [`UrlTableDatasets`] over the runtime's current Spicepod.
#[derive(Debug)]
pub(crate) struct RuntimeUrlTableDatasets {
    runtime: Weak<Runtime>,
}

impl RuntimeUrlTableDatasets {
    pub(crate) fn new(runtime: Weak<Runtime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl UrlTableDatasets for RuntimeUrlTableDatasets {
    async fn locations(&self) -> Vec<DatasetLocation> {
        let Some(runtime) = self.runtime.upgrade() else {
            return Vec::new();
        };
        let Some(app) = runtime.read_app().await else {
            return Vec::new();
        };
        app.datasets
            .iter()
            .filter_map(|dataset| {
                let url = Url::parse(&dataset.from).ok()?;
                let family = StoreFamily::of_scheme(url.scheme())?;
                // An account named by a secret is not known until the secret resolves, and
                // coverage is decided from the Spicepod as written.
                let account = (family == StoreFamily::Azure)
                    .then(|| {
                        AZURE_ACCOUNT_PARAMS
                            .iter()
                            .find_map(|key| dataset.params.as_ref()?.data.get(*key))
                            .map(spicepod::param::ParamValue::as_string)
                            .filter(|value| !value.contains("${"))
                    })
                    .flatten();
                Some(DatasetLocation {
                    name: dataset.name.clone(),
                    from: dataset.from.clone(),
                    location: ObjectLocation::parse(&url, account.as_deref())?,
                })
            })
            .collect()
    }

    async fn scope(
        &self,
        dataset: &str,
    ) -> Result<DatasetStoreScope, Box<dyn StdError + Send + Sync>> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or("the runtime is shutting down")?;
        let app = runtime
            .read_app()
            .await
            .ok_or("the Spicepod is not loaded")?;
        let spec = app
            .datasets
            .iter()
            .find(|candidate| candidate.name == dataset)
            .ok_or_else(|| format!("dataset '{dataset}' is no longer in the Spicepod"))?;
        let dataset = DatasetBuilder::try_from(spec.clone())?
            .with_app(Arc::clone(&app))
            .with_runtime(Arc::clone(&runtime))
            .build()?;

        let source = dataset.source();
        // Resolves the dataset's `${ secrets:… }` now; a secret that does not resolve fails
        // here rather than leaving the URL table to read without credentials.
        let params = ConnectorParamsBuilder::for_dataset(source.into(), &dataset)
            .build(runtime.secrets(), runtime.tokio_io_runtime())
            .await?;
        let connector = create_new_connector(
            source,
            params,
            &RuntimeConnectorContext::for_dataset(&dataset),
        )
        .await
        .ok_or_else(|| format!("no data connector named '{source}' is registered"))??;
        let scope = connector
            .object_store_scope(&dataset)
            .await?
            .ok_or_else(|| {
                format!("the '{source}' data connector does not read object-store files")
            })?;
        Ok(DatasetStoreScope {
            store_url: scope.store_url,
            listing_suffix: scope.listing_suffix,
        })
    }
}
