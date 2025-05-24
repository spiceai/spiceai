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

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::{any::Any, fmt::Debug, sync::Arc};

use crate::{Runtime, component::catalog::Catalog};

use super::CatalogConnector;

#[derive(Clone)]
pub struct DeferredCatalogProvider {
    rt: Arc<Runtime>,
    connector: Arc<dyn CatalogConnector>,
    catalog: Catalog,
}

impl Debug for DeferredCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredCatalogProvider")
            .field("catalog", &self.catalog)
            .field("connector", &"<connector>")
            .finish_non_exhaustive()
    }
}

impl DeferredCatalogProvider {
    pub fn new(rt: Arc<Runtime>, connector: Arc<dyn CatalogConnector>, catalog: Catalog) -> Self {
        Self {
            rt,
            connector,
            catalog,
        }
    }

    #[must_use]
    pub fn source(&self) -> Arc<dyn CatalogConnector> {
        Arc::clone(&self.connector)
    }

    pub async fn get_catalog_provider(&self) -> super::Result<Arc<dyn CatalogProvider>> {
        Ok(Arc::clone(&self.connector)
            .refreshable_catalog_provider(Arc::clone(&self.rt), &self.catalog)
            .await?)
    }
}

impl CatalogProvider for DeferredCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
        None
    }
}
