use async_trait::async_trait;
use deltalake::open_table_with_storage_options;
use snafu::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::DataConnector;

pub struct Databricks {
    auth_provider: AuthProvider,
    params: Arc<Option<HashMap<String, String>>>,
}

#[async_trait]
impl DataConnector for Databricks {
    fn new(
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        // Needed to be able to load the s3:// scheme
        deltalake::aws::register_handlers(None);
        Box::pin(async move {
            Ok(Self {
                auth_provider,
                params,
            })
        })
    }

    fn get_all_data(
        &self,
        _dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        unimplemented!()
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        _dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        // TODO: Translate the databricks URI
        let table_uri = "s3://databricks-workspace-stack-f0780-bucket/unity-catalog/686830279408652/__unitystorage/catalogs/1c3649de-309b-4c73-805e-8cf93aa4ee25/tables/c704d041-f240-43eb-b648-88efb26fdefc";

        let mut storage_options = HashMap::new();
        for (key, value) in self.auth_provider.iter() {
            if !key.starts_with("AWS_") {
                continue;
            }
            storage_options.insert(key.to_string(), value.to_string());
        }
        // TODO: Figure out a way to avoid this default hashmap
        let default_hashmap = HashMap::new();
        for (key, value) in self.params.as_ref().as_ref().unwrap_or(&default_hashmap) {
            storage_options.insert(key.to_string(), value.to_string());
        }
        drop(default_hashmap);

        let delta_table: deltalake::DeltaTable =
            open_table_with_storage_options(table_uri, storage_options)
                .await
                .boxed()
                .context(super::UnableToGetTableProviderSnafu)?;

        Ok(Arc::new(delta_table))
    }
}
