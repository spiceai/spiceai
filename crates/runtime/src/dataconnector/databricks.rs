use async_trait::async_trait;
use deltalake::open_table_with_storage_options;
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
        let table_uri = "s3://databricks-workspace-stack-f0780-bucket/unity-catalog/686830279408652/__unitystorage/catalogs/1c3649de-309b-4c73-805e-8cf93aa4ee25/tables/d8f8978b-565d-4322-9ef2-15f4c431f8f0";
        let _table = open_table_with_storage_options(table_uri, HashMap::new());

        unimplemented!()
    }

    fn has_table_provider(&self) -> bool {
        false
    }

    async fn get_table_provider(
        &self,
        _dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        unimplemented!()
    }
}
