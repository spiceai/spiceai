use async_trait::async_trait;
use deltalake::open_table_with_storage_options;
use snafu::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use deltalake::aws::storage::s3_constants::{AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY};

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
        unimplemented!()
    }
}
