use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ParquetReadOptions;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::Url;

use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::DataConnector;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No AWS access secret provided for credentials"))]
    NoAccessSecret,

    #[snafu(display("No AWS access key provided for credentials"))]
    NoAccessKey,
}

pub struct S3 {
    _s3: Arc<AmazonS3>,
}
impl S3 {
    #[must_use]
    pub fn get_from_params(
        params: &Arc<Option<HashMap<String, String>>>,
        key: &str,
    ) -> Option<String> {
        params
            .as_ref()
            .as_ref()
            .and_then(|params| params.get(key).cloned())
    }
}

#[async_trait]
impl DataConnector for S3 {
    fn new(
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let mut s3_builder = AmazonS3Builder::new().with_bucket_name("");

            if let Some(region) = Self::get_from_params(&params, "region") {
                s3_builder = s3_builder.with_region(region);
            }
            if let Some(endpoint) = Self::get_from_params(&params, "endpoint") {
                s3_builder = s3_builder.with_endpoint(endpoint);
            }

            if let Some(key) = auth_provider.get_param("key") {
                s3_builder = s3_builder.with_access_key_id(key);
            };
            if let Some(secret) = auth_provider.get_param("secret") {
                s3_builder = s3_builder.with_secret_access_key(secret);
            };
            let s3 = s3_builder
                .build()
                .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;

            Ok(Self { _s3: Arc::new(s3) })
        })
    }

    fn supports_data_streaming(&self, _dataset: &Dataset) -> bool {
        false
    }

    fn get_all_data(
        &self,
        _dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        todo!()
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        let ctx = SessionContext::new();

        let s3_url =
            Url::parse(&dataset.from).map_err(|_| super::Error::UnableToGetTableProvider {
                source: "unable to parse `from` field in spicepod as an S3 path".into(),
            })?;

        ctx.runtime_env()
            .register_object_store(&s3_url, self._s3.clone());

        let df = ctx
            .read_parquet(&dataset.from, ParquetReadOptions::default())
            .await
            .map_err(|e| super::Error::UnableToGetTableProvider { source: e.into() })?;

        Ok(df.into_view())
    }
}
