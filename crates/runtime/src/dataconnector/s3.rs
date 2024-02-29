use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ParquetReadOptions;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::Url;

use spicepod::component::dataset::Dataset;

use crate::auth::AuthProvider;

use super::DataConnector;
use object_store::aws::AwsCredential;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No AWS access secret provided for credentials"))]
    NoAccessSecret,

    #[snafu(display("No AWS access key provided for credentials"))]
    NoAccessKey,

    #[snafu(display("Unable to parse URL: {url}"))]
    UnableToParseURL { url: String },
}

pub struct S3 {
    auth_provider: AuthProvider,
    path: String,
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
            // let path: String = params
            //     .as_ref()
            //     .as_ref()
            //     .and_then(|params| params.get("path").cloned())
            //     .ok_or_else(|| super::Error::UnableToCreateDataConnector {
            //         source: "Missing required parameter: path".into(),
            //     })?;

            // let cred_builder = DefaultCredentialsChain::builder().build();

            Ok(Self {
                path: "".to_string(),
                auth_provider,
            })
        })
    }

    fn supports_data_streaming(&self, _dataset: &Dataset) -> bool {
        false
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        todo!()
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    fn has_object_store(&self) -> bool {
        true
    }

    fn get_object_store(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<(Url, Arc<dyn ObjectStore + 'static>), super::Error> {
        let region = "";
        let s3 = AmazonS3Builder::new()
            .with_region(region)
            .with_bucket_name("mldataplatform")
            .with_access_key_id("minio")
            .with_secret_access_key("minio123")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .build()
            .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;
        let path = dataset.path();

        let path = format!("s3:{path}");
        let s3_url = Url::parse(&path)
            .map_err(|e| super::Error::UnableToGetTableProvider { source: e.into() })?;

        Ok((s3_url, Arc::new(s3)))
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        let path = dataset.path();

        let ctx = SessionContext::new();

        let (url, store) = self
            .get_object_store(dataset)
            .map_err(|e| super::Error::UnableToGetTableProvider { source: e.into() })?;

        let _ = ctx.runtime_env().register_object_store(&url, store);

        let df = ctx
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(|e| super::Error::UnableToGetTableProvider { source: e.into() })?;

        Ok(df.into_view())
    }
}

pub fn from_auth_provider(auth: AuthProvider) -> Result<AwsCredential, Error> {
    Ok(AwsCredential {
        key_id: auth.get_param("key").context(NoAccessKeySnafu)?.to_string(),
        secret_key: auth
            .get_param("secret")
            .context(NoAccessSecretSnafu)?
            .to_string(),
        token: None,
    })
}
