use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ParquetReadOptions;
use object_store::aws::AmazonS3Builder;
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

            Ok(Self { path: "".to_string(), auth_provider })
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

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        let ctx = SessionContext::new();

        // the region must be set to the region where the bucket exists until the following
        // issue is resolved
        // https://github.com/apache/arrow-rs/issues/2795
        let region = "us-east-1";

        let s3 = AmazonS3Builder::new()
            .with_bucket_name("")
            .with_region(region)
            .with_access_key_id("minio")
            .with_secret_access_key("minio123")
            .with_endpoint("localhost:9000")
            .build()
            .unwrap();

        let path = format!("s3://btc1/blocks/start_block=00745100/end_block=00745199");
        let s3_url = Url::parse(&path).unwrap();
        ctx.runtime_env()
            .register_object_store(&s3_url, Arc::new(s3));

        let df = ctx.read_parquet("*", ParquetReadOptions::default()).await;

        Ok(df.unwrap().into_view())
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
