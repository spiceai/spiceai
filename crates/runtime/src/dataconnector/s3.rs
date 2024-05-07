/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::object_store_registry::default_runtime_env;

use super::{AnyErrorResult, DataConnector, DataConnectorFactory};
use async_trait::async_trait;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ParquetReadOptions;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No AWS access secret provided for credentials"))]
    NoAccessSecret,

    #[snafu(display("No AWS access key provided for credentials"))]
    NoAccessKey,

    #[snafu(display("Unable to parse URL {url}: {source}"))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadWriteProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToBuildObjectStore {
        source: object_store::Error,
    },

    #[snafu(display("The S3 URL is missing a forward slash: {url}"))]
    MissingForwardSlash {
        url: String,
    },

    ObjectStoreNotImplemented,

    #[snafu(display("{source}"))]
    UnableToBuildLogicalPlan {
        source: DataFusionError,
    },
}

pub struct S3 {
    secret: Option<Secret>,
    params: HashMap<String, String>,
}

impl DataConnectorFactory for S3 {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let s3 = Self {
                secret,
                params: params.as_ref().clone().map_or_else(HashMap::new, |x| x),
            };
            Ok(Arc::new(s3) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_object_store(
        &self,
        dataset: &Dataset,
    ) -> Option<AnyErrorResult<(Url, Arc<dyn ObjectStore + 'static>)>> {
        let result: AnyErrorResult<(Url, Arc<dyn ObjectStore + 'static>)> = (|| {
            let url = dataset.from.clone();
            let parts = url.clone().replace("s3://", "");

            let bucket = parts
                .split('/')
                .next()
                .ok_or_else(|| MissingForwardSlashSnafu { url: url.clone() }.build())?;

            let mut s3_builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_allow_http(true);

            if let Some(region) = self.params.get("region") {
                s3_builder = s3_builder.with_region(region);
            }
            if let Some(endpoint) = self.params.get("endpoint") {
                s3_builder = s3_builder.with_endpoint(endpoint);
            }
            if let Some(secret) = &self.secret {
                if let Some(key) = secret.get("key") {
                    s3_builder = s3_builder.with_access_key_id(key);
                };
                if let Some(secret) = secret.get("secret") {
                    s3_builder = s3_builder.with_secret_access_key(secret);
                };
            } else {
                s3_builder = s3_builder.with_skip_signature(true);
            };

            let s3 = s3_builder.build().context(UnableToBuildObjectStoreSnafu)?;

            let s3_url = Url::parse(&url).context(UnableToParseURLSnafu { url: url.clone() })?;

            Ok((s3_url, Arc::new(s3) as Arc<dyn ObjectStore>))
        })();

        Some(result)
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::AnyErrorResult<Arc<dyn TableProvider>> {
        let runtime = default_runtime_env();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

        let (url, _) = self
            .get_object_store(dataset)
            .ok_or_else(|| ObjectStoreNotImplementedSnafu.build())?
            .context(UnableToGetReadProviderSnafu)?;

        let table_path = ListingTableUrl::parse(url)?;
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

        let resolved_schema = options.infer_schema(&ctx.state(), &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?;

        Ok(Arc::new(table))
    }
}
