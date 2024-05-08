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

impl S3 {
    fn get_object_store_url(&self, dataset: &Dataset) -> AnyErrorResult<Url> {
        let url = dataset.from.clone();
        let mut params: HashMap<String, String> = HashMap::new();

        if let Some(region) = self.params.get("region") {
            let _ = params.insert("region".into(), region.into());
        }
        if let Some(endpoint) = self.params.get("endpoint") {
            let _ = params.insert("endpoint".into(), endpoint.into());
        }
        if let Some(secret) = &self.secret {
            if let Some(key) = secret.get("key") {
                let _ = params.insert("key".into(), key.into());
            };
            if let Some(secret) = secret.get("secret") {
                let _ = params.insert("secret".into(), secret.into());
            };
        }

        let mut s3_url = Url::parse_with_params(&url, params.clone())
            .context(UnableToParseURLSnafu { url: url.clone() })?;

        // infer_schema has a bug using is_collection which is determined by if url contains suffix of /
        // using a fragment with / suffix to trick df to think this is still a collection
        // will need to raise an issue with DF to use url without query and fragment to decide if
        // is_collection
        if url.ends_with('/') {
            s3_url.set_fragment(Some("dfiscollectionbugworkaround=hack/"));
        }

        Ok(s3_url)
    }
}

#[async_trait]
impl DataConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(&self, dataset: &Dataset) -> AnyErrorResult<Arc<dyn TableProvider>> {
        let runtime = default_runtime_env();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);

        let url = self
            .get_object_store_url(dataset)
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
