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
use datafusion::datasource::file_format::{csv::CsvFormat, parquet::ParquetFormat, FileFormat};
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
use std::clone::Clone;
use std::pin::Pin;
use std::string::String;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::{form_urlencoded, Url};

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

    #[snafu(display("Unsupported file format {format} in S3 Connector"))]
    UnsupportedFileFormat {
        format: String,
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
        let mut fragments = vec![];
        let mut fragment_builder = form_urlencoded::Serializer::new(String::new());

        if let Some(region) = self.params.get("region") {
            fragment_builder.append_pair("region", region);
        }
        if let Some(endpoint) = self.params.get("endpoint") {
            fragment_builder.append_pair("endpoint", endpoint);
        }
        if let Some(secret) = &self.secret {
            if let Some(key) = secret.get("key") {
                fragment_builder.append_pair("key", key);
            };
            if let Some(secret) = secret.get("secret") {
                fragment_builder.append_pair("secret", secret);
            };
        }
        fragments.push(fragment_builder.finish());

        let mut s3_url =
            Url::parse(&dataset.from).context(UnableToParseURLSnafu { url: &dataset.from })?;

        // infer_schema has a bug using is_collection which is determined by if url contains suffix of /
        // using a fragment with / suffix to trick df to think this is still a collection
        // will need to raise an issue with DF to use url without query and fragment to decide if
        // is_collection
        // PR: https://github.com/apache/datafusion/pull/10419/files
        if dataset.from.clone().ends_with('/') {
            fragments.push("dfiscollectionbugworkaround=hack/".into());
        }

        s3_url.set_fragment(Some(&fragments.join("&")));

        Ok(s3_url)
    }

    fn get_file_format_and_extension(&self) -> AnyErrorResult<(Arc<dyn FileFormat>, String)> {
        let params = &self.params;
        match params.get("file_format").map(String::as_str) {
            Some("csv") => Ok((
                get_csv_format(params),
                params
                    .get("file_extension")
                    .map_or(".csv".to_string(), Clone::clone),
            )),
            None | Some("parquet") => Ok((
                Arc::new(ParquetFormat::default()),
                params
                    .get("file_extension")
                    .map_or(".parquet".to_string(), Clone::clone),
            )),
            Some(format) => Err(Error::UnsupportedFileFormat {
                format: format.to_string(),
            }
            .into()),
        }
    }
}

fn get_csv_format(params: &HashMap<String, String>) -> Arc<CsvFormat> {
    Arc::new(
        CsvFormat::default()
            .with_has_header(params.get("has_header").map_or(true, |f| f == "true"))
            .with_delimiter(
                params
                    .get("delimiter")
                    .map_or(b',', |f| *f.as_bytes().first().unwrap_or(&b',')),
            ),
    )
}

#[async_trait]
impl DataConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            default_runtime_env(),
        );

        let url = self
            .get_object_store_url(dataset)
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "s3",
                message: "Unable to parse URL",
            })?;

        let table_path =
            ListingTableUrl::parse(url)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: "s3".to_string(),
                    message: "Unable to parse URL",
                })?;

        let (file_format, extension) =
            self.get_file_format_and_extension()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: "s3".to_string(),
                    message: "Unable to resolve file_format and file_extension",
                })?;
        let options = ListingOptions::new(file_format).with_file_extension(&extension);

        let resolved_schema = options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .boxed()
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "s3".to_string(),
                message: "Unable to infer files schema",
            })?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);

        let table =
            ListingTable::try_new(config)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: "s3".to_string(),
                    message: "Unable to list files in S3 bucket",
                })?;

        Ok(Arc::new(table))
    }
}
