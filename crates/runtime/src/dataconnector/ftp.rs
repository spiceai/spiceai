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

use crate::object_store_registry::macros::impl_listing_data_connector;

use super::{AnyErrorResult, DataConnector, DataConnectorFactory};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::error::DataFusionError;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
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

    ObjectStoreNotImplemented,

    #[snafu(display("{source}"))]
    UnableToBuildLogicalPlan {
        source: DataFusionError,
    },

    #[snafu(display("Unsupported file format {format} in S3 Connector"))]
    UnsupportedFileFormat {
        format: String,
    },

    #[snafu(display("Unsupported compression type for CSV"))]
    UnsupportedCompressionType {
        source: DataFusionError,
        compression_type: String,
    },
}

pub struct FTP {
    secret: Option<Secret>,
    params: HashMap<String, String>,
}

impl DataConnectorFactory for FTP {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let ftp = Self {
                secret,
                params: params.as_ref().clone().map_or_else(HashMap::new, |x| x),
            };
            Ok(Arc::new(ftp) as Arc<dyn DataConnector>)
        })
    }
}

impl FTP {
    fn get_object_store_url(&self, dataset: &Dataset) -> AnyErrorResult<Url> {
        Ok(Url::parse(&dataset.from).unwrap())
    }

    fn get_file_format_and_extension(&self) -> AnyErrorResult<(Arc<dyn FileFormat>, String)> {
        let params = &self.params;
        let extension = params.get("file_extension").cloned();

        match params.get("file_format").map(String::as_str) {
            Some("csv") => Ok((
                get_csv_format(params)?,
                extension.unwrap_or(".csv".to_string()),
            )),
            None | Some("parquet") => Ok((
                Arc::new(ParquetFormat::default()),
                extension.unwrap_or(".parquet".to_string()),
            )),
            Some(format) => Err(Error::UnsupportedFileFormat {
                format: format.to_string(),
            }
            .into()),
        }
    }
}

fn get_csv_format(params: &HashMap<String, String>) -> AnyErrorResult<Arc<CsvFormat>> {
    let compression_type = params.get("compression_type").map_or("", |f| f);
    let has_header = params.get("has_header").map_or(true, |f| f == "true");
    let delimiter = params
        .get("delimiter")
        .map_or(b',', |f| *f.as_bytes().first().unwrap_or(&b','));

    Ok(Arc::new(
        CsvFormat::default()
            .with_has_header(has_header)
            .with_file_compression_type(
                FileCompressionType::from_str(compression_type)
                    .context(UnsupportedCompressionTypeSnafu { compression_type })?,
            )
            .with_delimiter(delimiter),
    ))
}

impl_listing_data_connector!(FTP);
