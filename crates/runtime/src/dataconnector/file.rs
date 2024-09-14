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

use crate::component::dataset::Dataset;
use snafu::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any::Any, env};
use url::Url;

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, InvalidConfigurationSnafu,
    ListingTableConnector, ParameterSpec, Parameters,
};

pub struct File {
    params: Parameters,
}

impl std::fmt::Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file")
    }
}

#[derive(Default, Copy, Clone)]
pub struct FileFactory {}

impl FileFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    // Common listing table parameters
    ParameterSpec::runtime("file_format"),
    ParameterSpec::runtime("file_extension"),
    ParameterSpec::runtime("csv_has_header")
        .description("Set true to indicate that the first line is a header."),
    ParameterSpec::runtime("csv_quote").description("The quote character in a row."),
    ParameterSpec::runtime("csv_escape").description("The escape character in a row."),
    ParameterSpec::runtime("csv_schema_infer_max_records")
        .description("Set a limit in terms of records to scan to infer the schema."),
    ParameterSpec::runtime("csv_delimiter")
        .description("The character separating values within a row."),
    ParameterSpec::runtime("file_compression_type")
        .description("The type of compression used on the file. Supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
];

impl DataConnectorFactory for FileFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(File { params }) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "file"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

impl ListingTableConnector for File {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    /// Creates a valid file [`url::Url`], from the dataset, supporting both
    ///   1. Relative paths
    ///   2. Datasets prefixed with `file://` (not just `file:/`). This is to mirror the UX of [`Url::parse`].
    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let clean_from = dataset.from.replace("file://", "file:/");

        let Some(path) = clean_from.strip_prefix("file:") else {
            // Should be unreachable
            return Err(super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "File".to_string(),
                message: "'dataset.from' must start with 'file:'".to_string(),
            });
        };

        // Convert relative path to absolute path
        let url_str = if path.starts_with('/') {
            format!("file:{path}")
        } else {
            let absolute_path = env::current_dir()
                .boxed()
                .context(InvalidConfigurationSnafu {
                    dataconnector: "File".to_string(),
                    message: "could not determine directory for relative file".to_string(),
                })?
                .join(path)
                .to_string_lossy()
                .to_string();

            format!("file:{absolute_path}")
        };

        Url::parse(&url_str)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "File".to_string(),
                message: "Invalid URL".to_string(),
            })
    }
}
