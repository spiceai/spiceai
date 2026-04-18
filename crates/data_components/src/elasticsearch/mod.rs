/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Elasticsearch data components for the Spice.ai runtime.
//!
//! Provides [`TableProvider`] implementations for:
//! - Querying Elasticsearch indexes as tables
//! - kNN vector similarity search
//! - Full-text search

use snafu::Snafu;

pub mod query_table;
pub mod schema;
pub mod search_table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Elasticsearch client error: {source}"))]
    ClientError { source: elasticsearch::Error },

    #[snafu(display("Failed to convert Elasticsearch response to Arrow: {message}"))]
    ArrowConversionError { message: String },

    #[snafu(display("Elasticsearch index '{index}' not found"))]
    IndexNotFound { index: String },

    #[snafu(display("Elasticsearch mapping error: {message}"))]
    MappingError { message: String },
}

impl From<Error> for datafusion::error::DataFusionError {
    fn from(e: Error) -> Self {
        datafusion::error::DataFusionError::External(Box::new(e))
    }
}
