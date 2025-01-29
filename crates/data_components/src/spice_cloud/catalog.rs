/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use super::provider::Error;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use http::Method;
use iceberg::{arrow::schema_to_arrow_schema, spec::SchemaRef, TableIdent};
use iceberg_catalog_rest::{ErrorResponse, OK};
use serde::Deserialize;

use crate::iceberg::catalog::RestCatalog;

#[derive(Debug, Deserialize)]
struct LoadTableResponse {
    metadata: TableMetadata,
}

#[derive(Debug, Deserialize)]
struct TableMetadata {
    schemas: Vec<SchemaRef>,
}

pub struct SpiceCatalog {
    inner: Arc<RestCatalog>,
}

impl SpiceCatalog {
    #[must_use]
    pub fn new(inner: Arc<RestCatalog>) -> Self {
        Self { inner }
    }

    pub async fn get_table_schema(&self, table: &TableIdent) -> Result<ArrowSchemaRef, Error> {
        let client = self
            .inner
            .http_client()
            .map_err(|err| Error::LoadTable { source: err })?;

        let request = client
            .request(Method::GET, self.inner.catalog_config.table_endpoint(table))
            .build()
            .map_err(|err| Error::LoadTable { source: err.into() })?;

        let resp = client
            .query::<LoadTableResponse, ErrorResponse, OK>(request)
            .await
            .map_err(|err| Error::LoadTable { source: err })?;

        let schema = resp.metadata.schemas.first();
        if let Some(schema) = schema {
            Ok(Arc::new(
                schema_to_arrow_schema(schema).map_err(|err| Error::LoadTable { source: err })?,
            ))
        } else {
            Err(Error::NoSchemaFound {})
        }
    }
}

impl From<Arc<RestCatalog>> for SpiceCatalog {
    fn from(inner: Arc<RestCatalog>) -> Self {
        Self::new(inner)
    }
}
