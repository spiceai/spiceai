/*
Copyright 2025 The Spice.ai OSS Authors
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

use std::{pin::Pin, sync::Arc, time::SystemTime};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait DatasetCheckpointer: Send + Sync {
    async fn exists(&self) -> bool;
    async fn checkpoint(&self, schema: &SchemaRef, refresh_sql: Option<&str>) -> Result<()>;
    async fn get_schema(&self) -> Result<Option<SchemaRef>>;
    async fn last_checkpoint_time(&self) -> Result<Option<SystemTime>>;
    async fn get_refresh_sql(&self) -> Result<Option<String>>;

    /// Discards this dataset's checkpoint, so the next refresh treats the accelerated
    /// table as fresh.
    ///
    /// Called when a schema change forces the table to be recreated: a checkpoint
    /// describing the old schema would otherwise make the refresh believe the new,
    /// empty table is already populated.
    async fn delete(&self) -> Result<()>;
}

type CheckpointerFuture =
    Pin<Box<dyn Future<Output = Result<Arc<dyn DatasetCheckpointer>>> + Send>>;

pub type DatasetCheckpointerFactory = Arc<dyn Fn() -> CheckpointerFuture + Send + Sync>;

// Helper to turn any async closure into the factory type without boxing call-sites.
pub fn make_checkpointer_factory<F, Fut>(f: F) -> DatasetCheckpointerFactory
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Arc<dyn DatasetCheckpointer>>> + Send + 'static,
{
    Arc::new(move || Box::pin(f()))
}

/// Encodes a schema for the checkpoint's `schema_json` column.
///
/// # Errors
///
/// Returns the serde failure when the schema cannot be encoded.
pub fn serialize_schema(schema: &SchemaRef) -> Result<String> {
    serde_json::to_string(schema).map_err(|source| Box::new(source) as _)
}

/// Decodes a schema previously written by [`serialize_schema`].
///
/// # Errors
///
/// Returns the serde failure when the stored JSON is not a schema.
pub fn deserialize_schema(schema_json: &str) -> Result<SchemaRef> {
    let schema: arrow::datatypes::Schema = serde_json::from_str(schema_json)
        .map_err(|source| -> Box<dyn std::error::Error + Send + Sync> { Box::new(source) })?;
    Ok(Arc::new(schema))
}
