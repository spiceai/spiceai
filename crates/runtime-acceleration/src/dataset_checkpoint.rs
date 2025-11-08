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
    async fn checkpoint(&self, schema: &SchemaRef) -> Result<()>;
    async fn get_schema(&self) -> Result<Option<SchemaRef>>;
    async fn last_checkpoint_time(&self) -> Result<Option<SystemTime>>;
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
