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
use crate::datafusion::{query::Protocol, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use model_components::model::{Error as ModelError, Model};
use std::result::Result;
use std::sync::Arc;

mod chat;
mod embed;
mod tool_use;

pub use chat::{try_to_chat_model, LLMModelStore};
pub use embed::{try_to_embedding, EmbeddingModelStore};

use crate::DataFusion;

pub static ENABLE_MODEL_SUPPORT_MESSAGE: &str = "To enable model support, either: \n  1) `spice install ai` \n  2) Build spiced binary with flag `--features models`.";

pub async fn run(m: &Model, df: Arc<DataFusion>) -> Result<RecordBatch, ModelError> {
    match df
        .query_builder(
            &(format!(
                "select * from {SPICE_DEFAULT_CATALOG}.{SPICE_DEFAULT_SCHEMA}.{} order by ts asc",
                m.model.datasets[0]
            )),
            Protocol::Internal,
        )
        .build()
        .run()
        .await
    {
        Ok(query_result) => match query_result.data.try_collect().await {
            Ok(d) => m.run(d),
            Err(e) => Err(ModelError::UnableToRunModel {
                source: Box::new(e),
            }),
        },
        Err(e) => Err(ModelError::UnableToRunModel {
            source: Box::new(e),
        }),
    }
}
