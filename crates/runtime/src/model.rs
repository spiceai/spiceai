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

use arrow::record_batch::RecordBatch;
use llms::nql::{Error as LlmError, Nql};
use model_components::model::{Error as ModelError, Model};
use spicepod::component::llms::LlmPrefix;
use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::DataFusion;

pub async fn run(m: &Model, df: Arc<RwLock<DataFusion>>) -> Result<RecordBatch, ModelError> {
    match df
        .read()
        .await
        .ctx
        .sql(
            &(format!(
                "select * from datafusion.public.{} order by ts asc",
                m.model.datasets[0]
            )),
        )
        .await
    {
        Ok(data) => match data.collect().await {
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

/// Attempt to derive a runnable NQL model from a given component from the Spicepod definition.
pub fn try_to_nql(component: &spicepod::component::llms::Llm) -> Result<Box<dyn Nql>, LlmError> {
    match component.get_prefix() { 
        Some(prefix) => match prefix {
            // LlmPrefix::HuggingFace => Ok(Box::new(HuggingFace::new(component))),
            // LlmPrefix::SpiceAi => Ok(Box::new(SpiceAi::new(component))),
            // LlmPrefix::File => Ok(Box::new(File::new(component))),
            LlmPrefix::OpenAi => llms::nql::create_nsql(
                &llms::nql::NSQLRuntime::Openai,
                component.params.clone(),
            ),
            _ => Err(LlmError::UnknownModelSource {
                source: format!(
                    "Unknown model source for spicepod component from: {}",
                    component.from.clone()
                )
                .into(),
            }),
        },
        None => Err(LlmError::UnknownModelSource {
            source: format!(
                "Unknown model source for spicepod component from: {}",
                component.from.clone()
            )
            .into(),
        }),
    }
}
