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
use spicepod::component::llms::{Architecture, LlmParams, LlmPrefix};
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
    let prefix = component.get_prefix().ok_or(LlmError::UnknownModelSource {
        source: format!(
            "Unknown model source for spicepod component from: {}",
            component.from.clone()
        )
        .into(),
    })?;

    let model_id = component.get_model_id();

    match construct_llm_params(
        &prefix,
        &model_id,
        &(component.params).clone().unwrap_or_default(),
    ) {
        Ok(LlmParams::OpenAiParams {
            api_base,
            api_key,
            org_id,
            project_id,
        }) => Ok(llms::nql::create_openai(
            model_id, api_base, api_key, org_id, project_id,
        )),
        Ok(LlmParams::LocalModelParams {
            weights_path,
            tokenizer_path,
            tokenizer_config_path,
        }) => llms::nql::create_local_model(
            &weights_path,
            tokenizer_path.as_deref(),
            tokenizer_config_path.as_ref(),
        ),
        Ok(LlmParams::HuggingfaceParams {
            model_type,
            weights_path,
            tokenizer_path,
            tokenizer_config_path,
        }) => {
            match component.get_model_id() {
                Some(id) => {
                    llms::nql::create_hf_model(
                        &id,
                        model_type.map(|x| x.to_string()),
                        &weights_path,
                        &tokenizer_path,
                        &tokenizer_config_path, // TODO handle inline chat templates
                    )
                }
                None => Err(LlmError::FailedToLoadModel {
                    source: format!("Failed to load model from: {}", component.from).into(),
                }),
            }
        }
        Err(e) => Err(e),
        _ => Err(LlmError::UnknownModelSource {
            source: format!(
                "Unknown model source for spicepod component from: {}",
                component.from.clone()
            )
            .into(),
        }),
    }
}

/// Construct the parameters needed to create an LLM based on its source (i.e. prefix).
/// If a `model_id` is provided (in the `from: `), it is provided.
fn construct_llm_params(
    from: &LlmPrefix,
    model_id: &Option<String>,
    params: &HashMap<String, String>,
) -> Result<LlmParams, LlmError> {
    match from {
        LlmPrefix::HuggingFace => {
            let model_type = params.get("model_type").cloned();
            let arch = match model_type {
                Some(arch) => {
                    let a = Architecture::try_from(arch.as_str()).map_err(|_| {
                        LlmError::UnknownModelSource {
                            source: format!("Unknown model architecture {arch} for spicepod llm")
                                .into(),
                        }
                    })?;
                    Some(a)
                }
                None => None,
            };

            Ok(LlmParams::HuggingfaceParams {
                model_type: arch,
                weights_path: model_id.clone().or(params.get("weights_path").cloned()),
                tokenizer_path: params.get("tokenizer_path").cloned(),
                tokenizer_config_path: params.get("tokenizer_config_path").cloned(),
            })
        }
        LlmPrefix::File => {
            let weights_path = model_id
                .clone()
                .or(params.get("weights_path").cloned())
                .ok_or(LlmError::FailedToLoadModel {
                    source: "No 'weights_path' parameter provided".into(),
                })?
                .clone();
            let tokenizer_path = params.get("tokenizer_path").cloned();
            let tokenizer_config_path = params
                .get("tokenizer_config_path")
                .ok_or(LlmError::FailedToLoadTokenizer {
                    source: "No 'tokenizer_config_path' parameter provided".into(),
                })?
                .clone();
            Ok(LlmParams::LocalModelParams {
                weights_path,
                tokenizer_path,
                tokenizer_config_path,
            })
        }

        LlmPrefix::SpiceAi => Ok(LlmParams::SpiceAiParams {}),

        LlmPrefix::OpenAi => Ok(LlmParams::OpenAiParams {
            api_base: params.get("endpoint").cloned(),
            api_key: params.get("openai_api_key").cloned(),
            org_id: params.get("openai_org_id").cloned(),
            project_id: params.get("openai_project_id").cloned(),
        }),
    }
}
