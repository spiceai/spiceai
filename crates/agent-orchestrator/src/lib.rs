#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionNamedToolChoice,
        ChatCompletionRequestMessage, ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, ChatCompletionToolChoiceOption, ChatCompletionToolType,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse, FunctionCall, FunctionName,
    },
};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use llms::chat::{nsql::SqlGeneration, Chat};
use logical::{logical_plan_complete_summary, plan::LogicalPlan};
use physical::{executor::PhysicalJobExecutor, plan::PhysicalPlan};
use pipeline::{with_ending, with_starting};
use research::{
    model::{parse_response, research_complete_msg},
    Research,
};
use serde::{de::DeserializeOwned, Serialize};
use snafu::ResultExt;
use std::future::Future;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tools::SpiceModelTool;
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};
pub mod logical;
pub mod physical;
pub mod pipeline;
pub mod research;

#[derive(Clone)]
pub struct AgentModels {
    _orchestrator: String,
    executor: String,
    logical_planner: String,
    physical_tool_planner: String,
    physical_prompt_planner: String,
    researcher: String,
    verifier: String,
}

impl AgentModels {
    #[must_use]
    pub fn new(
        orchestrator: String,
        executor: String,
        logical_planner: String,
        physical_tool_planner: String,
        physical_prompt_planner: String,
        researcher: String,
        verifier: String,
    ) -> Self {
        Self {
            _orchestrator: orchestrator,
            executor,
            logical_planner,
            physical_tool_planner,
            physical_prompt_planner,
            researcher,
            verifier,
        }
    }
}

#[derive(Debug)]
pub enum ConversionError {
    SerdeJson(serde_json::Error),
    SerdeYaml(serde_yaml::Error),
    JsonSchema(jsonschema::ValidationError<'static>),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::SerdeJson(e) => write!(f, "{e}"),
            ConversionError::JsonSchema(e) => write!(f, "{e}"),
            ConversionError::SerdeYaml(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Clone)]
pub struct AgentChat {
    _objective: String,
    models: AgentModels,
    llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
    tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    max_retry: usize,
}

const DEFAULT_MAX_RETRY: usize = 1;
// This limitation is about single message in chat completion and not related to the model context window.
// TODO: Split one message into multiple messages if it exceeds the limit. https://github.com/spicehq/timmy/issues/47
const MAX_CONTENT_LENGTH: usize = 1048576;

impl AgentChat {
    pub fn new(
        objective: String,
        models: AgentModels,
        llms: Arc<RwLock<HashMap<String, Box<dyn Chat>>>>,
        tools: HashMap<String, Arc<dyn SpiceModelTool>>,
    ) -> Self {
        Self {
            _objective: objective,
            models,
            llms,
            tools,
            max_retry: DEFAULT_MAX_RETRY,
        }
    }

    pub fn with_max_retry(mut self, max_retry: usize) -> Self {
        self.max_retry = max_retry;
        self
    }

    async fn retry_stage<F, Fut, T>(
        &self,
        tx: &Sender<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
        retry_message: String,
        operation: F,
    ) -> Result<T, OpenAIError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, OpenAIError>>,
    {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(Some(self.max_retry))
            .max_duration(Some(Duration::from_secs(60)))
            .build();

        retry(retry_strategy, || async {
            match operation().await {
                Ok(result) => Ok(result),
                Err(e) => {
                    if should_retry_on_error(&e) {
                        tracing::warn!("Error: {e}. Retrying operation.");
                        tx.send(Err(OpenAIError::InvalidArgument(format!("RETRY ERROR"))))
                            .await;
                        return Err(RetryError::transient(e));
                    }
                    return Err(RetryError::Permanent(e));
                }
            }
        })
        .await
    }

    async fn generate_research(
        &self,
        research_model: &dyn Chat,
        prompt: &String,
        tx: &Sender<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
        retry_message: String,
    ) -> Result<Research, OpenAIError> {
        self.retry_stage(tx, retry_message, || {
            self.generate_research_single(research_model, prompt)
        })
        .await
    }

    async fn generate_logical_plan(
        &self,
        logical_planner_model: &dyn Chat,
        research: &Research,
        tx: &Sender<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
        retry_message: String,
    ) -> Result<LogicalPlan, OpenAIError> {
        self.retry_stage(tx, retry_message, || {
            self.generate_logical_plan_single(logical_planner_model, research)
        })
        .await
    }

    async fn generate_physical_plan(
        &self,
        plan: &LogicalPlan,
        physical_tool_planner_model: &dyn Chat,
        physical_prompt_planner_model: &dyn Chat,
        tx: &Sender<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
        retry_message: String,
    ) -> Result<PhysicalPlan, OpenAIError> {
        self.retry_stage(tx, retry_message, || {
            self.generate_physical_plan_single(
                &plan,
                physical_tool_planner_model,
                physical_prompt_planner_model,
            )
        })
        .await
    }

    #[allow(clippy::unused_async)]
    async fn generate_research_single(
        &self,
        research_model: &dyn Chat,
        prompt: &String,
    ) -> Result<Research, OpenAIError> {
        let mut initial_request = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(
                prompt.clone().into(),
            )])
            .build()?;

        initial_request.tool_choice = Some(ChatCompletionToolChoiceOption::Named(
            ChatCompletionNamedToolChoice {
                r#type: ChatCompletionToolType::Function,
                function: FunctionName {
                    name: "document_similarity".to_string(),
                },
            },
        ));

        let response = research_model.chat_request(initial_request).await?;
        let artifacts = parse_response(&response)?;

        let artifacts_prompt = artifacts
            .iter()
            .map(|artifact| format!("{artifact}"))
            .collect::<Vec<String>>()
            .join("\n\n");

        let logical_plan_prompt = format!("{}\n\n{}", artifacts_prompt, prompt);
        if logical_plan_prompt.len() > MAX_CONTENT_LENGTH {
            return Err(OpenAIError::ApiError(async_openai::error::ApiError {
                message: format!(
                    "Research artifacts exceeds size limit: {} (max: {})",
                    logical_plan_prompt.len(),
                    MAX_CONTENT_LENGTH
                )
                .to_string(),
                r#type: None,
                param: None,
                code: Some("string_above_max_length".to_string()),
            }));
        }

        Ok(Research {
            prompt: prompt.clone(),
            artifacts: artifacts,
        })
    }

    async fn generate_logical_plan_single(
        &self,
        logical_planner_model: &dyn Chat,
        research: &Research,
    ) -> Result<LogicalPlan, OpenAIError> {
        let artifacts_prompt = research
            .artifacts
            .iter()
            .map(|artifact| format!("{artifact}"))
            .collect::<Vec<String>>()
            .join("\n\n");
        let initial_request = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(
                format!("{}\n\n{}", artifacts_prompt, research.prompt).into(),
            )])
            .build()?;

        let response = logical_planner_model
            .chat_request(initial_request.clone())
            .await?;
        let plan = match LogicalPlan::from_chat_completion(&response) {
            Ok(plan) => plan,
            Err(ConversionError::JsonSchema(e)) => {
                tracing::warn!(
                    "Logical plan created did not satisfy JSONSchema format. Reattempting.\n   Initial Error: {e}"
                );
                let response = logical_planner_model.chat_request(initial_request).await?;
                LogicalPlan::from_chat_completion(&response)
                    .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            }
            Err(ConversionError::SerdeJson(e)) => {
                return Err(OpenAIError::InvalidArgument(format!(
                    "Failed to convert chat response to logical plan: {e}"
                )))
            }
            Err(ConversionError::SerdeYaml(e)) => {
                return Err(OpenAIError::InvalidArgument(format!(
                    "Failed to convert chat response to logical plan: {e}"
                )))
            }
        };

        Ok(plan)
    }

    async fn generate_physical_plan_single(
        &self,
        plan: &LogicalPlan,
        physical_tool_planner_model: &dyn Chat,
        physical_prompt_planner_model: &dyn Chat,
    ) -> Result<PhysicalPlan, OpenAIError> {
        let physical_plan = PhysicalPlan::plan(
            plan,
            physical_tool_planner_model,
            physical_prompt_planner_model,
            self.models.executor.clone(),
        )
        .await?;

        Ok(physical_plan)
    }

    #[allow(clippy::too_many_lines)]
    fn run_pipeline_stream(
        self: Arc<Self>,
        mut pipeline: pipeline::AgentPipeline,
        advance_mode: pipeline::AdvanceMode,
    ) -> ChatCompletionResponseStream {
        let (tx, rx) =
            mpsc::channel::<Result<CreateChatCompletionStreamResponse, OpenAIError>>(100);
        let id = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();

        let models = Arc::clone(&self.llms);

        tokio::spawn(async move {
            let models = models.read().await;
            let Some(researcher_model) = models.get(&self.models.researcher) else {
                let _ = tx
                    .send(Err(OpenAIError::InvalidArgument(format!(
                        "Model {} not found.",
                        self.models.researcher
                    ))))
                    .await;
                return;
            };
            let Some(logical_planner_model) = models.get(&self.models.logical_planner) else {
                let _ = tx
                    .send(Err(OpenAIError::InvalidArgument(format!(
                        "Model {} not found.",
                        self.models.logical_planner
                    ))))
                    .await;
                return;
            };
            let Some(physical_tool_planner_model) = models.get(&self.models.physical_tool_planner)
            else {
                let _ = tx
                    .send(Err(OpenAIError::InvalidArgument(format!(
                        "Model {} not found.",
                        self.models.physical_tool_planner
                    ))))
                    .await;
                return;
            };
            let Some(physical_prompt_planner_model) =
                models.get(&self.models.physical_prompt_planner)
            else {
                let _ = tx
                    .send(Err(OpenAIError::InvalidArgument(format!(
                        "Model {} not found.",
                        self.models.physical_prompt_planner
                    ))))
                    .await;
                return;
            };

            let service = Arc::clone(&self);
            let id = id.clone();
            loop {
                let _ = tx
                    .send(with_starting(
                        &pipeline.title(),
                        &pipeline.starting_message(),
                    ))
                    .await;
                let retry_message = pipeline.retry_message();
                match pipeline {
                    pipeline::AgentPipeline::Research { prompt } => {
                        let research = match service
                            .generate_research(
                                researcher_model.as_ref(),
                                &prompt,
                                &tx,
                                retry_message,
                            )
                            .await
                        {
                            Ok(l) => l,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        };

                        write_artifact("research/research_artifacts", &id, &research)
                            .expect("Failed to save research artifacts");

                        if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                            break;
                        }

                        pipeline = pipeline::AgentPipeline::LogicalPlan(research);
                    }
                    pipeline::AgentPipeline::LogicalPlan(research) => {
                        let logical_plan = match service
                            .generate_logical_plan(
                                logical_planner_model.as_ref(),
                                &research,
                                &tx,
                                retry_message,
                            )
                            .await
                        {
                            Ok(l) => l,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        };

                        write_artifact("logical/logical_plan", &id, &logical_plan)
                            .expect("Failed to save logical plan");

                        if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                            break;
                        }
                        pipeline = pipeline::AgentPipeline::PhysicalPlan(logical_plan);
                    }
                    pipeline::AgentPipeline::PhysicalPlan(logical_plan) => {
                        let physical_plan = match service
                            .generate_physical_plan(
                                &logical_plan,
                                physical_tool_planner_model.as_ref(),
                                physical_prompt_planner_model.as_ref(),
                                &tx,
                                retry_message,
                            )
                            .await
                        {
                            Ok(l) => l,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        };

                        write_artifact("physical/physical_plan", &id, &physical_plan)
                            .expect("Failed to save physical plan");

                        if matches!(advance_mode, pipeline::AdvanceMode::Stop) {
                            break;
                        }
                        pipeline = pipeline::AgentPipeline::Execution(physical_plan);
                    }
                    pipeline::AgentPipeline::Execution(physical_plan) => {
                        let mut executor = PhysicalJobExecutor::new(
                            physical_plan,
                            Arc::clone(&service.llms),
                            service.tools.clone(),
                            self.models.verifier.clone(),
                        );
                        let output = match executor.execute().await.map_err(|e| {
                            OpenAIError::InvalidArgument(format!(
                                "Error executing physical plan: {e}"
                            ))
                        }) {
                            Ok(l) => l,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        };
                        pipeline = pipeline::AgentPipeline::Output(output);
                    }
                    pipeline::AgentPipeline::Output(_) => {
                        let _ = tx
                            .send(with_ending(pipeline.previous_step_summary().as_str()))
                            .await;
                        break;
                    }
                }
                let _ = tx
                    .send(with_ending(pipeline.previous_step_summary().as_str()))
                    .await;
            }
        });

        Box::pin(ReceiverStream::new(rx))
    }
}

fn write_artifact<T: ?Sized + Serialize>(
    base_name: &str,
    id: &str,
    artifact: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let artifact_json =
        serde_json::to_string_pretty(artifact).expect("Failed to serialize logical plan");

    tracing::debug!("{base_name}: {artifact_json}");

    if let Some(base_dir) = PathBuf::from(format!("data/{base_name}")).parent() {
        if !base_dir.exists() {
            tracing::debug!("Creating directory(s) {base_dir:?}");
            std::fs::create_dir_all(base_dir).boxed()?;
        }
    }

    std::fs::write(format!("data/{base_name}_{id}.json"), &artifact_json).boxed()?;
    std::fs::write(format!("data/{base_name}_latest.json"), &artifact_json).boxed()?;
    Ok(())
}

fn should_retry_on_error(e: &OpenAIError) -> bool {
    if let OpenAIError::ApiError(api_error) = e {
        if let Some(code) = &api_error.code {
            return code == "string_above_max_length";
        }
    }
    false
}

#[async_trait]
impl Chat for AgentChat {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        todo!()
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let (pipeline, advance_mode) = pipeline::AgentPipeline::try_new(&req)
            .map_err(|e| OpenAIError::InvalidArgument(format!("Error parsing request: {e}")))?;

        Ok(Arc::new(self.clone()).run_pipeline_stream(pipeline, advance_mode))
    }

    // The non-streaming endpoint consumes the stream and returns the final result.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let stream = self.chat_stream(req).await?;

        // For chat request, the last item is to be used in the response.
        let final_stream_item = stream
            .try_fold(None, |_, item| async { Ok(Some(item)) })
            .await?
            .ok_or_else(|| OpenAIError::InvalidArgument("No output was produced".to_string()))?;

        Ok(stream_to_request_payload(final_stream_item))
    }
}

/// `stream` should be a full message.
#[allow(deprecated)]
fn stream_to_request_payload(
    stream: CreateChatCompletionStreamResponse,
) -> CreateChatCompletionResponse {
    CreateChatCompletionResponse {
        id: stream.id,
        object: stream.object,
        created: stream.created,
        model: stream.model,
        choices: stream
            .choices
            .into_iter()
            .map(|c| ChatChoice {
                index: c.index,
                message: ChatCompletionResponseMessage {
                    content: c.delta.content,
                    refusal: c.delta.refusal,
                    tool_calls: c.delta.tool_calls.map(|calls| {
                        calls
                            .into_iter()
                            .map(|call| ChatCompletionMessageToolCall {
                                id: call.id.unwrap_or_default(),
                                r#type: ChatCompletionToolType::Function,
                                function: FunctionCall {
                                    name: call
                                        .function
                                        .clone()
                                        .and_then(|f| f.name)
                                        .unwrap_or_default(),
                                    arguments: call
                                        .function
                                        .and_then(|f| f.arguments)
                                        .unwrap_or_default(),
                                },
                            })
                            .collect()
                    }),
                    role: c.delta.role.unwrap_or_default(),
                    function_call: None,
                    audio: None,
                },
                finish_reason: c.finish_reason,
                logprobs: c.logprobs,
            })
            .collect(),
        service_tier: stream.service_tier,
        system_fingerprint: stream.system_fingerprint,
        usage: stream.usage,
    }
}

#[allow(deprecated)]
fn get_output_message(id: &str, output: String) -> CreateChatCompletionStreamResponse {
    CreateChatCompletionStreamResponse {
        id: id.to_string(),
        object: String::new(),
        created: 0,
        model: String::new(),
        choices: vec![ChatChoiceStream {
            delta: ChatCompletionStreamResponseDelta {
                content: Some(output),
                function_call: None,
                tool_calls: None,
                role: None,
                refusal: None,
            },
            index: 0,
            finish_reason: None,
            logprobs: None,
        }],
        service_tier: None,
        system_fingerprint: None,
        usage: None,
    }
}

#[allow(dead_code)]
fn add_system_message(req: &mut CreateChatCompletionRequest, message: String) {
    req.messages
        .insert(0, ChatCompletionRequestMessage::System(message.into()));
}

pub fn validate_structured_output<T>(
    yaml_str: &str,
    completion: &CreateChatCompletionResponse,
) -> Result<T, ConversionError>
where
    T: DeserializeOwned,
{
    let body = completion
        .choices
        .first()
        .and_then(|choice| choice.message.content.clone())
        .unwrap_or_default();

    let as_value = serde_json::from_str(body.as_str()).map_err(ConversionError::SerdeJson)?;

    // First we validate against JSONSchema so the error message is more precise and informative.
    let yaml_value: serde_json::Value =
        serde_yaml::from_str(yaml_str).map_err(ConversionError::SerdeYaml)?;

    let v = jsonschema::validator_for(&yaml_value["json_schema"]["schema"])
        .map_err(|e| ConversionError::JsonSchema(e.to_owned()))?;
    v.validate(&as_value)
        .map_err(|e| ConversionError::JsonSchema(e.to_owned()))?;

    serde_json::from_value(as_value).map_err(ConversionError::SerdeJson)
}
