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

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, LazyLock},
};

use ::search::aggregation::reciprocal_rank::{
    DEFAULT_RRF_K, reciprocal_rank_fusion_scores, usize_to_f64,
};
use async_trait::async_trait;
use llms::embeddings::{Embed, EmbeddingInput};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu};
use tokio::sync::OnceCell;
use tools::SpiceModelTool;
use tracing_futures::Instrument;

use crate::{
    Runtime,
    tools::utils::{get_tools, parameters},
};
use runtime_tools::options::SpiceToolsOptions;

const TOOL_SEARCH_NAME: &str = "tool_search";
const TOOL_INVOKE_NAME: &str = "tool_invoke";
const LIST_DATASETS_TOOL_NAME: &str = "list_datasets";
pub(crate) const TOOL_EMBEDDING_MODEL_PARAM: &str = "tool_embedding_model";
const DEFAULT_SEARCH_LIMIT: usize = 5;
const MAX_SEARCH_LIMIT: usize = 20;
const AUTO_SEARCH_TOOL_THRESHOLD: usize = 20;
const TOOL_REGISTRY_SEARCH_TOOL_CACHE_MAX_ENTRIES: usize = 64;

static TOOL_REGISTRY_SEARCH_TOOL_CACHE: LazyLock<
    tokio::sync::RwLock<HashMap<ToolRegistrySearchCacheKey, Arc<ToolRegistrySearchTool>>>,
> = LazyLock::new(|| tokio::sync::RwLock::new(HashMap::new()));

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ToolRegistrySearchCacheKey {
    runtime_id: usize,
    embedding_model_name: String,
    tools_hash: u64,
}

#[derive(Debug, Snafu)]
enum ToolRegistryError {
    #[snafu(display(
        "Tool '{tool_id}' was not found in the searchable tool registry. Available tools: {available_tools}"
    ))]
    ToolNotFound {
        tool_id: String,
        available_tools: String,
    },

    #[snafu(display("Failed to invoke tool '{tool_id}' from searchable registry: {source}"))]
    ToolInvokeFailed {
        tool_id: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Tool name '{tool_name}' is reserved for the searchable tool registry. Rename the configured tool or disable searchable registry tools."
    ))]
    ReservedToolName { tool_name: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolRegistryPreparationMode {
    Required,
    Auto,
}

pub(crate) async fn prepare_model_tools(
    rt: Arc<Runtime>,
    opts: &SpiceToolsOptions,
    tools: Vec<Arc<dyn SpiceModelTool>>,
    embedding_model_name: Option<&str>,
) -> Result<Vec<Arc<dyn SpiceModelTool>>, Box<dyn std::error::Error + Send + Sync>> {
    match tool_registry_preparation_mode(opts, tools.len()) {
        Some(ToolRegistryPreparationMode::Required) => {
            ensure_no_reserved_tool_registry_name_conflicts(&tools)?;
            let embedding_model =
                resolve_tool_registry_embedding_model(Arc::clone(&rt), embedding_model_name)
                    .await?;
            Ok(tool_registry_tools(tools, embedding_model))
        }
        Some(ToolRegistryPreparationMode::Auto) => {
            if let Some(tool_name) = reserved_tool_registry_name_conflict(&tools) {
                tracing::warn!(
                    "Unable to use searchable tool registry for tools: auto: tool name '{}' is reserved for the searchable tool registry. Falling back to direct tool definitions.",
                    tool_name
                );
                return Ok(tools);
            }

            match resolve_tool_registry_embedding_model(Arc::clone(&rt), embedding_model_name).await
            {
                Ok(embedding_model) => Ok(tool_registry_tools(tools, embedding_model)),
                Err(error) => {
                    tracing::warn!(
                        "Unable to use searchable tool registry for tools: auto: {}. Falling back to direct tool definitions.",
                        error
                    );
                    Ok(tools)
                }
            }
        }
        None => Ok(tools),
    }
}

fn tool_registry_preparation_mode(
    opts: &SpiceToolsOptions,
    tool_count: usize,
) -> Option<ToolRegistryPreparationMode> {
    match opts {
        SpiceToolsOptions::SearchRegistry => Some(ToolRegistryPreparationMode::Required),
        SpiceToolsOptions::Auto if should_auto_search(tool_count) => {
            Some(ToolRegistryPreparationMode::Auto)
        }
        SpiceToolsOptions::Specific(requested_tools) => {
            specific_tool_registry_preparation_mode(requested_tools, tool_count)
        }
        SpiceToolsOptions::Auto
        | SpiceToolsOptions::All
        | SpiceToolsOptions::Nsql
        | SpiceToolsOptions::Disabled => None,
    }
}

fn specific_tool_registry_preparation_mode(
    requested_tools: &[String],
    tool_count: usize,
) -> Option<ToolRegistryPreparationMode> {
    let mut auto_requested = false;

    for requested_tool in requested_tools {
        match requested_tool.parse::<SpiceToolsOptions>() {
            Ok(SpiceToolsOptions::SearchRegistry) => {
                return Some(ToolRegistryPreparationMode::Required);
            }
            Ok(SpiceToolsOptions::Auto) => auto_requested = true,
            Ok(
                SpiceToolsOptions::All
                | SpiceToolsOptions::Nsql
                | SpiceToolsOptions::Disabled
                | SpiceToolsOptions::Specific(_),
            )
            | Err(_) => {}
        }
    }

    (auto_requested && should_auto_search(tool_count)).then_some(ToolRegistryPreparationMode::Auto)
}

fn should_auto_search(tool_count: usize) -> bool {
    tool_count > AUTO_SEARCH_TOOL_THRESHOLD
}

fn is_reserved_tool_registry_name(tool_name: &str) -> bool {
    matches!(tool_name, TOOL_SEARCH_NAME | TOOL_INVOKE_NAME)
}

fn reserved_tool_registry_name_conflict(tools: &[Arc<dyn SpiceModelTool>]) -> Option<String> {
    tools.iter().find_map(|tool| {
        let tool_name = tool.name();
        is_reserved_tool_registry_name(tool_name.as_ref()).then(|| tool_name.into_owned())
    })
}

fn ensure_no_reserved_tool_registry_name_conflicts(
    tools: &[Arc<dyn SpiceModelTool>],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tool_name) = reserved_tool_registry_name_conflict(tools) {
        return Err(Box::new(ToolRegistryError::ReservedToolName { tool_name }));
    }

    Ok(())
}

#[must_use]
fn tool_registry_tools(
    tools: Vec<Arc<dyn SpiceModelTool>>,
    embedding_model: Arc<dyn Embed>,
) -> Vec<Arc<dyn SpiceModelTool>> {
    if tools.is_empty() {
        return Vec::new();
    }

    let registry = Arc::new(tools);
    let search_tool = Arc::new(ToolRegistrySearchTool::new(
        registry.as_slice(),
        embedding_model,
    )) as Arc<dyn SpiceModelTool>;

    tool_registry_tools_with_search_tool(&registry, search_tool)
}

fn tool_registry_tools_with_search_tool(
    registry: &Arc<Vec<Arc<dyn SpiceModelTool>>>,
    search_tool: Arc<dyn SpiceModelTool>,
) -> Vec<Arc<dyn SpiceModelTool>> {
    let direct_tools = registry
        .iter()
        .filter(|tool| tool.name() == LIST_DATASETS_TOOL_NAME)
        .cloned()
        .collect::<Vec<_>>();
    let mut advertised_tools = vec![
        search_tool,
        Arc::new(ToolRegistryInvokeTool::new(Arc::clone(registry))) as Arc<dyn SpiceModelTool>,
    ];
    advertised_tools.extend(direct_tools);
    advertised_tools
}

pub(crate) async fn tool_registry_prompt_tools(
    rt: Arc<Runtime>,
    embedding_model_name: Option<&str>,
) -> Result<Vec<Arc<dyn SpiceModelTool>>, Box<dyn std::error::Error + Send + Sync>> {
    let tools = Arc::new(get_tools(Arc::clone(&rt), &SpiceToolsOptions::SearchRegistry).await);
    ensure_no_reserved_tool_registry_name_conflicts(tools.as_slice())?;
    let (resolved_embedding_model_name, embedding_model) =
        resolve_tool_registry_embedding_model_with_name(Arc::clone(&rt), embedding_model_name)
            .await?;
    let search_tool = cached_tool_registry_search_tool(
        &rt,
        Arc::clone(&tools),
        &resolved_embedding_model_name,
        embedding_model,
    )
    .await as Arc<dyn SpiceModelTool>;

    Ok(tool_registry_tools_with_search_tool(&tools, search_tool))
}

pub(crate) async fn get_tool_registry_tool(
    rt: Arc<Runtime>,
    tool_name: &str,
    embedding_model_name: Option<&str>,
) -> Result<Option<Arc<dyn SpiceModelTool>>, Box<dyn std::error::Error + Send + Sync>> {
    if is_reserved_tool_registry_name(tool_name) && rt.get_tool(tool_name).await.is_some() {
        return Ok(None);
    }

    match tool_name {
        TOOL_SEARCH_NAME => {
            let tools =
                Arc::new(get_tools(Arc::clone(&rt), &SpiceToolsOptions::SearchRegistry).await);
            let (resolved_embedding_model_name, embedding_model) =
                resolve_tool_registry_embedding_model_with_name(
                    Arc::clone(&rt),
                    embedding_model_name,
                )
                .await?;
            let search_tool = cached_tool_registry_search_tool(
                &rt,
                tools,
                &resolved_embedding_model_name,
                embedding_model,
            )
            .await as Arc<dyn SpiceModelTool>;
            Ok(Some(search_tool))
        }
        TOOL_INVOKE_NAME => {
            let tools = get_tools(Arc::clone(&rt), &SpiceToolsOptions::SearchRegistry).await;
            let registry = Arc::new(tools);
            Ok(Some(
                Arc::new(ToolRegistryInvokeTool::new(registry)) as Arc<dyn SpiceModelTool>
            ))
        }
        _ => Ok(None),
    }
}

pub(crate) async fn resolve_tool_registry_embedding_model(
    rt: Arc<Runtime>,
    model_name: Option<&str>,
) -> Result<Arc<dyn Embed>, Box<dyn std::error::Error + Send + Sync>> {
    let (_, embedding_model) =
        resolve_tool_registry_embedding_model_with_name(rt, model_name).await?;
    Ok(embedding_model)
}

async fn resolve_tool_registry_embedding_model_with_name(
    rt: Arc<Runtime>,
    model_name: Option<&str>,
) -> Result<(String, Arc<dyn Embed>), Box<dyn std::error::Error + Send + Sync>> {
    let configured_model_names = configured_embedding_model_names(&rt).await;
    let model_name =
        select_tool_registry_embedding_model_name(&configured_model_names, model_name)?;

    let Some(embedding_model) = rt.embeds().read().await.get(&model_name).cloned() else {
        return Err(format!("Embedding model '{model_name}' configured for searchable tool discovery was not loaded. Check earlier embedding model errors and verify the `embeddings` configuration").into());
    };
    Ok((model_name, embedding_model))
}

async fn cached_tool_registry_search_tool(
    rt: &Arc<Runtime>,
    tools: Arc<Vec<Arc<dyn SpiceModelTool>>>,
    embedding_model_name: &str,
    embedding_model: Arc<dyn Embed>,
) -> Arc<ToolRegistrySearchTool> {
    let key = ToolRegistrySearchCacheKey {
        runtime_id: Arc::as_ptr(rt).addr(),
        embedding_model_name: embedding_model_name.to_string(),
        tools_hash: tool_registry_tools_hash(&tools),
    };

    if let Some(tool) = TOOL_REGISTRY_SEARCH_TOOL_CACHE.read().await.get(&key) {
        return Arc::clone(tool);
    }

    let mut cache = TOOL_REGISTRY_SEARCH_TOOL_CACHE.write().await;
    if cache.len() >= TOOL_REGISTRY_SEARCH_TOOL_CACHE_MAX_ENTRIES
        && !cache.contains_key(&key)
        && let Some(evicted_key) = cache.keys().next().cloned()
    {
        cache.remove(&evicted_key);
    }

    let tool = cache.entry(key).or_insert_with(|| {
        Arc::new(ToolRegistrySearchTool::new(
            tools.as_slice(),
            embedding_model,
        ))
    });
    Arc::clone(tool)
}

fn tool_registry_tools_hash(tools: &[Arc<dyn SpiceModelTool>]) -> u64 {
    let mut hasher = DefaultHasher::new();
    tools.len().hash(&mut hasher);
    for tool in tools {
        tool.name().hash(&mut hasher);
        tool.description().hash(&mut hasher);
        tool.parameters()
            .map(|parameters| parameters.to_string())
            .hash(&mut hasher);
    }
    hasher.finish()
}

async fn configured_embedding_model_names(rt: &Arc<Runtime>) -> Vec<String> {
    let mut names = rt
        .read_app()
        .await
        .map(|app| {
            app.embeddings
                .iter()
                .map(|embedding| embedding.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn select_tool_registry_embedding_model_name(
    configured_model_names: &[String],
    model_name: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(model_name) = model_name {
        return configured_model_names
            .iter()
            .find(|configured_model_name| configured_model_name.as_str() == model_name)
            .cloned()
            .ok_or_else(|| {
                format!("Embedding model '{model_name}' specified by `{TOOL_EMBEDDING_MODEL_PARAM}` was not found in the `embeddings` section").into()
            });
    }

    match configured_model_names {
        [] => Err(format!("No embedding model configured for searchable tool discovery. Add one model to the `embeddings` section, or set `{TOOL_EMBEDDING_MODEL_PARAM}` to reference a configured embedding model").into()),
        [model_name] => Ok(model_name.clone()),
        model_names => Err(format!("Multiple embedding models are configured for searchable tool discovery: {}. Set `{TOOL_EMBEDDING_MODEL_PARAM}` to one of them", model_names.join(", ")).into()),
    }
}

struct ToolRegistrySearchTool {
    documents: Arc<Vec<ToolDocument>>,
    document_texts: Arc<Vec<String>>,
    embedding_model: Arc<dyn Embed>,
    tool_embeddings: OnceCell<Vec<Vec<f32>>>,
}

impl ToolRegistrySearchTool {
    fn new(tools: &[Arc<dyn SpiceModelTool>], embedding_model: Arc<dyn Embed>) -> Self {
        let documents = tools.iter().map(ToolDocument::new).collect::<Vec<_>>();
        let document_texts = documents
            .iter()
            .map(ToolDocument::vector_text)
            .collect::<Vec<_>>();

        Self {
            documents: Arc::new(documents),
            document_texts: Arc::new(document_texts),
            embedding_model,
            tool_embeddings: OnceCell::new(),
        }
    }
}

#[async_trait]
impl SpiceModelTool for ToolRegistrySearchTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(TOOL_SEARCH_NAME)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(
            "Search the Spice tool registry for tools relevant to the current task. Call this before tool_invoke; it returns tool_id, description, parameters, and score for the best matches.",
        ))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<ToolSearchParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::tool_search", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: ToolSearchParams = serde_json::from_str(arg)?;
            let limit = params
                .limit
                .unwrap_or(DEFAULT_SEARCH_LIMIT)
                .clamp(1, MAX_SEARCH_LIMIT);
            let min_score = params.min_score.unwrap_or(0.0).clamp(0.0, 1.0);

            let mut ranked_tools = hybrid_rank_tools(
                self.documents.as_slice(),
                self.document_texts.as_slice(),
                &params,
                &self.embedding_model,
                &self.tool_embeddings,
            )
            .await?;
            ranked_tools.sort_by(|left, right| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.tool_id.cmp(&right.tool_id))
            });

            let max_score = ranked_tools
                .first()
                .map_or(0.0, |ranked_tool| ranked_tool.score);
            let tools = ranked_tools
                .into_iter()
                .filter(|ranked_tool| ranked_tool.score >= min_score || max_score == 0.0)
                .take(limit)
                .map(ToolSearchResult::from)
                .collect::<Vec<_>>();

            Ok(json!({
                "query": params.query,
                "keywords": params.keywords,
                "search_mode": "hybrid_rrf",
                "tools": tools,
            }))
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

struct ToolRegistryInvokeTool {
    tools: Arc<Vec<Arc<dyn SpiceModelTool>>>,
}

impl ToolRegistryInvokeTool {
    fn new(tools: Arc<Vec<Arc<dyn SpiceModelTool>>>) -> Self {
        Self { tools }
    }

    fn find_tool(&self, tool_id: &str) -> Option<Arc<dyn SpiceModelTool>> {
        self.tools
            .iter()
            .find(|tool| tool.name() == tool_id)
            .cloned()
    }
}

#[async_trait]
impl SpiceModelTool for ToolRegistryInvokeTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(TOOL_INVOKE_NAME)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(
            "Invoke one Spice tool returned by tool_search. Pass the selected tool_id and an arguments object matching that tool's parameters.",
        ))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<ToolInvokeParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::tool_invoke", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params: ToolInvokeParams = serde_json::from_str(arg)?;
            let Some(tool) = self.find_tool(&params.tool_id) else {
                let available_tools = self
                    .tools
                    .iter()
                    .map(|tool| tool.name().to_string())
                    .take(MAX_SEARCH_LIMIT)
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Box::new(ToolRegistryError::ToolNotFound {
                    tool_id: params.tool_id,
                    available_tools,
                })
                    as Box<dyn std::error::Error + Send + Sync>);
            };

            let tool_id = tool.name().to_string();
            let arguments = match params.arguments {
                Some(Value::String(arguments)) => arguments,
                Some(Value::Null) | None => "{}".to_string(),
                Some(arguments) => serde_json::to_string(&arguments)?,
            };

            let result = tool.call(&arguments).await.map_err(|source| {
                ToolRegistryError::ToolInvokeFailed {
                    tool_id: tool_id.clone(),
                    source,
                }
            })?;
            Ok(json!({
                "tool_id": tool_id,
                "result": result,
            }))
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ToolSearchParams {
    /// Natural-language description of the capability needed.
    query: String,

    /// Optional keywords to boost exact lexical matches during hybrid lookup.
    #[serde(default)]
    keywords: Vec<String>,

    /// Maximum number of matching tools to return. Defaults to 5 and is capped at 20.
    #[serde(default)]
    limit: Option<usize>,

    /// Optional minimum score from 0.0 to 1.0. Leave unset for fallback results.
    #[serde(default)]
    min_score: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ToolInvokeParams {
    /// Tool identifier returned by `tool_search`.
    tool_id: String,

    /// JSON object matching the selected tool's parameter schema.
    #[serde(default)]
    arguments: Option<Value>,
}

#[derive(Debug)]
struct RankedTool {
    tool_id: String,
    description: Option<String>,
    parameters: Option<Value>,
    score: f64,
    matched_terms: Vec<String>,
    match_sources: Vec<MatchSource>,
}

#[derive(Debug, Serialize)]
struct ToolSearchResult {
    tool_id: String,
    description: Option<String>,
    parameters: Option<Value>,
    score: f64,
    matched_terms: Vec<String>,
    match_sources: Vec<MatchSource>,
}

impl From<RankedTool> for ToolSearchResult {
    fn from(ranked_tool: RankedTool) -> Self {
        Self {
            tool_id: ranked_tool.tool_id,
            description: ranked_tool.description,
            parameters: ranked_tool.parameters,
            score: (ranked_tool.score * 1000.0).round() / 1000.0,
            matched_terms: ranked_tool.matched_terms,
            match_sources: ranked_tool.match_sources,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct MatchSource {
    source: &'static str,
    rank: usize,
    score: f64,
}

#[derive(Debug)]
struct ToolDocument {
    tool_id: String,
    description: Option<String>,
    parameters: Option<Value>,
    name_text: String,
    description_text: String,
    parameter_text: String,
    name_tokens: Vec<String>,
    description_tokens: Vec<String>,
    parameter_tokens: Vec<String>,
    name_token_set: HashSet<String>,
    description_token_set: HashSet<String>,
    parameter_token_set: HashSet<String>,
    all_token_set: HashSet<String>,
    name_token_counts: HashMap<String, usize>,
    description_token_counts: HashMap<String, usize>,
    parameter_token_counts: HashMap<String, usize>,
    parameter_key_tokens: HashSet<String>,
}

impl ToolDocument {
    fn new(tool: &Arc<dyn SpiceModelTool>) -> Self {
        let tool_id = tool.name().to_string();
        let description = tool
            .description()
            .map(|description| description.to_string());
        let parameters = tool.parameters();
        let parameter_text = parameters
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let mut parameter_key_tokens = HashSet::new();
        if let Some(parameters) = parameters.as_ref() {
            collect_json_key_tokens(parameters, &mut parameter_key_tokens);
        }
        let name_tokens = tokenize_to_vec(&tool_id);
        let description_tokens = tokenize_to_vec(description.as_deref().unwrap_or_default());
        let parameter_tokens = tokenize_to_vec(&parameter_text);
        let name_token_set = token_set(&name_tokens);
        let description_token_set = token_set(&description_tokens);
        let parameter_token_set = token_set(&parameter_tokens);
        let all_token_set = name_tokens
            .iter()
            .chain(&description_tokens)
            .chain(&parameter_tokens)
            .cloned()
            .collect::<HashSet<_>>();
        let name_token_counts = token_counts(&name_tokens);
        let description_token_counts = token_counts(&description_tokens);
        let parameter_token_counts = token_counts(&parameter_tokens);

        Self {
            name_text: normalize_text(&tool_id),
            description_text: normalize_text(description.as_deref().unwrap_or_default()),
            parameter_text: normalize_text(&parameter_text),
            name_tokens,
            description_tokens,
            parameter_tokens,
            name_token_set,
            description_token_set,
            parameter_token_set,
            all_token_set,
            name_token_counts,
            description_token_counts,
            parameter_token_counts,
            parameter_key_tokens,
            tool_id,
            description,
            parameters,
        }
    }

    fn total_tokens(&self) -> usize {
        self.name_tokens.len() + self.description_tokens.len() + self.parameter_tokens.len()
    }

    fn vector_text(&self) -> String {
        format!(
            "tool_id: {}\ndescription: {}\nparameters: {}",
            self.tool_id, self.description_text, self.parameter_text
        )
    }
}

#[derive(Debug)]
struct ChannelMatch {
    document_index: usize,
    score: f64,
    matched_terms: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct FusedMatch {
    fused_score: f64,
    matched_terms: Vec<String>,
    match_sources: Vec<MatchSource>,
}

async fn hybrid_rank_tools(
    documents: &[ToolDocument],
    document_texts: &[String],
    params: &ToolSearchParams,
    embedding_model: &Arc<dyn Embed>,
    tool_embeddings: &OnceCell<Vec<Vec<f32>>>,
) -> Result<Vec<RankedTool>, Box<dyn std::error::Error + Send + Sync>> {
    let query_tokens = tokenize_to_vec(&params.query);
    let keyword_tokens = params
        .keywords
        .iter()
        .flat_map(|keyword| tokenize_to_vec(keyword))
        .collect::<Vec<_>>();
    let search_tokens = unique_tokens(query_tokens.iter().chain(&keyword_tokens).cloned());

    let mut channels = vec![
        (
            "full_text",
            full_text_channel_matches(documents, &search_tokens),
        ),
        (
            "keyword",
            keyword_channel_matches(documents, &params.query, &params.keywords, &search_tokens),
        ),
        ("schema", schema_channel_matches(documents, &search_tokens)),
    ];
    channels.push((
        "vector",
        vector_channel_matches(
            document_texts,
            &params.query,
            embedding_model,
            tool_embeddings,
        )
        .await?,
    ));
    let fused_matches = reciprocal_rank_fusion(channels);
    let max_score = fused_matches
        .values()
        .map(|fused_match| fused_match.fused_score)
        .fold(0.0, f64::max);

    Ok(documents
        .iter()
        .enumerate()
        .map(|(document_index, document)| {
            let mut fused_match = fused_matches
                .get(&document_index)
                .cloned()
                .unwrap_or_default();
            fused_match.matched_terms.sort();
            fused_match.matched_terms.dedup();
            fused_match
                .match_sources
                .sort_by_key(|source| (source.rank, source.source));

            RankedTool {
                tool_id: document.tool_id.clone(),
                description: document.description.clone(),
                parameters: document.parameters.clone(),
                score: if max_score > 0.0 {
                    fused_match.fused_score / max_score
                } else {
                    0.0
                },
                matched_terms: fused_match.matched_terms,
                match_sources: fused_match.match_sources,
            }
        })
        .collect())
}

async fn vector_channel_matches(
    document_texts: &[String],
    query: &str,
    embedding_model: &Arc<dyn Embed>,
    tool_embeddings: &OnceCell<Vec<Vec<f32>>>,
) -> Result<Vec<ChannelMatch>, llms::embeddings::Error> {
    if query.trim().is_empty() || document_texts.is_empty() {
        return Ok(Vec::new());
    }

    let query_embeddings = embedding_model
        .embed(EmbeddingInput::String(query.to_string()))
        .await?;
    let Some(query_embedding) = query_embeddings.first() else {
        return Ok(Vec::new());
    };

    let document_embeddings = tool_embeddings
        .get_or_try_init(|| {
            let embedding_model = Arc::clone(embedding_model);
            let document_texts = document_texts.to_vec();
            async move {
                embedding_model
                    .embed(EmbeddingInput::StringArray(document_texts))
                    .await
            }
        })
        .await?;

    Ok(document_embeddings
        .iter()
        .enumerate()
        .filter_map(|(document_index, document_embedding)| {
            non_zero_channel_match(
                document_index,
                cosine_similarity(query_embedding, document_embedding).max(0.0),
                Vec::new(),
            )
        })
        .collect())
}

fn full_text_channel_matches(
    documents: &[ToolDocument],
    query_tokens: &[String],
) -> Vec<ChannelMatch> {
    if query_tokens.is_empty() {
        return Vec::new();
    }

    let document_count = usize_to_f64(documents.len());
    let document_frequency = document_frequency(documents, query_tokens);
    documents
        .iter()
        .enumerate()
        .filter_map(|(document_index, document)| {
            let mut score = 0.0;
            let mut matched_terms = Vec::new();

            for query_token in query_tokens {
                let field_score =
                    (usize_to_f64(token_count(&document.name_token_counts, query_token)) * 3.0)
                        + (usize_to_f64(token_count(
                            &document.description_token_counts,
                            query_token,
                        )) * 2.0)
                        + usize_to_f64(token_count(&document.parameter_token_counts, query_token));
                if field_score > 0.0 {
                    let frequency = usize_to_f64(
                        document_frequency
                            .get(query_token)
                            .copied()
                            .unwrap_or_default(),
                    );
                    let inverse_document_frequency =
                        ((document_count + 1.0) / (frequency + 0.5)).ln().max(0.0) + 1.0;
                    score += inverse_document_frequency * field_score;
                    matched_terms.push(query_token.clone());
                }
            }

            let length_normalizer = usize_to_f64(document.total_tokens().max(1)).sqrt();
            non_zero_channel_match(document_index, score / length_normalizer, matched_terms)
        })
        .collect()
}

fn keyword_channel_matches(
    documents: &[ToolDocument],
    query: &str,
    keywords: &[String],
    query_tokens: &[String],
) -> Vec<ChannelMatch> {
    let phrases = if keywords.is_empty() {
        vec![query.to_string()]
    } else {
        keywords.to_vec()
    };

    documents
        .iter()
        .enumerate()
        .filter_map(|(document_index, document)| {
            let mut score = 0.0;
            let mut matched_terms = Vec::new();

            for phrase in &phrases {
                let normalized_phrase = normalize_text(phrase);
                if normalized_phrase.is_empty() {
                    continue;
                }

                if document.name_text == normalized_phrase {
                    score += 10.0;
                    matched_terms.push(normalized_phrase.clone());
                } else if document.name_text.contains(&normalized_phrase) {
                    score += 6.0;
                    matched_terms.push(normalized_phrase.clone());
                } else if document.description_text.contains(&normalized_phrase) {
                    score += 4.0;
                    matched_terms.push(normalized_phrase.clone());
                } else if document.parameter_text.contains(&normalized_phrase) {
                    score += 2.0;
                    matched_terms.push(normalized_phrase.clone());
                }
            }

            for query_token in query_tokens {
                let token_score = if document.name_token_set.contains(query_token) {
                    3.0
                } else if document.description_token_set.contains(query_token) {
                    2.0
                } else if document.parameter_token_set.contains(query_token) {
                    1.0
                } else {
                    0.0
                };
                if token_score > 0.0 {
                    score += token_score;
                    matched_terms.push(query_token.clone());
                }
            }

            non_zero_channel_match(document_index, score, matched_terms)
        })
        .collect()
}

fn schema_channel_matches(
    documents: &[ToolDocument],
    query_tokens: &[String],
) -> Vec<ChannelMatch> {
    if query_tokens.is_empty() {
        return Vec::new();
    }

    documents
        .iter()
        .enumerate()
        .filter_map(|(document_index, document)| {
            let mut score = 0.0;
            let mut matched_terms = Vec::new();
            for query_token in query_tokens {
                if document.parameter_key_tokens.contains(query_token) {
                    score += 4.0;
                    matched_terms.push(query_token.clone());
                } else if document.parameter_token_set.contains(query_token) {
                    score += 1.0;
                    matched_terms.push(query_token.clone());
                }
            }
            non_zero_channel_match(document_index, score, matched_terms)
        })
        .collect()
}

fn reciprocal_rank_fusion(
    channels: Vec<(&'static str, Vec<ChannelMatch>)>,
) -> HashMap<usize, FusedMatch> {
    let mut fused_matches = HashMap::new();
    let mut ranked_channels = Vec::with_capacity(channels.len());

    for (source, mut channel_matches) in channels {
        channel_matches.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.document_index.cmp(&right.document_index))
        });
        ranked_channels.push(
            channel_matches
                .iter()
                .map(|channel_match| channel_match.document_index)
                .collect::<Vec<_>>(),
        );

        for (rank_index, channel_match) in channel_matches.into_iter().enumerate() {
            let rank = rank_index + 1;
            let fused_match = fused_matches
                .entry(channel_match.document_index)
                .or_insert_with(FusedMatch::default);
            fused_match
                .matched_terms
                .extend(channel_match.matched_terms);
            fused_match.match_sources.push(MatchSource {
                source,
                rank,
                score: (channel_match.score * 1000.0).round() / 1000.0,
            });
        }
    }

    for (document_index, fused_score) in
        reciprocal_rank_fusion_scores(ranked_channels, DEFAULT_RRF_K)
    {
        fused_matches
            .entry(document_index)
            .or_insert_with(FusedMatch::default)
            .fused_score = fused_score;
    }

    fused_matches
}

fn non_zero_channel_match(
    document_index: usize,
    score: f64,
    matched_terms: Vec<String>,
) -> Option<ChannelMatch> {
    (score > 0.0).then_some(ChannelMatch {
        document_index,
        score,
        matched_terms,
    })
}

fn document_frequency(
    documents: &[ToolDocument],
    query_tokens: &[String],
) -> HashMap<String, usize> {
    let mut frequency = HashMap::new();
    for document in documents {
        for query_token in query_tokens {
            if document.all_token_set.contains(query_token) {
                frequency
                    .entry(query_token.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }
    }
    frequency
}

fn token_set(tokens: &[String]) -> HashSet<String> {
    tokens.iter().cloned().collect()
}

fn token_counts(tokens: &[String]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for token in tokens {
        counts
            .entry(token.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    counts
}

fn token_count(counts: &HashMap<String, usize>, token: &str) -> usize {
    counts.get(token).copied().unwrap_or_default()
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }

    let (dot_product, left_norm, right_norm) = left.iter().zip(right).fold(
        (0.0_f64, 0.0_f64, 0.0_f64),
        |(dot_product, left_norm, right_norm), (&left_value, &right_value)| {
            let left_value = f64::from(left_value);
            let right_value = f64::from(right_value);
            (
                dot_product + (left_value * right_value),
                left_norm + (left_value * left_value),
                right_norm + (right_value * right_value),
            )
        },
    );

    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot_product / (left_norm.sqrt() * right_norm.sqrt())
    }
}

fn normalize_text(text: impl AsRef<str>) -> String {
    text.as_ref()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn tokenize_to_vec(text: &str) -> Vec<String> {
    unique_tokens(
        normalize_text(text)
            .split_whitespace()
            .filter_map(normalize_search_token),
    )
}

fn unique_tokens(tokens: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for token in tokens {
        if seen.insert(token.clone()) {
            unique.push(token);
        }
    }
    unique
}

fn normalize_search_token(token: &str) -> Option<String> {
    if token.len() <= 1 || is_stop_word(token) {
        return None;
    }

    let token = if token.len() > 4 && token.ends_with("ies") {
        format!("{}y", token.trim_end_matches("ies"))
    } else if token.len() > 3
        && token.ends_with('s')
        && !token.ends_with("ss")
        && !token.ends_with("us")
    {
        token.trim_end_matches('s').to_string()
    } else {
        token.to_string()
    };

    (!token.is_empty()).then_some(token)
}

fn collect_json_key_tokens(value: &Value, tokens: &mut HashSet<String>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                tokens.extend(tokenize_to_vec(key));
                collect_json_key_tokens(value, tokens);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_json_key_tokens(value, tokens);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn is_stop_word(token: &str) -> bool {
    matches!(
        token,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "by"
            | "for"
            | "from"
            | "in"
            | "into"
            | "is"
            | "of"
            | "on"
            | "or"
            | "that"
            | "the"
            | "this"
            | "to"
            | "with"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use llms::embeddings::Error as EmbeddingError;

    use super::*;

    struct MockTool {
        name: &'static str,
        description: &'static str,
        parameters: Value,
        response: Value,
        received_args: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SpiceModelTool for MockTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.name)
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.description))
        }

        fn parameters(&self) -> Option<Value> {
            Some(self.parameters.clone())
        }

        async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            if let Ok(mut received_args) = self.received_args.lock() {
                received_args.push(arg.to_string());
            }
            Ok(self.response.clone())
        }
    }

    struct FailingTool {
        name: &'static str,
    }

    #[async_trait]
    impl SpiceModelTool for FailingTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.name)
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("Always fails"))
        }

        fn parameters(&self) -> Option<Value> {
            Some(json!({"type": "object"}))
        }

        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Err("tool failed".into())
        }
    }

    fn mock_tool(
        name: &'static str,
        description: &'static str,
        response: Value,
    ) -> (Arc<dyn SpiceModelTool>, Arc<Mutex<Vec<String>>>) {
        mock_tool_with_parameters(
            name,
            description,
            json!({"type": "object", "properties": {}}),
            response,
        )
    }

    fn mock_tool_with_parameters(
        name: &'static str,
        description: &'static str,
        parameters: Value,
        response: Value,
    ) -> (Arc<dyn SpiceModelTool>, Arc<Mutex<Vec<String>>>) {
        let received_args = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(MockTool {
                name,
                description,
                parameters,
                response,
                received_args: Arc::clone(&received_args),
            }),
            received_args,
        )
    }

    #[derive(Debug)]
    struct MockEmbed;

    #[async_trait]
    impl Embed for MockEmbed {
        async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            let texts = match input {
                EmbeddingInput::String(text) => vec![text],
                EmbeddingInput::StringArray(texts) => texts,
                EmbeddingInput::IntegerArray(_) | EmbeddingInput::ArrayOfIntegerArray(_) => {
                    Vec::new()
                }
            };

            Ok(texts
                .iter()
                .map(|text| {
                    let normalized = normalize_text(text);
                    if normalized.contains("forecast") || normalized.contains("weather") {
                        vec![1.0, 0.0]
                    } else if normalized.contains("sql") || normalized.contains("query") {
                        vec![0.0, 1.0]
                    } else {
                        vec![0.1, 0.1]
                    }
                })
                .collect())
        }

        fn size(&self) -> i32 {
            2
        }
    }

    fn mock_embed() -> Arc<dyn Embed> {
        Arc::new(MockEmbed)
    }

    #[test]
    fn embedding_selection_uses_explicit_configured_model() {
        let configured_models = vec!["first".to_string(), "second".to_string()];
        let selected =
            select_tool_registry_embedding_model_name(&configured_models, Some("second"))
                .expect("explicit configured embedding model should be selected");

        assert_eq!(selected, "second");
    }

    #[test]
    fn embedding_selection_rejects_explicit_missing_model() {
        let configured_models = vec!["configured".to_string()];
        let error = select_tool_registry_embedding_model_name(&configured_models, Some("missing"))
            .expect_err("missing explicit embedding model should fail");

        assert!(
            error
                .to_string()
                .contains("was not found in the `embeddings` section")
        );
    }

    #[test]
    fn embedding_selection_uses_single_configured_model_when_unset() {
        let configured_models = vec!["only_embedding".to_string()];
        let selected = select_tool_registry_embedding_model_name(&configured_models, None)
            .expect("single configured embedding model should be inferred");

        assert_eq!(selected, "only_embedding");
    }

    #[test]
    fn embedding_selection_requires_configuration_when_unset() {
        let configured_models = Vec::new();
        let error = select_tool_registry_embedding_model_name(&configured_models, None)
            .expect_err("missing embedding model should fail");

        assert!(
            error
                .to_string()
                .contains("No embedding model configured for searchable tool discovery")
        );
    }

    #[test]
    fn embedding_selection_requires_explicit_model_when_multiple_configured() {
        let configured_models = vec!["first".to_string(), "second".to_string()];
        let error = select_tool_registry_embedding_model_name(&configured_models, None)
            .expect_err("ambiguous embedding model should fail");

        assert!(error.to_string().contains(TOOL_EMBEDDING_MODEL_PARAM));
    }

    #[test]
    fn auto_search_threshold_only_triggers_for_large_tool_sets() {
        assert!(!should_auto_search(AUTO_SEARCH_TOOL_THRESHOLD));
        assert!(should_auto_search(AUTO_SEARCH_TOOL_THRESHOLD + 1));
    }

    #[test]
    fn specific_search_registry_requests_registry_wrapping() {
        let opts =
            SpiceToolsOptions::Specific(vec!["search_registry".to_string(), "my_tool".to_string()]);

        assert_eq!(
            tool_registry_preparation_mode(&opts, 1),
            Some(ToolRegistryPreparationMode::Required)
        );
    }

    #[test]
    fn specific_auto_uses_registry_threshold() {
        let opts = SpiceToolsOptions::Specific(vec!["auto".to_string(), "my_tool".to_string()]);

        assert_eq!(
            tool_registry_preparation_mode(&opts, AUTO_SEARCH_TOOL_THRESHOLD),
            None
        );
        assert_eq!(
            tool_registry_preparation_mode(&opts, AUTO_SEARCH_TOOL_THRESHOLD + 1),
            Some(ToolRegistryPreparationMode::Auto)
        );
    }

    #[test]
    fn specific_all_keeps_direct_tool_mode() {
        let opts = SpiceToolsOptions::Specific(vec!["all".to_string(), "my_tool".to_string()]);

        assert_eq!(tool_registry_preparation_mode(&opts, usize::MAX), None);
    }

    #[test]
    fn reserved_registry_tool_names_are_rejected() {
        let (reserved_tool, _) = mock_tool(
            TOOL_SEARCH_NAME,
            "User configured tool with a reserved registry name",
            json!(null),
        );
        let tools = vec![reserved_tool];

        let error = ensure_no_reserved_tool_registry_name_conflicts(&tools)
            .expect_err("reserved registry tool names should be rejected");

        assert!(error.to_string().contains("reserved"));
    }

    #[tokio::test]
    async fn search_ranks_relevant_tools_first() {
        let (sql_tool, _) = mock_tool(
            "sql",
            "Run SQL queries against datasets and return query results",
            json!(null),
        );
        let (readiness_tool, _) = mock_tool_with_parameters(
            "get_readiness",
            "Retrieve component readiness status",
            json!(null),
            json!(null),
        );
        let advertised_tools = tool_registry_tools(vec![readiness_tool, sql_tool], mock_embed());
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_SEARCH_NAME)
            .expect("tool_search should be advertised");

        let result = search_tool
            .call(r#"{"query":"run a SQL query","limit":2}"#)
            .await
            .expect("tool search should succeed");
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .expect("tool search response should contain tools array");
        let first_tool_id = tools
            .first()
            .and_then(|tool| tool.get("tool_id"))
            .and_then(Value::as_str)
            .expect("first search result should have a tool_id");

        assert_eq!(first_tool_id, "sql");
        assert_eq!(result.get("search_mode"), Some(&json!("hybrid_rrf")));
        let match_sources = tools
            .first()
            .and_then(|tool| tool.get("match_sources"))
            .and_then(Value::as_array)
            .expect("first search result should include match sources");
        assert!(
            match_sources.iter().any(|source| {
                source.get("source").and_then(Value::as_str) == Some("full_text")
            }),
            "hybrid search should include full-text matches"
        );
    }

    #[tokio::test]
    async fn search_uses_keyword_channel() {
        let (sql_tool, _) = mock_tool(
            "sql",
            "Run SQL queries against datasets and return query results",
            json!(null),
        );
        let (readiness_tool, _) = mock_tool(
            "get_readiness",
            "Retrieve component readiness status",
            json!(null),
        );
        let advertised_tools = tool_registry_tools(vec![sql_tool, readiness_tool], mock_embed());
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_SEARCH_NAME)
            .expect("tool_search should be advertised");

        let result = search_tool
            .call(r#"{"query":"component state","keywords":["get readiness"],"limit":2}"#)
            .await
            .expect("tool search should succeed");
        let first_tool = result
            .get("tools")
            .and_then(Value::as_array)
            .and_then(|tools| tools.first())
            .expect("tool search should return at least one result");

        assert_eq!(first_tool.get("tool_id"), Some(&json!("get_readiness")));
        assert!(
            first_tool
                .get("match_sources")
                .and_then(Value::as_array)
                .is_some_and(|sources| sources.iter().any(|source| {
                    source.get("source").and_then(Value::as_str) == Some("keyword")
                })),
            "hybrid search should include keyword matches"
        );
    }

    #[tokio::test]
    async fn search_uses_parameter_schema_channel() {
        let (weather_tool, _) = mock_tool_with_parameters(
            "weather",
            "Fetch conditions for a location",
            json!({
                "type": "object",
                "properties": {
                    "city": {"type": "string"}
                }
            }),
            json!(null),
        );
        let (calculator_tool, _) = mock_tool_with_parameters(
            "calculator",
            "Evaluate arithmetic expressions",
            json!({
                "type": "object",
                "properties": {
                    "expression": {"type": "string"}
                }
            }),
            json!(null),
        );
        let advertised_tools =
            tool_registry_tools(vec![calculator_tool, weather_tool], mock_embed());
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_SEARCH_NAME)
            .expect("tool_search should be advertised");

        let result = search_tool
            .call(r#"{"query":"tool with city argument","limit":2}"#)
            .await
            .expect("tool search should succeed");
        let first_tool = result
            .get("tools")
            .and_then(Value::as_array)
            .and_then(|tools| tools.first())
            .expect("tool search should return at least one result");

        assert_eq!(first_tool.get("tool_id"), Some(&json!("weather")));
        assert!(
            first_tool
                .get("match_sources")
                .and_then(Value::as_array)
                .is_some_and(|sources| sources.iter().any(|source| {
                    source.get("source").and_then(Value::as_str) == Some("schema")
                })),
            "hybrid search should include parameter-schema matches"
        );
    }

    #[tokio::test]
    async fn search_uses_vector_channel_when_embedding_model_is_available() {
        let (weather_tool, _) = mock_tool(
            "weather",
            "Get weather forecasts and current conditions",
            json!(null),
        );
        let (sql_tool, _) = mock_tool("sql", "Run SQL queries", json!(null));
        let advertised_tools = tool_registry_tools(vec![sql_tool, weather_tool], mock_embed());
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_SEARCH_NAME)
            .expect("tool_search should be advertised");

        let result = search_tool
            .call(r#"{"query":"weather outlook","limit":2}"#)
            .await
            .expect("tool search should succeed");
        let first_tool = result
            .get("tools")
            .and_then(Value::as_array)
            .and_then(|tools| tools.first())
            .expect("tool search should return at least one result");

        assert_eq!(first_tool.get("tool_id"), Some(&json!("weather")));
        assert!(
            first_tool
                .get("match_sources")
                .and_then(Value::as_array)
                .is_some_and(|sources| sources.iter().any(|source| {
                    source.get("source").and_then(Value::as_str) == Some("vector")
                })),
            "hybrid search should include vector matches"
        );
    }

    #[tokio::test]
    async fn invoke_calls_selected_tool_with_arguments() {
        let (sql_tool, received_args) = mock_tool("sql", "Run SQL queries", json!({"rows": 1}));
        let advertised_tools = tool_registry_tools(vec![sql_tool], mock_embed());
        let invoke_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_INVOKE_NAME)
            .expect("tool_invoke should be advertised");

        let result = invoke_tool
            .call(r#"{"tool_id":"sql","arguments":{"query":"select 1"}}"#)
            .await
            .expect("tool invoke should succeed");

        assert_eq!(
            result.get("tool_id"),
            Some(&Value::String("sql".to_string()))
        );
        assert_eq!(result.get("result"), Some(&json!({"rows": 1})));

        let received_args = received_args
            .lock()
            .expect("received args lock should not be poisoned");
        assert_eq!(received_args.as_slice(), [r#"{"query":"select 1"}"#]);
    }

    #[tokio::test]
    async fn invoke_returns_error_when_tool_id_is_missing() {
        let (sql_tool, _) = mock_tool("sql", "Run SQL queries", json!({"rows": 1}));
        let advertised_tools = tool_registry_tools(vec![sql_tool], mock_embed());
        let invoke_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_INVOKE_NAME)
            .expect("tool_invoke should be advertised");

        let error = invoke_tool
            .call(r#"{"tool_id":"missing","arguments":{}}"#)
            .await
            .expect_err("missing tool id should return an error");

        assert!(
            error
                .to_string()
                .contains("was not found in the searchable tool registry")
        );
    }

    #[tokio::test]
    async fn invoke_returns_error_when_selected_tool_fails() {
        let failing_tool = Arc::new(FailingTool { name: "failing" }) as Arc<dyn SpiceModelTool>;
        let advertised_tools = tool_registry_tools(vec![failing_tool], mock_embed());
        let invoke_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_INVOKE_NAME)
            .expect("tool_invoke should be advertised");

        let error = invoke_tool
            .call(r#"{"tool_id":"failing","arguments":{}}"#)
            .await
            .expect_err("failing tool should return an error");

        assert!(
            error
                .to_string()
                .contains("Failed to invoke tool 'failing' from searchable registry")
        );
    }

    #[tokio::test]
    async fn invoke_requires_exact_tool_id_match() {
        let (namespaced_tool, _) = mock_tool(
            "catalog/sql",
            "Run SQL queries through a catalog tool",
            json!({"namespaced": true}),
        );
        let (encoded_name_tool, _) = mock_tool(
            "catalog_sql",
            "A different tool whose raw name matches the encoded namespaced tool",
            json!({"encoded": true}),
        );
        let advertised_tools =
            tool_registry_tools(vec![namespaced_tool, encoded_name_tool], mock_embed());
        let invoke_tool = advertised_tools
            .iter()
            .find(|tool| tool.name() == TOOL_INVOKE_NAME)
            .expect("tool_invoke should be advertised");

        let result = invoke_tool
            .call(r#"{"tool_id":"catalog_sql","arguments":{}}"#)
            .await
            .expect("exact tool id should invoke matching raw tool name");

        assert_eq!(result.get("result"), Some(&json!({"encoded": true})));
    }

    #[test]
    fn registry_keeps_list_datasets_directly_advertised() {
        let (list_datasets_tool, _) = mock_tool(
            LIST_DATASETS_TOOL_NAME,
            "List all SQL tables available",
            json!([]),
        );
        let (sql_tool, _) = mock_tool("sql", "Run SQL queries", json!(null));

        let mut names = tool_registry_tools(vec![list_datasets_tool, sql_tool], mock_embed())
            .iter()
            .map(|tool| tool.name().to_string())
            .collect::<Vec<_>>();
        names.sort();

        assert_eq!(names, vec!["list_datasets", "tool_invoke", "tool_search"]);
    }
}
