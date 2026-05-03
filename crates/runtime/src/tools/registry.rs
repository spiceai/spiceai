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
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tools::SpiceModelTool;

use crate::tools::utils::parameters;

const TOOL_SEARCH_NAME: &str = "tool_search";
const TOOL_INVOKE_NAME: &str = "tool_invoke";
const LIST_DATASETS_TOOL_NAME: &str = "list_datasets";
const DEFAULT_SEARCH_LIMIT: usize = 5;
const MAX_SEARCH_LIMIT: usize = 20;
const RRF_K: f64 = 60.0;

#[must_use]
pub(crate) fn tool_registry_tools(
    tools: Vec<Arc<dyn SpiceModelTool>>,
) -> Vec<Arc<dyn SpiceModelTool>> {
    if tools.is_empty() {
        return Vec::new();
    }

    let direct_tools = tools
        .iter()
        .filter(|tool| tool.name().as_ref() == LIST_DATASETS_TOOL_NAME)
        .cloned()
        .collect::<Vec<_>>();
    let registry = Arc::new(tools);
    let mut advertised_tools = vec![
        Arc::new(ToolRegistrySearchTool::new(Arc::clone(&registry))) as Arc<dyn SpiceModelTool>,
        Arc::new(ToolRegistryInvokeTool::new(registry)) as Arc<dyn SpiceModelTool>,
    ];
    advertised_tools.extend(direct_tools);
    advertised_tools
}

struct ToolRegistrySearchTool {
    tools: Arc<Vec<Arc<dyn SpiceModelTool>>>,
}

impl ToolRegistrySearchTool {
    fn new(tools: Arc<Vec<Arc<dyn SpiceModelTool>>>) -> Self {
        Self { tools }
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
        let params: ToolSearchParams = serde_json::from_str(arg)?;
        let limit = params
            .limit
            .unwrap_or(DEFAULT_SEARCH_LIMIT)
            .clamp(1, MAX_SEARCH_LIMIT);
        let min_score = params.min_score.unwrap_or(0.0).clamp(0.0, 1.0);

        let mut ranked_tools = hybrid_rank_tools(&self.tools, &params);
        ranked_tools.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.tool_id.cmp(&right.tool_id))
        });

        let max_score = ranked_tools
            .first()
            .map(|ranked_tool| ranked_tool.score)
            .unwrap_or(0.0);
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
            .find(|tool| tool_id_matches(tool.as_ref(), tool_id))
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
        let params: ToolInvokeParams = serde_json::from_str(arg)?;
        let Some(tool) = self.find_tool(&params.tool_id) else {
            return Ok(json!({
                "tool_id": params.tool_id,
                "error": "Tool not found in registry",
                "available_tools": self.tools.iter().map(|tool| tool.name().to_string()).take(MAX_SEARCH_LIMIT).collect::<Vec<_>>(),
            }));
        };

        let tool_id = tool.name().to_string();
        let arguments = match params.arguments {
            Some(Value::String(arguments)) => arguments,
            Some(Value::Null) | None => "{}".to_string(),
            Some(arguments) => serde_json::to_string(&arguments)?,
        };

        match tool.call(&arguments).await {
            Ok(result) => Ok(json!({
                "tool_id": tool_id,
                "result": result,
            })),
            Err(error) => Ok(json!({
                "tool_id": tool_id,
                "error": error.to_string(),
            })),
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
    /// Tool identifier returned by tool_search.
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

        Self {
            name_text: normalize_text(&tool_id),
            description_text: normalize_text(description.as_deref().unwrap_or_default()),
            parameter_text: normalize_text(&parameter_text),
            name_tokens: tokenize_to_vec(&tool_id),
            description_tokens: tokenize_to_vec(description.as_deref().unwrap_or_default()),
            parameter_tokens: tokenize_to_vec(&parameter_text),
            parameter_key_tokens,
            tool_id,
            description,
            parameters,
        }
    }

    fn all_tokens(&self) -> HashSet<&str> {
        self.name_tokens
            .iter()
            .chain(&self.description_tokens)
            .chain(&self.parameter_tokens)
            .map(String::as_str)
            .collect()
    }

    fn total_tokens(&self) -> usize {
        self.name_tokens.len() + self.description_tokens.len() + self.parameter_tokens.len()
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

fn hybrid_rank_tools(
    tools: &[Arc<dyn SpiceModelTool>],
    params: &ToolSearchParams,
) -> Vec<RankedTool> {
    let documents = tools.iter().map(ToolDocument::new).collect::<Vec<_>>();
    let query_tokens = tokenize_to_vec(&params.query);
    let keyword_tokens = params
        .keywords
        .iter()
        .flat_map(|keyword| tokenize_to_vec(keyword))
        .collect::<Vec<_>>();
    let search_tokens = unique_tokens(query_tokens.iter().chain(&keyword_tokens).cloned());

    let channels = vec![
        (
            "full_text",
            full_text_channel_matches(&documents, &search_tokens),
        ),
        (
            "keyword",
            keyword_channel_matches(&documents, &params.query, &params.keywords, &search_tokens),
        ),
        ("schema", schema_channel_matches(&documents, &search_tokens)),
    ];
    let fused_matches = reciprocal_rank_fusion(channels);
    let max_score = fused_matches
        .values()
        .map(|fused_match| fused_match.fused_score)
        .fold(0.0, f64::max);

    documents
        .into_iter()
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
                tool_id: document.tool_id,
                description: document.description,
                parameters: document.parameters,
                score: if max_score > 0.0 {
                    fused_match.fused_score / max_score
                } else {
                    0.0
                },
                matched_terms: fused_match.matched_terms,
                match_sources: fused_match.match_sources,
            }
        })
        .collect()
}

fn full_text_channel_matches(
    documents: &[ToolDocument],
    query_tokens: &[String],
) -> Vec<ChannelMatch> {
    if query_tokens.is_empty() {
        return Vec::new();
    }

    let document_count = documents.len() as f64;
    let document_frequency = document_frequency(documents, query_tokens);
    documents
        .iter()
        .enumerate()
        .filter_map(|(document_index, document)| {
            let name_counts = token_counts(&document.name_tokens);
            let description_counts = token_counts(&document.description_tokens);
            let parameter_counts = token_counts(&document.parameter_tokens);
            let mut score = 0.0;
            let mut matched_terms = Vec::new();

            for query_token in query_tokens {
                let field_score = (token_count(&name_counts, query_token) as f64 * 3.0)
                    + (token_count(&description_counts, query_token) as f64 * 2.0)
                    + token_count(&parameter_counts, query_token) as f64;
                if field_score > 0.0 {
                    let frequency = *document_frequency.get(query_token).unwrap_or(&0) as f64;
                    let inverse_document_frequency =
                        ((document_count + 1.0) / (frequency + 0.5)).ln().max(0.0) + 1.0;
                    score += inverse_document_frequency * field_score;
                    matched_terms.push(query_token.clone());
                }
            }

            let length_normalizer = (document.total_tokens().max(1) as f64).sqrt();
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
            let name_set = document.name_tokens.iter().collect::<HashSet<_>>();
            let description_set = document.description_tokens.iter().collect::<HashSet<_>>();
            let parameter_set = document.parameter_tokens.iter().collect::<HashSet<_>>();
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
                let token_score = if name_set.contains(query_token) {
                    3.0
                } else if description_set.contains(query_token) {
                    2.0
                } else if parameter_set.contains(query_token) {
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
            let parameter_set = document.parameter_tokens.iter().collect::<HashSet<_>>();
            let mut score = 0.0;
            let mut matched_terms = Vec::new();
            for query_token in query_tokens {
                if document.parameter_key_tokens.contains(query_token) {
                    score += 4.0;
                    matched_terms.push(query_token.clone());
                } else if parameter_set.contains(query_token) {
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

    for (source, mut channel_matches) in channels {
        channel_matches.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.document_index.cmp(&right.document_index))
        });

        for (rank_index, channel_match) in channel_matches.into_iter().enumerate() {
            let rank = rank_index + 1;
            let fused_match = fused_matches
                .entry(channel_match.document_index)
                .or_insert_with(FusedMatch::default);
            fused_match.fused_score += 1.0 / (rank as f64 + RRF_K);
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
        let document_tokens = document.all_tokens();
        for query_token in query_tokens {
            if document_tokens.contains(query_token.as_str()) {
                frequency
                    .entry(query_token.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }
    }
    frequency
}

fn token_counts<'a>(tokens: &'a [String]) -> HashMap<&'a str, usize> {
    let mut counts = HashMap::new();
    for token in tokens {
        counts
            .entry(token.as_str())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    counts
}

fn token_count(counts: &HashMap<&str, usize>, token: &str) -> usize {
    counts.get(token).copied().unwrap_or_default()
}

fn tool_id_matches(tool: &dyn SpiceModelTool, requested_tool_id: &str) -> bool {
    let tool_name = tool.name();
    tool_name.as_ref() == requested_tool_id
        || encode_tool_name(tool_name.as_ref()) == requested_tool_id
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

fn encode_tool_name(name: &str) -> String {
    if name.contains('/') {
        name.replace('_', "__").replace('/', "_")
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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

    fn mock_tool(
        name: &'static str,
        description: &'static str,
        response: Value,
    ) -> (Arc<dyn SpiceModelTool>, Arc<Mutex<Vec<String>>>) {
        mock_tool_with_parameters(
            name,
            description,
            json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"}
                }
            }),
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

    #[tokio::test]
    async fn search_ranks_relevant_tools_first() {
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
        let advertised_tools = tool_registry_tools(vec![readiness_tool, sql_tool]);
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name().as_ref() == TOOL_SEARCH_NAME)
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
        let advertised_tools = tool_registry_tools(vec![sql_tool, readiness_tool]);
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name().as_ref() == TOOL_SEARCH_NAME)
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
        let advertised_tools = tool_registry_tools(vec![calculator_tool, weather_tool]);
        let search_tool = advertised_tools
            .iter()
            .find(|tool| tool.name().as_ref() == TOOL_SEARCH_NAME)
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
    async fn invoke_calls_selected_tool_with_arguments() {
        let (sql_tool, received_args) = mock_tool("sql", "Run SQL queries", json!({"rows": 1}));
        let advertised_tools = tool_registry_tools(vec![sql_tool]);
        let invoke_tool = advertised_tools
            .iter()
            .find(|tool| tool.name().as_ref() == TOOL_INVOKE_NAME)
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

    #[test]
    fn registry_keeps_list_datasets_directly_advertised() {
        let (list_datasets_tool, _) = mock_tool(
            LIST_DATASETS_TOOL_NAME,
            "List all SQL tables available",
            json!([]),
        );
        let (sql_tool, _) = mock_tool("sql", "Run SQL queries", json!(null));

        let mut names = tool_registry_tools(vec![list_datasets_tool, sql_tool])
            .iter()
            .map(|tool| tool.name().to_string())
            .collect::<Vec<_>>();
        names.sort();

        assert_eq!(names, vec!["list_datasets", "tool_invoke", "tool_search"]);
    }
}
