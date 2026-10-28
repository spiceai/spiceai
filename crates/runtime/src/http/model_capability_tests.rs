/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Models loaded from a Spicepod against a mock provider, called through the runtime's
//! own routes: what each path accepts must be what the model's capabilities say.

use std::sync::{Arc, Mutex};

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use datafusion::datasource::MemTable;
use http_body_util::BodyExt;
use model_capabilities::{Capability, DecisionsBackend, MessagesBackend, ModelCapabilities};
use runtime_auth::{HttpAuth, api_key::ApiKeyAuth, layer::http::AuthLayer};
use runtime_search::search_engine::{SearchEngine, parse_explicit_primary_keys};
use serde_json::{Value, json};
use spicepod::component::embeddings::Embeddings;
use spicepod::component::model::Model;
use spicepod::component::runtime::{ApiKey, CorsConfig};
use spicepod::component::worker::Worker;
use tower::ServiceExt;

use crate::{Runtime, config::Config};

/// Every request the mock provider received: its path and the upstream model it named.
type Seen = Arc<Mutex<Vec<(String, String)>>>;

fn record(seen: &Seen, path: &str, body: &Value) -> String {
    let model = body["model"].as_str().unwrap_or_default().to_string();
    seen.lock()
        .expect("seen lock")
        .push((path.to_string(), model.clone()));
    model
}

/// The question the NSQL path asks, and the SQL the mock answers it with.
const NSQL_QUESTION: &str = "How many documents are there?";
const NSQL_SQL: &str = "SELECT 42 AS answer";

/// The answer schema of a decision asked through chat: in `response_format`, or in the
/// system prompt when the evaluator prompts for JSON instead.
fn decision_schema(body: &Value) -> Option<Value> {
    let schema = &body["response_format"]["json_schema"]["schema"];
    if schema["properties"]["answers"].is_object() {
        return Some(schema.clone());
    }
    body["messages"].as_array()?.iter().find_map(|message| {
        let (_, schema) = message["content"]
            .as_str()?
            .split_once("matches this schema exactly:")?;
        serde_json::from_str(schema.trim_start().lines().next()?).ok()
    })
}

/// What the mock replies to a chat request: SQL for NSQL, answers for a decision asked
/// through chat, scores for a rerank prompt, and `pong` for anything else.
fn chat_reply(body: &Value) -> String {
    let format = &body["response_format"];
    if format["json_schema"]["name"] == "sql_mode" {
        return json!({"sql": NSQL_SQL}).to_string();
    }
    if let Some(schema) = decision_schema(body)
        && let Some(questions) = schema["properties"]["answers"]["properties"].as_object()
    {
        let answers: serde_json::Map<String, Value> = questions
            .keys()
            .map(|id| (id.clone(), json!(0.9)))
            .collect();
        return json!({"answers": answers}).to_string();
    }
    let prompt = body["messages"].to_string();
    if prompt.contains(NSQL_QUESTION) {
        return if format["type"] == "json_object" {
            json!({"sql": NSQL_SQL}).to_string()
        } else {
            NSQL_SQL.to_string()
        };
    }
    if prompt.contains("relevance reranker") {
        return json!([{"id": 1, "score": 0.9}]).to_string();
    }
    "pong".to_string()
}

/// An `OpenAI` chat completion, streamed when the request asks.
async fn chat(State(seen): State<Seen>, Json(body): Json<Value>) -> Response {
    let model = record(&seen, "/v1/chat/completions", &body);
    let content = chat_reply(&body);
    if body["stream"] == json!(true) {
        let chunk = |delta: Value, finish: Value| json!({"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 0, "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
        let events = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            chunk(
                json!({"role": "assistant", "content": content}),
                Value::Null
            ),
            chunk(json!({}), json!("stop")),
        );
        return ([(header::CONTENT_TYPE, "text/event-stream")], events).into_response();
    }
    Json(json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "created": 0,
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
    }))
    .into_response()
}

/// An `OpenAI` Responses API response.
async fn responses(State(seen): State<Seen>, Json(body): Json<Value>) -> Json<Value> {
    let model = record(&seen, "/v1/responses", &body);
    Json(json!({
        "id": "resp_1",
        "object": "response",
        "created_at": 0,
        "model": model,
        "status": "completed",
        "output": [{"type": "message", "id": "msg_1", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": "pong", "annotations": []}]}],
        "parallel_tool_calls": true,
        "tool_choice": "auto",
        "tools": [],
        "usage": {"input_tokens": 1, "input_tokens_details": {"cached_tokens": 0}, "output_tokens": 1, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 2}
    }))
}

/// `OpenAI`'s Decisions API: every predicate holds with probability 0.9, and every choice
/// and score question is answered with its first option or level.
async fn decisions(State(seen): State<Seen>, Json(body): Json<Value>) -> Json<Value> {
    let model = record(&seen, "/v1/decisions", &body);
    let answers: Vec<Value> = body["questions"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|question| match question["type"].as_str() {
            Some("choice") => {
                let probabilities: Vec<Value> = question["choices"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .map(|(i, option)| json!({"value": option["value"], "probability": if i == 0 { 1.0 } else { 0.0 }}))
                    .collect();
                json!({"type": "choice", "name": question["name"], "choice": question["choices"][0]["value"], "probabilities": probabilities, "confidence": 1.0})
            }
            Some("score") => {
                let probabilities: Vec<Value> = question["levels"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .map(|(i, level)| json!({"value": i, "label": level["label"], "probability": if i == 0 { 1.0 } else { 0.0 }}))
                    .collect();
                json!({"type": "score", "name": question["name"], "score": 0.0, "probabilities": probabilities, "confidence": 1.0})
            }
            _ => json!({"type": "predicate", "name": question["name"], "probability": 0.9}),
        })
        .collect();
    Json(json!({
        "model": model,
        "answers": answers,
        "usage": {"input_tokens": 1, "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0}, "output_tokens": 0, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 1}
    }))
}

/// `OpenAI` embeddings: every input is embedded as `[0.25, 0.5, 0.75]`.
async fn embeddings(State(seen): State<Seen>, Json(body): Json<Value>) -> Json<Value> {
    let model = record(&seen, "/v1/embeddings", &body);
    let count = body["input"].as_array().map_or(1, Vec::len);
    let data: Vec<Value> = (0..count)
        .map(|index| json!({"object": "embedding", "index": index, "embedding": [0.25, 0.5, 0.75]}))
        .collect();
    Json(json!({
        "object": "list",
        "data": data,
        "model": model,
        "usage": {"prompt_tokens": 1, "total_tokens": 1}
    }))
}

/// `TypeSafe`'s System One API: every question is answered with 0.9.
async fn systemone(State(seen): State<Seen>, Json(body): Json<Value>) -> Json<Value> {
    let model = record(&seen, "/v1/systemone", &body);
    let answers: serde_json::Map<String, Value> = body["questions"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(id, _)| (id.clone(), json!({"type": "noul", "noul": 0.9})))
        .collect();
    Json(
        json!({"model": model, "answers": answers, "usage": {"input_tokens": 1, "output_tokens": 1}}),
    )
}

/// `TypeSafe`'s model list, which its health check reads.
async fn typesafe_models() -> Json<Value> {
    Json(json!({"models": [{"name": "jev-latest"}]}))
}

/// An `OpenAI` model, which a native decisions health check reads.
async fn openai_model(axum::extract::Path(id): axum::extract::Path<String>) -> Json<Value> {
    Json(json!({"id": id, "object": "model", "owned_by": "openai"}))
}

/// Starts the mock provider and returns its base URL.
async fn start_provider(seen: &Seen) -> String {
    let app = Router::new()
        .route("/v1/chat/completions", post(chat))
        .route("/v1/responses", post(responses))
        .route("/v1/decisions", post(decisions))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/systemone", post(systemone))
        .route("/v1/models", get(typesafe_models))
        .route("/v1/models/{id}", get(openai_model))
        .with_state(Arc::clone(seen));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the mock provider");
    let address = listener.local_addr().expect("mock provider address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{address}")
}

/// A Spicepod model `name` served by the mock provider at `endpoint`.
fn model(name: &str, from: &str, endpoint: &str) -> Model {
    let mut model = Model::new(from, name);
    let params: &[(&str, String)] = match from.split(':').next() {
        Some("openai") => &[
            ("endpoint", format!("{endpoint}/v1")),
            ("openai_api_key", "test-key".to_string()),
        ],
        Some("typesafe") => &[
            ("typesafe_endpoint", endpoint.to_string()),
            ("typesafe_api_key", "test-key".to_string()),
        ],
        Some("spiceai") => &[
            ("spiceai_endpoint", endpoint.to_string()),
            ("spiceai_api_key", "test-key".to_string()),
        ],
        other => panic!("no mock params for {other:?}"),
    };
    for (key, value) in params {
        model
            .params
            .insert((*key).to_string(), Value::String(value.clone()));
    }
    model
}

/// A worker that routes every request to `model`.
fn worker_for(model: &str) -> Worker {
    serde_json::from_value(json!({
        "name": format!("{model}_worker"),
        "load_balance": {"routing": [{"from": model}]}
    }))
    .expect("worker")
}

/// A runtime with `models` (and a worker for each) loaded, and its HTTP routes.
async fn runtime_with(models: Vec<Model>) -> (Arc<Runtime>, Router) {
    let mut app = AppBuilder::new("model_capabilities");
    for model in models {
        app = app.with_worker(worker_for(&model.name)).with_model(model);
    }
    let rt = Arc::new(Runtime::builder().with_app(app.build()).build().await);
    // Starting the runtime registers `list_udfs`, which the NSQL context lists functions from.
    rt.df.ctx.register_udtf(
        crate::udtfs::LIST_UDFS_UDTF_NAME,
        Arc::new(crate::udtfs::ListUDFTableFunc::new(Arc::clone(&rt.df.ctx))),
    );
    Arc::clone(&rt).load_models().await;
    Arc::clone(&rt).load_workers().await;

    let search = Arc::new(SearchEngine::new(
        Arc::clone(&rt.df) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
        parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
        crate::search::util::RuntimeTableProviderExplorer,
    ));
    let routes = super::routes::routes(
        &rt,
        Arc::new(Config::new()),
        search,
        None,
        &CorsConfig::default(),
        false,
        #[cfg(feature = "mcp")]
        None,
    );
    (rt, routes)
}

/// Sends `body` to `path` and returns the status and body text.
async fn send(routes: &Router, path: &str, body: Body, content_type: &str) -> (StatusCode, String) {
    let request = Request::post(path)
        .header(header::CONTENT_TYPE, content_type)
        .body(body)
        .expect("request");
    let response = routes.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn post_json(routes: &Router, path: &str, body: &Value) -> (StatusCode, String) {
    send(
        routes,
        path,
        Body::from(body.to_string()),
        "application/json",
    )
    .await
}

async fn post_sql(routes: &Router, sql: &str) -> (StatusCode, String) {
    send(routes, "/v1/sql", Body::from(sql.to_string()), "text/plain").await
}

// regression test for #14838: an `OpenAI` model with a native Decisions API lost chat.
#[tokio::test]
async fn luna_serves_chat_and_native_decisions() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let (_rt, routes) = runtime_with(vec![model(
        "luna",
        "openai:gpt-6-luna-2026-10-01",
        &endpoint,
    )])
    .await;
    seen.lock().expect("seen lock").clear();

    let (status, body) = post_json(
        &routes,
        "/v1/chat/completions",
        &json!({"model": "luna", "messages": [{"role": "user", "content": "ping"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body: Value = serde_json::from_str(&body).expect("chat completion");
    assert_eq!(body["choices"][0]["message"]["content"], "pong");

    let (status, body) = post_json(
        &routes,
        "/v1/decisions",
        &json!({"model": "luna", "input": "The screen arrived broken.", "questions": [{"type": "predicate", "name": "damaged", "instructions": "Is the item damaged?"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body: Value = serde_json::from_str(&body).expect("decision");
    assert_eq!(
        body["answers"],
        json!([{"type": "predicate", "name": "damaged", "probability": 0.9}])
    );

    assert_eq!(
        *seen.lock().expect("seen lock"),
        [
            ("/v1/chat/completions", "gpt-6-luna-2026-10-01"),
            ("/v1/decisions", "gpt-6-luna-2026-10-01"),
        ]
        .map(|(path, model)| (path.to_string(), model.to_string()))
    );
}

/// SQL model functions call a model, so a read-only API key cannot run them; a read-write
/// key can.
#[tokio::test(flavor = "multi_thread")]
async fn model_functions_need_a_read_write_api_key() {
    const READ_ONLY_KEY: &str = "read-only-test-key";
    const READ_WRITE_KEY: &str = "read-write-test-key";
    const REFUSAL: &str = "requires a read-write API key";

    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    let mut embedding = Embeddings::new("openai:text-embedding-3-small", "vectors");
    for (key, value) in [
        ("endpoint", format!("{endpoint}/v1")),
        ("openai_api_key", "test-key".to_string()),
    ] {
        embedding
            .params
            .insert(key.to_string(), Value::String(value));
    }
    let app = AppBuilder::new("model_function_access")
        .with_model(model("luna", "openai:gpt-6-luna-2026-10-01", &endpoint))
        .with_embedding(embedding)
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    Arc::clone(&rt).load_models().await;
    rt.load_embeddings().await;
    let search = Arc::new(SearchEngine::new(
        Arc::clone(&rt.df) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
        parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
        crate::search::util::RuntimeTableProviderExplorer,
    ));
    let auth = AuthLayer::new(Arc::new(ApiKeyAuth::new(vec![
        ApiKey::parse_str(&format!("{READ_ONLY_KEY}:ro")),
        ApiKey::parse_str(&format!("{READ_WRITE_KEY}:rw")),
    ])) as Arc<dyn HttpAuth + Send + Sync>);
    let routes = super::routes::routes(
        &rt,
        Arc::new(Config::new()),
        search,
        Some(auth),
        &CorsConfig::default(),
        false,
        #[cfg(feature = "mcp")]
        None,
    );
    let post_sql_as = |api_key: &'static str, sql: &'static str| {
        let routes = routes.clone();
        async move {
            let request = Request::post("/v1/sql")
                .header(header::CONTENT_TYPE, "text/plain")
                .header("x-api-key", api_key)
                .body(Body::from(sql))
                .expect("request");
            let response = routes.oneshot(request).await.expect("response");
            let status = response.status();
            let bytes = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            (status, String::from_utf8_lossy(&bytes).into_owned())
        }
    };

    // (SQL, the read-write answer)
    let calls = [
        ("SELECT ai('ping', 'luna') AS answer", json!("pong")),
        (
            "SELECT ai_if('The screen arrived broken.', 'Is the item damaged?', model => 'luna') AS answer",
            json!(true),
        ),
        (
            "SELECT ai_probability('The screen arrived broken.', 'Is the item damaged?', model => 'luna') AS answer",
            json!(0.9),
        ),
        (
            "SELECT ai_classify('The screen arrived broken.', ['damaged', 'fine'], model => 'luna') AS answer",
            json!("damaged"),
        ),
        (
            "SELECT ai_score('The screen arrived broken.', 'How damaged?', ['none', 'some', 'broken'], model => 'luna') AS answer",
            json!(0.0),
        ),
        (
            r#"SELECT ai_decide('The screen arrived broken.', '{"damaged": {"type": "noul", "instructions": "Is the item damaged?"}}', model => 'luna') AS answer"#,
            json!({"damaged": {"probability": 0.9}}),
        ),
        (
            "SELECT embed('ping', 'vectors') AS answer",
            json!([0.25, 0.5, 0.75]),
        ),
    ];
    for (sql, answer) in calls {
        let (status, body) = post_sql_as(READ_ONLY_KEY, sql).await;
        assert!(
            !status.is_success() && body.contains(REFUSAL),
            "{sql} with a read-only key: {status} {body}"
        );

        let (status, body) = post_sql_as(READ_WRITE_KEY, sql).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{sql} with a read-write key: {body}"
        );
        assert_eq!(
            serde_json::from_str::<Value>(&body).expect("query result"),
            json!([{"answer": answer}]),
            "{sql} with a read-write key"
        );
    }
}

/// A path that serves a model, and the capability it needs from it.
#[derive(Clone, Copy, Debug)]
enum Path {
    ChatCompletions,
    Nsql,
    Ai,
    Worker,
    Responses,
    Messages,
    Decisions,
    AiIf,
    Rerank,
}

impl Path {
    const ALL: [Path; 9] = [
        Path::ChatCompletions,
        Path::Nsql,
        Path::Ai,
        Path::Worker,
        Path::Responses,
        Path::Messages,
        Path::Decisions,
        Path::AiIf,
        Path::Rerank,
    ];

    /// Calls the path with the model named `name`, and returns whether it answered.
    async fn call(self, routes: &Router, name: &str) -> (bool, String) {
        let (status, body) = match self {
            Path::ChatCompletions => {
                post_json(
                    routes,
                    "/v1/chat/completions",
                    &json!({"model": name, "messages": [{"role": "user", "content": "ping"}]}),
                )
                .await
            }
            Path::Worker => {
                post_json(
                    routes,
                    "/v1/chat/completions",
                    &json!({"model": format!("{name}_worker"), "messages": [{"role": "user", "content": "ping"}]}),
                )
                .await
            }
            Path::Nsql => {
                post_json(
                    routes,
                    "/v1/nsql",
                    &json!({"query": NSQL_QUESTION, "model": name}),
                )
                .await
            }
            Path::Responses => {
                post_json(
                    routes,
                    "/v1/responses",
                    &json!({"model": name, "input": "ping"}),
                )
                .await
            }
            Path::Messages => {
                post_json(
                    routes,
                    "/v1/messages",
                    &json!({"model": name, "max_tokens": 16, "messages": [{"role": "user", "content": "ping"}]}),
                )
                .await
            }
            Path::Decisions => {
                post_json(
                    routes,
                    "/v1/decisions",
                    &json!({"model": name, "input": "The screen arrived broken.", "questions": [{"type": "predicate", "name": "damaged", "instructions": "Is the item damaged?"}]}),
                )
                .await
            }
            Path::Ai => post_sql(routes, &format!("SELECT ai('ping', '{name}')")).await,
            Path::AiIf => {
                post_sql(
                    routes,
                    &format!(
                        "SELECT ai_if('The screen arrived broken.', 'Is the item damaged?', model => '{name}')"
                    ),
                )
                .await
            }
            Path::Rerank => {
                post_sql(
                    routes,
                    &format!(
                        "SELECT id FROM rerank(docs, document => 'content', query => 'battery', model => '{name}')"
                    ),
                )
                .await
            }
        };
        (status.is_success(), body)
    }

    /// Text that the answer of a call that succeeded contains.
    fn answer(self) -> &'static str {
        match self {
            Path::ChatCompletions | Path::Worker | Path::Ai | Path::Responses | Path::Messages => {
                "pong"
            }
            Path::Nsql => "42",
            Path::Decisions => r#""probability":0.9"#,
            Path::AiIf => "true",
            Path::Rerank => r#"{"id":1}"#,
        }
    }

    /// The provider paths a call reaches for a model with `capabilities`; none when the
    /// model lacks what the path needs.
    fn reaches(self, capabilities: &ModelCapabilities, native: &'static str) -> Vec<&'static str> {
        let chat = "/v1/chat/completions";
        let decisions = match capabilities.decisions() {
            Some(DecisionsBackend::Native) => vec![native],
            Some(DecisionsBackend::ViaChat) => vec![chat],
            None => vec![],
        };
        match self {
            Path::ChatCompletions | Path::Nsql | Path::Ai | Path::Worker => {
                if capabilities.has(Capability::Chat) {
                    vec![chat]
                } else {
                    vec![]
                }
            }
            Path::Responses => {
                if capabilities.has(Capability::Responses) {
                    vec!["/v1/responses"]
                } else {
                    vec![]
                }
            }
            Path::Messages => match capabilities.messages() {
                Some(MessagesBackend::Native) => vec!["/v1/messages"],
                Some(MessagesBackend::ViaChat) => vec![chat],
                None => vec![],
            },
            Path::Decisions | Path::AiIf => decisions,
            // A model with chat reranks through it, even one with native decisions.
            Path::Rerank => {
                if capabilities.has(Capability::Chat) {
                    vec![chat]
                } else {
                    decisions
                }
            }
        }
    }
}

/// For a decision-only, a chat-and-decisions, a chat-only, and an unlisted model, every
/// path answers exactly the models whose capabilities include what it needs, reaches the
/// provider the way those capabilities say, and refuses the rest without calling it.
#[tokio::test]
async fn every_path_accepts_what_the_capabilities_say() {
    let seen = Seen::default();
    let endpoint = start_provider(&seen).await;
    // (name, from, upstream model id, native decisions path)
    let models = [
        ("jev", "typesafe:jev", "jev-latest", "/v1/systemone"),
        (
            "luna",
            "openai:gpt-6-luna-2026-10-01",
            "gpt-6-luna-2026-10-01",
            "/v1/decisions",
        ),
        ("chat_only", "spiceai:chat-only", "chat-only", ""),
        ("unlisted", "openai:gpt-7", "gpt-7", ""),
    ];
    let (rt, routes) = runtime_with(
        models
            .iter()
            .map(|(name, from, _, _)| model(name, from, &endpoint))
            .collect(),
    )
    .await;
    let docs = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["The battery lasts a day."])),
        ],
    )
    .expect("docs batch");
    rt.df
        .ctx
        .register_table(
            "docs",
            Arc::new(MemTable::try_new(docs.schema(), vec![vec![docs]]).expect("docs table")),
        )
        .expect("register docs");

    let registered = rt.model_capabilities().read().await.clone();
    let summary = |name: &str| {
        let found = &registered[name];
        (
            found.has(Capability::Chat),
            found.has(Capability::Responses),
            found.decisions(),
        )
    };
    assert_eq!(
        models.map(|(name, ..)| (name, summary(name))),
        [
            ("jev", (false, false, Some(DecisionsBackend::Native))),
            ("luna", (true, true, Some(DecisionsBackend::Native))),
            ("chat_only", (true, false, Some(DecisionsBackend::ViaChat))),
            ("unlisted", (true, true, Some(DecisionsBackend::ViaChat))),
        ],
        "Jev answers decisions only; Luna adds native decisions to OpenAI's chat and Responses; an unlisted id gets its provider's default"
    );

    let mut mismatches = Vec::new();
    for (name, _, upstream, native) in models {
        for path in Path::ALL {
            seen.lock().expect("seen lock").clear();
            let (answered, body) = path.call(&routes, name).await;
            let reached: Vec<String> = seen
                .lock()
                .expect("seen lock")
                .iter()
                .filter(|(_, model)| model == upstream)
                .map(|(provider_path, _)| provider_path.clone())
                .collect();
            let mut reached_paths = reached.clone();
            reached_paths.dedup();
            let expected = path.reaches(&registered[name], native);
            // An accepted call answers with the provider's reply; a refused call does neither.
            let accepted = !expected.is_empty();
            let answered_as_expected = if accepted {
                answered && body.contains(path.answer())
            } else {
                !answered
            };
            if reached_paths != expected || !answered_as_expected {
                mismatches.push(format!(
                    "{name} via {path:?}: expected the provider to see {expected:?}, it saw {reached:?}; answered={answered}: {body}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
