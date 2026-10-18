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

use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::*;
use crate::model::messages_tool_history::replay_spice_tool_calls;

/// A `sql` Spice tool that answers every query with one row, and counts its calls.
struct Sql(AtomicUsize);

#[async_trait]
impl SpiceModelTool for Sql {
    fn name(&self) -> Cow<'_, str> {
        "sql".into()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some("Runs SQL.".into())
    }

    fn parameters(&self) -> Option<Value> {
        Some(json!({"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}))
    }

    async fn call(&self, _arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!([{"count": 3}]))
    }
}

/// A model whose answer in round `n` is `rounds[n]`, a Message, sent whole or as its events. It
/// records every request.
struct Scripted {
    rounds: Vec<Value>,
    requests: Mutex<Vec<Map<String, Value>>>,
    /// A delta type to stream in the first block of each round, which no client can complete
    /// a block from.
    foreign_delta: Option<&'static str>,
}

impl Scripted {
    fn new(rounds: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            rounds,
            requests: Mutex::new(Vec::new()),
            foreign_delta: None,
        })
    }

    fn next(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        let mut requests = self.requests.lock();
        let round = requests.len();
        requests.push(request.body);
        self.rounds
            .get(round)
            .cloned()
            .ok_or_else(|| MessagesError::api(format!("no round {round} scripted")))
    }

    fn requests(&self) -> Vec<Map<String, Value>> {
        self.requests.lock().clone()
    }
}

/// `message` as the events Anthropic streams it as: each block's text and input in two deltas.
fn events_of(message: &Value, foreign_delta: Option<&str>) -> Vec<Value> {
    let mut start = message.clone();
    start["content"] = json!([]);
    start["stop_reason"] = Value::Null;
    let usage = message["usage"].clone();
    start["usage"] = json!({"input_tokens": usage["input_tokens"], "output_tokens": 1});
    let mut events = vec![json!({"type": "message_start", "message": start})];
    for (index, block) in message["content"].as_array().expect("content").iter().enumerate() {
        let halves = |text: &str| {
            let middle = text.char_indices().nth(text.chars().count() / 2).map_or(0, |(i, _)| i);
            [text[..middle].to_string(), text[middle..].to_string()]
        };
        let (skeleton, deltas) = match block["type"].as_str().expect("a typed block") {
            "text" => (
                json!({"type": "text", "text": ""}),
                halves(block["text"].as_str().expect("text"))
                    .map(|text| json!({"type": "text_delta", "text": text}))
                    .to_vec(),
            ),
            "thinking" => (
                json!({"type": "thinking", "thinking": ""}),
                vec![
                    json!({"type": "thinking_delta", "thinking": block["thinking"]}),
                    json!({"type": "signature_delta", "signature": block["signature"]}),
                ],
            ),
            "tool_use" => (
                json!({"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}}),
                halves(&block["input"].to_string())
                    .map(|json| json!({"type": "input_json_delta", "partial_json": json}))
                    .to_vec(),
            ),
            other => panic!("no events for {other}"),
        };
        events.push(json!({"type": "content_block_start", "index": index, "content_block": skeleton}));
        if index == 0 && let Some(delta_type) = foreign_delta {
            events.push(json!({"type": "content_block_delta", "index": index, "delta": {"type": delta_type}}));
        }
        for delta in deltas {
            events.push(json!({"type": "content_block_delta", "index": index, "delta": delta}));
        }
        events.push(json!({"type": "content_block_stop", "index": index}));
    }
    events.push(json!({
        "type": "message_delta",
        "delta": {"stop_reason": message["stop_reason"], "stop_sequence": null},
        "usage": {"output_tokens": usage["output_tokens"]},
    }));
    events.push(json!({"type": "message_stop"}));
    events
}

#[async_trait]
impl Messages for Scripted {
    async fn create(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        self.next(request)
    }

    async fn create_stream(&self, request: MessagesRequest) -> Result<MessageStream, MessagesError> {
        let message = self.next(request)?;
        Ok(Box::pin(futures::stream::iter(
            events_of(&message, self.foreign_delta).into_iter().map(Ok),
        )))
    }

    async fn count_tokens(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        Ok(json!({"input_tokens": request.body.get("tools").and_then(Value::as_array).map_or(0, Vec::len)}))
    }
}

fn round(id: &str, content: &Value, stop_reason: &str, input: u64, output: u64) -> Value {
    json!({
        "id": id, "type": "message", "role": "assistant", "model": "claude",
        "content": content, "stop_reason": stop_reason, "stop_sequence": null,
        "usage": {"input_tokens": input, "output_tokens": output},
    })
}

fn sql_call(id: &str) -> Value {
    json!({"type": "tool_use", "id": id, "name": "sql", "input": {"query": "SELECT count(*) FROM trips"}})
}

fn request() -> MessagesRequest {
    MessagesRequest::new(
        json!({
            "model": "claude",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "How many trips?"}],
        })
        .as_object()
        .cloned()
        .expect("an object"),
    )
}

fn using(scripted: &Arc<Scripted>, sql: &Arc<Sql>, recursion_limit: usize) -> ToolUsingMessages {
    ToolUsingMessages::new(
        Arc::clone(scripted) as Arc<dyn Messages>,
        vec![Arc::clone(sql) as Arc<dyn SpiceModelTool>],
        recursion_limit,
    )
}

/// The Message a client builds from `events`, as the Anthropic SDKs accumulate a stream.
fn accumulate(events: &[Value]) -> Value {
    let mut message = Value::Null;
    let mut partial: BTreeMap<usize, String> = BTreeMap::new();
    for event in events {
        let index = event["index"].as_u64().and_then(|i| usize::try_from(i).ok());
        match event["type"].as_str().expect("a typed event") {
            "message_start" => message = event["message"].clone(),
            "content_block_start" => {
                let content = message["content"].as_array_mut().expect("content");
                assert_eq!(Some(content.len()), index, "blocks start in order");
                content.push(event["content_block"].clone());
            }
            "content_block_delta" => {
                let index = index.expect("an index");
                let block = &mut message["content"][index];
                let delta = &event["delta"];
                let field = match delta["type"].as_str().expect("a typed delta") {
                    "text_delta" => "text",
                    "thinking_delta" => "thinking",
                    "signature_delta" => {
                        block["signature"] = delta["signature"].clone();
                        continue;
                    }
                    "input_json_delta" => {
                        partial.entry(index).or_default().push_str(delta["partial_json"].as_str().expect("json"));
                        continue;
                    }
                    other => panic!("unexpected delta {other}"),
                };
                let text = format!("{}{}", block[field].as_str().unwrap_or_default(), delta[field].as_str().unwrap_or_default());
                block[field] = Value::from(text);
            }
            "content_block_stop" => {
                let index = index.expect("an index");
                if let Some(json) = partial.remove(&index) {
                    message["content"][index]["input"] = serde_json::from_str(&json).expect("whole input");
                }
            }
            "message_delta" => {
                message["stop_reason"] = event["delta"]["stop_reason"].clone();
                message["stop_sequence"] = event["delta"]["stop_sequence"].clone();
                message["usage"] = event["usage"].clone();
            }
            "message_stop" => {}
            other => panic!("unexpected event {other}"),
        }
    }
    message
}

async fn streamed(messages: &ToolUsingMessages, request: MessagesRequest) -> Vec<Value> {
    messages
        .create_stream(request)
        .await
        .expect("a stream")
        .map(|event| event.unwrap_or_else(|error| error.to_envelope()))
        .collect()
        .await
}

fn two_rounds() -> Vec<Value> {
    vec![
        round(
            "msg_1",
            &json!([
                {"type": "thinking", "thinking": "Count them.", "signature": "sig1"},
                {"type": "text", "text": "Querying."},
                sql_call("toolu_1"),
            ]),
            "tool_use",
            100,
            20,
        ),
        round("msg_2", &json!([{"type": "text", "text": "There are 3 trips."}]), "end_turn", 150, 10),
    ]
}

#[tokio::test]
async fn a_loop_over_spice_tools_answers_with_one_message() {
    let scripted = Scripted::new(two_rounds());
    let sql = Arc::new(Sql(AtomicUsize::new(0)));
    let message = using(&scripted, &sql, 10)
        .create(request())
        .await
        .expect("a message");

    let outcome = ToolOutcome::Completed(json!([{"count": 3}]));
    assert_eq!(
        message,
        json!({
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude",
            "content": [
                {"type": "thinking", "thinking": "Count them.", "signature": "sig1"},
                {"type": "text", "text": "Querying."},
                mcp_tool_use(&sql_call("toolu_1")),
                mcp_tool_result("toolu_1", &outcome),
                {"type": "text", "text": "There are 3 trips."},
            ],
            "stop_reason": "end_turn", "stop_sequence": null,
            "usage": {"input_tokens": 250, "output_tokens": 30},
        })
    );
    assert_eq!(sql.0.load(Ordering::SeqCst), 1);

    let requests = scripted.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["tools"],
        json!([{
            "name": "sql",
            "description": "Runs SQL.",
            "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"], "additionalProperties": false},
        }])
    );
    assert_eq!(
        requests[1]["messages"],
        json!([
            {"role": "user", "content": "How many trips?"},
            {"role": "assistant", "content": two_rounds()[0]["content"]},
            {"role": "user", "content": [tool_result("toolu_1", &outcome)]},
        ])
    );

    // The next turn replays the loop exactly as the model saw it, so the provider's prompt
    // cache serves it up to the end of this one.
    let mut next_turn = request().body;
    next_turn["messages"] = json!([
        {"role": "user", "content": "How many trips?"},
        {"role": "assistant", "content": message["content"]},
        {"role": "user", "content": "And yesterday?"},
    ]);
    replay_spice_tool_calls(&mut next_turn);
    let mut expected = requests[1]["messages"].as_array().expect("messages").clone();
    expected.push(json!({"role": "assistant", "content": [{"type": "text", "text": "There are 3 trips."}]}));
    expected.push(json!({"role": "user", "content": "And yesterday?"}));
    assert_eq!(next_turn["messages"], Value::Array(expected));
}

#[tokio::test]
async fn a_streamed_loop_carries_the_same_message() {
    let unstreamed = using(&Scripted::new(two_rounds()), &Arc::new(Sql(AtomicUsize::new(0))), 10)
        .create(request())
        .await
        .expect("a message");

    let scripted = Scripted::new(two_rounds());
    let mut streamed_request = request();
    streamed_request.body.insert("stream".to_string(), Value::Bool(true));
    let events = streamed(&using(&scripted, &Arc::new(Sql(AtomicUsize::new(0))), 10), streamed_request).await;

    assert_eq!(accumulate(&events), unstreamed);
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "message_start" || event["type"] == "message_stop")
            .count(),
        2,
        "one message_start and one message_stop for the whole loop"
    );
    // The second round is sent what the first streamed, completed from its deltas.
    assert_eq!(
        scripted.requests()[1]["messages"][1],
        json!({"role": "assistant", "content": two_rounds()[0]["content"]})
    );
}

#[tokio::test]
async fn a_call_to_a_client_tool_ends_the_message_after_spice_runs_its_own() {
    let mixed = vec![round(
        "msg_1",
        &json!([
            sql_call("toolu_s"),
            {"type": "tool_use", "id": "toolu_c", "name": "read_file", "input": {"path": "a"}},
        ]),
        "tool_use",
        10,
        5,
    )];
    let outcome = ToolOutcome::Completed(json!([{"count": 3}]));
    let expected_content = json!([
        mcp_tool_use(&sql_call("toolu_s")),
        {"type": "tool_use", "id": "toolu_c", "name": "read_file", "input": {"path": "a"}},
        mcp_tool_result("toolu_s", &outcome),
    ]);

    let scripted = Scripted::new(mixed.clone());
    let message = using(&scripted, &Arc::new(Sql(AtomicUsize::new(0))), 10)
        .create(request())
        .await
        .expect("a message");
    assert_eq!(
        (&message["content"], &message["stop_reason"]),
        (&expected_content, &json!("tool_use"))
    );
    assert_eq!(scripted.requests().len(), 1, "the client runs its tool before another round");

    let events = streamed(&using(&Scripted::new(mixed), &Arc::new(Sql(AtomicUsize::new(0))), 10), request()).await;
    assert_eq!(accumulate(&events), message);
}

#[tokio::test]
async fn the_last_round_runs_its_calls_and_pauses_the_turn() {
    let looping = vec![round("msg_1", &json!([sql_call("toolu_1")]), "tool_use", 10, 5)];
    let scripted = Scripted::new(looping.clone());
    let sql = Arc::new(Sql(AtomicUsize::new(0)));
    let message = using(&scripted, &sql, 1)
        .create(request())
        .await
        .expect("a message");
    assert_eq!(message["stop_reason"], "pause_turn");
    assert_eq!(
        message["content"],
        json!([
            mcp_tool_use(&sql_call("toolu_1")),
            mcp_tool_result("toolu_1", &ToolOutcome::Completed(json!([{"count": 3}]))),
        ])
    );
    assert_eq!(sql.0.load(Ordering::SeqCst), 1);

    let events = streamed(&using(&Scripted::new(looping), &Arc::new(Sql(AtomicUsize::new(0))), 1), request()).await;
    assert_eq!(accumulate(&events), message);
}

#[tokio::test]
async fn a_client_tool_of_the_same_name_is_the_clients_to_run() {
    let scripted = Scripted::new(vec![round("msg_1", &json!([sql_call("toolu_1")]), "tool_use", 10, 5)]);
    let sql = Arc::new(Sql(AtomicUsize::new(0)));
    let mut shadowing = request();
    shadowing.body.insert(
        "tools".to_string(),
        json!([{"name": "sql", "input_schema": {"type": "object"}}]),
    );
    let message = using(&scripted, &sql, 10)
        .create(shadowing)
        .await
        .expect("a message");
    assert_eq!(message["content"], json!([sql_call("toolu_1")]));
    assert_eq!(sql.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        scripted.requests()[0]["tools"],
        json!([{"name": "sql", "input_schema": {"type": "object"}}])
    );
}

#[tokio::test]
async fn a_round_that_cannot_be_sent_back_ends_the_stream_with_an_error() {
    let scripted = Arc::new(Scripted {
        rounds: two_rounds(),
        requests: Mutex::new(Vec::new()),
        foreign_delta: Some("compaction_delta"),
    });
    let events = streamed(&using(&scripted, &Arc::new(Sql(AtomicUsize::new(0))), 10), request()).await;
    assert_eq!(
        events.last(),
        Some(&json!({"type": "error", "error": {"type": "api_error", "message": "Spice ran its tools, but cannot send the model its own answer back to continue: the stream carried a `compaction_delta` delta, which Spice cannot complete a content block from. Send the request without `stream`."}}))
    );
    assert_eq!(scripted.requests().len(), 1);
}

#[tokio::test]
async fn tokens_are_counted_with_spice_tools() {
    let counted = using(&Scripted::new(Vec::new()), &Arc::new(Sql(AtomicUsize::new(0))), 10)
        .count_tokens(request())
        .await
        .expect("a count");
    assert_eq!(counted, json!({"input_tokens": 1}));
}

#[tokio::test]
async fn a_forced_tool_choice_is_not_forced_again_after_a_round_that_called_tools() {
    for (forced, relaxed) in [
        (json!({"type": "tool", "name": "sql"}), json!({"type": "auto"})),
        (
            json!({"type": "any", "disable_parallel_tool_use": true}),
            json!({"type": "auto", "disable_parallel_tool_use": true}),
        ),
        (json!({"type": "auto"}), json!({"type": "auto"})),
    ] {
        let scripted = Scripted::new(two_rounds());
        let mut forcing = request();
        forcing.body.insert("tool_choice".to_string(), forced.clone());
        using(&scripted, &Arc::new(Sql(AtomicUsize::new(0))), 10)
            .create(forcing)
            .await
            .expect("a message");
        let requests = scripted.requests();
        assert_eq!(
            (&requests[0]["tool_choice"], &requests[1]["tool_choice"]),
            (&forced, &relaxed)
        );
    }
}
