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

//! Decision functions planned and run by `DataFusion` with both planner rules, against
//! a model that answers from the text it is given and records every request. Each test
//! asserts the exact rows and the exact requests the model received.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::Optimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use evaluate_api::{Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, Question};
use runtime_decide::{DecisionFunctions, guard_async_calls};
use runtime_status::RuntimeStatus;
use tokio::sync::RwLock;

/// Answers from the text: a noul is 0.9 when the text contains the question's
/// instructions (lowercased) and 0.1 otherwise; a choice is the first option whose
/// label appears in the text, else the first option; a score is the number of `!` in
/// the text, capped at the top level.
#[derive(Debug, Default)]
struct Mock {
    decision: bool,
    fail_when_contains: Option<&'static str>,
    /// Declines every choice question about a text containing this.
    decline_choices_when_contains: Option<&'static str>,
    requests: Mutex<Vec<EvaluateRequest>>,
}

impl Mock {
    fn decision_model() -> Arc<Self> {
        Arc::new(Self {
            decision: true,
            ..Self::default()
        })
    }

    fn chat_model() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn failing_on(text: &'static str) -> Arc<Self> {
        Arc::new(Self {
            decision: true,
            fail_when_contains: Some(text),
            ..Self::default()
        })
    }

    fn declining_choices_on(text: &'static str) -> Arc<Self> {
        Arc::new(Self {
            decision: true,
            decline_choices_when_contains: Some(text),
            ..Self::default()
        })
    }

    fn requests(&self) -> Vec<EvaluateRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

fn text_of(state: &EvaluateState) -> String {
    match state {
        EvaluateState::String(text) => text.clone(),
        other => serde_json::to_string(other).expect("state serializes"),
    }
}

fn instructions_text(question: &Question) -> String {
    let value = serde_json::to_value(question).expect("question serializes");
    value["instructions"]
        .as_str()
        .unwrap_or_default()
        .to_lowercase()
}

#[async_trait]
impl Evaluate for Mock {
    async fn evaluate(&self, request: EvaluateRequest) -> evaluate_api::Result<EvaluateResponse> {
        self.requests
            .lock()
            .expect("requests lock")
            .push(request.clone());
        let text = text_of(&request.state).to_lowercase();
        if let Some(bad) = self.fail_when_contains
            && text.contains(bad)
        {
            return evaluate_api::InvalidRequestSnafu {
                model: request.model,
                message: "the input cannot be answered",
            }
            .fail();
        }

        let mut answers = BTreeMap::new();
        for (id, question) in &request.questions {
            let answer = match question {
                Question::Noul { .. } => Answer::Noul {
                    noul: if text.contains(&instructions_text(question)) {
                        0.9
                    } else {
                        0.1
                    },
                },
                Question::Choice { .. }
                    if self
                        .decline_choices_when_contains
                        .is_some_and(|declined| text.contains(declined)) =>
                {
                    Answer::Refusal {}
                }
                Question::Choice { criteria, .. } => {
                    let labels: Vec<&String> = criteria.keys().collect();
                    let chosen = labels
                        .iter()
                        .find(|label| text.contains(label.as_str()))
                        .unwrap_or(&labels[0]);
                    #[expect(clippy::cast_precision_loss, reason = "a handful of labels")]
                    let rest = 0.2 / (labels.len() - 1) as f64;
                    Answer::Choice {
                        choice: (*chosen).clone(),
                        probabilities: labels
                            .iter()
                            .map(|label| {
                                let p = if label == chosen { 0.8 } else { rest };
                                ((*label).clone(), p)
                            })
                            .collect(),
                        confidence: 0.6,
                    }
                }
                Question::Score { criteria, .. } => {
                    let level = text.matches('!').count().min(criteria.len() - 1);
                    #[expect(clippy::cast_precision_loss, reason = "at most ten levels")]
                    let score = level as f64;
                    Answer::Score {
                        score,
                        legend: BTreeMap::new(),
                        probabilities: (0..criteria.len())
                            .map(|i| (i.to_string(), if i == level { 1.0 } else { 0.0 }))
                            .collect(),
                        confidence: 1.0,
                    }
                }
            };
            answers.insert(id.clone(), answer);
        }
        Ok(EvaluateResponse {
            model: request.model,
            answers,
            usage: None,
        })
    }

    async fn health(&self) -> evaluate_api::Result<()> {
        Ok(())
    }

    fn is_decision_model(&self) -> bool {
        self.decision
    }
}

const BODIES: [&str; 4] = [
    "I want a refund for this billing charge!!",
    "The app shows a technical error",
    "Refund me now!!! This is a billing mistake",
    "Thanks, the account works",
];

/// `tickets(id, status, body)`: `rows` rows, one in every `open_every` open, with
/// bodies cycling through [`BODIES`] and made distinct by their id.
fn tickets(rows: i64, open_every: i64) -> RecordBatch {
    let ids: Vec<i64> = (0..rows).collect();
    let status: Vec<&str> = ids
        .iter()
        .map(|id| {
            if id % open_every == 0 {
                "open"
            } else {
                "closed"
            }
        })
        .collect();
    let bodies: Vec<String> = ids
        .iter()
        .map(|id| {
            let index = usize::try_from(*id).expect("non-negative id") % BODIES.len();
            format!("#{id}: {}", BODIES[index])
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(status)),
            Arc::new(StringArray::from(bodies)),
        ],
    )
    .expect("tickets batch")
}

/// A session planned the way Spice plans: the `PostgreSQL` dialect, which records the
/// names of `=>` arguments, and the leaf-pushdown rules guarded against async calls.
fn session(models: Vec<(&str, Arc<Mock>)>, batch: RecordBatch) -> SessionContext {
    // A fixed partition count keeps plans the same on every machine.
    session_with(
        models,
        batch,
        SessionConfig::new().with_target_partitions(4),
    )
}

fn session_with(
    models: Vec<(&str, Arc<Mock>)>,
    batch: RecordBatch,
    config: SessionConfig,
) -> SessionContext {
    let store: HashMap<String, Arc<dyn Evaluate>> = models
        .into_iter()
        .map(|(name, model)| (name.to_string(), model as Arc<dyn Evaluate>))
        .collect();
    let config = config.set_str("datafusion.sql_parser.dialect", "PostgreSQL");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_optimizer_rules(guard_async_calls(Optimizer::new().rules))
        .build();
    let ctx = SessionContext::new_with_state(state);
    DecisionFunctions::new(Arc::new(RwLock::new(store)), RuntimeStatus::new()).register(&ctx);
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("table");
    ctx.register_table("tickets", Arc::new(table))
        .expect("register tickets");
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    pretty_format_batches(&batches).expect("format").to_string()
}

async fn run_err(ctx: &SessionContext, sql: &str) -> String {
    let result = match ctx.sql(sql).await {
        Ok(frame) => frame.collect().await.map(|_| ()),
        Err(e) => Err(e),
    };
    result.expect_err("the query must fail").strip_backtrace()
}

/// The model sees only the rows that pass the other predicates, one request each.
#[tokio::test]
async fn ai_if_runs_after_the_other_predicates() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(200, 25));

    let rows = run(
        &ctx,
        "SELECT id FROM tickets WHERE status = 'open' AND ai_if(body, 'refund') ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        "+-----+\n| id  |\n+-----+\n| 0   |\n| 50  |\n| 100 |\n| 150 |\n+-----+"
    );

    let requests = jev.requests();
    assert_eq!(
        requests.len(),
        8,
        "one request per open row, none for the 192 others"
    );
    assert!(
        requests
            .iter()
            .all(|r| text_of(&r.state).starts_with('#') && r.questions.len() == 1)
    );
}

/// The placement rule runs the other predicates first itself: with a single optimizer
/// pass, no later `push_down_filter` can move them below the model call.
#[tokio::test]
async fn ai_if_runs_after_the_other_predicates_in_one_optimizer_pass() {
    let jev = Mock::decision_model();
    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_usize("datafusion.optimizer.max_passes", 1);
    let ctx = session_with(vec![("jev", Arc::clone(&jev))], tickets(200, 25), config);

    assert_eq!(
        run(
            &ctx,
            "SELECT id FROM tickets WHERE status = 'open' AND ai_if(body, 'refund') ORDER BY id",
        )
        .await,
        "+-----+\n| id  |\n+-----+\n| 0   |\n| 50  |\n| 100 |\n| 150 |\n+-----+"
    );
    assert_eq!(jev.requests().len(), 8);
}

/// Typed calls on the same input share one request per row, whatever clause they are in.
#[tokio::test]
async fn calls_on_the_same_input_share_one_request() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(100, 25));

    let rows = run(
        &ctx,
        "SELECT id, \
                ai_if(body, 'refund') AS refund, \
                ai_probability(body, 'refund') AS p, \
                ai_classify(body, ['billing', 'technical', 'account']) AS team, \
                ai_score(body, 'How upset is the customer?', ['calm', 'annoyed', 'furious']) AS upset \
         FROM tickets WHERE status = 'open' ORDER BY id",
    )
    .await;
    insta::assert_snapshot!("shared_request_rows", rows);

    let requests = jev.requests();
    assert_eq!(requests.len(), 4, "one request per open row");
    for request in requests {
        let kinds: Vec<&str> = request
            .questions
            .values()
            .map(|q| match q {
                Question::Noul { .. } => "noul",
                Question::Choice { .. } => "choice",
                Question::Score { .. } => "score",
            })
            .collect();
        // `ai_if` and `ai_probability` ask the same question, so it is asked once.
        assert_eq!(kinds, vec!["choice", "noul", "score"]);
        assert_eq!(
            request.questions.keys().collect::<Vec<_>>(),
            vec!["ai_classify_1", "ai_if_0", "ai_score_2"]
        );
    }
}

#[tokio::test]
async fn identical_inputs_are_asked_once() {
    let jev = Mock::decision_model();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![
            Some("refund please"),
            Some("refund please"),
            None,
            Some("all good"),
            Some("refund please"),
        ]))],
    )
    .expect("batch");
    let ctx = session(vec![("jev", Arc::clone(&jev))], batch);

    let rows = run(
        &ctx,
        "SELECT body, ai_if(body, 'refund') AS refund FROM tickets",
    )
    .await;
    assert_eq!(
        rows,
        "+---------------+--------+\n| body          | refund |\n+---------------+--------+\n| refund please | true   |\n| refund please | true   |\n|               |        |\n| all good      | false  |\n| refund please | true   |\n+---------------+--------+"
    );
    assert_eq!(jev.requests().len(), 2, "two distinct non-NULL inputs");
}

/// Identical `ai_decide` calls in one node are one call, however the query reads them:
/// one request per row, whose answer every occurrence reads. Distinct calls are
/// separate requests.
#[tokio::test]
async fn identical_ai_decide_calls_share_one_request() {
    let urgent = r#"'{"urgent": {"type": "noul", "instructions": "refund"}}'"#;
    let team = r#"'{"team": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": null, "technical": null}}}'"#;
    let cases = [
        (
            format!(
                "SELECT id, ai_decide(body, {urgent}) AS a, ai_decide(body, {urgent}) AS b FROM tickets ORDER BY id"
            ),
            "+----+------------------------------+------------------------------+\n| id | a                            | b                            |\n+----+------------------------------+------------------------------+\n| 0  | {urgent: {probability: 0.9}} | {urgent: {probability: 0.9}} |\n| 1  | {urgent: {probability: 0.1}} | {urgent: {probability: 0.1}} |\n+----+------------------------------+------------------------------+",
            2,
        ),
        (
            format!(
                "SELECT id, ai_decide(body, {urgent})['urgent']['probability'] AS a, ai_decide(body, {urgent})['urgent']['probability'] AS b FROM tickets ORDER BY id"
            ),
            "+----+-----+-----+\n| id | a   | b   |\n+----+-----+-----+\n| 0  | 0.9 | 0.9 |\n| 1  | 0.1 | 0.1 |\n+----+-----+-----+",
            2,
        ),
        (
            format!(
                "SELECT id, ai_decide(body, {urgent}) AS a, ai_decide(body, {urgent})['urgent']['probability'] AS b FROM tickets ORDER BY id"
            ),
            "+----+------------------------------+-----+\n| id | a                            | b   |\n+----+------------------------------+-----+\n| 0  | {urgent: {probability: 0.9}} | 0.9 |\n| 1  | {urgent: {probability: 0.1}} | 0.1 |\n+----+------------------------------+-----+",
            2,
        ),
        (
            format!(
                "SELECT id FROM tickets WHERE ai_decide(body, {urgent})['urgent']['probability'] > 0.5 AND ai_decide(body, {urgent})['urgent']['probability'] > 0.2 ORDER BY id"
            ),
            "+----+\n| id |\n+----+\n| 0  |\n+----+",
            2,
        ),
        (
            format!(
                "SELECT id, ai_decide(body, {urgent})['urgent']['probability'] AS a, ai_decide(body, {team})['team']['choice'] AS b FROM tickets ORDER BY id"
            ),
            "+----+-----+-----------+\n| id | a   | b         |\n+----+-----+-----------+\n| 0  | 0.9 | billing   |\n| 1  | 0.1 | technical |\n+----+-----+-----------+",
            4,
        ),
    ];
    for (sql, rows, requests) in cases {
        let jev = Mock::decision_model();
        let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(2, 1));
        assert_eq!(run(&ctx, &sql).await, rows, "{sql}");
        assert_eq!(jev.requests().len(), requests, "{sql}");
    }
}

/// `DataFusion` cannot run an async function in these clauses; the planner computes
/// the decision below them.
#[tokio::test]
async fn decisions_work_in_order_by_group_by_having_windows_and_aggregates() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(200, 25));

    assert_eq!(
        run(
            &ctx,
            "SELECT id FROM tickets WHERE status = 'open' \
             ORDER BY ai_score(body, 'How upset?', ['calm', 'annoyed', 'furious']) DESC, id LIMIT 3",
        )
        .await,
        "+-----+\n| id  |\n+-----+\n| 0   |\n| 50  |\n| 100 |\n+-----+"
    );

    assert_eq!(
        run(
            &ctx,
            "SELECT ai_classify(body, ['billing', 'technical']) AS team, count(*) AS tickets \
             FROM tickets WHERE status = 'open' \
             GROUP BY ai_classify(body, ['billing', 'technical']) ORDER BY team",
        )
        .await,
        "+-----------+---------+\n| team      | tickets |\n+-----------+---------+\n| billing   | 6       |\n| technical | 2       |\n+-----------+---------+"
    );

    assert_eq!(
        run(
            &ctx,
            "SELECT status, round(sum(ai_probability(body, 'refund')), 2) AS expected_refunds \
             FROM tickets WHERE id < 8 GROUP BY status \
             HAVING sum(ai_probability(body, 'refund')) > 1 ORDER BY status",
        )
        .await,
        "+--------+------------------+\n| status | expected_refunds |\n+--------+------------------+\n| closed | 3.1              |\n+--------+------------------+"
    );

    assert_eq!(
        run(
            &ctx,
            "SELECT id, rank() OVER (ORDER BY ai_score(body, 'How upset?', ['calm', 'annoyed', 'furious']) DESC) AS r \
             FROM tickets WHERE status = 'open' ORDER BY id LIMIT 4",
        )
        .await,
        "+----+---+\n| id | r |\n+----+---+\n| 0  | 1 |\n| 25 | 5 |\n| 50 | 1 |\n| 75 | 5 |\n+----+---+"
    );

    assert_eq!(
        run(
            &ctx,
            "SELECT count(*) FILTER (WHERE ai_if(body, 'refund')) AS refunds FROM tickets WHERE status = 'open'",
        )
        .await,
        "+---------+\n| refunds |\n+---------+\n| 4       |\n+---------+"
    );
}

#[tokio::test]
async fn an_inner_join_condition_becomes_a_filter_and_an_outer_one_is_refused() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(4, 1));

    assert_eq!(
        run(
            &ctx,
            "SELECT a.id AS a, b.id AS b FROM tickets a JOIN tickets b \
             ON a.id < b.id AND ai_if(concat(a.body, ' ', b.body), 'refund') ORDER BY a, b",
        )
        .await,
        "+---+---+\n| a | b |\n+---+---+\n| 0 | 1 |\n| 0 | 2 |\n| 0 | 3 |\n| 1 | 2 |\n| 2 | 3 |\n+---+---+"
    );
    assert_eq!(
        jev.requests().len(),
        6,
        "one request per pair that passes a.id < b.id"
    );

    assert_eq!(
        run_err(
            &ctx,
            "SELECT a.id FROM tickets a LEFT JOIN tickets b ON ai_if(concat(a.body, b.body), 'refund')",
        )
        .await,
        "Optimizer rule 'decision_placement' failed\ncaused by\nError during planning: Decision functions cannot be used in the condition of a Left join. Use an inner join, or apply the decision in WHERE over the joined rows."
    );
}

#[tokio::test]
async fn ai_decide_returns_every_answer() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(2, 1));

    let rows = run(
        &ctx,
        r#"SELECT id, d['urgent']['probability'] AS urgent, d['team']['choice'] AS team, d['team']['confidence'] AS confidence, d
           FROM (SELECT id, ai_decide(body, '{
             "urgent": {"type": "noul", "instructions": "refund"},
             "team": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": "Payments", "technical": null}}
           }') AS d FROM tickets) ORDER BY id"#,
    )
    .await;
    insta::assert_snapshot!("decide_rows", rows);
    assert_eq!(jev.requests().len(), 2);
}

/// `ai_decide` questions are checked when the query is planned, so a malformed one fails
/// before any model call. The bounds are those of `TypeSafe`'s and Databricks' grammar:
/// a choice of 1 to 255 non-empty labels, and a score of 2 to 10 levels.
#[tokio::test]
async fn ai_decide_questions_are_checked_when_the_query_is_planned() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(2, 1));

    assert_eq!(
        run_err(
            &ctx,
            r#"SELECT ai_decide(body, '{"tone": {"type": "score", "instructions": "How upset?", "criteria": ["calm"]}}') FROM tickets"#,
        )
        .await,
        "Error during planning: ai_decide: `questions` is not a valid questions object: score criteria must contain between two and ten non-null levels at line 1 column 79. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`."
    );
    assert_eq!(
        run_err(
            &ctx,
            r#"SELECT ai_decide(body, '{"team": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": null, "billing": "Payments"}}}') FROM tickets"#,
        )
        .await,
        "Error during planning: ai_decide: `questions` is not a valid questions object: choice option 'billing' is listed more than once at line 1 column 113. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`."
    );
    assert_eq!(
        jev.requests().len(),
        0,
        "a query that fails planning calls no model"
    );

    assert_eq!(
        run(
            &ctx,
            r#"SELECT id, ai_decide(body, '{"team": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": null}}}')['team']['choice'] AS team FROM tickets ORDER BY id"#,
        )
        .await,
        "+----+---------+\n| id | team    |\n+----+---------+\n| 0  | billing |\n| 1  | billing |\n+----+---------+"
    );
    assert_eq!(jev.requests().len(), 2);
}

#[tokio::test]
async fn structured_input_is_sent_as_json() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(1, 1));

    run(
        &ctx,
        "SELECT ai_if(named_struct('id', id, 'body', body), 'refund') FROM tickets",
    )
    .await;
    let requests = jev.requests();
    assert_eq!(
        serde_json::to_value(&requests[0].state).expect("state"),
        serde_json::json!({"id": 0, "body": "#0: I want a refund for this billing charge!!"})
    );
}

#[tokio::test]
async fn named_options_are_never_confused() {
    let jev = Mock::failing_on("technical");
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(4, 1));

    // `on_error => 'null'` alone arrives in the same position `model` would.
    assert_eq!(
        run(
            &ctx,
            "SELECT id, ai_if(body, 'refund', on_error => 'null') AS refund FROM tickets ORDER BY id",
        )
        .await,
        "+----+--------+\n| id | refund |\n+----+--------+\n| 0  | true   |\n| 1  |        |\n| 2  | true   |\n| 3  | false  |\n+----+--------+"
    );
    assert_eq!(
        run(
            &ctx,
            "SELECT id, ai_if(body, 'refund', 'jev', 'null') AS refund FROM tickets WHERE id = 1",
        )
        .await,
        "+----+--------+\n| id | refund |\n+----+--------+\n| 1  |        |\n+----+--------+"
    );
    assert_eq!(
        run_err(&ctx, "SELECT ai_if(body, model => 'jev') FROM tickets").await,
        "Error during planning: ai_if: missing required argument `condition`. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])"
    );
}

/// A refusal answers one question. Calls that share a request keep their own answers
/// under `on_error => 'null'`, and by default the refusal stops the query.
#[tokio::test]
async fn a_declined_question_is_null_and_the_shared_answers_stand() {
    let jev = Mock::declining_choices_on("technical");
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(4, 1));

    assert_eq!(
        run(
            &ctx,
            "SELECT id, ai_if(body, 'refund', on_error => 'null') AS refund, \
             ai_classify(body, ['billing', 'technical'], on_error => 'null') AS team \
             FROM tickets ORDER BY id",
        )
        .await,
        "+----+--------+---------+\n| id | refund | team    |\n+----+--------+---------+\n| 0  | true   | billing |\n| 1  | false  |         |\n| 2  | true   | billing |\n| 3  | false  | billing |\n+----+--------+---------+"
    );
    assert_eq!(jev.requests().len(), 4, "one shared request per row");

    assert_eq!(
        run_err(
            &ctx,
            "SELECT ai_classify(body, ['billing', 'technical']) FROM tickets"
        )
        .await,
        "Execution error: ai_classify: model 'jev' could not answer a row, so the query stopped. Cause: the model declined to answer 'ai_classify_0'. Retry the query, or pass `on_error => 'null'` to return NULL for rows the model cannot answer."
    );
}

/// A decision moved out of a join key keeps the join's NULL semantics: on
/// `IS NOT DISTINCT FROM`, a NULL answer matches a NULL key.
#[tokio::test]
async fn a_decision_join_key_keeps_null_matching() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(1, 1));
    let notes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("a billing question"),
                None,
                Some("a technical issue"),
            ])),
        ],
    )
    .expect("notes batch");
    ctx.register_batch("notes", notes).expect("register notes");
    let teams = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("team", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![
            Some("billing"),
            None,
            Some("technical"),
        ]))],
    )
    .expect("teams batch");
    ctx.register_batch("teams", teams).expect("register teams");

    assert_eq!(
        run(
            &ctx,
            "SELECT n.id, t.team FROM notes n JOIN teams t \
             ON ai_classify(n.body, ['billing', 'technical']) IS NOT DISTINCT FROM t.team \
             ORDER BY n.id",
        )
        .await,
        "+----+-----------+\n| id | team      |\n+----+-----------+\n| 0  | billing   |\n| 1  |           |\n| 2  | technical |\n+----+-----------+"
    );
}

#[tokio::test]
async fn a_row_the_model_cannot_answer_fails_the_query_by_default() {
    let jev = Mock::failing_on("technical");
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(4, 1));

    assert_eq!(
        run_err(&ctx, "SELECT ai_if(body, 'refund') FROM tickets").await,
        "Execution error: ai_if: model 'jev' could not answer a row, so the query stopped. Cause: Invalid evaluation request for model 'jev': the input cannot be answered. Retry the query, or pass `on_error => 'null'` to return NULL for rows the model cannot answer."
    );
}

#[tokio::test]
async fn the_model_is_named_or_found() {
    let jev = Mock::decision_model();
    let judge = Mock::chat_model();
    let ctx = session(
        vec![("jev", Arc::clone(&jev)), ("judge", Arc::clone(&judge))],
        tickets(2, 1),
    );

    // With a chat model and one decision model, the decision model answers.
    run(&ctx, "SELECT ai_if(body, 'refund') FROM tickets").await;
    assert_eq!((jev.requests().len(), judge.requests().len()), (2, 0));

    run(
        &ctx,
        "SELECT ai_if(body, 'refund', model => 'judge') FROM tickets",
    )
    .await;
    assert_eq!(judge.requests().len(), 2);
    assert!(judge.requests().iter().all(|r| r.model == "judge"));

    assert_eq!(
        run_err(
            &ctx,
            "SELECT ai_if(body, 'refund', model => 'nope') FROM tickets"
        )
        .await,
        "Execution error: ai_if: no model named 'nope' can answer decisions. Models that can: jev, judge. Name one with `model => '<name>'`. See: https://spiceai.org/docs/components/models"
    );

    let chats = session(
        vec![("a", Mock::chat_model()), ("b", Mock::chat_model())],
        tickets(1, 1),
    );
    assert_eq!(
        run_err(&chats, "SELECT ai_if(body, 'refund') FROM tickets").await,
        "Execution error: ai_if: several models can answer decisions (a, b). Name one with `model => '<name>'`."
    );

    let none = session(vec![], tickets(1, 1));
    assert_eq!(
        run_err(&none, "SELECT ai_if(body, 'refund') FROM tickets").await,
        "Execution error: ai_if: no model can answer decisions. Add a decision model such as `from: typesafe:jev`, or any chat model, under `models` in the Spicepod. See: https://spiceai.org/docs/components/models"
    );
}

/// More rows than one invocation takes, in one input batch.
#[tokio::test]
async fn large_inputs_are_split_into_invocations() {
    let jev = Mock::decision_model();
    let ctx = session(vec![("jev", Arc::clone(&jev))], tickets(3_000, 1));

    assert_eq!(
        run(
            &ctx,
            "SELECT count(*) AS refunds FROM tickets WHERE ai_if(body, 'refund')",
        )
        .await,
        "+---------+\n| refunds |\n+---------+\n| 1500    |\n+---------+"
    );
    assert_eq!(jev.requests().len(), 3_000);
}

#[tokio::test]
async fn the_plan_shows_one_decision_below_the_filter() {
    let ctx = session(vec![("jev", Mock::decision_model())], tickets(1, 1));
    let plan = run(
        &ctx,
        "EXPLAIN SELECT id FROM tickets WHERE status = 'open' AND ai_if(body, 'refund') AND ai_if(body, 'billing')",
    )
    .await;
    insta::assert_snapshot!("filter_plan", plan);
}
