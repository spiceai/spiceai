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

//! Asking a model the questions of a decision call, one request per distinct input row.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use evaluate_api::{
    Answer, Error as EvaluateError, Evaluate, EvaluateModelStore, EvaluateRequest, EvaluateState,
    Question,
};
use futures::{StreamExt, TryStreamExt, stream};
use runtime_request_context::{AsyncMarker, RequestContext};
use runtime_status::RuntimeStatus;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::Instrument;

use crate::args::OnError;

/// Requests one invocation keeps in flight. The model's own rate controls
/// (`max_concurrency`, `requests_per_minute_limit`) still apply across all queries.
const MAX_IN_FLIGHT: usize = 32;

/// Attempts per request, counting the first. Only transient failures are retried.
const MAX_ATTEMPTS: u32 = 3;

/// Wait before the first retry; each later retry waits four times longer.
const FIRST_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Where models are found for a decision call.
const MODELS_DOCS: &str = "https://spiceai.org/docs/components/models";

/// The answers to one row's questions, by question id. `None` for a NULL input, or a
/// row the model could not answer under `on_error => 'null'`.
pub(crate) type RowAnswers = Option<BTreeMap<String, Answer>>;

/// Asks models the questions of decision calls.
#[derive(Debug)]
pub(crate) struct Decider {
    models: Arc<RwLock<EvaluateModelStore>>,
    status: Arc<RuntimeStatus>,
}

impl Decider {
    pub(crate) fn new(models: Arc<RwLock<EvaluateModelStore>>, status: Arc<RuntimeStatus>) -> Self {
        Self { models, status }
    }

    /// The model a call names, or the one it means when it names none: the only model
    /// that can answer, or else the only decision model among several.
    async fn resolve_model(
        &self,
        function: &str,
        requested: Option<&str>,
    ) -> Result<(String, Arc<dyn Evaluate>)> {
        let models = self.models.read().await;
        if let Some(name) = requested {
            if let Some(model) = models.get(name) {
                return Ok((name.to_string(), Arc::clone(model)));
            }
            if let Some(reason) = self.status.unavailable_model_reason(name) {
                return exec_err!("{function}: {reason}");
            }
            let mut available: Vec<&str> = models.keys().map(String::as_str).collect();
            available.sort_unstable();
            return exec_err!(
                "{function}: no model named '{name}' can answer decisions. Models that can: {}. Name one with `model => '<name>'`. See: {MODELS_DOCS}",
                if available.is_empty() {
                    "none".to_string()
                } else {
                    available.join(", ")
                }
            );
        }

        let mut names: Vec<&String> = models.keys().collect();
        names.sort_unstable();
        match names.as_slice() {
            [] => {
                let mut reasons: Vec<String> = self
                    .status
                    .get_model_statuses()
                    .into_iter()
                    .filter_map(|(name, status)| {
                        runtime_status::unavailable_model_message(&name, Some(status))
                    })
                    .collect();
                reasons.sort_unstable();
                if reasons.is_empty() {
                    exec_err!(
                        "{function}: no model can answer decisions. Add a decision model such as `from: typesafe:jev`, or any chat model, under `models` in the Spicepod. See: {MODELS_DOCS}"
                    )
                } else {
                    exec_err!("{function}: {}", reasons.join("; "))
                }
            }
            [only] => Ok(((*only).clone(), Arc::clone(&models[*only]))),
            several => {
                let decision_models: Vec<&&String> = several
                    .iter()
                    .filter(|name| models[name.as_str()].is_decision_model())
                    .collect();
                if let [only] = decision_models.as_slice() {
                    return Ok(((**only).clone(), Arc::clone(&models[only.as_str()])));
                }
                exec_err!(
                    "{function}: several models can answer decisions ({}). Name one with `model => '<name>'`.",
                    several
                        .iter()
                        .map(|name| name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }

    /// Asks `questions` about every non-NULL row of `input`, one request per distinct
    /// input, and returns each row's answers in row order.
    ///
    /// # Errors
    ///
    /// With [`OnError::Fail`], the first row the model cannot answer — after transient
    /// failures are retried — fails the call, naming the model and the cause.
    pub(crate) async fn decide(
        &self,
        function: &str,
        input: &ArrayRef,
        questions: &BTreeMap<String, Question>,
        model: Option<&str>,
        on_error: OnError,
    ) -> Result<Vec<RowAnswers>> {
        let states = input_states(function, input)?;

        // Identical inputs are asked once.
        let mut distinct: Vec<EvaluateState> = Vec::new();
        let mut index_of: HashMap<String, usize> = HashMap::new();
        let mut row_request: Vec<Option<usize>> = Vec::with_capacity(states.len());
        for state in states {
            let Some(state) = state else {
                row_request.push(None);
                continue;
            };
            let key = serde_json::to_string(&state)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let next = distinct.len();
            let index = *index_of.entry(key).or_insert(next);
            if index == next {
                distinct.push(state);
            }
            row_request.push(Some(index));
        }
        // A batch of NULL inputs asks nothing, so it needs no model.
        if distinct.is_empty() {
            return Ok(vec![None; row_request.len()]);
        }
        let (model_name, model) = self.resolve_model(function, model).await?;

        let span = tracing::span!(
            target: "task_history",
            tracing::Level::INFO,
            "ai_decide",
            input = %format!("{function}(<{} rows>, {} question(s))", row_request.len(), questions.len()),
            model = %model_name,
            rows = row_request.len(),
            requests = distinct.len(),
        );

        let context = RequestContext::current(AsyncMarker::new().await);
        let results: Vec<Asked> =
            stream::iter(distinct.into_iter().enumerate())
                .map(|(index, state)| {
                    let model = Arc::clone(&model);
                    let request = EvaluateRequest {
                        model: model_name.clone(),
                        state,
                        questions: questions.clone(),
                        safety_identifier: None,
                        reasoning_effort: None,
                        typed_choices: BTreeMap::new(),
                    };
                    Arc::clone(&context).scope(async move {
                        // A refusal answers one question. Under `on_error => 'null'` only
                        // that question's value is NULL, so the calls that share the
                        // request keep their answers; otherwise it stops the query.
                        let result = ask(model.as_ref(), request).await.and_then(|answers| {
                            match (on_error, declined(&answers)) {
                                (OnError::Fail, Some(question)) => Err(Failure::Refused {
                                    question: question.clone(),
                                }),
                                _ => Ok(answers),
                            }
                        });
                        match (result, on_error) {
                            (Ok(answers), _) => Ok((index, Ok(answers))),
                            (Err(failure), OnError::Null) => Ok((index, Err(failure))),
                            (Err(failure), OnError::Fail) => Err(failure),
                        }
                    })
                })
                .buffer_unordered(MAX_IN_FLIGHT)
                .try_collect()
                .instrument(span.clone())
                .await
                .map_err(|failure| {
                    span.in_scope(|| {
                        tracing::error!(target: "task_history", "{}", failure.telemetry());
                    });
                    exec_datafusion_err!(
                        "{function}: model '{model_name}' could not answer a row, so the query stopped. Cause: {failure}. Retry the query, or pass `on_error => 'null'` to return NULL for rows the model cannot answer."
                    )
                })?;

        let mut answers: Vec<Option<BTreeMap<String, Answer>>> = vec![None; results.len()];
        let mut failed = 0_usize;
        let mut first_failure = None;
        for (index, result) in results {
            match result {
                Ok(row) => answers[index] = Some(row),
                Err(failure) => {
                    failed += 1;
                    first_failure.get_or_insert(failure);
                }
            }
        }
        if let Some(failure) = first_failure {
            tracing::warn!(
                "{}",
                null_rows_warning(function, &model_name, failed, answers.len(), &failure)
            );
        }
        let declined_inputs = answers
            .iter()
            .flatten()
            .filter(|row| declined(row).is_some())
            .count();
        if declined_inputs > 0 {
            tracing::warn!(
                "{function}: model '{model_name}' declined to answer a question for {declined_inputs} of {} distinct inputs, so those answers are NULL (`on_error => 'null'`).",
                answers.len()
            );
        }

        Ok(row_request
            .into_iter()
            .map(|request| request.and_then(|index| answers[index].clone()))
            .collect())
    }
}

/// The warning for rows that return NULL under `on_error => 'null'`. Logs persist, so the
/// cause is the redacted text: a provider's response body can carry the input or a
/// secret.
fn null_rows_warning(
    function: &str,
    model: &str,
    failed: usize,
    distinct: usize,
    first: &Failure,
) -> String {
    format!(
        "{function}: model '{model}' could not answer {failed} of {distinct} distinct inputs, so their rows return NULL (`on_error => 'null'`). First cause: {}",
        first.telemetry()
    )
}

/// One distinct input's answers, or why it has none, by its index among the inputs.
type Asked = (
    usize,
    std::result::Result<BTreeMap<String, Answer>, Failure>,
);

/// Why a row has no answer.
#[derive(Debug)]
enum Failure {
    /// The model call failed, after any retries.
    Model(EvaluateError),
    /// The model declined to answer a question.
    Refused { question: String },
}

impl Failure {
    /// Text for `runtime.task_history`, without provider response bodies.
    fn telemetry(&self) -> String {
        match self {
            Self::Model(error) => error.telemetry_message(),
            Self::Refused { question } => format!("the model declined to answer '{question}'"),
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Model(error) => write!(f, "{error}"),
            Self::Refused { question } => write!(f, "the model declined to answer '{question}'"),
        }
    }
}

/// Whether a failed call may succeed when made again.
fn is_transient(error: &EvaluateError) -> bool {
    matches!(
        error,
        EvaluateError::RateLimited { .. }
            | EvaluateError::ServiceUnavailable { .. }
            | EvaluateError::ModelCallFailed { .. }
            | EvaluateError::UnparseableResponse { .. }
    )
}

/// Asks one row's questions, retrying transient failures.
async fn ask(
    model: &dyn Evaluate,
    request: EvaluateRequest,
) -> std::result::Result<BTreeMap<String, Answer>, Failure> {
    let mut delay = FIRST_RETRY_DELAY;
    let mut attempt = 1;
    let response = loop {
        match model.evaluate(request.clone()).await {
            Ok(response) => break response,
            Err(error) if attempt < MAX_ATTEMPTS && is_transient(&error) => {
                tokio::time::sleep(delay).await;
                delay *= 4;
                attempt += 1;
            }
            Err(error) => return Err(Failure::Model(error)),
        }
    };
    Ok(response.answers)
}

/// The first question the model declined to answer, if any.
fn declined(answers: &BTreeMap<String, Answer>) -> Option<&String> {
    answers
        .iter()
        .find_map(|(question, answer)| matches!(answer, Answer::Refusal {}).then_some(question))
}

/// Each row's input as System One state: text as text, a struct or map as a JSON
/// object, a list as a JSON array, and any other value as its text. `None` for NULL.
fn input_states(function: &str, input: &ArrayRef) -> Result<Vec<Option<EvaluateState>>> {
    match input.data_type() {
        DataType::Null => Ok(vec![None; input.len()]),
        DataType::Dictionary(_, value_type) => {
            let values = arrow::compute::cast(input, value_type)?;
            input_states(function, &values)
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => exec_err!(
            "{function}: `input` is binary data, which a model cannot read. Cast it to text first."
        ),
        DataType::Struct(_)
        | DataType::Map(_, _)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => json_states(input),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => text_states(input),
        _ => {
            let text = arrow::compute::cast(input, &DataType::Utf8)?;
            text_states(&text)
        }
    }
}

fn text_states(input: &ArrayRef) -> Result<Vec<Option<EvaluateState>>> {
    let text = arrow::compute::cast(input, &DataType::Utf8)?;
    Ok(text
        .as_string::<i32>()
        .iter()
        .map(|value| value.map(|v| EvaluateState::String(v.to_string())))
        .collect())
}

/// Structured rows as JSON, through Arrow's JSON writer so every nested type is
/// written the way the rest of Spice writes it.
fn json_states(input: &ArrayRef) -> Result<Vec<Option<EvaluateState>>> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        input.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::clone(input)])?;
    let mut buffer = Vec::new();
    {
        let mut writer = arrow_json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow_json::writer::JsonArray>(&mut buffer);
        writer.write(&batch)?;
        writer.finish()?;
    }
    let rows: Vec<serde_json::Map<String, Value>> =
        serde_json::from_slice(&buffer).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(rows
        .into_iter()
        .enumerate()
        .map(|(row, mut object)| {
            if input.is_null(row) {
                return None;
            }
            match object.remove("v") {
                Some(Value::Object(fields)) => Some(EvaluateState::Object(fields)),
                Some(Value::Array(items)) => Some(EvaluateState::Array(items)),
                Some(Value::String(text)) => Some(EvaluateState::String(text)),
                Some(Value::Null) | None => None,
                Some(other) => Some(EvaluateState::String(other.to_string())),
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, StructArray};

    /// Logs persist, so the warning carries the redacted cause, not the provider's body.
    #[test]
    fn the_null_rows_warning_omits_the_provider_body() {
        let failure = Failure::Model(EvaluateError::AuthenticationFailed {
            model: "luna".to_string(),
            message: "Incorrect API key provided: sk-live-secret".to_string(),
        });
        assert_eq!(
            null_rows_warning("ai_if", "luna", 3, 8, &failure),
            "ai_if: model 'luna' could not answer 3 of 8 distinct inputs, so their rows return NULL (`on_error => 'null'`). First cause: Evaluation of model 'luna' failed: authentication failed"
        );
    }

    #[test]
    fn inputs_become_state_by_type() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("hello"), None]));
        let states = input_states("ai_if", &text).expect("text");
        assert_eq!(
            states,
            vec![Some(EvaluateState::String("hello".to_string())), None]
        );

        let number: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), None]));
        let states = input_states("ai_if", &number).expect("number");
        assert_eq!(states, vec![Some(EvaluateState::String("42".into())), None]);

        let fields: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("subject", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("Refund"), None])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("priority", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(2), Some(3)])) as ArrayRef,
            ),
        ]));
        let states = input_states("ai_if", &fields).expect("struct");
        assert_eq!(
            serde_json::to_value(&states).expect("serializes"),
            serde_json::json!([
                {"subject": "Refund", "priority": 2},
                {"subject": null, "priority": 3}
            ])
        );
    }

    #[test]
    fn binary_input_is_refused_with_the_fix() {
        let binary: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![b"x".as_ref()]));
        let err = input_states("ai_if", &binary).expect_err("binary");
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: ai_if: `input` is binary data, which a model cannot read. Cast it to text first."
        );
    }
}
