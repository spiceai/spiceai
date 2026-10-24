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

//! Decision models through the whole runtime: `POST /v1/decisions` and the `ai_*` SQL
//! functions, for each provider that can answer them.
//!
//! The per-provider contract is held in `crates/llms/tests/llms/evaluate.rs`; these tests
//! check what only the runtime adds: loading the model from a Spicepod, routing a request
//! to it by name, and translating answers for the HTTP and SQL surfaces.

use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::util::display::array_value_to_string;
use evaluate_api::openai::{ChoiceValue, DecisionAnswer, DecisionResponse};
use futures::TryStreamExt;
use reqwest::{Client, StatusCode};
use rstest::rstest;
use runtime::{Runtime, auth::EndpointAuth};
use serde_json::{Value, json};
use spicepod::component::model::Model;

use super::{create_api_bindings_config, get_anthropic_model, get_xai_model};
use crate::{
    init_tracing,
    models::openai::get_openai_model,
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};

const REFUND: &str = "I was charged twice for my order and I want my money back today.";
const FURIOUS: &str =
    "THIS IS UNACCEPTABLE. Third outage this week and nobody will answer me. I am cancelling.";

/// A provider that answers decisions, and what loading it needs.
struct Provider {
    /// Names the provider's test, and the model in the Spicepod.
    label: &'static str,
    /// Secrets the model reads, which a run must have.
    secrets: &'static [&'static str],
    build: fn(&str) -> Model,
}

/// A chat model of the Spicepod, which answers decisions through its chat evaluator.
fn openai_chat(name: &str) -> Model {
    let mut model = get_openai_model("gpt-4o-mini", name);
    // The text-to-SQL system prompt is for chat; a decision is asked with its own prompt.
    model.params.remove("system_prompt");
    model
}

/// An `OpenAI` decision model, which answers through `OpenAI`'s Decisions API.
fn openai_decisions(name: &str) -> Model {
    let mut model = openai_chat(name);
    model.from = "openai:gpt-6-luna".to_string();
    model
}

fn anthropic(name: &str) -> Model {
    get_anthropic_model("claude-haiku-4-5-20251001", name)
}

fn xai(name: &str) -> Model {
    get_xai_model("grok-4.20-non-reasoning", name)
}

/// `TypeSafe` Jev, a System One model that does not chat.
fn typesafe(name: &str) -> Model {
    let mut model = Model::new("typesafe:jev", name);
    model.params.insert(
        "typesafe_api_key".to_string(),
        "${ secrets:TYPESAFE_API_KEY }".into(),
    );
    model
}

#[cfg(feature = "bedrock")]
fn bedrock(name: &str) -> Model {
    super::bedrock::chat::create_nova_lite_model(name)
}

const PROVIDERS: &[Provider] = &[
    Provider {
        label: "openai_chat",
        secrets: &["SPICE_OPENAI_API_KEY"],
        build: openai_chat,
    },
    Provider {
        label: "openai_decisions",
        secrets: &["SPICE_OPENAI_API_KEY"],
        build: openai_decisions,
    },
    Provider {
        label: "anthropic",
        secrets: &["SPICE_ANTHROPIC_API_KEY"],
        build: anthropic,
    },
    Provider {
        label: "xai",
        secrets: &["SPICE_XAI_API_KEY"],
        build: xai,
    },
    Provider {
        label: "typesafe",
        secrets: &["TYPESAFE_API_KEY"],
        build: typesafe,
    },
    #[cfg(feature = "bedrock")]
    Provider {
        label: "bedrock",
        secrets: &["AWS_BEDROCK_KEY", "AWS_BEDROCK_SECRET"],
        build: bedrock,
    },
];

/// A runtime serving `models`, loaded and ready, and the base URL of its HTTP API.
async fn start(models: Vec<Model>) -> Result<(Arc<Runtime>, String), anyhow::Error> {
    let app = models
        .into_iter()
        .fold(AppBuilder::new("decisions"), AppBuilder::with_model)
        .build();
    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let serving = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(serving.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::time::timeout(Duration::from_mins(1), Arc::clone(&rt).load_components())
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for components to load"))?;
    runtime_ready_check(&rt).await;
    Ok((rt, http_base_url))
}

/// The first column of the first row `sql` returns, as text.
async fn query_value(rt: &Runtime, sql: &str) -> Result<String, anyhow::Error> {
    let batches: Vec<_> = tokio::time::timeout(Duration::from_mins(1), async {
        let result = rt
            .datafusion()
            .query_builder(sql)
            .build()
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("{sql} failed to run: {e}"))?;
        result
            .data
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("{sql} failed: {e}"))
    })
    .await
    .map_err(|_| anyhow::anyhow!("{sql} did not finish within a minute"))??;
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .ok_or_else(|| anyhow::anyhow!("{sql} returned no rows"))?;
    Ok(array_value_to_string(batch.column(0).as_ref(), 0)?)
}

async fn post_decisions(
    base_url: &str,
    body: &Value,
) -> Result<(StatusCode, String), anyhow::Error> {
    let response = Client::new()
        .post(format!("{base_url}/v1/decisions"))
        .json(body)
        .send()
        .await?;
    Ok((response.status(), response.text().await?))
}

/// The request every provider answers: one question of each kind over one piece of evidence.
fn decisions_request(model: &str) -> Value {
    json!({
        "model": model,
        "input": REFUND,
        "questions": [
            {
                "type": "predicate",
                "name": "refund",
                "instructions": "Is the customer asking for a refund?"
            },
            {
                "type": "choice",
                "name": "team",
                "instructions": "Which team should handle this message?",
                "choices": [
                    {"value": "billing", "description": "Charges, invoices and refunds"},
                    {"value": "technical", "description": "Bugs and outages"},
                    {"value": "account", "description": "Logins and profile changes"}
                ]
            },
            {
                "type": "score",
                "name": "mood",
                "instructions": "How upset is the customer?",
                "levels": [{"label": "calm"}, {"label": "annoyed"}, {"label": "furious"}]
            }
        ]
    })
}

/// `POST /v1/decisions` answers each question, in order, as the evidence says.
async fn assert_http_decisions(base_url: &str, model: &str) -> Result<(), anyhow::Error> {
    let (status, body) = post_decisions(base_url, &decisions_request(model)).await?;
    assert_eq!(status, StatusCode::OK, "{model}: {body}");
    let response: DecisionResponse = serde_json::from_str(&body)
        .map_err(|e| anyhow::anyhow!("{model}: the response is not a decision response: {e}"))?;

    assert_eq!(response.model, model);
    let [refund, team, mood] = response.answers.as_slice() else {
        anyhow::bail!(
            "{model}: expected three answers in question order, got {:?}",
            response.answers
        );
    };
    assert!(
        matches!(refund, DecisionAnswer::Predicate { name, probability }
            if name.as_deref() == Some("refund") && *probability > 0.5),
        "{model}: a customer asking for money back is asking for a refund: {refund:?}"
    );
    assert!(
        matches!(team, DecisionAnswer::Choice { name, choice, probabilities, .. }
            if name.as_deref() == Some("team")
                && *choice == ChoiceValue::String("billing".to_string())
                && probabilities.len() == 3),
        "{model}: a double charge is for billing: {team:?}"
    );
    assert!(
        matches!(mood, DecisionAnswer::Score { name, probabilities, .. }
            if name.as_deref() == Some("mood") && probabilities.len() == 3),
        "{model}: the score is over the three levels: {mood:?}"
    );
    Ok(())
}

/// The `ai_*` functions run the same decisions from SQL.
async fn assert_sql_decisions(rt: &Runtime, model: &str) -> Result<(), anyhow::Error> {
    assert_eq!(
        query_value(
            rt,
            &format!("SELECT ai_if('{REFUND}', 'asks for a refund', model => '{model}')")
        )
        .await?,
        "true"
    );
    assert_eq!(
        query_value(
            rt,
            &format!(
                "SELECT ai_if('Thank you, it arrived early and works perfectly.', \
                 'asks for a refund', model => '{model}')"
            )
        )
        .await?,
        "false"
    );
    assert_eq!(
        query_value(
            rt,
            &format!(
                "SELECT ai_classify('{REFUND}', ['billing', 'technical', 'account'], \
                 model => '{model}')"
            )
        )
        .await?,
        "billing"
    );
    let mood: f64 = query_value(
        rt,
        &format!(
            "SELECT ai_score('{FURIOUS}', 'How upset is the customer?', \
             ['calm', 'annoyed', 'furious'], model => '{model}')"
        ),
    )
    .await?
    .parse()?;
    assert!(
        (1.0..=2.0).contains(&mood),
        "{model}: an angry message scores annoyed or furious, not {mood}"
    );
    Ok(())
}

/// A provider's model, loaded from a Spicepod, answers decisions over HTTP and SQL.
#[rstest]
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn provider_answers_decisions(
    #[values(
        "openai_chat",
        "openai_decisions",
        "anthropic",
        "xai",
        "typesafe",
        "bedrock"
    )]
    label: &str,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let Some(provider) = PROVIDERS.iter().find(|provider| provider.label == label) else {
        // Bedrock is a cargo feature; a build without it has no such provider.
        return Ok(());
    };

    test_request_context()
        .scope(async {
            for secret in provider.secrets {
                verify_env_secret_exists(secret)
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            let (rt, base_url) = start(vec![(provider.build)(label)]).await?;
            assert_http_decisions(&base_url, label).await?;
            assert_sql_decisions(&rt, label).await
        })
        .await
}

/// Decisions name their model: with two loaded, each request is answered by the one it names,
/// and the response says which.
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn decisions_route_by_model_name() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            for secret in ["SPICE_OPENAI_API_KEY", "SPICE_ANTHROPIC_API_KEY"] {
                verify_env_secret_exists(secret)
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            let (rt, base_url) = start(vec![openai_chat("first"), anthropic("second")]).await?;
            assert_http_decisions(&base_url, "first").await?;
            assert_http_decisions(&base_url, "second").await?;
            assert_sql_decisions(&rt, "second").await
        })
        .await
}

/// A request the runtime refuses is refused before any provider is called, so these need
/// no credentials.
#[tokio::test]
async fn decisions_reject_what_the_runtime_cannot_route() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let (_rt, base_url) = start(vec![]).await?;

            let (status, body) =
                post_decisions(&base_url, &decisions_request("no_such_model")).await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

            let (status, body) = post_decisions(
                &base_url,
                &json!({"model": "m", "input": "x", "questions": [], "surprise": true}),
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            Ok(())
        })
        .await
}
