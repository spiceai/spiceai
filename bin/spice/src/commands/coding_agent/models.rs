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

//! Which of the runtime's models an agent starts on, once it can serve.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use runtime_api_types::v1::{ComponentStatus, ModelInfo, ModelListResponse};

use super::agent::{Agent, Limits, RequestedModel};
use crate::context::RuntimeContext;
use crate::error::{
    CodingAgentModelFailedSnafu, CodingAgentModelNotFoundSnafu, CodingAgentModelTimeoutSnafu,
    CodingAgentNoModelsSnafu, InvalidResponseSnafu, InvalidUsageSnafu, Result, UnauthorizedSnafu,
};

/// How long the runtime has to finish loading the agent's model. Generous,
/// because a local model may be downloaded first; the wait reports progress
/// and the user can interrupt it.
const LOAD_BUDGET: Duration = Duration::from_mins(30);

/// How often the wait says what it is still waiting for.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(15);

const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// The models the runtime serves, and the one the agent starts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selection {
    pub model: String,
    /// Every model that is ready, `model` included.
    pub available: Vec<String>,
    /// Whether every model in `available` answers the Messages API natively
    /// rather than through a translation into chat completions, which cannot
    /// carry every Messages feature (deferred tool loading, for one).
    pub native_messages: bool,
    /// The token limits of each model in `available` that reports any.
    pub limits: HashMap<String, Limits>,
}

/// Whether the runtime serves `model`'s Messages API natively: only a model
/// whose provider is Anthropic does, and the runtime reports the provider as
/// the start of `owned_by` (the model's `from`).
fn serves_messages_natively(model: &ModelInfo) -> bool {
    model.owned_by == "anthropic" || model.owned_by.starts_with("anthropic:")
}

/// How a model's state reads for the agent waiting on it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Readiness {
    Ready,
    Loading,
    Failed(String),
}

fn readiness(model: &ModelInfo) -> Readiness {
    match &model.status {
        // A runtime that does not report status serves what it lists.
        None | Some(ComponentStatus::Ready | ComponentStatus::Refreshing) => Readiness::Ready,
        Some(ComponentStatus::Initializing | ComponentStatus::NotLoaded) => Readiness::Loading,
        Some(ComponentStatus::Error(message)) => Readiness::Failed(
            model
                .error_message
                .as_deref()
                .or(message.as_deref())
                // The message is quoted mid-sentence, which supplies its own stop.
                .map(|message| message.trim().trim_end_matches('.').to_string())
                .filter(|message| !message.is_empty())
                .unwrap_or_else(|| "the runtime reported an error".to_string()),
        ),
        Some(other) => Readiness::Failed(format!("the model is {other}")),
    }
}

/// Wait until the runtime can serve the agent, and choose its model.
///
/// With `requested`, that model is waited on and every other ready model is
/// offered alongside it. Without, the runtime is waited on until no model is
/// still loading; one ready model is chosen outright, and several are offered
/// in a picker when there is a terminal to show it on.
///
/// # Errors
///
/// Returns an error when the runtime rejects the request, the model is unknown
/// or fails to load, nothing is ready in time, or a choice is needed and there
/// is no terminal to make it on.
pub(crate) async fn select(
    ctx: &RuntimeContext,
    agent: Agent,
    requested: &RequestedModel,
    interactive: bool,
) -> Result<Selection> {
    let settled = settle(
        ctx,
        requested.spice_model(),
        |models| match misspelled_model(agent, requested, models) {
            Some(message) => InvalidUsageSnafu { message }.fail(),
            None => Ok(()),
        },
    )
    .await?;
    match settled {
        Settled::Ready(selection) => Ok(selection),
        Settled::Choose(choices) => choose(choices, interactive, agent),
    }
}

/// Wait until no model is still loading and at least one is ready: what a
/// launcher that starts many agents, each choosing its own model, waits on.
///
/// # Errors
///
/// Returns an error when the runtime rejects the request, has no model, every
/// model fails to load, or loading takes longer than the budget.
pub(crate) async fn wait_until_ready(ctx: &RuntimeContext) -> Result<()> {
    settle(ctx, None, |_| Ok(())).await.map(drop)
}

/// What the runtime's models came to once none the agent waits on is loading.
enum Settled {
    Ready(Selection),
    Choose(Choices),
}

/// Poll the runtime's models, checking each listing with `check`, until
/// [`decide`] stops waiting, reporting progress, and failing once loading
/// outlasts [`LOAD_BUDGET`].
async fn settle(
    ctx: &RuntimeContext,
    requested: Option<&str>,
    check: impl Fn(&[ModelInfo]) -> Result<()>,
) -> Result<Settled> {
    let deadline = Instant::now() + LOAD_BUDGET;
    let mut next_progress = Instant::now() + PROGRESS_INTERVAL;
    loop {
        let models = list(ctx).await?;
        check(&models)?;
        match decide(&models, requested) {
            Decision::Ready(selection) => return Ok(Settled::Ready(selection)),
            Decision::Choose(choices) => return Ok(Settled::Choose(choices)),
            Decision::Wait(waiting) => {
                if Instant::now() >= deadline {
                    return Err(CodingAgentModelTimeoutSnafu {
                        models: waiting.join(", "),
                        minutes: LOAD_BUDGET.as_secs() / 60,
                    }
                    .build());
                }
                if Instant::now() >= next_progress {
                    tracing::info!(
                        "Waiting for the Spice runtime to load {}...",
                        quote_list(&waiting)
                    );
                    next_progress = Instant::now() + PROGRESS_INTERVAL;
                }
            }
            Decision::Fail(error) => return Err(error),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[derive(Debug)]
enum Decision {
    Ready(Selection),
    /// More than one model is ready and none was named.
    Choose(Choices),
    /// These models are still loading.
    Wait(Vec<String>),
    Fail(crate::error::Error),
}

fn decide(models: &[ModelInfo], requested: Option<&str>) -> Decision {
    let ready_models: Vec<&ModelInfo> = models
        .iter()
        .filter(|model| readiness(model) == Readiness::Ready)
        .collect();
    let native_messages = !ready_models.is_empty()
        && ready_models
            .iter()
            .all(|model| serves_messages_natively(model));
    let ready: Vec<String> = ready_models.iter().map(|model| model.id.clone()).collect();
    let limits: HashMap<String, Limits> = ready_models
        .iter()
        .map(|model| {
            let limits = Limits {
                context_window: model.context_window,
                max_output_tokens: model.max_output_tokens,
            };
            (model.id.clone(), limits)
        })
        .filter(|(_, limits)| *limits != Limits::default())
        .collect();

    if let Some(requested) = requested {
        let Some(model) = models.iter().find(|model| model.id == requested) else {
            return Decision::Fail(
                CodingAgentModelNotFoundSnafu {
                    model: requested.to_string(),
                    available: describe_available(models),
                }
                .build(),
            );
        };
        return match readiness(model) {
            Readiness::Ready => Decision::Ready(Selection {
                model: requested.to_string(),
                available: ready,
                native_messages,
                limits,
            }),
            Readiness::Loading => Decision::Wait(vec![requested.to_string()]),
            Readiness::Failed(message) => Decision::Fail(
                CodingAgentModelFailedSnafu {
                    model: requested.to_string(),
                    message,
                }
                .build(),
            ),
        };
    }

    let loading: Vec<String> = models
        .iter()
        .filter(|model| readiness(model) == Readiness::Loading)
        .map(|model| model.id.clone())
        .collect();
    if !loading.is_empty() {
        return Decision::Wait(loading);
    }

    match ready.as_slice() {
        [] => {
            // Every model failed, or none is configured.
            if let Some(failed) = models.first() {
                let Readiness::Failed(message) = readiness(failed) else {
                    return Decision::Fail(CodingAgentNoModelsSnafu.build());
                };
                return Decision::Fail(
                    CodingAgentModelFailedSnafu {
                        model: failed.id.clone(),
                        message,
                    }
                    .build(),
                );
            }
            Decision::Fail(CodingAgentNoModelsSnafu.build())
        }
        [only] => Decision::Ready(Selection {
            model: only.clone(),
            available: ready.clone(),
            native_messages,
            limits,
        }),
        _ => Decision::Choose(Choices {
            available: ready,
            native_messages,
            limits,
        }),
    }
}

/// What to tell a user whose model option names a Spice model without the
/// provider the agent requires, which the agent would not read as that model.
fn misspelled_model(
    agent: Agent,
    requested: &RequestedModel,
    models: &[ModelInfo],
) -> Option<String> {
    let RequestedModel::AgentOwned(value) = requested else {
        return None;
    };
    let model = agent.without_provider(value)?;
    models.iter().any(|listed| listed.id == model).then(|| {
        format!(
            "{} names a model with its provider, so '{value}' names no Spice model. Rerun with `spice {} {}`.",
            agent.display_name(),
            agent.command(),
            agent.model_argument(model)
        )
    })
}

/// Ready models for the user to choose between.
#[derive(Debug)]
struct Choices {
    available: Vec<String>,
    native_messages: bool,
    limits: HashMap<String, Limits>,
}

fn choose(choices: Choices, interactive: bool, agent: Agent) -> Result<Selection> {
    let Choices {
        available,
        native_messages,
        limits,
    } = choices;
    if !interactive {
        let example = available.first().map_or_else(String::new, |model| {
            format!(
                ", as in `spice {} {}`",
                agent.command(),
                agent.model_argument(model)
            )
        });
        return Err(InvalidUsageSnafu {
            message: format!(
                "The Spice runtime serves several models ({}). Choose one{example}.",
                available.join(", ")
            ),
        }
        .build());
    }
    let index = dialoguer::Select::with_theme(&dialoguer::theme::ColorfulTheme::default())
        .with_prompt("Select a Spice model")
        .items(&available)
        .default(0)
        .interact()
        .map_err(|error| match error {
            dialoguer::Error::IO(source) if source.kind() == std::io::ErrorKind::Interrupted => {
                crate::error::Error::Interrupted
            }
            dialoguer::Error::IO(source) => crate::error::Error::RuntimeExecution { source },
        })?;
    Ok(Selection {
        model: available[index].clone(),
        available,
        native_messages,
        limits,
    })
}

async fn list(ctx: &RuntimeContext) -> Result<Vec<ModelInfo>> {
    let response = ctx.get("/v1/models?status=true").await?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(UnauthorizedSnafu.build());
    }
    let response = crate::error::check_response(response, ctx.http_endpoint()).await?;
    let models: ModelListResponse = response.json().await.map_err(|error| {
        InvalidResponseSnafu {
            message: format!("Failed to parse the runtime's model list: {error}"),
        }
        .build()
    })?;
    Ok(models.data)
}

fn describe_available(models: &[ModelInfo]) -> String {
    if models.is_empty() {
        "none".to_string()
    } else {
        models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn quote_list(models: &[String]) -> String {
    let quoted: Vec<String> = models
        .iter()
        .map(|model| format!("model '{model}'"))
        .collect();
    quoted.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(id: &str, status: Option<ComponentStatus>) -> ModelInfo {
        ModelInfo {
            id: id.to_string(),
            object: "model".to_string(),
            owned_by: "openai:gpt-4o".to_string(),
            datasets: None,
            status,
            error: None,
            error_message: None,
            metadata: None,
            context_window: None,
            max_output_tokens: None,
        }
    }

    fn ready(selection: Decision) -> Selection {
        match selection {
            Decision::Ready(selection) => selection,
            other => panic!("expected a ready selection, got {other:?}"),
        }
    }

    #[test]
    fn the_only_ready_model_is_chosen() {
        let models = [model("qwen", Some(ComponentStatus::Ready))];
        assert_eq!(
            ready(decide(&models, None)),
            Selection {
                model: "qwen".to_string(),
                available: vec!["qwen".to_string()],
                native_messages: false,
                limits: HashMap::new(),
            }
        );
    }

    #[test]
    fn messages_are_native_only_when_every_ready_model_is_anthropic() {
        let mut claude = model("claude", Some(ComponentStatus::Ready));
        claude.owned_by = "anthropic:claude-sonnet-5".to_string();
        assert!(ready(decide(std::slice::from_ref(&claude), None)).native_messages);

        // One translated model is enough: the agent can switch to it mid-session.
        let models = [claude, model("gpt", Some(ComponentStatus::Ready))];
        assert!(!ready(decide(&models, Some("claude"))).native_messages);

        // `owned_by` is the model's `from`; a name that merely starts alike is not Anthropic.
        let mut lookalike = model("x", Some(ComponentStatus::Ready));
        lookalike.owned_by = "anthropic-proxy:claude".to_string();
        assert!(!ready(decide(&[lookalike], None)).native_messages);
    }

    #[test]
    fn the_windows_the_runtime_reports_are_kept_for_the_ready_models() {
        let mut sized = model("qwen", Some(ComponentStatus::Ready));
        sized.context_window = Some(40_960);
        let mut loading = model("big", Some(ComponentStatus::Initializing));
        loading.context_window = Some(1_000_000);
        let selection = ready(decide(
            &[sized, loading, model("gpt", Some(ComponentStatus::Ready))],
            Some("qwen"),
        ));
        assert_eq!(
            selection.limits,
            HashMap::from([(
                "qwen".to_string(),
                Limits {
                    context_window: Some(40_960),
                    max_output_tokens: None,
                }
            )]),
            "a model reporting no limit, or not ready, contributes none"
        );
    }

    #[test]
    fn several_ready_models_need_a_choice() {
        let models = [
            model("qwen", Some(ComponentStatus::Ready)),
            model("gpt", Some(ComponentStatus::Ready)),
        ];
        let Decision::Choose(choices) = decide(&models, None) else {
            panic!("two ready models must need a choice");
        };
        assert_eq!(choices.available, vec!["qwen", "gpt"]);
        let error = choose(choices, false, Agent::Codex).expect_err("no terminal to choose on");
        assert_eq!(
            error.to_string(),
            "Invalid argument: The Spice runtime serves several models (qwen, gpt). Choose one, as in `spice codex --model qwen`."
        );
        // In the spelling of the agent's own model option.
        let Decision::Choose(choices) = decide(&models, None) else {
            panic!("two ready models must need a choice");
        };
        let error = choose(choices, false, Agent::OpenCode).expect_err("no terminal to choose on");
        assert_eq!(
            error.to_string(),
            "Invalid argument: The Spice runtime serves several models (qwen, gpt). Choose one, as in `spice opencode -m spice/qwen`."
        );
    }

    #[test]
    fn a_spice_model_named_without_its_provider_is_refused_with_the_fix() {
        let models = [
            model("qwen", Some(ComponentStatus::Ready)),
            model("gpt", Some(ComponentStatus::Initializing)),
        ];
        let owned = |value: &str| RequestedModel::AgentOwned(value.to_string());
        assert_eq!(
            misspelled_model(Agent::OpenCode, &owned("qwen"), &models).as_deref(),
            Some(
                "OpenCode names a model with its provider, so 'qwen' names no Spice model. Rerun with `spice opencode -m spice/qwen`."
            )
        );
        // A model still loading is a Spice model too.
        assert_eq!(
            misspelled_model(Agent::Pi, &owned("gpt:high"), &models).as_deref(),
            Some(
                "pi names a model with its provider, so 'gpt:high' names no Spice model. Rerun with `spice pi --model spice/gpt`."
            )
        );
        // Not a Spice model, another provider's model, or an agent that takes bare names.
        assert_eq!(
            misspelled_model(Agent::OpenCode, &owned("llama"), &models),
            None
        );
        assert_eq!(
            misspelled_model(Agent::OpenCode, &owned("anthropic/qwen"), &models),
            None
        );
        assert_eq!(
            misspelled_model(Agent::Codex, &owned("qwen"), &models),
            None
        );
        assert_eq!(
            misspelled_model(Agent::OpenCode, &RequestedModel::Unspecified, &models),
            None
        );
    }

    #[test]
    fn a_loading_model_is_waited_on_before_anything_is_chosen() {
        let models = [
            model("qwen", Some(ComponentStatus::Ready)),
            model("big", Some(ComponentStatus::Initializing)),
        ];
        // Choosing now would hide `big` from the agent's model picker.
        let Decision::Wait(waiting) = decide(&models, None) else {
            panic!("a loading model must be waited on");
        };
        assert_eq!(waiting, vec!["big"]);
    }

    #[test]
    fn a_requested_model_is_waited_on_alone() {
        let models = [
            model("qwen", Some(ComponentStatus::Initializing)),
            model("big", Some(ComponentStatus::Initializing)),
        ];
        let Decision::Wait(waiting) = decide(&models, Some("qwen")) else {
            panic!("the requested model must be waited on");
        };
        assert_eq!(waiting, vec!["qwen"]);

        let models = [
            model("qwen", Some(ComponentStatus::Ready)),
            model("big", Some(ComponentStatus::Initializing)),
        ];
        assert_eq!(
            ready(decide(&models, Some("qwen"))),
            Selection {
                model: "qwen".to_string(),
                available: vec!["qwen".to_string()],
                native_messages: false,
                limits: HashMap::new(),
            }
        );
    }

    #[test]
    fn an_unknown_model_names_the_ones_that_exist() {
        let models = [model("qwen", Some(ComponentStatus::Ready))];
        let Decision::Fail(error) = decide(&models, Some("gpt")) else {
            panic!("an unknown model must fail");
        };
        assert_eq!(
            error.to_string(),
            "Failed to start the coding agent: the Spice runtime has no model 'gpt'. Available models: qwen. Pass one of them with --model, or add the model to the Spicepod the runtime serves. See: https://spiceai.org/docs/components/models"
        );
    }

    #[test]
    fn a_failed_model_reports_why() {
        let mut failed = model("qwen", Some(ComponentStatus::Error(None)));
        failed.error_message = Some("invalid API key".to_string());
        let Decision::Fail(error) = decide(&[failed], None) else {
            panic!("a failed model must fail");
        };
        assert_eq!(
            error.to_string(),
            "Failed to load model 'qwen', so the coding agent has no model to use. Cause: invalid API key. Check the model's configuration in spicepod.yaml and the runtime log. See: https://spiceai.org/docs/components/models"
        );
    }

    #[test]
    fn no_models_says_how_to_add_one() {
        let Decision::Fail(error) = decide(&[], None) else {
            panic!("no models must fail");
        };
        assert_eq!(
            error.to_string(),
            "The Spice runtime serves no models, so the coding agent has nothing to use. Add one to spicepod.yaml, for example `spice model add llm --from openai:gpt-4o-mini`, and rerun. See: https://spiceai.org/docs/components/models"
        );
    }
}
