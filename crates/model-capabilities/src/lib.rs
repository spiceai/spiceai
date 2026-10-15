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

//! What each model can serve, and the gate every serving path asks.
//!
//! A model's capabilities start from its provider's default, and a rule on its model id
//! can only add to them. Loading builds one handler for each capability a model has, and
//! every path that serves a model asks [`require`] for the handler it needs.

use std::collections::HashMap;
use std::hash::BuildHasher;

use spicepod::component::model::{Model, ModelSource};

/// A kind of request a model can serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Chat completions: `/v1/chat/completions`, NSQL, `ai()`, workers and chat-based rerank.
    Chat,
    /// The `OpenAI` Responses API: `/v1/responses`.
    Responses,
    /// Decisions: `/v1/decisions`, the SQL decision functions and decision-based rerank.
    Decisions,
}

/// How a model answers decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionsBackend {
    /// The provider's own decisions API, such as `TypeSafe` Jev or `OpenAI`'s Decisions API.
    Native,
    /// A prompt sent through the model's chat completions.
    ViaChat,
}

/// What a model can serve.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelCapabilities {
    provider: ModelSource,
    chat: bool,
    responses: bool,
    native_decisions: bool,
}

/// What a model-id rule adds to its provider's default. It has no way to remove a
/// capability, so no model id can take away what its provider serves.
#[derive(Default)]
struct Additions {
    native_decisions: bool,
}

impl ModelCapabilities {
    /// The capabilities of a Spicepod model, or `None` when its provider is unknown.
    #[must_use]
    pub fn of(model: &Model) -> Option<Self> {
        let provider = model.get_source()?;
        let additions = model_id_additions(&provider, model.get_model_id().as_deref());
        Some(provider_default(provider).with(additions))
    }

    /// The provider that serves the model.
    #[must_use]
    pub fn provider(&self) -> &ModelSource {
        &self.provider
    }

    #[must_use]
    pub fn has(&self, capability: Capability) -> bool {
        match capability {
            Capability::Chat => self.chat,
            Capability::Responses => self.responses,
            Capability::Decisions => self.decisions().is_some(),
        }
    }

    /// How the model answers decisions: through its provider's decisions API when it has
    /// one, else through chat. `None` for a model with neither.
    #[must_use]
    pub fn decisions(&self) -> Option<DecisionsBackend> {
        if self.native_decisions {
            Some(DecisionsBackend::Native)
        } else if self.chat {
            Some(DecisionsBackend::ViaChat)
        } else {
            None
        }
    }

    fn with(mut self, additions: Additions) -> Self {
        self.native_decisions |= additions.native_decisions;
        self
    }
}

/// What every model of a provider serves. The match has no wildcard arm, so a new
/// provider does not compile until it states its capabilities.
fn provider_default(provider: ModelSource) -> ModelCapabilities {
    match provider {
        ModelSource::OpenAi | ModelSource::Azure | ModelSource::Xai => ModelCapabilities {
            provider,
            chat: true,
            responses: true,
            native_decisions: false,
        },
        ModelSource::Anthropic
        | ModelSource::Bedrock
        | ModelSource::Databricks
        | ModelSource::File
        | ModelSource::Google
        | ModelSource::HuggingFace
        | ModelSource::SpiceAI => ModelCapabilities {
            provider,
            chat: true,
            responses: false,
            native_decisions: false,
        },
        ModelSource::TypeSafe => ModelCapabilities {
            provider,
            chat: false,
            responses: false,
            native_decisions: true,
        },
    }
}

/// What a model id adds to its provider's default.
fn model_id_additions(provider: &ModelSource, model_id: Option<&str>) -> Additions {
    match (provider, model_id) {
        (ModelSource::OpenAi, Some(id)) if is_openai_decision_model_id(id) => Additions {
            native_decisions: true,
        },
        _ => Additions::default(),
    }
}

/// `OpenAI`'s decision model: `gpt-6-luna`, or a snapshot such as `gpt-6-luna-2026-10-01`,
/// but not an id that only starts the same way, such as `gpt-6-lunar`.
fn is_openai_decision_model_id(model_id: &str) -> bool {
    model_id
        .strip_prefix("gpt-6-luna")
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('-'))
}

/// Every loaded model's capabilities, by model name.
pub type CapabilityStore = HashMap<String, ModelCapabilities>;

/// Why a model has no handler for a capability.
#[derive(Clone, Debug, PartialEq)]
pub enum Refusal {
    /// No loaded model has this name.
    NotFound,
    /// The model does not have the capability. Holds the capabilities it has.
    Unsupported(ModelCapabilities),
    /// The model has the capability, but its handler failed to load.
    Unavailable,
}

/// The handler that serves `capability` for `model`.
///
/// Loading builds a handler for each capability a model has, so `handlers` holds one
/// exactly when the model has the capability and the handler loaded. A handler
/// registered without capabilities, such as a worker's, is served like any other.
///
/// # Errors
///
/// Returns why there is no handler, told apart by `capabilities`.
pub fn require<H: Clone, S: BuildHasher, T: BuildHasher>(
    capabilities: &HashMap<String, ModelCapabilities, S>,
    handlers: &HashMap<String, H, T>,
    model: &str,
    capability: Capability,
) -> Result<H, Refusal> {
    if let Some(handler) = handlers.get(model) {
        return Ok(handler.clone());
    }
    match capabilities.get(model) {
        None => Err(Refusal::NotFound),
        Some(found) if found.has(capability) => Err(Refusal::Unavailable),
        Some(found) => Err(Refusal::Unsupported(found.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities(from: &str) -> ModelCapabilities {
        ModelCapabilities::of(&Model::new(from, "model")).expect("a known provider")
    }

    #[test]
    fn only_luna_and_its_snapshots_add_native_decisions() {
        for (from, decisions) in [
            ("openai:gpt-6-luna", DecisionsBackend::Native),
            ("openai:gpt-6-luna-2026-10-01", DecisionsBackend::Native),
            ("openai:gpt-6-lunar", DecisionsBackend::ViaChat),
            ("openai:gpt-6-lunatic-preview", DecisionsBackend::ViaChat),
            ("openai:gpt-4o-mini", DecisionsBackend::ViaChat),
            ("azure:gpt-6-luna", DecisionsBackend::ViaChat),
        ] {
            let found = capabilities(from);
            assert_eq!(found.decisions(), Some(decisions), "{from}");
            assert!(
                found.has(Capability::Chat),
                "an id rule never removes chat: {from}"
            );
        }
    }

    #[test]
    fn require_tells_a_missing_handler_apart() {
        let store = CapabilityStore::from([
            ("jev".to_string(), capabilities("typesafe:jev")),
            ("mini".to_string(), capabilities("openai:gpt-4o-mini")),
        ]);
        let handlers: HashMap<String, &str> = HashMap::from([
            ("mini".to_string(), "mini handler"),
            ("worker".to_string(), "worker handler"),
        ]);

        assert_eq!(
            [
                require(&store, &handlers, "mini", Capability::Chat),
                require(&store, &handlers, "worker", Capability::Chat),
                require(&store, &handlers, "jev", Capability::Chat),
                require(&store, &handlers, "jev", Capability::Decisions),
                require(&store, &handlers, "absent", Capability::Chat),
            ],
            [
                Ok("mini handler"),
                Ok("worker handler"),
                Err(Refusal::Unsupported(capabilities("typesafe:jev"))),
                Err(Refusal::Unavailable),
                Err(Refusal::NotFound),
            ]
        );
    }
}
