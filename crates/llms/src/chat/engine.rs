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

//! Engines that serve a local model's weights in place of mistral.rs, registered by the
//! crates that implement them.
//!
//! An engine crate registers itself with [`register_local_chat_engine!`], and a binary
//! that links the crate gets the engine; `llms` itself never depends on one. A model is
//! loaded by the first registered engine that serves its weight files, and by mistral.rs when
//! none does. A model distributed over several nodes goes to that engine only when it splits
//! models of the architecture the weights name ([`LocalChatEngine::splits`]), and to
//! mistral.rs's ring otherwise.

use std::path::PathBuf;
use std::sync::Arc;

use futures::future::BoxFuture;

use super::{Chat, LocalModelOptions, Result};

/// Loads a model from local weight files.
pub type LoadLocalModel =
    for<'a> fn(&'a [PathBuf], LocalModelOptions<'a>) -> BoxFuture<'a, Result<Arc<dyn Chat>>>;

/// An engine for local model weights.
pub struct LocalChatEngine {
    /// The engine's name, as logs and errors give it.
    pub name: &'static str,
    /// Whether the engine serves a single-node model with these weight files. It sees file
    /// names only, so a remote model can be matched before its files are downloaded.
    pub serves: fn(&[PathBuf]) -> bool,
    /// Whether the engine also serves a model split across several nodes, with the topology in
    /// [`LocalModelOptions::distributed`], given the architecture its first shard's header names
    /// (`general.architecture`); `None` when it serves no model split across nodes. A model the
    /// engine does not split goes to mistral.rs's ring.
    pub distributed: Option<fn(&str) -> bool>,
    /// Loads the model from local weight files.
    pub load: LoadLocalModel,
}

impl LocalChatEngine {
    /// Whether the engine serves a model split across nodes whose first shard's header names
    /// `architecture`. Every node decides from that header, so all of them decide alike. A node
    /// that cannot read it, one that does not hold the weights, can only hold a share for the
    /// node that serves the model: mistral.rs's ring reads the weights on every node.
    #[must_use]
    pub fn splits(&self, architecture: Option<&str>) -> bool {
        match (self.distributed, architecture) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(splits), Some(architecture)) => splits(architecture),
        }
    }

    /// Whether the engine serves the model of local GGUF `weights` split across nodes:
    /// [`Self::splits`] with the architecture their first shard's header names, when this node
    /// holds it.
    pub async fn splits_weights(&self, weights: &[PathBuf]) -> bool {
        if self.distributed.is_none() {
            return false;
        }
        self.splits(
            super::gguf_header::local_architecture(weights)
                .await
                .as_deref(),
        )
    }
}

#[linkme::distributed_slice]
pub static LOCAL_CHAT_ENGINES: [LocalChatEngine];

/// The registered engine that serves these weights, if any.
#[must_use]
pub fn engine_for(weights: &[PathBuf]) -> Option<&'static LocalChatEngine> {
    LOCAL_CHAT_ENGINES
        .iter()
        .find(|engine| (engine.serves)(weights))
}

/// Registers a [`LocalChatEngine`] as the static `$name`.
///
/// The registration is a `linkme` distributed-slice entry, which the linker keeps only when
/// the engine's crate is linked: a binary that should serve with the engine names the
/// crate (`use llms_llamacpp as _;`), as `spiced` does behind its feature.
#[macro_export]
macro_rules! register_local_chat_engine {
    ($name:ident, $engine:expr $(,)?) => {
        #[$crate::linkme::distributed_slice($crate::chat::engine::LOCAL_CHAT_ENGINES)]
        #[linkme(crate = $crate::linkme)]
        static $name: $crate::chat::engine::LocalChatEngine = $engine;
    };
}
