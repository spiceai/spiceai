/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use crate::{model::builtin_scorer, Runtime};

impl Runtime {
    #[allow(clippy::implicit_hasher)]
    pub(crate) async fn load_eval_scorer(&self) {
        let mut scorers = self.eval_scorers.write().await;
        for (name, scorer) in builtin_scorer() {
            scorers.insert(name.to_string(), Arc::clone(&scorer));
            tracing::debug!("Successfully loaded eval scorer {name}");
        }
    }
}
