/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use crate::secrets;
use runtime_request_context::Extension;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SecretsContextExtension {
    secrets: Arc<RwLock<secrets::Secrets>>,
}

impl SecretsContextExtension {
    #[must_use]
    pub fn new(secrets: Arc<RwLock<secrets::Secrets>>) -> Self {
        Self { secrets }
    }

    #[must_use]
    pub fn secrets(&self) -> Arc<RwLock<secrets::Secrets>> {
        Arc::clone(&self.secrets)
    }
}

impl Extension for SecretsContextExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
