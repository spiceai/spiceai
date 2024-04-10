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

use async_trait::async_trait;

use super::{Secret, SecretStore};

#[allow(clippy::module_name_repetitions)]
pub struct AwsSecretsManager {}

impl Default for AwsSecretsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsSecretsManager {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SecretStore for AwsSecretsManager {
    #[must_use]
    async fn get_secret(&self, _secret_name: &str) -> Option<Secret> {
        None
    }
}
