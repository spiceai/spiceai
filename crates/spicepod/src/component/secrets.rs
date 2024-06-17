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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::WithDependsOn;

/// The secrets configuration for a Spicepod.
///
/// Example:
/// ```yaml
/// secrets:
///   - store: file
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecretStore {
    pub store: SecretStoreType,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,
}

impl WithDependsOn<SecretStore> for SecretStore {
    fn depends_on(&self, depends_on: &[String]) -> SecretStore {
        SecretStore {
            store: self.store.clone(),
            params: self.params.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SecretStoreType {
    File,
    Env,
    Kubernetes,
    Keyring,
    #[serde(rename = "aws_secrets_manager")]
    AwsSecretsManager,
}
