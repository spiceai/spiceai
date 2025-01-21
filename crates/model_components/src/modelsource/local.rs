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

use async_trait::async_trait;

use super::ModelSource;
use secrecy::{ExposeSecret, Secret, SecretString};
use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

pub struct Local {}
#[async_trait]
impl ModelSource for Local {
    async fn pull(&self, params: Arc<HashMap<String, SecretString>>) -> super::Result<String> {
        let name = params
            .get("name")
            .map(Secret::expose_secret)
            .map(ToString::to_string);

        let Some(name) = name else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "The 'name' parameter is required, and was not provided.",
            }
            .build());
        };

        // it is not copying local model into .spice folder
        let _ = super::ensure_model_path(name.as_str())?;

        let path = params
            .get("from")
            .map(Secret::expose_secret)
            .map(ToString::to_string);

        let Some(path) = path else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "The 'from' parameter is required, and was not provided.",
            }
            .build());
        };

        Ok(path.trim_start_matches("file:").to_string())
    }
}
