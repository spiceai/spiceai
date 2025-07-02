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

pub mod embed;

use aws_config::SdkConfig;

#[derive(Debug, Clone)]
pub struct BedrockClient {
    pub(crate) client: aws_sdk_bedrockruntime::Client,
}

impl BedrockClient {
    #[must_use]
    pub fn new(config: &SdkConfig) -> Self {
        let client = aws_sdk_bedrockruntime::Client::new(config);
        Self { client }
    }
}
