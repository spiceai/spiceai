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
#![allow(clippy::missing_errors_doc)]

mod chat;
mod embed;

use secrecy::{ExposeSecret, SecretString};

use crate::google::embed::EmbedGoogle;

#[derive(Debug)]
pub struct Google {
    client: google_genai::Client,
    model: String,
}

impl Google {
    pub fn new(api_key: &SecretString, model: &str) -> Result<Self, google_genai::Error> {
        Ok(Self {
            client: google_genai::Client::new(api_key.expose_secret().to_string())?,
            model: model.to_string(),
        })
    }

    pub fn new_embeddings(
        api_key: SecretString,
        model: &str,
        dimensions: Option<u32>,
    ) -> Result<EmbedGoogle, google_genai::Error> {
        Ok(EmbedGoogle {
            g: Self::new(api_key, model)?,
            dimensions,
        })
    }
}
