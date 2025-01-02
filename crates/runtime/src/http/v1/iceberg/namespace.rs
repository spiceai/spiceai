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

use serde::{ser::SerializeSeq, Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Namespace {
    pub parts: Vec<String>,
}

impl Namespace {
    pub fn from_encoded(encoded: &str) -> Self {
        let decoded_str = percent_encoding::percent_decode_str(encoded).decode_utf8_lossy();
        let parts = decoded_str
            .split('\u{1F}')
            .map(ToString::to_string)
            .collect();
        Self { parts }
    }

    pub fn from_single_part(part: String) -> Self {
        Self { parts: vec![part] }
    }

    pub fn from_parts(parts: Vec<String>) -> Self {
        Self { parts }
    }
}

impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let opt_str = Option::<String>::deserialize(deserializer)?;
        match opt_str {
            Some(encoded) => Ok(Namespace::from_encoded(&encoded)),
            None => Ok(Namespace::default()),
        }
    }
}

impl Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.parts.len()))?;
        for part in &self.parts {
            seq.serialize_element(part)?;
        }
        seq.end()
    }
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespacePath(String);

impl From<NamespacePath> for Namespace {
    fn from(path: NamespacePath) -> Self {
        // Since path is already URL-decoded by Axum, split directly
        Self {
            parts: path.0.split('\u{1F}').map(ToString::to_string).collect(),
        }
    }
}
