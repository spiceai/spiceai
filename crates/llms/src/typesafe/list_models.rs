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

//! Model listing for `TypeSafe` (`GET /v1/models`).

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};
use std::collections::HashMap;
use std::fmt;

use crate::provider::{
    ListModels, ListModelsError, ListModelsResult, create_http_client, get_required_param,
    map_status_to_error,
};

use super::DEFAULT_BASE_URL;

const PROVIDER_NAME: &str = "TypeSafe";

#[derive(Debug, Deserialize)]
pub(super) struct ModelsResponse {
    /// `alias` accepts the OpenAI-style `{ "data": [...] }` envelope as well.
    #[serde(default, alias = "data")]
    models: Vec<ModelCard>,
}

impl ModelsResponse {
    /// Canonical model ids plus every listed alias, flattened for health checks.
    pub(super) fn into_names(self) -> Vec<String> {
        self.models
            .into_iter()
            .flat_map(|m| m.name.into_iter().chain(m.id).chain(m.alias.into_vec()))
            .collect()
    }
}

/// `GET /v1/models` may send `alias` as one string or `aliases` as an array.
#[derive(Debug, Default)]
struct AliasList(Vec<String>);

impl AliasList {
    fn into_vec(self) -> Vec<String> {
        self.0
    }
}

impl<'de> Deserialize<'de> for AliasList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AliasListVisitor;

        impl<'de> Visitor<'de> for AliasListVisitor {
            type Value = AliasList;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a model alias string or a list of alias strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(AliasList(vec![value.to_string()]))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(AliasList(vec![value]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut names = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(name) = seq.next_element()? {
                    names.push(name);
                }
                Ok(AliasList(names))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(AliasList::default())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(AliasList::default())
            }
        }

        deserializer.deserialize_any(AliasListVisitor)
    }
}

#[derive(Debug, Deserialize)]
struct ModelCard {
    /// A card may carry a display name, a canonical id, or both; treating them as one
    /// field makes the documented pair a duplicate and the listing undecodable.
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    id: Option<String>,
    /// Additional names callers may configure (e.g. `jev-latest`). Accepts a
    /// singular `alias` string or the documented `aliases` array.
    #[serde(default, alias = "aliases")]
    alias: AliasList,
}

/// `TypeSafe` model lister (`GET /v1/models`).
pub struct TypeSafeModelLister {
    api_key: SecretString,
    base_url: String,
}

impl TypeSafeModelLister {
    /// Required parameter: `typesafe_api_key`. Optional: `typesafe_endpoint`.
    ///
    /// # Errors
    ///
    /// Returns [`ListModelsError::MissingParameter`] when neither `typesafe_api_key`
    /// nor `typesafe_ai_api_key` is present in `params`.
    pub fn from_params(params: &HashMap<String, SecretString>) -> ListModelsResult<Self> {
        let api_key = match get_required_param(params, "typesafe_api_key") {
            Ok(k) => k,
            Err(_) => get_required_param(params, "typesafe_ai_api_key")?,
        };
        let base_url = params.get("typesafe_endpoint").map_or_else(
            || DEFAULT_BASE_URL.to_string(),
            |s| s.expose_secret().to_string(),
        );

        Ok(Self {
            api_key: api_key.clone(),
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }
}

#[async_trait]
impl ListModels for TypeSafeModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        let client = create_http_client().ok_or_else(|| ListModelsError::NetworkError {
            provider: PROVIDER_NAME.to_string(),
            message: "failed to build HTTP client".to_string(),
        })?;

        let response = client
            .get(format!("{}/v1/models", self.base_url))
            .bearer_auth(self.api_key.expose_secret())
            .send()
            .await
            .map_err(|e| ListModelsError::NetworkError {
                provider: PROVIDER_NAME.to_string(),
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(map_status_to_error(response.status(), PROVIDER_NAME));
        }

        let parsed: ModelsResponse =
            response
                .json()
                .await
                .map_err(|e| ListModelsError::NetworkError {
                    provider: PROVIDER_NAME.to_string(),
                    message: e.to_string(),
                })?;

        Ok(parsed.into_names())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_params_missing_key() {
        let params = HashMap::new();
        assert!(matches!(
            TypeSafeModelLister::from_params(&params),
            Err(ListModelsError::MissingParameter { .. })
        ));
    }

    /// Both the `TypeSafe` shape and the `OpenAI`-style envelope yield the name the
    /// health check looks for, whichever field carried it.
    #[test]
    fn models_response_accepts_either_envelope() {
        let native: ModelsResponse =
            serde_json::from_str(r#"{"models":[{"name":"jev-latest"}]}"#).expect("native");
        assert_eq!(native.into_names(), vec!["jev-latest".to_string()]);

        let openai_style: ModelsResponse =
            serde_json::from_str(r#"{"data":[{"id":"jev-1.13.0"}]}"#).expect("openai style");
        assert_eq!(openai_style.into_names(), vec!["jev-1.13.0".to_string()]);
    }

    #[test]
    fn into_names_includes_aliases() {
        let parsed: ModelsResponse = serde_json::from_value(serde_json::json!({
            "models": [
                {"id": "jev-1.13.0", "alias": ["jev-latest", "jev-preview"]}
            ]
        }))
        .expect("model card");
        let names = parsed.into_names();
        assert_eq!(
            names,
            vec![
                "jev-1.13.0".to_string(),
                "jev-latest".to_string(),
                "jev-preview".to_string()
            ]
        );
    }

    #[test]
    fn from_params_accepts_ai_api_key_alias() {
        let mut params = HashMap::new();
        params.insert(
            "typesafe_ai_api_key".to_string(),
            SecretString::from("sk-test"),
        );
        TypeSafeModelLister::from_params(&params).expect("alias key");
    }

    /// The documented listing shape uses `aliases`; dropping it makes the
    /// documented `jev-latest` alias look unavailable at startup.
    #[test]
    fn plural_aliases_are_listed() {
        let body = serde_json::json!({
            "models": [{"id": "jev-1.13.0", "aliases": ["jev-latest", "jev-preview"]}]
        });
        let parsed: ModelsResponse =
            serde_json::from_value(body).expect("the documented listing shape parses");
        let names = parsed.into_names();
        assert!(names.contains(&"jev-latest".to_string()), "{names:?}");
        assert!(names.contains(&"jev-1.13.0".to_string()), "{names:?}");
    }

    /// `GET /v1/models` may return a singular `alias` string rather than an array.
    #[test]
    fn singular_alias_string_is_listed() {
        let parsed: ModelsResponse =
            serde_json::from_str(r#"{"models":[{"id":"jev-1.13.0","alias":"jev-latest"}]}"#)
                .expect("a singular alias string parses");
        let names = parsed.into_names();
        assert_eq!(
            names,
            vec!["jev-1.13.0".to_string(), "jev-latest".to_string()]
        );
    }

    /// The documented card carries `name` and `id` together; `alias = "id"` makes
    /// them one field, so serde rejects the pair and the listing never decodes.
    #[test]
    fn a_card_carrying_both_name_and_id_decodes() {
        let body = serde_json::json!({
            "models": [{"name": "jev-latest", "id": "jev-1.13.0"}]
        });
        let parsed: ModelsResponse =
            serde_json::from_value(body).expect("the documented card shape parses");
        let names = parsed.into_names();
        assert!(names.contains(&"jev-latest".to_string()), "{names:?}");
        assert!(names.contains(&"jev-1.13.0".to_string()), "{names:?}");
    }
}
