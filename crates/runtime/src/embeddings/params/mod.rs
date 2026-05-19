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

pub mod azure;
pub mod bedrock;
pub mod databricks;
pub mod file;
pub mod google;
pub mod huggingface;
pub mod model2vec;
pub mod openai;

pub use crate::parameters::ParameterSpec;
use spicepod::component::embeddings::EmbeddingPrefix;

/// Returns the parameter specifications for a given embedding source.
#[must_use]
pub fn get_params_spec(source: &EmbeddingPrefix) -> &'static [ParameterSpec] {
    match source {
        EmbeddingPrefix::OpenAi => openai::PARAMETERS,
        EmbeddingPrefix::Azure => azure::PARAMETERS,
        EmbeddingPrefix::Google => google::PARAMETERS,
        EmbeddingPrefix::HuggingFace => huggingface::PARAMETERS,
        EmbeddingPrefix::Databricks => databricks::PARAMETERS,
        EmbeddingPrefix::Bedrock => bedrock::PARAMETERS,
        EmbeddingPrefix::File => file::PARAMETERS,
        EmbeddingPrefix::Model2Vec => model2vec::PARAMETERS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::component::embeddings::EmbeddingPrefix;

    #[test]
    fn test_openai_params_includes_api_key() {
        let params = get_params_spec(&EmbeddingPrefix::OpenAi);
        assert!(params.iter().any(|p| p.name == "api_key"));
    }

    #[test]
    fn test_openai_params_includes_endpoint() {
        let params = get_params_spec(&EmbeddingPrefix::OpenAi);
        assert!(params.iter().any(|p| p.name == "endpoint"));
    }

    #[test]
    fn test_azure_params_includes_api_key() {
        let params = get_params_spec(&EmbeddingPrefix::Azure);
        assert!(params.iter().any(|p| p.name == "api_key"));
    }

    #[test]
    fn test_azure_params_includes_endpoint() {
        let params = get_params_spec(&EmbeddingPrefix::Azure);
        assert!(params.iter().any(|p| p.name == "endpoint"));
    }

    #[test]
    fn test_google_params_includes_api_key() {
        let params = get_params_spec(&EmbeddingPrefix::Google);
        assert!(params.iter().any(|p| p.name == "api_key"));
    }

    #[test]
    fn test_huggingface_params_includes_hf_token() {
        let params = get_params_spec(&EmbeddingPrefix::HuggingFace);
        assert!(params.iter().any(|p| p.name == "hf_token"));
    }

    #[test]
    fn test_databricks_params_includes_endpoint() {
        let params = get_params_spec(&EmbeddingPrefix::Databricks);
        assert!(params.iter().any(|p| p.name == "endpoint"));
    }

    #[test]
    fn test_databricks_params_includes_token() {
        let params = get_params_spec(&EmbeddingPrefix::Databricks);
        assert!(params.iter().any(|p| p.name == "token"));
    }

    #[test]
    fn test_bedrock_params_includes_dimensions() {
        let params = get_params_spec(&EmbeddingPrefix::Bedrock);
        assert!(params.iter().any(|p| p.name == "dimensions"));
    }

    #[test]
    fn test_all_providers_return_non_empty_params() {
        let providers = vec![
            EmbeddingPrefix::OpenAi,
            EmbeddingPrefix::Azure,
            EmbeddingPrefix::Google,
            EmbeddingPrefix::HuggingFace,
            EmbeddingPrefix::Databricks,
            EmbeddingPrefix::Bedrock,
            EmbeddingPrefix::File,
            EmbeddingPrefix::Model2Vec,
        ];
        for provider in providers {
            let params = get_params_spec(&provider);
            assert!(
                !params.is_empty(),
                "Provider should have at least one param"
            );
        }
    }
}
