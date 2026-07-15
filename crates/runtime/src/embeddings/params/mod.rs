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
    use crate::parameters::Parameters;
    use runtime_secrets::Secrets;
    use spicepod::component::embeddings::EmbeddingPrefix;
    use spicepod::param::Params;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// Parse YAML into a string map, construct `Parameters`, then exercise every
    /// `params.get(key)` the provider function calls. A panic here means a key
    /// is missing from the `ParameterSpec`.
    async fn build_params(prefix: &EmbeddingPrefix, yaml: &str) -> Parameters {
        let string_map: std::collections::HashMap<String, String> = yaml::from_str::<Params>(yaml)
            .expect("YAML must parse")
            .as_string_map();
        let secrets = Arc::new(RwLock::new(Secrets::default()));
        let params_with_secrets =
            runtime_secrets::get_params_with_secrets(Arc::clone(&secrets), &string_map)
                .await
                .into_iter()
                .collect::<Vec<_>>();
        Parameters::try_new(
            &format!("embedding test_{prefix}"),
            params_with_secrets,
            prefix.to_string().leak(),
            Arc::clone(&secrets),
            get_params_spec(prefix),
        )
        .await
        .expect("Parameters::try_new must not fail")
    }

    #[tokio::test]
    async fn test_openai_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::OpenAi,
            "openai_api_key: sk-test\nopenai_org_id: org-1\nopenai_project_id: proj-1\nendpoint: https://api.openai.com\nopenai_usage_tier: tier1\n",
        )
        .await;
        // Every key that embed.rs accesses for openai
        let _ = params.get("api_key");
        let _ = params.get("endpoint");
        let _ = params.get("org_id");
        let _ = params.get("project_id");
        let _ = params.get("usage_tier");
    }

    #[tokio::test]
    async fn test_azure_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::Azure,
            "azure_api_key: key\nazure_api_version: 2024-02-01\nazure_deployment_name: my-deploy\nendpoint: https://my.azure.com\nazure_entra_token: tok\n",
        )
        .await;
        let _ = params.get("api_key");
        let _ = params.get("api_version");
        let _ = params.get("deployment_name");
        let _ = params.get("endpoint");
        let _ = params.get("entra_token");
    }

    #[tokio::test]
    async fn test_google_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::Google,
            "google_api_key: key\ndimensions: 768\n",
        )
        .await;
        let _ = params.get("api_key");
        let _ = params.get("dimensions");
    }

    #[tokio::test]
    async fn test_huggingface_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::HuggingFace,
            "hf_token: hf_abc\npooling: mean\nmax_seq_length: 512\n",
        )
        .await;
        let _ = params.get("hf_token");
        let _ = params.get("pooling");
        let _ = params.get("max_seq_length");
    }

    #[tokio::test]
    async fn test_databricks_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::Databricks,
            "databricks_endpoint: https://my.databricks.com\ndatabricks_token: dapi-abc\ndatabricks_client_id: cid\ndatabricks_client_secret: csec\n",
        )
        .await;
        let _ = params.get("endpoint");
        let _ = params.get("token");
        let _ = params.get("client_id");
        let _ = params.get("client_secret");
    }

    #[tokio::test]
    async fn test_bedrock_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::Bedrock,
            // AWS params (no prefix — runtime type)
            "aws_region: us-east-1\naws_access_key_id: AKIA\naws_secret_access_key: secret\naws_session_token: token\naws_iam_role_source: auto\n\
             # Titan/Nova model params\ndimensions: 256\nnormalize: true\n\
             # Cohere/Nova truncation params\ntruncate_mode: END\ntruncate: END\ninput_type: classification\nembedding_purpose: GENERIC_INDEX\n\
             # Rate-limit and profile overrides\naws_profile: default\nrequests_per_min_limit: 1500\nmax_concurrent_invocations: 10\n",
        )
        .await;
        // AWS params consumed via get_runtime_params()
        let runtime = params.get_runtime_params();
        assert!(
            runtime.contains_key("aws_region"),
            "aws_region missing from runtime params"
        );
        assert!(
            runtime.contains_key("aws_access_key_id"),
            "aws_access_key_id missing from runtime params"
        );
        assert!(
            runtime.contains_key("aws_secret_access_key"),
            "aws_secret_access_key missing from runtime params"
        );
        assert!(
            runtime.contains_key("aws_profile"),
            "aws_profile missing from runtime params"
        );
        assert!(
            runtime.contains_key("requests_per_min_limit"),
            "requests_per_min_limit missing from runtime params"
        );
        assert!(
            runtime.contains_key("max_concurrent_invocations"),
            "max_concurrent_invocations missing from runtime params"
        );
        // Model-specific params accessed directly in embed.rs
        let _ = params.get("dimensions");
        let _ = params.get("normalize");
        let _ = params.get("truncate_mode");
        let _ = params.get("truncate"); // alias — must not panic
        let _ = params.get("input_type");
        let _ = params.get("embedding_purpose");
    }

    #[tokio::test]
    async fn test_file_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::File,
            "pooling: cls\nmax_seq_length: 256\n",
        )
        .await;
        let _ = params.get("pooling");
        let _ = params.get("max_seq_length");
    }

    #[tokio::test]
    async fn test_model2vec_params_roundtrip() {
        let params = build_params(
            &EmbeddingPrefix::Model2Vec,
            "hf_token: hf_abc\nsubfolder: onnx\nnormalize: true\nparallelism: 4\nembed_max_token_length: 512\nembed_custom_batch_size: 32\n",
        )
        .await;
        let _ = params.get("hf_token");
        let _ = params.get("subfolder");
        let _ = params.get("normalize");
        let _ = params.get("parallelism");
        let _ = params.get("embed_max_token_length");
        let _ = params.get("embed_custom_batch_size");
    }
}
