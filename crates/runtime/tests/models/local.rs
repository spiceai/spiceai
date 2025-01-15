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
mod embeddings {
    use std::{fs::File, io::Write, path::PathBuf, time::Duration};

    use spicepod::component::{embeddings::Embeddings, model::ModelFile};

    use crate::{
        init_tracing, models::embedding::run_beta_functionality_criteria_test,
        utils::test_request_context,
    };

    use super::*;

    /// Create a local embedding model by downloading `intfloat/e5-small-v2` from HuggingFace, finding the
    /// directory where the model was downloaded, and creating a local `Embeddings` component from it.
    async fn create_local_embedding_from_hf(name: impl Into<String>) -> Embeddings {
        let root_dir = std::env::temp_dir();
        download_to_temp_dir(
            root_dir.join("tokenizer.json"),
            "https://huggingface.co/intfloat/e5-small-v2/resolve/main/tokenizer.json?download=true",
        )
        .await;
        download_to_temp_dir(
            root_dir.join("config.json"),
            "https://huggingface.co/intfloat/e5-small-v2/resolve/main/config.json?download=true",
        )
        .await;
        download_to_temp_dir(
            root_dir.join("model.safetensors"),
            "https://huggingface.co/intfloat/e5-small-v2/resolve/main/model.safetensors?download=true",
        ).await;

        tracing::warn!("Foort: {}", root_dir.display());
        let mut embedding = Embeddings::new(
            format!(
                "file:/{}",
                root_dir.join("model.safetensors").display().to_string()
            ),
            name,
        );
        embedding.files = vec![
            ModelFile::from_path(&root_dir.join("tokenizer.json")),
            ModelFile::from_path(&root_dir.join("config.json")),
        ];
        tracing::warn!("Embedding: {:?}", embedding);
        embedding
    }

    async fn download_to_temp_dir(filename: PathBuf, url: &str) {
        let resp = reqwest::get(url)
            .await
            .expect(format!("Failed to get url={url}").as_str());
        let mut out = File::create(filename.clone())
            .expect(format!("Failed to download to file={filename:?}").as_str());

        let bytz = resp
            .bytes()
            .await
            .expect(format!("Failed to read bytes from url={url}").as_str());
        let _ = out.write_all(&bytz);
        out.flush().expect("Failed to flush file");
    }

    #[tokio::test]
    async fn local_embeddings_beta_requirements() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context()
            .scope(async {
                run_beta_functionality_criteria_test(
                    create_local_embedding_from_hf("hf_e5").await,
                    Duration::from_secs(3 * 60),
                )
                .await
            })
            .await;

        Ok(())
    }
}
