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
    use std::{
        fs::{create_dir_all, File},
        io::Write,
        path::PathBuf,
        time::Duration,
    };

    use anyhow::Result;
    use spicepod::component::{embeddings::Embeddings, model::ModelFile};

    use crate::{
        init_tracing, models::embedding::run_beta_functionality_criteria_test,
        utils::test_request_context,
    };

    /// Create a local embedding model by downloading the `model_id` from `HuggingFace` if it doesn't exist.
    ///
    /// Currently expects model to only need `tokenizer.json`, `config.json`, and `model.safetensors` files.
    async fn create_local_embedding_from_hf(model_id: &str, name: &str) -> Result<Embeddings> {
        let mut model_path =
            dirs::home_dir().ok_or(anyhow::anyhow!("Could not find home directory"))?;
        model_path.push(".spice/models");
        model_path.push(name);
        create_dir_all(&model_path)?;

        check_and_download_to_temp_dir(
            model_path.join("tokenizer.json"),
            format!("https://huggingface.co/{model_id}/resolve/main/tokenizer.json?download=true")
                .as_str(),
        )
        .await?;
        check_and_download_to_temp_dir(
            model_path.join("config.json"),
            format!("https://huggingface.co/{model_id}/resolve/main/config.json?download=true")
                .as_str(),
        )
        .await?;
        check_and_download_to_temp_dir(
            model_path.join("model.safetensors"),
            format!(
                "https://huggingface.co/{model_id}/resolve/main/model.safetensors?download=true"
            )
            .as_str(),
        )
        .await?;

        let mut embedding = Embeddings::new(
            format!("file:/{}", model_path.join("model.safetensors").display()),
            name,
        );
        embedding.files = vec![
            ModelFile::from_path(&model_path.join("tokenizer.json")),
            ModelFile::from_path(&model_path.join("config.json")),
        ];
        Ok(embedding)
    }

    async fn check_and_download_to_temp_dir(filename: PathBuf, url: &str) -> Result<()> {
        if filename.exists() {
            tracing::debug!("File {filename:?} already exists.");
            return Ok(());
        }
        let resp = reqwest::get(url).await?;
        let mut out = File::create(filename.clone())?;

        let bytz = resp.bytes().await?;
        let _ = out.write_all(&bytz);
        out.flush()?;

        Ok(())
    }

    #[tokio::test]
    async fn local_embeddings_beta_requirements() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context()
            .scope(async {
                run_beta_functionality_criteria_test(
                    create_local_embedding_from_hf("intfloat/e5-small-v2", "hf_e5").await?,
                    Duration::from_secs(3 * 60),
                )
                .await
            })
            .await?;

        Ok(())
    }
}
