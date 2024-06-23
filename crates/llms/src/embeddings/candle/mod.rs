use std::path::PathBuf;

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
use super::{Embed, FailedToInstantiateEmbeddingModelSnafu, Result};

use async_openai::types::EmbeddingInput;
use snafu::ResultExt;
use tei_core::{ModelType, Pool};
use tokenizers::Tokenizer;
use tei_candle::CandleBackend;
use candle_core::DType;

pub struct CandleEmbedding {
    backend: CandleBackend,
    tok: Tokenizer,
    output_size: Option<i32>, // Lazily compute output_size based on [`embed`]
}

impl CandleEmbedding {
     pub fn try_new(model_root: PathBuf, dtype: DType) -> Result<Self> {
          let str_dtype = if dtype == DType::F32 {
               "float32"
           } else if dtype == DType::F32 {
               "float16"
           } else {
               dtype.as_str()
           };

          let b = CandleBackend::new(model_root.clone(), str_dtype.to_string(), ModelType::Embedding(Pool::Cls)).boxed().context(FailedToInstantiateEmbeddingModelSnafu)?;
          let tok = Tokenizer::from_file(model_root.join("tokenizer.json")).context(FailedToInstantiateEmbeddingModelSnafu)?;
          Ok(Self {
               backend: b,
               tok: tok,
               output_size: None,
          })
     }

     pub async fn tokenise(&self) -> Result<()> {
          self.tok
            .tokenize(inputs.into(), add_special_tokens)
            .await
            .map_err(|err| {
                metrics::increment_counter!("te_request_failure", "err" => "tokenization");
                tracing::error!("{err}");
                err
            })
     }
     
}

/// For a given HuggingFace repo, download the needed files to create a `CandleEmbedding`.
pub fn download_hf_artifacts(
     model_id: &'static str,
     revision: Option<&'static str>,
 ) -> Result<PathBuf> {
     let builder = ApiBuilder::new().with_progress(false);
 
     let api = builder.build().unwrap();
     let api_repo = if let Some(revision) = revision {
         api.repo(Repo::with_revision(
             model_id.to_string(),
             RepoType::Model,
             revision.to_string(),
         ))
     } else {
         api.repo(Repo::new(model_id.to_string(), RepoType::Model))
     };
 
     api_repo.get("config.json")?;
     api_repo.get("tokenizer.json")?;
 
     let model_root = match api_repo.get("model.safetensors") {
         Ok(p) => p,
         Err(_) => {
             let p = api_repo.get("pytorch_model.bin")?;
             tracing::warn!("`model.safetensors` not found. Using `pytorch_model.bin` instead. Model loading will be significantly slower.");
             p
         }
     }
         .parent().unwrap()
         .to_path_buf();
     Ok(model_root)
 }
 

impl Embed for CandleEmbedding {
     async fn embed(&mut self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>> {

          
     }
 
     /// Returns the size of the embedding vector returned by the model.
     fn size(&self) -> i32 {
          match self.output_size {
               Some(size) => size,
               None => {
                    let output_size = ;
                    self.output_size = Some(output_size);
                    output_size
               }
          }
     }

}