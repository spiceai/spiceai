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

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        CreateEmbeddingRequest, CreateEmbeddingResponse, Embedding, EmbeddingInput, EmbeddingUsage,
    },
};
use async_trait::async_trait;
use futures::future::join_all;
use snafu::ResultExt;
use tei_backend::{Backend, DType, ModelType, Pool};
use tei_core::{
    infer::{Infer, PooledEmbeddingsInferResponse},
    queue::Queue,
    tokenization::{EncodingInput, Tokenization},
    TextEmbeddingsError,
};
use tokenizers::{Tokenizer, TruncationDirection};

use crate::{
    chunking::{Chunker, ChunkingConfig, RecursiveSplittingChunker},
    embeddings::{
        candle::util::link_files_into_tmp_dir, encode_embedding, Embed, Error,
        FailedToCreateEmbeddingSnafu, FailedToInstantiateEmbeddingModelSnafu, Result,
    },
};

use super::util::{
    download_hf_artifacts, inputs_from_openai, load_config, load_tokenizer,
    max_seq_length_from_st_config, pool_from_str, position_offset,
};

pub struct TeiEmbed {
    pub infer: Infer,
    pub model_size: i32,     // Used for `size` method.
    pub tok: Arc<Tokenizer>, // Used for `chunker` method.
}

impl TeiEmbed {
    const DEFAULT_POOLING_OPERATOR: Pool = Pool::Mean;

    pub fn from_local(
        model_path: &Path,
        config_path: &Path,
        tokenizer_path: &Path,
        pooling_overwrite: Option<String>,
        max_seq_length_overwrite: Option<usize>,
    ) -> Result<Self> {
        let model_filename = model_path
            .file_name()
            .ok_or("model path must be a file".into())
            .context(FailedToCreateEmbeddingSnafu)?
            .to_string_lossy()
            .to_string();

        // `text-embeddings-inference` expects the model artifacts to to be in a single folder with specific filenames.
        let files: HashMap<String, PathBuf> = vec![
            (model_filename, model_path.to_path_buf()),
            ("config.json".to_string(), config_path.to_path_buf()),
            ("tokenizer.json".to_string(), tokenizer_path.to_path_buf()),
        ]
        .into_iter()
        .collect();

        let model_root = link_files_into_tmp_dir(files)?;
        tracing::trace!(
            "Embedding model has files linked at location={:?}",
            model_root
        );

        // Check if user provided pooling is valid, and only default to mean when user doesn't provide one.
        let pool = if let Some(pooling) = pooling_overwrite {
            match pool_from_str(&pooling) {
                Some(pool) => pool,
                None => {
                    return Err(Error::FailedToCreateEmbedding {
                        source: format!("Invalid pooling mode: {pooling}").into(),
                    });
                }
            }
        } else {
            tracing::warn!(
                "`params.pooling` not provided for embedding model. Often this can be found in `1_Pooling/config.json`. Defaulting to mean pooling."
            );
            Self::DEFAULT_POOLING_OPERATOR
        };

        Self::from_dir(&model_root, Some(pool), max_seq_length_overwrite)
    }

    pub async fn from_hf(
        model_id: &str,
        revision: Option<&str>,
        hf_token: Option<&str>,
        pooling_overwrite: Option<&str>,
        max_seq_length_overwrite: Option<usize>,
    ) -> Result<Self> {
        // Only error if user-provided value is incorrect.
        let pool = pooling_overwrite
            .map(|pp| {
                let p = pool_from_str(pp);
                if p.is_none() {
                    return Err(Error::InvalidPoolingMode {
                        value: pp.to_string(),
                    });
                }
                Ok(p)
            })
            .transpose()?
            .flatten();
        let model_root = download_hf_artifacts(model_id, revision, hf_token).await?;
        Self::from_dir(&model_root, pool, max_seq_length_overwrite)
    }

    /// Instantiates a text-embedding-inference service with model, tokenizer, config, etc files in a single directory.
    pub fn from_dir(
        root: &Path,
        pooling_overwrite: Option<Pool>,
        max_seq_length_overwrite: Option<usize>,
    ) -> Result<Self> {
        let tokenizer = load_tokenizer(root)?;
        let config = load_config(root)?;

        // Load [`Tokenization`]
        let position_offset = position_offset(&config);

        let max_input_length = if let Some(max_seq_length) = max_seq_length_overwrite {
            max_seq_length
        } else {
            // Some models will have `sentence_*_config.json` file defining a specific `max_seq_length`.
            match max_seq_length_from_st_config(root) {
                Ok(max_seq_length_opt) => {
                    max_seq_length_opt.unwrap_or(config.max_position_embeddings - position_offset)
                }
                Err(e) => {
                    tracing::warn!("Failed to load max_seq_length from ST config: {e}");
                    config.max_position_embeddings - position_offset
                }
            }
        };

        let token = Tokenization::new(
            1,
            tokenizer.clone(),
            max_input_length,
            position_offset,
            None,
            None,
        );

        // Load [`Backend`]
        // TODO: add pooling parameter from https://github.com/spiceai/spiceai/pull/3174
        let model_type =
            ModelType::Embedding(pooling_overwrite.unwrap_or(Self::DEFAULT_POOLING_OPERATOR));

        // Last 3 parameters are not used (since we are using `candle` feature flag).
        let backend = Backend::new(
            root.into(),
            DType::Float32,
            model_type,
            String::new(), // Not used
            None,          // Not used
            String::new(), // Not used
        )
        .boxed()
        .context(FailedToInstantiateEmbeddingModelSnafu)?;

        // TODO: handle user args
        // See https://github.com/huggingface/text-embeddings-inference/blob/main/router/src/main.rs#L65-L74
        let max_concurrent_requests = 512;
        let max_batch_tokens = 16384;

        let queue = Queue::new(
            backend.padded_model,
            max_batch_tokens,
            None,
            max_concurrent_requests,
        );

        let infer = Infer::new(token, queue, max_concurrent_requests, backend);

        Ok(Self {
            infer,
            model_size: config.hidden_size,
            tok: Arc::new(tokenizer),
        })
    }

    pub(crate) async fn embed_futures(
        &self,
        inputs: Vec<EncodingInput>,
    ) -> std::result::Result<Vec<PooledEmbeddingsInferResponse>, TextEmbeddingsError> {
        let batch_size = inputs.len();

        let mut futures = Vec::with_capacity(batch_size);
        for input in inputs {
            let local_infer = self.infer.clone();
            futures.push(async move {
                let permit = local_infer.acquire_permit().await;
                local_infer
                    .embed_pooled(
                        input,
                        false, // Don't automatically truncate, error.
                        TruncationDirection::Right,
                        None,
                        true,
                        permit,
                    )
                    .await
            });
        }
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<PooledEmbeddingsInferResponse>, _>>()
    }
}

#[async_trait]
impl Embed for TeiEmbed {
    async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>> {
        let inputs = inputs_from_openai(&input);
        let resp =
            self.embed_futures(inputs)
                .await
                .map_err(|e| Error::FailedToCreateEmbedding {
                    source: Box::new(e),
                })?;

        Ok(resp.into_iter().map(|r| r.results).collect())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let model_name = req.model.clone();
        let inputs = inputs_from_openai(&req.input);
        let format = req.encoding_format.unwrap_or_default();

        let batch_size = inputs.len();
        let results = self.embed_futures(inputs).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        let mut embeddings = Vec::with_capacity(batch_size);
        let mut prompt_tokens: u32 = 0;
        for (i, r) in results.into_iter().enumerate() {
            embeddings.push(Embedding {
                object: "embedding".to_string(),
                embedding: encode_embedding(&format, r.results),
                index: i as u32,
            });
            prompt_tokens += r.metadata.prompt_tokens as u32;
        }

        Ok(CreateEmbeddingResponse {
            object: "list".to_string(),
            model: model_name,
            data: embeddings,
            usage: EmbeddingUsage {
                prompt_tokens,
                total_tokens: prompt_tokens,
            },
        })
    }

    fn size(&self) -> i32 {
        self.model_size
    }

    fn chunker(&self, cfg: &ChunkingConfig) -> Result<Arc<dyn Chunker>> {
        Ok(Arc::new(
            RecursiveSplittingChunker::with_tokenizer_sizer(cfg, Arc::clone(&self.tok))
                .boxed()
                .map_err(|e| Error::FailedToCreateChunker { source: e })?,
        ))
    }
}
