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

//! Native local reranker backed by the vendored `text-embeddings-inference`
//! candle backend.
//!
//! Cross-encoder reranker models (`BAAI/bge-reranker-base`,
//! `BAAI/bge-reranker-large`, `Alibaba-NLP/gte-reranker-modernbert-base`, …)
//! are loaded through the same backend as [`crate::embeddings::candle::tei::TeiEmbed`],
//! but with [`ModelType::Classifier`] instead of `ModelType::Embedding`. Scoring
//! encodes each `(query, document)` pair as a dual-sequence input and reads the
//! classification head's score, mirroring the TEI router's own `/rerank` path.

use std::path::Path;

use async_trait::async_trait;
use futures::future::join_all;
use snafu::ResultExt;
use tei_backend::{Backend, DType, ModelType};
use tei_core::{infer::Infer, queue::Queue};
use tokenizers::TruncationDirection;

use crate::embeddings::candle::util::{
    download_hf_artifacts, link_files_into_tmp_dir, load_tokenization,
};
use crate::rerank::{Error, LocalModelLoadFailedSnafu, Rerank, Result};

/// A cross-encoder reranker running in-process via the candle TEI backend.
///
/// Holds a [`tei_core::infer::Infer`] configured for a classifier model. Unlike
/// the remote rerankers (Cohere/Voyage/Jina/HTTP), scoring never leaves the
/// process, so [`Rerank::is_remote`] returns `false`.
pub struct TeiRerank {
    infer: Infer,
    name: String,

    // When `Some`, `(query, document)` pairs longer than the model's maximum
    // sequence length are truncated in that direction instead of failing the
    // scoring call.
    truncation: Option<TruncationDirection>,
}

impl std::fmt::Debug for TeiRerank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeiRerank")
            .field("name", &self.name)
            .field("truncation", &self.truncation)
            .finish_non_exhaustive()
    }
}

impl TeiRerank {
    /// Download a reranker model from `HuggingFace` and load it as a
    /// cross-encoder classifier.
    pub async fn from_hf(
        name: impl Into<String>,
        model_id: &str,
        revision: Option<&str>,
        hf_token: Option<&str>,
        max_seq_length_overwrite: Option<usize>,
        truncation: Option<TruncationDirection>,
    ) -> Result<Self> {
        let name = name.into();
        let model_root = download_hf_artifacts(model_id, revision, hf_token)
            .await
            .map_err(|e| Error::LocalModelLoadFailed {
                model: name.clone(),
                source: Box::new(e),
            })?;
        Self::from_dir(name, &model_root, max_seq_length_overwrite, truncation).await
    }

    /// Load a reranker model from explicit local artifact paths (weights,
    /// config, tokenizer). The three files are linked into a single directory
    /// with the filenames the TEI backend expects, then loaded via
    /// [`Self::from_dir`].
    pub async fn from_local(
        name: impl Into<String>,
        model_path: &Path,
        config_path: &Path,
        tokenizer_path: &Path,
        max_seq_length_overwrite: Option<usize>,
        truncation: Option<TruncationDirection>,
    ) -> Result<Self> {
        let name = name.into();
        let model_filename = model_path
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .ok_or_else(|| Error::LocalModelLoadFailed {
                model: name.clone(),
                source: "model path must be a file".into(),
            })?;

        let files = vec![
            (model_filename, model_path.to_path_buf()),
            ("config.json".to_string(), config_path.to_path_buf()),
            ("tokenizer.json".to_string(), tokenizer_path.to_path_buf()),
        ]
        .into_iter()
        .collect();

        let model_root =
            link_files_into_tmp_dir(files).map_err(|e| Error::LocalModelLoadFailed {
                model: name.clone(),
                source: Box::new(e),
            })?;
        Self::from_dir(name, &model_root, max_seq_length_overwrite, truncation).await
    }

    /// Load a reranker model from a directory containing `config.json`,
    /// `tokenizer.json`, and the model weights.
    pub async fn from_dir(
        name: impl Into<String>,
        root: &Path,
        max_seq_length_overwrite: Option<usize>,
        truncation: Option<TruncationDirection>,
    ) -> Result<Self> {
        let name = name.into();

        let (_, _, token) = load_tokenization(root, max_seq_length_overwrite)
            .boxed()
            .context(LocalModelLoadFailedSnafu {
                model: name.clone(),
            })?;

        // A cross-encoder reranker is a sequence-classification model: load it
        // with `Classifier` (no pooling) so `Infer::predict` — gated on
        // `is_classifier()` — is available.
        let model_type = ModelType::Classifier;

        // Last 3 parameters are unused with the `candle` feature flag (mirrors
        // `TeiEmbed::from_dir`).
        let backend = Backend::new(
            root.into(),
            None,
            DType::Float32,
            model_type,
            None,          // Not used
            String::new(), // Not used
            None,          // Not used
            String::new(), // Not used
        )
        .await
        .boxed()
        .context(LocalModelLoadFailedSnafu {
            model: name.clone(),
        })?;

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
            name,
            truncation,
        })
    }
}

#[async_trait]
impl Rerank for TeiRerank {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let truncate = self.truncation.is_some();
        let truncation_direction = self.truncation.unwrap_or_default();

        // Fan out one scoring request per document; `Infer` batches internally,
        // so this is not N sequential round-trips. Each future owns a cheap
        // `Infer` clone rather than sharing `&self` across concurrent tasks,
        // mirroring `TeiEmbed::embed_futures`.
        let mut futures = Vec::with_capacity(documents.len());
        for doc in documents {
            let infer = self.infer.clone();
            let name = self.name.clone();
            let query = query.to_string();
            let document = doc.clone();
            futures.push(async move {
                let permit = infer.acquire_permit().await;
                let response = infer
                    .predict(
                        (query, document),
                        truncate,
                        truncation_direction,
                        // `raw_scores == false` applies sigmoid to a single-logit
                        // head (softmax for multi-logit), matching the TEI
                        // router's own `/rerank` path so scores land in `[0, 1]`.
                        false,
                        permit,
                    )
                    .await
                    .map_err(|e| Error::ModelCallFailed {
                        model: name.clone(),
                        source: Box::new(e),
                    })?;

                extract_single_score(&response.results, &name)
            });
        }

        let scores = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<f32>>>()?;

        // `join_all` preserves order and yields exactly one score per document,
        // satisfying the `Rerank` contract; the length assertion is a
        // defense-in-depth check the trait callers rely on.
        if scores.len() != documents.len() {
            return Err(Error::MismatchedScoreCount {
                model: self.name.clone(),
                expected: documents.len(),
                actual: scores.len(),
            });
        }

        Ok(scores)
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn is_remote(&self) -> bool {
        false
    }
}

/// Reads the reranker classification head's relevance score out of a `predict` response.
///
/// The TEI `/rerank` route only serves single-class reranker/cross-encoder models, so exactly one
/// finite score is expected per document. An ordinary multi-class classifier would return several
/// logits, and a broken or incompatible model can return `NaN`; both are rejected here as a
/// structured error instead of silently reading `results[0]` as if it were a relevance score.
fn extract_single_score(results: &[f32], model: &str) -> Result<f32> {
    let &[score] = results else {
        return if results.is_empty() {
            Err(Error::EmptyPrediction {
                model: model.to_string(),
            })
        } else {
            Err(Error::UnexpectedScoreCount {
                model: model.to_string(),
                actual: results.len(),
            })
        };
    };

    if !score.is_finite() {
        return Err(Error::NonFiniteScore {
            model: model.to_string(),
        });
    }

    Ok(score)
}

#[cfg(test)]
mod tests {
    use super::extract_single_score;
    use crate::rerank::Error;

    #[test]
    fn extract_single_score_accepts_one_finite_score() {
        let score = extract_single_score(&[0.42], "m").expect("single finite score");
        assert!((score - 0.42).abs() < 1e-6);
    }

    #[test]
    fn extract_single_score_rejects_empty_results() {
        let err = extract_single_score(&[], "m").expect_err("empty results must error");
        assert!(matches!(err, Error::EmptyPrediction { .. }));
    }

    #[test]
    fn extract_single_score_rejects_multi_class_output() {
        // An ordinary multi-class classifier returns one logit per class; a reranker
        // must not silently treat the first one as the relevance score.
        let err =
            extract_single_score(&[0.1, 0.2, 0.7], "m").expect_err("multi-logit output must error");
        assert!(matches!(err, Error::UnexpectedScoreCount { actual: 3, .. }));
    }

    #[test]
    fn extract_single_score_rejects_nan() {
        let err = extract_single_score(&[f32::NAN], "m").expect_err("NaN must error");
        assert!(matches!(err, Error::NonFiniteScore { .. }));
    }

    #[test]
    fn extract_single_score_rejects_infinite() {
        let err =
            extract_single_score(&[f32::INFINITY], "m").expect_err("infinite score must error");
        assert!(matches!(err, Error::NonFiniteScore { .. }));
    }
}
