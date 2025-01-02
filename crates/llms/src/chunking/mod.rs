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

use std::sync::Arc;

use snafu::ResultExt;
use text_splitter::{Characters, ChunkCapacity, ChunkConfig, ChunkConfigError, ChunkSizer};
use tokenizers::Tokenizer;

use tiktoken_rs::{
    get_bpe_from_tokenizer,
    tokenizer::{get_tokenizer, Tokenizer as OpenAITokenizer},
    CoreBPE,
};

#[derive(Debug, Clone)]
pub struct ChunkingConfig<'a> {
    // The desired size of each chunk, in tokens.
    pub target_chunk_size: usize,

    // The amount of overlap between chunks, in tokens.
    pub overlap_size: usize,

    // Whether to trim the chunks to remove leading and trailing whitespace.
    pub trim_whitespace: bool,

    pub file_format: Option<&'a str>,
}

type ChunkIndicesIter<'a> = Box<dyn Iterator<Item = (usize, &'a str)> + 'a>;
type ChunkIter<'a> = Box<dyn Iterator<Item = &'a str> + 'a>;

pub trait Chunker: Sync + Send {
    fn chunk_indices<'a>(&self, text: &'a str) -> ChunkIndicesIter<'a>;

    fn chunks<'a>(&self, text: &'a str) -> ChunkIter<'a> {
        Box::new(self.chunk_indices(text).map(|(_, chunk)| chunk))
    }
}

enum Splitter<Sizer: ChunkSizer> {
    Markdown(text_splitter::MarkdownSplitter<Sizer>),
    Text(text_splitter::TextSplitter<Sizer>),
}

pub struct RecursiveSplittingChunker<Sizer: ChunkSizer> {
    splitter: Splitter<Sizer>,
}

impl<Sizer: ChunkSizer> RecursiveSplittingChunker<Sizer> {
    pub fn try_new(cfg: &ChunkingConfig, sizer: Sizer) -> Result<Self, ChunkConfigError> {
        let cfg_with_overlap: ChunkConfig<Sizer> =
            ChunkConfig::new(ChunkCapacity::new(cfg.target_chunk_size))
                .with_trim(cfg.trim_whitespace)
                .with_sizer(sizer)
                .with_overlap(cfg.overlap_size)
                .inspect_err(|_| {
                    tracing::warn!(
                "Cannot have overlap ({overlap}) >= target_chunk_size ({target_chunk_size})",
                overlap = cfg.overlap_size,
                target_chunk_size = cfg.target_chunk_size
            );
                })?;

        let splitter = match cfg.file_format {
            Some("md" | ".md" | "mdx" | ".mdx") => {
                Splitter::Markdown(text_splitter::MarkdownSplitter::new(cfg_with_overlap))
            }
            _ => Splitter::Text(text_splitter::TextSplitter::new(cfg_with_overlap)),
        };

        Ok(Self { splitter })
    }
}

impl RecursiveSplittingChunker<Characters> {
    pub fn with_character_sizer(cfg: &ChunkingConfig) -> Result<Self, ChunkConfigError> {
        Self::try_new(cfg, Characters)
    }
}

pub(crate) struct ArcSizer(Arc<dyn ChunkSizer + Send + Sync>);
impl ChunkSizer for ArcSizer {
    fn size(&self, chunk: &str) -> usize {
        self.0.size(chunk)
    }
}

impl From<Arc<dyn ChunkSizer + Send + Sync>> for ArcSizer {
    fn from(sizer: Arc<dyn ChunkSizer + Send + Sync>) -> Self {
        ArcSizer(sizer)
    }
}

/// Basic wrapper around a [`Arc<Tokenizer>`], so as to be able to `impl ChunkSizer for TokenizerWrapper`.
pub(crate) struct TokenizerWrapper(Arc<Tokenizer>);

impl ChunkSizer for TokenizerWrapper {
    fn size(&self, chunk: &str) -> usize {
        self.0.as_ref().size(chunk)
    }
}

impl From<Arc<Tokenizer>> for TokenizerWrapper {
    fn from(tokenizer: Arc<Tokenizer>) -> Self {
        TokenizerWrapper(tokenizer)
    }
}

impl RecursiveSplittingChunker<TokenizerWrapper> {
    pub fn with_tokenizer_sizer(
        cfg: &ChunkingConfig,
        tokenizer: Arc<Tokenizer>,
    ) -> Result<Self, ChunkConfigError> {
        Self::try_new(cfg, tokenizer.into())
    }
}

impl RecursiveSplittingChunker<CoreBPE> {
    pub fn for_openai_model(
        model_id: &str,
        cfg: &ChunkingConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bpe =
            get_bpe_from_tokenizer(get_tokenizer(model_id).unwrap_or(OpenAITokenizer::Cl100kBase))
                .map_err(|e| format!("Could not create BPE tokenizer: {e:?}"))?;
        Self::try_new(cfg, bpe).boxed()
    }
}

impl<Sizer: ChunkSizer + Send + Sync> Chunker for RecursiveSplittingChunker<Sizer> {
    fn chunk_indices<'a>(&self, text: &'a str) -> ChunkIndicesIter<'a> {
        let z: Vec<_> = match &self.splitter {
            Splitter::Markdown(splitter) => splitter.chunk_indices(text).collect(),
            Splitter::Text(splitter) => splitter.chunk_indices(text).collect(),
        };
        Box::new(z.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::{
        embeddings::Embed,
        openai::{
            embed::{OpenaiEmbed, DEFAULT_EMBEDDING_MODEL},
            new_openai_client,
        },
    };

    #[test]
    fn test_openai_chunker() {
        let cfg = ChunkingConfig {
            target_chunk_size: 3,
            overlap_size: 1,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = OpenaiEmbed::new(new_openai_client(
            DEFAULT_EMBEDDING_MODEL.to_string(),
            None,
            None,
            None,
            None,
        ))
        .chunker(&cfg)
        .expect("Failed to create OpenAI chunker");
        let chunks: Vec<_> = chunker
            .chunks("let cfg = ChunkingConfig {\ntarget_chunk_size: 3\noverlap_size: 1")
            .collect();

        assert_eq!(
            chunks,
            vec![
                "let cfg =",
                "ChunkingConfig",
                "{",
                "target_chunk_size",
                ": 3",
                "overlap_size:",
                ": 1"
            ]
        );
    }

    #[test]
    fn test_file_format() {
        let cfg = ChunkingConfig {
            target_chunk_size: 3,
            overlap_size: 1,
            trim_whitespace: true,
            file_format: Some("md"),
        };

        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");
        assert!(matches!(chunker.splitter, Splitter::Markdown(_)));
    }
}
