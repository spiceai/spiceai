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
use text_splitter::{Characters, ChunkCapacity, ChunkConfig, ChunkConfigError};
use tokenizers::Tokenizer;

use tiktoken_rs::{
    CoreBPE, get_bpe_from_tokenizer,
    tokenizer::{Tokenizer as OpenAITokenizer, get_tokenizer},
};

pub use text_splitter::ChunkSizer;

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

    /// Chunks a given `text`, and for each returning the starting (inclusive) and ending (exclusive) indexes into the input `text`.
    fn chunk_with_offsets<'a>(
        &self,
        text: &'a str,
    ) -> Box<dyn Iterator<Item = ((usize, usize), &'a str)> + 'a> {
        Box::new(
            self.chunk_indices(text)
                .map(|(idx, chunk)| ((idx, idx + chunk.len()), chunk)),
        )
    }

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
        let cfg_with_overlap: ChunkConfig<Sizer> = ChunkConfig::new(ChunkCapacity::new(
            cfg.target_chunk_size,
        ))
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

pub struct ArcSizer(Arc<dyn ChunkSizer + Send + Sync>);
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
pub struct TokenizerWrapper(Arc<Tokenizer>);

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
        // Note: collect() is required here because the underlying text_splitter iterator
        // borrows from &self, but the trait signature only allows borrowing from text.
        // The Vec allocation decouples the iterator from self's lifetime.
        let chunks: Vec<_> = match &self.splitter {
            Splitter::Markdown(splitter) => splitter.chunk_indices(text).collect(),
            Splitter::Text(splitter) => splitter.chunk_indices(text).collect(),
        };
        Box::new(chunks.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec;

    #[test]
    fn test_openai_chunker() {
        let cfg = ChunkingConfig {
            target_chunk_size: 3,
            overlap_size: 1,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = Arc::new(
            RecursiveSplittingChunker::for_openai_model("text-embedding-3-small", &cfg)
                .expect("failed to make chunker"),
        ) as Arc<dyn Chunker>;

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

    #[test]
    fn test_chunk_indices_returns_owned_iterator() {
        // This test verifies that chunk_indices returns an iterator that doesn't
        // borrow from self, allowing the chunker to be dropped while the iterator
        // is still in use. This is the behavior documented by the collect() comment.
        let cfg = ChunkingConfig {
            target_chunk_size: 10,
            overlap_size: 0,
            trim_whitespace: true,
            file_format: None,
        };

        let text = "Hello world, this is a test of chunking functionality.";

        // Create chunker, get iterator, then ensure we can collect after moving text reference
        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");

        // Get the iterator - it should only borrow from `text`, not from `chunker`
        let chunks_iter = chunker.chunk_indices(text);

        // Collect chunks - this works because the iterator doesn't borrow from chunker
        let chunks: Vec<_> = chunks_iter.collect();

        // Verify we got reasonable chunks
        assert!(!chunks.is_empty(), "Should produce at least one chunk");

        // Verify each chunk index is valid and points to the correct text
        for (idx, chunk) in &chunks {
            assert!(
                *idx < text.len(),
                "Chunk index {idx} should be within text bounds"
            );
            assert_eq!(
                &text[*idx..*idx + chunk.len()],
                *chunk,
                "Chunk content should match text at index"
            );
        }
    }

    #[test]
    fn test_chunk_with_offsets() {
        let cfg = ChunkingConfig {
            target_chunk_size: 5,
            overlap_size: 0,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");

        let text = "Hello world";
        let chunks: Vec<_> = chunker.chunk_with_offsets(text).collect();

        // Verify offset tuples are (start, end) and correctly span the chunk
        for ((start, end), chunk) in &chunks {
            assert!(*start < *end, "Start offset should be less than end offset");
            assert_eq!(
                *end - *start,
                chunk.len(),
                "Offset range should equal chunk length"
            );
            assert_eq!(
                &text[*start..*end],
                *chunk,
                "Offset range should extract the correct chunk"
            );
        }
    }
}
