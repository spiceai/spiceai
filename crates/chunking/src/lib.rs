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
    target_chunk_size: usize,
    sizer: Sizer,
}

impl<Sizer: ChunkSizer + Clone> RecursiveSplittingChunker<Sizer> {
    pub fn try_new(cfg: &ChunkingConfig, sizer: Sizer) -> Result<Self, ChunkConfigError> {
        let target_chunk_size = cfg.target_chunk_size;
        let sizer_clone = sizer.clone();

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

        Ok(Self {
            splitter,
            target_chunk_size,
            sizer: sizer_clone,
        })
    }
}

impl RecursiveSplittingChunker<Characters> {
    pub fn with_character_sizer(cfg: &ChunkingConfig) -> Result<Self, ChunkConfigError> {
        Self::try_new(cfg, Characters)
    }
}

#[derive(Clone)]
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
#[derive(Clone)]
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

/// Chunk sizer wrapper around `OpenAI` `CoreBPE` tokenizers.
#[derive(Clone)]
pub struct CoreBpeSizer(CoreBPE);

impl ChunkSizer for CoreBpeSizer {
    fn size(&self, chunk: &str) -> usize {
        self.0.encode_ordinary(chunk).len()
    }
}

impl From<CoreBPE> for CoreBpeSizer {
    fn from(bpe: CoreBPE) -> Self {
        Self(bpe)
    }
}

impl RecursiveSplittingChunker<CoreBpeSizer> {
    pub fn for_openai_model(
        model_id: &str,
        cfg: &ChunkingConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bpe =
            get_bpe_from_tokenizer(get_tokenizer(model_id).unwrap_or(OpenAITokenizer::Cl100kBase))
                .map_err(|e| format!("Could not create BPE tokenizer: {e:?}"))?;
        Self::try_new(cfg, bpe.into()).boxed()
    }
}

impl<Sizer: ChunkSizer + Clone + Send + Sync> Chunker for RecursiveSplittingChunker<Sizer> {
    fn chunk_indices<'a>(&self, text: &'a str) -> ChunkIndicesIter<'a> {
        // Note: collect() is required here because the underlying text_splitter iterator
        // borrows from &self, but the trait signature only allows borrowing from text.
        // The Vec allocation decouples the iterator from self's lifetime.
        let chunks: Vec<_> = match &self.splitter {
            Splitter::Markdown(splitter) => splitter.chunk_indices(text).collect(),
            Splitter::Text(splitter) => splitter.chunk_indices(text).collect(),
        };

        // Enforce target_chunk_size as a hard maximum. The underlying text_splitter
        // library may produce chunks that slightly exceed the target when it cannot
        // find a clean semantic split point (e.g. a single long word/token). Any
        // oversized chunk is split into sub-chunks of at most target_chunk_size,
        // splitting on the last valid UTF-8 char boundary that fits.
        let max_size = self.target_chunk_size;
        let sizer = &self.sizer;

        let mut enforced: Vec<(usize, &'a str)> = Vec::with_capacity(chunks.len());
        for (idx, chunk) in chunks {
            if sizer.size(chunk) <= max_size {
                enforced.push((idx, chunk));
            } else {
                // Split the oversized chunk into sub-chunks that each fit within max_size.
                let mut remaining = chunk;
                let mut offset = idx;
                while !remaining.is_empty() {
                    if sizer.size(remaining) <= max_size {
                        enforced.push((offset, remaining));
                        break;
                    }
                    // Binary search for the longest prefix whose size <= max_size.
                    let split_at = find_max_prefix_len(remaining, max_size, sizer);
                    if split_at == 0 {
                        // A single character exceeds max_size (extremely rare).
                        // Include at least one character to guarantee forward progress.
                        let one_char_len = remaining
                            .char_indices()
                            .nth(1)
                            .map_or(remaining.len(), |(i, _)| i);
                        enforced.push((offset, &remaining[..one_char_len]));
                        remaining = &remaining[one_char_len..];
                        offset += one_char_len;
                    } else {
                        enforced.push((offset, &remaining[..split_at]));
                        remaining = &remaining[split_at..];
                        offset += split_at;
                    }
                }
            }
        }

        Box::new(enforced.into_iter())
    }
}

/// Binary-search for the longest prefix of `text` (on a UTF-8 char boundary)
/// whose measured size is at most `max_size`.
fn find_max_prefix_len(text: &str, max_size: usize, sizer: &dyn ChunkSizer) -> usize {
    // Use char_indices to only consider valid UTF-8 boundaries.
    let char_boundaries: Vec<usize> = text
        .char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(text.len()))
        .collect();

    // char_boundaries has len+1 entries: [0, ..., text.len()]
    // We search among indices 1..char_boundaries.len() (the *ends* of prefixes).
    let mut lo: usize = 0; // prefix of length 0 always fits
    let mut hi: usize = char_boundaries.len() - 1; // index into char_boundaries
    let mut best = 0usize; // best byte length found so far

    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let byte_len = char_boundaries[mid];
        if sizer.size(&text[..byte_len]) <= max_size {
            best = byte_len;
            if mid == hi {
                break;
            }
            lo = mid + 1;
        } else {
            if mid == 0 {
                break;
            }
            hi = mid - 1;
        }
    }
    best
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

    #[test]
    fn test_chunks_never_exceed_target_size_characters() {
        let target = 10;
        let cfg = ChunkingConfig {
            target_chunk_size: target,
            overlap_size: 0,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");

        // Include a long word that cannot be split at a word boundary within the target.
        let text = "abcdefghijklmnopqrstuvwxyz short words here and a verylongwordthatexceedsthetargetsize end";
        let chunks: Vec<_> = chunker.chunks(text).collect();

        for chunk in &chunks {
            assert!(
                Characters.size(chunk) <= target,
                "Chunk exceeds target_chunk_size ({target}): size={}, chunk={chunk:?}",
                Characters.size(chunk),
            );
        }

        // Verify all text content is preserved (no data loss).
        let reassembled: String = chunks.join("");
        let original_no_ws: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        let reassembled_no_ws: String =
            reassembled.chars().filter(|c| !c.is_whitespace()).collect();
        assert_eq!(
            original_no_ws, reassembled_no_ws,
            "Chunking should preserve all non-whitespace content"
        );
    }

    #[test]
    fn test_chunks_never_exceed_target_size_with_overlap() {
        let target = 10;
        let overlap = 3;
        let cfg = ChunkingConfig {
            target_chunk_size: target,
            overlap_size: overlap,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");

        // Text with a word longer than target to trigger the enforcement path.
        let text = "hello world abcdefghijklmnop and more text here";
        let chunks: Vec<_> = chunker.chunks(text).collect();

        for chunk in &chunks {
            assert!(
                Characters.size(chunk) <= target,
                "Chunk with overlap exceeds target_chunk_size ({target}): size={}, chunk={chunk:?}",
                Characters.size(chunk),
            );
        }
    }

    #[test]
    fn test_chunks_never_exceed_target_size_openai_tokenizer() {
        let target = 5;
        let cfg = ChunkingConfig {
            target_chunk_size: target,
            overlap_size: 1,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = RecursiveSplittingChunker::for_openai_model("text-embedding-3-small", &cfg)
            .expect("failed to make chunker");

        // Use text that may produce chunks exceeding token limit with the BPE tokenizer.
        let text = "supercalifragilisticexpialidocious is a very long word that might exceed token limits when chunking is applied to embedding models with strict input size constraints";
        let chunks: Vec<_> = chunker.chunks(text).collect();

        let sizer = chunker.sizer.clone();
        for chunk in &chunks {
            let size = sizer.size(chunk);
            assert!(
                size <= target,
                "Chunk exceeds target_chunk_size ({target} tokens): size={size}, chunk={chunk:?}",
            );
        }
    }

    #[test]
    fn test_find_max_prefix_len_basic() {
        // Characters sizer: size == number of chars.
        let text = "abcdefghij"; // 10 chars
        let result = find_max_prefix_len(text, 5, &Characters);
        assert_eq!(result, 5);
        assert!(Characters.size(&text[..result]) <= 5);
    }

    #[test]
    fn test_find_max_prefix_len_utf8() {
        // Multi-byte UTF-8 characters: each is 1 char but multiple bytes.
        let text = "\u{00e9}\u{00e9}\u{00e9}\u{00e9}\u{00e9}"; // 5 x 'e' with accent (2 bytes each)
        let result = find_max_prefix_len(text, 3, &Characters);
        // Should fit exactly 3 characters = 6 bytes
        assert_eq!(result, 6);
        assert_eq!(Characters.size(&text[..result]), 3);
    }

    #[test]
    fn test_single_long_word_is_split() {
        let target = 5;
        let cfg = ChunkingConfig {
            target_chunk_size: target,
            overlap_size: 0,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");

        let text = "abcdefghijklmnop"; // 16 chars, single word
        let chunks: Vec<_> = chunker.chunks(text).collect();

        assert!(
            chunks.len() > 1,
            "Long word should be split into multiple chunks"
        );
        for chunk in &chunks {
            assert!(
                Characters.size(chunk) <= target,
                "Each sub-chunk must fit within target: size={}, chunk={chunk:?}",
                Characters.size(chunk),
            );
        }
    }

    #[test]
    fn test_markdown_chunks_respect_target_size() {
        let target = 15;
        let cfg = ChunkingConfig {
            target_chunk_size: target,
            overlap_size: 0,
            trim_whitespace: true,
            file_format: Some("md"),
        };

        let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
            .expect("failed to create chunker");

        let text = "# Header\n\nSome text with a verylongwordthatexceedsthetarget and more content.\n\n## Another section\n\nMore text here.";
        let chunks: Vec<_> = chunker.chunks(text).collect();

        for chunk in &chunks {
            assert!(
                Characters.size(chunk) <= target,
                "Markdown chunk exceeds target ({target}): size={}, chunk={chunk:?}",
                Characters.size(chunk),
            );
        }
    }
}
