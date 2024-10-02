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

use std::sync::Arc;

use text_splitter::{ChunkCapacity, ChunkConfig};

#[derive(Debug, Clone)]
pub struct ChunkingConfig {
    // The desired size of each chunk, in tokens.
    pub target_chunk_size: usize,

    // The amount of overlap between chunks, in tokens.
    pub overlap_size: usize,

    // Whether to trim the chunks to remove leading and trailing whitespace.
    pub trim_whitespace: bool,
}

type ChunkIndicesIter<'a> = Box<dyn Iterator<Item = (usize, &'a str)> + 'a>;
type ChunkIter<'a> = Box<dyn Iterator<Item = &'a str> + 'a>;

pub trait Chunker: Sync + Send {
    fn chunk_indices<'a>(&self, text: &'a str) -> ChunkIndicesIter<'a>;

    fn chunks<'a>(&self, text: &'a str) -> ChunkIter<'a> {
        Box::new(self.chunk_indices(text).map(|(_, chunk)| chunk))
    }
}

pub struct CharacterSplittingChunker {
    splitter: Arc<text_splitter::TextSplitter<text_splitter::Characters>>,
}

impl CharacterSplittingChunker {
    #[must_use]
    pub fn new(cfg: &ChunkingConfig) -> Self {
        let cfg_with_overlap = ChunkConfig::new(ChunkCapacity::new(cfg.target_chunk_size))
            .with_trim(cfg.trim_whitespace)
            .with_overlap(cfg.overlap_size);

        let split_cfg = if let Ok(overlap_config) = cfg_with_overlap {
            overlap_config
        } else {
            tracing::warn!(
                "Overlap={} cannot be used with chunking when target_chunk_size={} is set. Overlap will be ignored.",
                cfg.overlap_size,
                cfg.target_chunk_size
            );
            ChunkConfig::new(ChunkCapacity::new(cfg.target_chunk_size))
                .with_trim(cfg.trim_whitespace)
        };
        Self {
            splitter: Arc::new(text_splitter::TextSplitter::new(split_cfg)),
        }
    }
}

impl Chunker for CharacterSplittingChunker {
    fn chunk_indices<'a>(&self, text: &'a str) -> ChunkIndicesIter<'a> {
        let z: Vec<_> = self.splitter.chunk_indices(text).collect();
        Box::new(z.into_iter())
    }
}
