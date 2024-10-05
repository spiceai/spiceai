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

use text_splitter::{ChunkCapacity, ChunkConfig};

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

enum Splitter {
    Markdown(text_splitter::MarkdownSplitter<text_splitter::Characters>),
    Text(text_splitter::TextSplitter<text_splitter::Characters>),
}

pub struct CharacterSplittingChunker {
    splitter: Splitter,
}

impl CharacterSplittingChunker {
    #[must_use]
    pub fn new(cfg: &ChunkingConfig) -> Self {
        let cfg_with_overlap = ChunkConfig::new(ChunkCapacity::new(cfg.target_chunk_size))
            .with_trim(cfg.trim_whitespace);

        let splitter = match cfg.file_format {
            Some("md" | ".md") => {
                Splitter::Markdown(text_splitter::MarkdownSplitter::new(cfg_with_overlap))
            }
            _ => Splitter::Text(text_splitter::TextSplitter::new(cfg_with_overlap)),
        };

        Self { splitter }
    }
}

impl Chunker for CharacterSplittingChunker {
    fn chunk_indices<'a>(&self, text: &'a str) -> ChunkIndicesIter<'a> {
        let z: Vec<_> = match &self.splitter {
            Splitter::Markdown(splitter) => splitter.chunk_indices(text).collect(),
            Splitter::Text(splitter) => splitter.chunk_indices(text).collect(),
        };
        Box::new(z.into_iter())
    }
}
