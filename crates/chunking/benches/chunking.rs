/*
Copyright 2025 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]

use chunking::{Chunker, ChunkingConfig, RecursiveSplittingChunker};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;

const SMALL_TEXT: &str = "This is a small text sample for chunking. It contains just a few sentences to test basic chunking functionality.";

const MEDIUM_TEXT: &str = "# Introduction\n\n\
    This is a medium-sized text document that demonstrates various chunking capabilities. \
    It includes multiple paragraphs, different types of content, and serves as a realistic \
    test case for text processing.\n\n\
    ## Features\n\n\
    The chunking system supports various configurations:\n\
    - Variable chunk sizes\n\
    - Overlap between chunks\n\
    - Whitespace trimming\n\
    - Format-specific handling (Markdown, plain text)\n\n\
    ## Implementation Details\n\n\
    The implementation uses recursive splitting to ensure chunks respect natural boundaries \
    while maintaining the target size constraints. This approach provides better semantic \
    coherence compared to naive fixed-size splitting.";

const LARGE_TEXT: &str = "# Comprehensive Documentation\n\n\
    This is a large text document designed to test chunking performance with substantial content. \
    It simulates real-world documentation or articles that need to be processed and chunked.\n\n\
    ## Chapter 1: Introduction\n\n\
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut \
    labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco \
    laboris nisi ut aliquip ex ea commodo consequat.\n\n\
    Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
    pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt \
    mollit anim id est laborum.\n\n\
    ## Chapter 2: Technical Details\n\n\
    The system architecture is designed around several key components:\n\n\
    ### Component A\n\
    This component handles the initial processing of input data. It validates the format, \
    normalizes the content, and prepares it for further processing stages.\n\n\
    ### Component B\n\
    The second component performs the actual chunking operation. It applies the configured \
    strategy to split the text into appropriately sized pieces while maintaining context.\n\n\
    ### Component C\n\
    Finally, the output component formats and delivers the chunks to downstream consumers. \
    It ensures that metadata is preserved and chunks are properly indexed.\n\n\
    ## Chapter 3: Performance Considerations\n\n\
    When working with large documents, several performance factors come into play:\n\n\
    1. **Memory Usage**: The chunking algorithm must be memory-efficient to handle large inputs\n\
    2. **Processing Speed**: Fast processing is crucial for real-time applications\n\
    3. **Chunk Quality**: The semantic coherence of chunks affects downstream task quality\n\
    4. **Overlap Management**: Proper handling of overlap ensures no information is lost\n\n\
    ## Chapter 4: Use Cases\n\n\
    Text chunking is valuable in numerous applications:\n\n\
    - **Search Systems**: Breaking documents into searchable units\n\
    - **RAG Systems**: Preparing context for language models\n\
    - **Data Processing**: Parallel processing of large text corpora\n\
    - **Analytics**: Text analysis on manageable segments\n\n\
    ## Conclusion\n\n\
    This documentation has covered the key aspects of the chunking system. With proper \
    configuration and understanding of the parameters, the system can handle diverse text \
    processing needs efficiently.";

fn bench_character_sizer(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunking/character_sizer");

    let texts = [
        ("small", SMALL_TEXT),
        ("medium", MEDIUM_TEXT),
        ("large", LARGE_TEXT),
    ];
    let chunk_sizes = [3, 10, 30, 100, 300];
    let overlaps = [0, 3, 10, 30];

    for (text_name, text) in &texts {
        for &chunk_size in &chunk_sizes {
            for &overlap in &overlaps {
                if overlap >= chunk_size {
                    continue;
                }
                let cfg = ChunkingConfig {
                    target_chunk_size: chunk_size,
                    overlap_size: overlap,
                    trim_whitespace: true,
                    file_format: None,
                };

                let chunker = RecursiveSplittingChunker::with_character_sizer(&cfg)
                    .expect("Failed to create chunker");
                group.throughput(Throughput::Bytes(text.len() as u64));
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!(
                        "{text_name}/chunk_{chunk_size}/overlap_{overlap}"
                    )),
                    &(&chunker, text),
                    |b, (chunker, text)| {
                        b.iter(|| {
                            let chunks: Vec<_> = chunker.chunks(black_box(text)).collect();
                            black_box(chunks);
                        });
                    },
                );
            }
        }
    }

    group.finish();
}

fn bench_openai_tokenizer(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunking/openai_tokenizer");

    for chunk_size in [10, 30, 100, 300] {
        let cfg = ChunkingConfig {
            target_chunk_size: chunk_size,
            overlap_size: 5,
            trim_whitespace: true,
            file_format: None,
        };

        let chunker = Arc::new(
            RecursiveSplittingChunker::for_openai_model("text-embedding-3-small", &cfg)
                .expect("Failed to create chunker"),
        ) as Arc<dyn Chunker>;

        group.throughput(Throughput::Bytes(LARGE_TEXT.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &chunker,
            |b, chunker| {
                b.iter(|| {
                    let chunks: Vec<_> = chunker.chunks(black_box(LARGE_TEXT)).collect();
                    black_box(chunks);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_character_sizer, bench_openai_tokenizer);
criterion_main!(benches);
