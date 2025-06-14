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
use bytes::Bytes;
use clap::Parser;
use std::{path::PathBuf, str::from_utf8, sync::Arc};

use document_parse::{DocumentParser, DocxParser, PdfParser};
use llms::chunking::{Chunker, ChunkingConfig, RecursiveSplittingChunker};

use crate::output::StructuredOutput;

mod output;

/// Parses and chunks documents in the same manner used internally to Spice.
///
/// Helpful for debugging `spiced`, or optimising various configurations for your spicepod dataset.
#[derive(Parser)]
pub struct Args {
    /// Filepath of the document to consider.
    #[arg(long)]
    pub file: PathBuf,

    /// The desired size of each chunk, in tokens.
    #[arg(long, default_value_t = 0)]
    pub target_chunk_size: usize,

    /// The amount of overlap between chunks, in tokens.
    #[arg(long, default_value_t = 0)]
    pub overlap_size: usize,

    /// Whether to trim the chunks to remove leading and trailing whitespace.
    #[arg(long, default_value_t = false)]
    pub trim_whitespace: bool,

    /// If set, return the parsed document within a JSON structure.
    #[arg(long, help = "Output in JSON format")]
    json: bool,
}

impl Args {
    fn parser(&self) -> Option<Arc<dyn DocumentParser>> {
        match self.file.extension().and_then(|s| s.to_str())? {
            "pdf" => Some(Arc::new(PdfParser::default())),
            "docx" => Some(Arc::new(DocxParser::default())),
            _ => None,
        }
    }

    fn chunker(&self) -> Option<Arc<dyn Chunker>> {
        if self.target_chunk_size == 0 && self.overlap_size == 0 && !self.trim_whitespace {
            return None;
        };
        // TODO use openai or tokenizer sizer.
        let chunker = RecursiveSplittingChunker::with_character_sizer(&ChunkingConfig {
            target_chunk_size: self.target_chunk_size,
            trim_whitespace: self.trim_whitespace,
            overlap_size: self.overlap_size,
            file_format: self.file.extension().and_then(|s| s.to_str()),
        })
        .unwrap();

        Some(Arc::new(chunker))
    }
}

fn main() {
    let args = Args::parse();

    // Get raw content.
    let bytz: Bytes = match std::fs::read(&args.file) {
        Ok(b) => b.into(),
        Err(e) => {
            eprintln!(
                "Could not load file '{}'. Error: {}",
                args.file.display(),
                e
            );
            return;
        }
    };

    // Convert to Utf8 string content. Either use parser, or interpret as raw UTF8.
    let content = if let Some(parser) = args.parser() {
        match parser.parse(&bytz) {
            Ok(doc) => match doc.as_flat_utf8() {
                Ok(content) => content,
                Err(e) => {
                    eprintln!("File could not be parsed into UTF8 correctly: {e}");
                    return;
                }
            },
            Err(e) => {
                eprintln!("File could not be parsed correctly: {e}");
                return;
            }
        }
    } else {
        let z: Vec<_> = bytz.into();
        match from_utf8(z.as_slice()) {
            Ok(s) => s.to_string(),
            Err(e) => {
                eprintln!("File could not be parsed into UTF8 correctly: {e}");
                return;
            }
        }
    };

    let result = if let Some(chunker) = args.chunker() {
        StructuredOutput::from_chunks(
            args.file.to_string_lossy(),
            chunker.chunks(content.as_str()).collect(),
        )
    } else {
        StructuredOutput::from_content(args.file.to_string_lossy(), content.as_str())
    };

    // Output dependent on return type.
    if args.json {
        match serde_json::to_string(&result) {
            Ok(s) => println!("{s}"),
            Err(e) => {
                eprintln!("Cannot parse output to JSON: {e}");
                return;
            }
        }
    } else {
        println!("{result}")
    }
}
