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

//! `pdf-split` — split a multi-page PDF (or every PDF in a directory) into one
//! single-page PDF per page, naming each output `pNNNN.pdf` with a zero-indexed
//! page number.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use pdf_split::{split_pdf, split_pdf_dir};

#[derive(Parser, Debug)]
#[command(
    name = "pdf-split",
    about = "Split a multi-page PDF, or every *.pdf in a directory, into one single-page PDF per page."
)]
struct Args {
    /// Input PDF file, or a directory of `*.pdf` files to split.
    input: PathBuf,

    /// Output directory. A single input writes `<out>/pNNNN.pdf`; a directory
    /// input writes `<out>/<stem>/pNNNN.pdf` per source document.
    #[arg(long)]
    out: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let result = if args.input.is_dir() {
        split_pdf_dir(&args.input, &args.out)
    } else {
        split_pdf(&args.input, &args.out)
    };

    match result {
        Ok(paths) => {
            for path in &paths {
                println!("{}", path.display());
            }
            eprintln!("Wrote {} page(s).", paths.len());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}
