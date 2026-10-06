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

//! `pdf-parse` — parse a single PDF with either the `liteparse` (`PDFium`-backed)
//! parser used by `crates/document_parse`, the candidate pure-Rust
//! `pdf-inspector` parser, or both, so a human can eyeball or `diff` the
//! extracted text and judge fidelity before swapping backends.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Instant;

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use document_parse::{DocumentParser, PdfParser};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read PDF file {}: {source}", path.display()))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse PDF with liteparse: {source}"))]
    Liteparse { source: document_parse::Error },

    #[snafu(display("Failed to parse PDF with pdf-inspector: {source}"))]
    PdfInspector { source: pdf_inspector::PdfError },

    #[snafu(display("Failed to parse PDF with pdf-inspector: worker task failed: {source}"))]
    PdfInspectorTask { source: tokio::task::JoinError },

    #[snafu(display("Failed to read extracted liteparse text: {source}"))]
    LiteparseText { source: document_parse::Error },

    #[snafu(display("Failed to create output directory {}: {source}", path.display()))]
    CreateOutDir {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write output file {}: {source}", path.display()))]
    WriteOut {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to serialize JSON output: {source}"))]
    SerializeJson { source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Which parser backend(s) to run.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum ParserChoice {
    /// The current PDF parser (`PDFium`-backed) used by `crates/document_parse`.
    Liteparse,
    /// The candidate pure-Rust replacement.
    PdfInspector,
    /// Run both backends and compare their output.
    Both,
}

/// Output format to request.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    /// Plain extracted text.
    Text,
    /// Markdown (pdf-inspector only; liteparse falls back to text).
    Markdown,
    /// JSON object containing each backend's plain extracted text and timing.
    Json,
}

impl OutputFormat {
    /// File extension for an output file in this format.
    fn extension(self) -> &'static str {
        match self {
            OutputFormat::Text => "txt",
            OutputFormat::Markdown => "md",
            OutputFormat::Json => "json",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "pdf-parse",
    about = "Parse a PDF with liteparse (PDFium) and/or pdf-inspector (pure Rust) to compare their extracted text",
    version
)]
struct Args {
    /// Path to the input PDF.
    pdf_path: PathBuf,

    /// Which parser backend(s) to run.
    #[arg(long, value_enum, default_value_t = ParserChoice::Both)]
    parser: ParserChoice,

    /// Output format. `json` emits a single object keyed by parser name, with text and timing.
    /// `markdown` only affects pdf-inspector; liteparse always emits text.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,

    /// If given, write each parser's output to a file in this directory (in addition to stdout).
    #[arg(long)]
    out: Option<PathBuf>,
}

/// The result of running one backend, for reporting.
struct BackendOutput {
    /// Stable JSON key for this parser.
    key: &'static str,
    /// Human-readable backend label.
    label: &'static str,
    /// Format actually produced (may differ from the request, e.g. liteparse markdown -> text).
    format: OutputFormat,
    /// Extracted content.
    content: String,
    /// Wall-clock duration from reading the input through materializing the
    /// extracted content, in milliseconds (fractional, ~µs resolution).
    duration_ms: f64,
    /// A note to surface to the user (e.g. a format fallback), if any.
    note: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = run(args).await {
        eprintln!("{e}");
        exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    if let Some(dir) = &args.out {
        tokio::fs::create_dir_all(dir)
            .await
            .context(CreateOutDirSnafu { path: dir.clone() })?;
    }

    let mut outputs = Vec::new();
    let mut had_backend_error = false;

    if matches!(args.parser, ParserChoice::Liteparse | ParserChoice::Both) {
        match run_liteparse(&args.pdf_path, args.format).await {
            Ok(output) => outputs.push(output),
            // In `both` mode, report this backend's failure but keep going so
            // the other backend still runs and its result is still reported.
            Err(e) if args.parser == ParserChoice::Both => {
                eprintln!("{e}");
                had_backend_error = true;
            }
            Err(e) => return Err(e),
        }
    }

    if matches!(args.parser, ParserChoice::PdfInspector | ParserChoice::Both) {
        match run_pdf_inspector(&args.pdf_path, args.format).await {
            Ok(output) => outputs.push(output),
            Err(e) if args.parser == ParserChoice::Both => {
                eprintln!("{e}");
                had_backend_error = true;
            }
            Err(e) => return Err(e),
        }
    }

    if args.format == OutputFormat::Json {
        report_json(&outputs, args.out.as_deref())?;
    } else {
        for output in &outputs {
            report(output, args.out.as_deref())?;
        }
    }

    if had_backend_error {
        exit(1);
    }

    Ok(())
}

/// Parse with the `liteparse`/`PDFium` backend, exactly as `crates/document_parse`
/// configures it (via its public `PdfParser`), so the comparison reflects real
/// runtime behavior — including input I/O and the `PDFium` preload.
async fn run_liteparse(path: &Path, format: OutputFormat) -> Result<BackendOutput> {
    let note = (format == OutputFormat::Markdown).then(|| {
        "liteparse only produces plain text; showing text output for the requested markdown format"
            .to_string()
    });

    let start = Instant::now();
    let raw = tokio::fs::read(path).await.context(ReadFileSnafu {
        path: path.to_path_buf(),
    })?;
    let bytes = Bytes::from(raw);
    let parser = PdfParser::default();
    let doc = parser.parse(&bytes).await.context(LiteparseSnafu)?;
    let content = doc.as_flat_utf8().context(LiteparseTextSnafu)?;
    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(BackendOutput {
        key: "liteparse",
        label: "liteparse (PDFium)",
        format: OutputFormat::Text,
        content,
        duration_ms,
        note,
    })
}

/// Parse with the candidate `pdf-inspector` backend. For text we use
/// `extract_text`; for markdown we use `process_pdf`, whose full pipeline
/// populates `PdfProcessResult::markdown`.
async fn run_pdf_inspector(path: &Path, format: OutputFormat) -> Result<BackendOutput> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || run_pdf_inspector_blocking(&path, format))
        .await
        .context(PdfInspectorTaskSnafu)?
}

fn run_pdf_inspector_blocking(path: &Path, format: OutputFormat) -> Result<BackendOutput> {
    let start = Instant::now();
    let (content, actual_format, note) = match format {
        OutputFormat::Text | OutputFormat::Json => {
            let text = pdf_inspector::extract_text(path).context(PdfInspectorSnafu)?;
            (text, OutputFormat::Text, None)
        }
        OutputFormat::Markdown => {
            let result = pdf_inspector::process_pdf(path).context(PdfInspectorSnafu)?;
            if let Some(md) = result.markdown {
                (md, OutputFormat::Markdown, None)
            } else {
                // The full pipeline did not produce markdown (e.g. a scanned
                // page needing OCR); fall back to plain text so the run is
                // still comparable rather than empty.
                let text = pdf_inspector::extract_text(path).context(PdfInspectorSnafu)?;
                (
                    text,
                    OutputFormat::Text,
                    Some(
                        "pdf-inspector produced no markdown for this PDF; showing extracted text instead"
                            .to_string(),
                    ),
                )
            }
        }
    };
    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

    Ok(BackendOutput {
        key: "pdf_inspector",
        label: "pdf-inspector (pure Rust)",
        format: actual_format,
        content,
        duration_ms,
        note,
    })
}

/// Print a JSON object mapping parser names to their plain extracted text and timing.
///
/// JSON output intentionally omits display metadata so stdout can be consumed
/// directly by a JSON parser.
fn report_json(outputs: &[BackendOutput], out_dir: Option<&Path>) -> Result<()> {
    let results_by_parser = outputs
        .iter()
        .map(|output| {
            (
                output.key,
                serde_json::json!({
                    "text": output.content,
                    "duration_ms": output.duration_ms,
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let json = serde_json::to_string(&results_by_parser).context(SerializeJsonSnafu)?;

    if let Some(dir) = out_dir {
        let path = dir.join("pdf-parse.json");
        std::fs::write(&path, &json).context(WriteOutSnafu { path })?;
    }

    println!("{json}");
    Ok(())
}

/// Print a labeled header, the summary line, any note, and either the full
/// content (to stdout) or a written-file confirmation when `--out` is set.
fn report(output: &BackendOutput, out_dir: Option<&Path>) -> Result<()> {
    let chars = output.content.chars().count();
    let lines = output.content.lines().count();

    println!(
        "===== {} [{}] =====",
        output.label,
        format_name(output.format)
    );
    println!(
        "summary: {chars} chars, {lines} lines, processed end-to-end in {:.3} ms",
        output.duration_ms
    );
    if let Some(note) = &output.note {
        println!("note: {note}");
    }

    if let Some(dir) = out_dir {
        let file_name = format!("{}.{}", file_stem(output.label), output.format.extension());
        let path = dir.join(&file_name);
        std::fs::write(&path, &output.content).context(WriteOutSnafu { path: path.clone() })?;
        println!("wrote output to {}", path.display());
    } else {
        println!("{}", output.content);
    }
    println!();

    Ok(())
}

/// Display name for a format.
fn format_name(format: OutputFormat) -> &'static str {
    match format {
        OutputFormat::Text => "text",
        OutputFormat::Markdown => "markdown",
        OutputFormat::Json => "json",
    }
}

/// Stable file stem for a backend's output file.
fn file_stem(label: &str) -> &'static str {
    if label.starts_with("liteparse") {
        "liteparse"
    } else {
        "pdf-inspector"
    }
}
