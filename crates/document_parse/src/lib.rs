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

use async_trait::async_trait;
use bytes::Bytes;
use snafu::prelude::*;
use std::{
    any::Any,
    collections::HashMap,
    fmt::Display,
    sync::{Arc, LazyLock},
};
use tokio::sync::Mutex;

mod archive;
mod docx;
mod pdf;
mod pdfium;
mod pptx;
mod xlsx;
pub use docx::DocxParser;
pub use pdf::PdfParser;
pub use pptx::PptxParser;
pub use xlsx::XlsxParser;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal parsing {format} document. Error: {source:?}"))]
    InternalParsingError {
        format: DocumentType,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to parse PDF document: the PDFium library could not be loaded ({reason}). \
        Docker images ship PDFium next to the runtime; for a standalone install, ensure the \
        host can reach GitHub so it can be downloaded automatically, or set the PDFIUM_LIB_PATH \
        environment variable to a directory containing the PDFium shared library. \
        See: https://spiceai.org/docs/features/document-processing"
    ))]
    PdfiumUnavailable { reason: String },

    #[snafu(display(
        "Failed to parse {format} document: {reason}. \
        Documents this large are not supported, and the file may be maliciously crafted (for example, a decompression bomb). \
        See: https://spiceai.org/docs/features/document-processing"
    ))]
    DocumentTooLarge {
        format: DocumentType,
        reason: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Runs CPU-bound document parsing on the blocking thread pool so it cannot
/// stall the async runtime (decompressing and parsing a large document can take
/// well over the ~100µs budget a runtime thread may hold). A panic inside the
/// closure, or runtime shutdown, surfaces as a structured parsing error for the
/// given `format` rather than propagating the panic.
pub(crate) async fn parse_blocking<T: Send + 'static>(
    format: DocumentType,
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|source| Error::InternalParsingError {
            format,
            source: Box::new(source),
        })?
}

static DOCUMENT_PARSER_FACTORY_REGISTRY: LazyLock<
    Mutex<HashMap<String, Arc<dyn DocumentParserFactory>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

// [`DocumentParserFactory`] added here should not hold live resources (e.g. cached connection pools).
// If a factory is ever added that owns a live resource, must reimplement an `unregister_all`.
pub async fn register_all() {
    register_parser_factory("docx", Arc::new(docx::DocxParserFactory {})).await;
    register_parser_factory("pdf", Arc::new(pdf::PdfParserFactory {})).await;
    register_parser_factory("pptx", Arc::new(pptx::PptxParserFactory {})).await;
    register_parser_factory("xlsx", Arc::new(xlsx::XlsxParserFactory {})).await;
}

/// Registry key for a format name or path suffix. A leading `.` is ignored and
/// the lookup is case-insensitive, so `pdf`, `PDF`, and `.PDF` all match.
fn parser_registry_key(name: &str) -> String {
    name.strip_prefix('.').unwrap_or(name).to_ascii_lowercase()
}

/// Look up a document parser by format name or file extension. The lookup is
/// case-insensitive and a leading `.` is ignored, so `pdf`, `PDF`, and `.PDF`
/// all resolve to the PDF parser.
pub async fn get_parser_factory(ext: &str) -> Option<Arc<dyn DocumentParserFactory>> {
    let key = parser_registry_key(ext);
    let registry = DOCUMENT_PARSER_FACTORY_REGISTRY.lock().await;
    registry.get(&key).cloned()
}

pub async fn register_parser_factory(
    name: &str,
    connector_factory: Arc<dyn DocumentParserFactory>,
) {
    let key = parser_registry_key(name);
    let mut registry = DOCUMENT_PARSER_FACTORY_REGISTRY.lock().await;
    registry.insert(key, connector_factory);
}

pub trait DocumentParserFactory: Send + Sync {
    fn create(&self, parser_options: &HashMap<String, String>) -> Result<Arc<dyn DocumentParser>>;

    /// Initialize a [`DocumentParser`] with all options set to default values
    fn default(&self) -> Arc<dyn DocumentParser>;

    /// Returns the table source as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

#[async_trait]
pub trait DocumentParser: Send + Sync {
    async fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>>;
}

pub trait Document {
    fn as_flat_utf8(&self) -> Result<String>;
    fn type_(&self) -> DocumentType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentType {
    Pdf,
    Docx,
    Pptx,
    Xlsx,
}

impl Display for DocumentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DocumentType::Pdf => write!(f, "PDF"),
            DocumentType::Docx => write!(f, "DOCX"),
            DocumentType::Pptx => write!(f, "PPTX"),
            DocumentType::Xlsx => write!(f, "XLSX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_parser_factory_is_case_insensitive() {
        register_all().await;
        for key in ["pdf", "PDF", "Pdf", ".pdf", ".PDF"] {
            assert!(
                get_parser_factory(key).await.is_some(),
                "parser lookup must find pdf for {key}"
            );
        }
        assert!(
            get_parser_factory("bin").await.is_none(),
            "unknown extensions still have no parser"
        );
    }

    #[tokio::test]
    async fn uppercase_pdf_extension_parses_sample() {
        register_all().await;
        let factory = get_parser_factory(".PDF")
            .await
            .expect("uppercase .PDF must resolve to the pdf parser");
        let doc = factory
            .default()
            .parse(&Bytes::from_static(include_bytes!(
                "../tests/fixtures/hello.pdf"
            )))
            .await
            .expect("sample PDF should parse");
        let text = doc.as_flat_utf8().expect("flat utf8 text");
        assert!(
            text.contains("Spice"),
            "expected extracted text to contain 'Spice', got: {text:?}"
        );
    }
}
