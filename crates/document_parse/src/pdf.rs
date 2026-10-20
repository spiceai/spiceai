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
use liteparse::{LiteParse, LiteParseConfig, OutputFormat, types::PdfInput};
use snafu::ResultExt;
use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{
    Document, DocumentParser, DocumentParserFactory, DocumentType, InternalParsingSnafu, Result,
    archive::ensure_pdf_within_budget,
};

pub struct PdfParserFactory {}

impl DocumentParserFactory for PdfParserFactory {
    fn create(&self, parser_options: &HashMap<String, String>) -> Result<Arc<dyn DocumentParser>> {
        Ok(Arc::new(PdfParser::new(parser_options)))
    }

    fn default(&self) -> Arc<dyn DocumentParser> {
        Arc::new(PdfParser::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
pub struct PdfParser {}
impl PdfParser {
    #[must_use]
    pub fn new(_parser_options: &HashMap<String, String>) -> Self {
        PdfParser::default()
    }
}

#[async_trait::async_trait]
impl DocumentParser for PdfParser {
    async fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>> {
        // PDFium inflates `/FlateDecode` streams internally with no size limit,
        // so bound the decompressed size before loading it or handing it the
        // bytes: a bomb is rejected without provisioning the library at all. The
        // scan inflates up to the budget, so it runs off the async runtime.
        let scan_bytes = raw.clone();
        crate::parse_blocking(DocumentType::Pdf, move || {
            ensure_pdf_within_budget(&scan_bytes)
        })
        .await?;

        // `liteparse` loads PDFium at runtime via `dlopen` and panics if it is
        // missing. Provision it first (found next to the binary in Docker, or
        // downloaded on demand for standalone installs) so an absent library
        // surfaces as a structured error instead of a panic.
        crate::pdfium::ensure_loaded().await?;

        let config = LiteParseConfig {
            ocr_enabled: false,
            output_format: OutputFormat::Text,
            quiet: true,
            ..Default::default()
        };

        let doc = LiteParse::new(config)
            .parse_input(PdfInput::Bytes(raw.to_vec()))
            .await
            .map(|parsed| parsed.text)
            .boxed()
            .context(InternalParsingSnafu {
                format: DocumentType::Pdf,
            })?;
        Ok(Arc::new(PdfDocument { doc }))
    }
}

struct PdfDocument {
    pub doc: String,
}

impl Document for PdfDocument {
    fn as_flat_utf8(&self) -> Result<String> {
        Ok(self.doc.clone())
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Pdf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal, well-formed single-page PDF whose content stream draws the
    /// text "Spice Hello".
    const SAMPLE_PDF: &[u8] = include_bytes!("../tests/fixtures/hello.pdf");

    #[tokio::test]
    async fn extracts_text_from_pdf() {
        let parser = PdfParser::default();
        let doc = parser
            .parse(&Bytes::from_static(SAMPLE_PDF))
            .await
            .expect("sample PDF should parse");
        let text = doc.as_flat_utf8().expect("flat utf8 text");
        assert!(
            text.contains("Spice"),
            "expected extracted text to contain 'Spice', got: {text:?}"
        );
    }

    /// A minimal PDF carrying one `/FlateDecode` stream whose deflate data
    /// expands to `decompressed_len` bytes. Deflate of a single repeated byte is
    /// ~1000:1, so the file stays tiny however large `decompressed_len` is. The
    /// structure (header, one indirect object, the stream, `endstream`) is only
    /// what the budget scanner walks — it never has to be a loadable document.
    fn flate_bomb_pdf(decompressed_len: usize) -> Bytes {
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        let chunk = vec![b' '; 1024 * 1024];
        let mut left = decompressed_len;
        while left > 0 {
            let n = left.min(chunk.len());
            encoder.write_all(&chunk[..n]).expect("deflate chunk");
            left -= n;
        }
        let deflated = encoder.finish().expect("finish deflate");

        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n");
        pdf.extend_from_slice(
            format!(
                "1 0 obj\n<< /Length {} /Filter /FlateDecode >>\nstream\n",
                deflated.len()
            )
            .as_bytes(),
        );
        pdf.extend_from_slice(&deflated);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        Bytes::from(pdf)
    }

    /// Regression test for a PDF content-stream decompression bomb: a small file
    /// whose Flate stream decompresses past the per-document budget must be
    /// rejected before `PDFium` (which decompresses with no limit) is ever loaded.
    #[tokio::test]
    async fn rejects_a_flate_stream_bomb_before_loading_pdfium() {
        let over_budget =
            usize::try_from(crate::archive::MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        let raw = flate_bomb_pdf(over_budget);
        assert!(
            raw.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            raw.len()
        );

        let Err(err) = PdfParser::default().parse(&raw).await else {
            panic!("a PDF whose streams exceed the budget must be rejected");
        };
        let crate::Error::DocumentTooLarge {
            format: DocumentType::Pdf,
            reason,
        } = &err
        else {
            panic!("expected a PDF size-limit error, got: {err}");
        };
        assert_eq!(
            reason,
            "its compressed streams decompress to more than 256 MiB, the per-document limit"
        );
    }

    /// A nested `[/FlateDecode /FlateDecode]` stream whose inner payload exceeds
    /// the budget must be rejected through the parser, before `PDFium` applies
    /// the chain and expands it unbounded. A single-pass scan would only see the
    /// small intermediate and let it through.
    #[tokio::test]
    async fn rejects_a_nested_flate_bomb_before_loading_pdfium() {
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;

        let over = usize::try_from(crate::archive::MAX_PACKAGE_BYTES).expect("fits usize")
            + 1024 * 1024;
        let mut inner = ZlibEncoder::new(Vec::new(), Compression::best());
        let chunk = vec![b' '; 1024 * 1024];
        let mut left = over;
        while left > 0 {
            let n = left.min(chunk.len());
            inner.write_all(&chunk[..n]).expect("deflate chunk");
            left -= n;
        }
        let inner = inner.finish().expect("finish inner deflate");
        let mut outer = ZlibEncoder::new(Vec::new(), Compression::best());
        outer.write_all(&inner).expect("deflate outer");
        let data = outer.finish().expect("finish outer deflate");

        let mut pdf = Vec::new();
        pdf.extend_from_slice(b"%PDF-1.7\n1 0 obj\n<< /Length ");
        pdf.extend_from_slice(data.len().to_string().as_bytes());
        pdf.extend_from_slice(b" /Filter [/FlateDecode /FlateDecode] >>\nstream\n");
        pdf.extend_from_slice(&data);
        pdf.extend_from_slice(b"\nendstream\nendobj\n");
        let raw = Bytes::from(pdf);
        assert!(
            raw.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            raw.len()
        );

        let Err(crate::Error::DocumentTooLarge {
            format: DocumentType::Pdf,
            ..
        }) = PdfParser::default().parse(&raw).await
        else {
            panic!("a nested-Flate PDF whose streams exceed the budget must be rejected");
        };
    }

    /// A legitimate PDF whose streams are well within the budget still parses,
    /// so the scanner does not reject ordinary documents.
    #[tokio::test]
    async fn accepts_a_pdf_whose_streams_are_within_budget() {
        ensure_pdf_within_budget(&Bytes::from_static(SAMPLE_PDF))
            .expect("the sample PDF is within the decompression budget");
    }

    #[tokio::test]
    async fn invalid_pdf_returns_error_without_panicking() {
        let parser = PdfParser::default();
        let result = parser
            .parse(&Bytes::from_static(
                b"this is definitely not a pdf document",
            ))
            .await;
        assert!(
            result.is_err(),
            "an invalid PDF must return a structured error, not succeed or panic"
        );
    }
}
