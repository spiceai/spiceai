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
    /// small intermediate and let it through. The fixture is a loadable
    /// single-page PDF (Catalog/Pages/Page/xref), so `PDFium` would expand the
    /// chain if the preflight missed it.
    #[tokio::test]
    async fn rejects_a_nested_flate_bomb_before_loading_pdfium() {
        use crate::archive::test_support::loadable_pdf_with_content_stream;
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;

        let over =
            usize::try_from(crate::archive::MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
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

        let raw = loadable_pdf_with_content_stream(b"[/FlateDecode /FlateDecode]", &data);
        assert!(
            raw.len() < 2 * 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            raw.len()
        );
        assert!(
            raw.windows(b"xref".len()).any(|w| w == b"xref"),
            "the fixture must be a loadable PDF with an xref"
        );

        let Err(crate::Error::DocumentTooLarge {
            format: DocumentType::Pdf,
            ..
        }) = PdfParser::default().parse(&raw).await
        else {
            panic!("a nested-Flate PDF whose streams exceed the budget must be rejected");
        };
    }

    /// Same loadable shape with an ASCII85 wrapper around Flate: a single-pass
    /// inflate of the raw stream bytes cannot see the bomb, but budgeting the
    /// declared chain must still reject it before `PDFium` runs.
    #[tokio::test]
    async fn rejects_an_ascii85_wrapped_flate_bomb_before_loading_pdfium() {
        use crate::archive::test_support::loadable_pdf_with_content_stream;
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;

        let over =
            usize::try_from(crate::archive::MAX_PACKAGE_BYTES).expect("fits usize") + 1024 * 1024;
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        let chunk = vec![b' '; 1024 * 1024];
        let mut left = over;
        while left > 0 {
            let n = left.min(chunk.len());
            encoder.write_all(&chunk[..n]).expect("deflate chunk");
            left -= n;
        }
        let deflated = encoder.finish().expect("finish deflate");

        // ASCII85-encode the deflated payload (same encoding the archive tests use).
        let mut ascii85 = Vec::new();
        for chunk in deflated.chunks(4) {
            let mut buf = [0u8; 4];
            buf[..chunk.len()].copy_from_slice(chunk);
            let mut value = u32::from_be_bytes(buf);
            let mut group = [0u8; 5];
            for slot in group.iter_mut().rev() {
                *slot = (value % 85) as u8 + b'!';
                value /= 85;
            }
            ascii85.extend_from_slice(&group[..=chunk.len()]);
        }
        ascii85.extend_from_slice(b"~>");

        let raw = loadable_pdf_with_content_stream(b"[/ASCII85Decode /FlateDecode]", &ascii85);
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
            panic!("an ASCII85+Flate PDF whose streams exceed the budget must be rejected");
        };
    }

    /// A PDF 1.5 document as many writers produce it: the page content and the
    /// cross-reference stream are Flate streams with a PNG predictor
    /// (`/Predictor 12`, every row tagged 0 = None).
    fn predicted_xref_stream_pdf() -> Bytes {
        use flate2::{Compression, write::ZlibEncoder};
        use std::io::Write;

        fn predicted_flate(data: &[u8], columns: usize) -> Vec<u8> {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            for row in data.chunks(columns) {
                encoder.write_all(&[0]).expect("row tag");
                encoder.write_all(row).expect("row");
            }
            encoder.finish().expect("finish deflate")
        }

        let content = predicted_flate(b"BT /F1 24 Tf 60 120 Td (Spice Hello) Tj ET\n", 8);
        let mut content_obj = format!(
            "<< /Length {} /Filter /FlateDecode /DecodeParms << /Predictor 12 /Columns 8 >> >>\nstream\n",
            content.len()
        )
        .into_bytes();
        content_obj.extend_from_slice(&content);
        content_obj.extend_from_slice(b"\nendstream");

        let objs: [&[u8]; 5] = [
            b"<< /Type /Catalog /Pages 2 0 R >>",
            b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
            content_obj.as_slice(),
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        ];
        let mut pdf = b"%PDF-1.5\n".to_vec();
        let mut offsets = Vec::new();
        for (i, body) in objs.iter().enumerate() {
            offsets.push(pdf.len());
            pdf.extend_from_slice(format!("{} 0 obj\n", i + 1).as_bytes());
            pdf.extend_from_slice(body);
            pdf.extend_from_slice(b"\nendobj\n");
        }

        // Object 6 is the cross-reference stream: `/W [1 4 1]` rows for
        // objects 0 to 6, including itself.
        let xref_offset = pdf.len();
        offsets.push(xref_offset);
        let mut rows = vec![0u8, 0, 0, 0, 0, 255];
        for offset in offsets {
            rows.push(1);
            rows.extend_from_slice(
                &u32::try_from(offset)
                    .expect("offset fits u32")
                    .to_be_bytes(),
            );
            rows.push(0);
        }
        let xref = predicted_flate(&rows, 6);
        pdf.extend_from_slice(
            format!(
                "6 0 obj\n<< /Type /XRef /Size 7 /W [1 4 1] /Root 1 0 R /Length {} /Filter /FlateDecode /DecodeParms << /Columns 6 /Predictor 12 >> >>\nstream\n",
                xref.len()
            )
            .as_bytes(),
        );
        pdf.extend_from_slice(&xref);
        pdf.extend_from_slice(
            format!("\nendstream\nendobj\nstartxref\n{xref_offset}\n%%EOF").as_bytes(),
        );
        Bytes::from(pdf)
    }

    /// Predictors on single Flate streams are common (every PDF 1.5
    /// cross-reference stream has one), so such a document passes the budget
    /// and `PDFium` extracts its text.
    #[tokio::test]
    async fn extracts_text_from_a_pdf_with_predicted_flate_streams() {
        let doc = PdfParser::default()
            .parse(&predicted_xref_stream_pdf())
            .await
            .expect("a PDF with predicted Flate streams should parse");
        let text = doc.as_flat_utf8().expect("flat utf8 text");
        assert!(
            text.contains("Spice Hello"),
            "expected extracted text to contain 'Spice Hello', got: {text:?}"
        );
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
