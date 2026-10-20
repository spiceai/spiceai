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
use docx_rs::Render;
use docx_rs::{Docx, read_docx};
use snafu::ResultExt;
use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{
    Document, DocumentParser, DocumentParserFactory, DocumentType, InternalParsingSnafu, Result,
    archive::{Limits, ensure_text_within_limit, rebuild_package},
};

pub struct DocxParserFactory {}

impl DocumentParserFactory for DocxParserFactory {
    fn create(&self, parser_options: &HashMap<String, String>) -> Result<Arc<dyn DocumentParser>> {
        Ok(Arc::new(DocxParser::new(parser_options)))
    }

    fn default(&self) -> Arc<dyn DocumentParser> {
        Arc::new(DocxParser::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
pub struct DocxParser {}
impl DocxParser {
    #[must_use]
    pub fn new(_parser_options: &HashMap<String, String>) -> Self {
        DocxParser::default()
    }
}

#[async_trait::async_trait]
impl DocumentParser for DocxParser {
    async fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>> {
        // `read_docx` decompresses every part it reads without a size limit.
        let package = rebuild_package(raw, DocumentType::Docx, Limits::DEFAULT)?;
        let doc = read_docx(&package).boxed().context(InternalParsingSnafu {
            format: DocumentType::Docx,
        })?;
        Ok(Arc::new(DocxDocument { doc }))
    }
}

struct DocxDocument {
    pub doc: Docx,
}

impl Document for DocxDocument {
    fn as_flat_utf8(&self) -> Result<String> {
        let text = self.doc.document.render_ascii();
        ensure_text_within_limit(text.len() as u64, DocumentType::Docx)?;
        Ok(text)
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Docx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::test_support::{Part, deflated_package};
    use docx_rs::{Paragraph, Run, Table, TableCell, TableRow};
    use std::io::Cursor;

    /// A `.docx` built in memory rather than committed as a fixture, so the bytes
    /// under test are produced by the same fork the extraction is read back
    /// through — a fixture would also be asserting that whatever wrote it once
    /// still agrees with the reader.
    fn docx_bytes(build: impl FnOnce(Docx) -> Docx) -> Bytes {
        let mut buffer = Cursor::new(Vec::new());
        build(Docx::new())
            .build()
            .pack(&mut buffer)
            .expect("packs the in-memory .docx");
        Bytes::from(buffer.into_inner())
    }

    async fn extracted_text(raw: &Bytes) -> String {
        DocxParser::default()
            .parse(raw)
            .await
            .expect("the in-memory .docx parses")
            .as_flat_utf8()
            .expect("flat utf8 text")
    }

    const CONTENT_TYPES: &[u8] = br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>"#;
    const PACKAGE_RELS: &[u8] = br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>"#;
    const DOCUMENT_RELS: &[u8] = br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"></Relationships>"#;
    const DOCUMENT_PREFIX: &[u8] = br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>"#;
    const DOCUMENT_SUFFIX: &[u8] = b"</w:t></w:r></w:p></w:body></w:document>";

    /// A minimal hand-built `.docx` whose single `<w:t>` holds `text_len`
    /// repetitions of `x`, written by a plain zip writer rather than `docx-rs`
    /// so the package has exactly the parts a hostile file would carry.
    fn single_run_docx(text_len: usize) -> Bytes {
        deflated_package(&[
            ("[Content_Types].xml", Part::Bytes(CONTENT_TYPES)),
            ("_rels/.rels", Part::Bytes(PACKAGE_RELS)),
            ("word/_rels/document.xml.rels", Part::Bytes(DOCUMENT_RELS)),
            (
                "word/document.xml",
                Part::Repeated {
                    prefix: DOCUMENT_PREFIX,
                    byte: b'x',
                    len: text_len,
                    suffix: DOCUMENT_SUFFIX,
                },
            ),
        ])
    }

    /// The hand-built package parses at a normal size, so the rejection below
    /// is the decompression limit and not a malformed package.
    #[tokio::test]
    async fn the_minimal_single_run_docx_parses() {
        let text = extracted_text(&single_run_docx(5)).await;
        assert_eq!(text, "xxxxx");
    }

    /// Regression test for a decompression bomb: a `.docx` of well under a
    /// megabyte whose `word/document.xml` inflates past the per-entry limit must
    /// fail with a size error instead of being decompressed in full.
    #[tokio::test]
    async fn rejects_a_document_part_that_decompresses_past_the_entry_limit() {
        let text_len = usize::try_from(crate::archive::MAX_ENTRY_BYTES).expect("fits usize");
        let raw = single_run_docx(text_len);
        assert!(
            raw.len() < 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            raw.len()
        );

        let Err(err) = DocxParser::default().parse(&raw).await else {
            panic!("a document part past the decompression limit must be rejected");
        };
        let crate::Error::DocumentTooLarge {
            format: DocumentType::Docx,
            reason,
        } = &err
        else {
            panic!("expected a DOCX size-limit error, got: {err}");
        };
        assert_eq!(
            reason,
            "archive entry 'word/document.xml' decompresses to more than 64 MiB, the per-entry limit"
        );
    }

    /// Where the newlines land when a `.docx` becomes text, which is what decides
    /// where the chunker splits it and therefore what gets embedded.
    ///
    /// Both halves are a Spice patch to the `spiceai/docx-rs` fork: upstream has no
    /// `Render` at all, and the placement was got wrong once *inside* the fork
    /// before it was fixed there — paragraph children were separated instead of
    /// document children, which is exactly the shape a re-cut can restore. The
    /// consequence is not a failure: extraction succeeds, and the text it returns
    /// merges two paragraphs into one sentence or splits one sentence in two.
    /// Nothing downstream can tell that from a document that was written that way.
    #[tokio::test]
    async fn a_docx_separates_paragraphs_and_not_the_runs_inside_one() {
        let raw = docx_bytes(|docx| {
            docx.add_paragraph(
                Paragraph::new()
                    .add_run(Run::new().add_text("first"))
                    .add_run(Run::new().add_text("-continued")),
            )
            .add_paragraph(Paragraph::new().add_run(Run::new().add_text("second")))
        });

        let text = extracted_text(&raw).await;

        assert!(
            text.contains("first-continued"),
            "two runs of one paragraph must not be separated — a break inserted mid-sentence \
             moves the chunk boundary: {text:?}"
        );
        assert!(
            text.contains("first-continued\nsecond"),
            "two paragraphs must be separated by exactly one newline, or the extracted text runs \
             them together into one sentence: {text:?}"
        );
    }

    /// A table's own newline placement, which the same patch carries: cells are
    /// delimited within a row and rows are delimited from each other, so a
    /// table reads as rows rather than as one run of concatenated cell values.
    #[tokio::test]
    async fn a_docx_table_separates_its_rows_and_cells() {
        let cell = |text: &str| {
            TableCell::new().add_paragraph(Paragraph::new().add_run(Run::new().add_text(text)))
        };
        let raw = docx_bytes(|docx| {
            docx.add_table(Table::new(vec![
                TableRow::new(vec![cell("a1"), cell("b1")]),
                TableRow::new(vec![cell("a2"), cell("b2")]),
            ]))
        });

        let text = extracted_text(&raw).await;

        for row in ["a1", "b1", "a2", "b2"] {
            assert!(
                text.contains(row),
                "the extracted text lost the cell {row:?}: {text:?}"
            );
        }
        assert!(
            text.contains("a1") && text.contains("b1") && !text.contains("a1b1"),
            "two cells of one row must be delimited, not concatenated: {text:?}"
        );
        let a1_line = text
            .lines()
            .find(|line| line.contains("a1"))
            .expect("a line holding the first row");
        assert!(
            !a1_line.contains("a2"),
            "two rows must be on separate lines, or the table reads as one run of values: \
             {text:?}"
        );
    }
}
