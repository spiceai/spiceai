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
        let doc = read_docx(raw).boxed().context(InternalParsingSnafu {
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
        Ok(self.doc.document.render_ascii())
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Docx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
