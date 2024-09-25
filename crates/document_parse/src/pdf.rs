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

use bytes::Bytes;
use lopdf::Document as LopdfDocument;
use snafu::ResultExt;
use std::{any::Any, collections::HashMap, io::Cursor, sync::Arc};

use crate::{
    Document, DocumentParser, DocumentParserFactory, DocumentType, InternalParsingSnafu, Result,
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
    pub fn new(_parser_options: &HashMap<String, String>) -> Self {
        PdfParser::default()
    }
}

impl DocumentParser for PdfParser {
    fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>> {
        let doc = LopdfDocument::load_from(Cursor::new(raw.to_vec()))
            .boxed()
            .context(InternalParsingSnafu {
                format: DocumentType::Pdf,
            })?;
        Ok(Arc::new(PdfDocument { doc }))
    }
}

struct PdfDocument {
    pub doc: LopdfDocument,
}

impl Document for PdfDocument {
    fn as_flat_utf8(&self) -> Result<String> {
        let pages = self.doc.get_pages().keys().copied().collect::<Vec<_>>();
        self.doc
            .extract_text(&pages)
            .boxed()
            .context(InternalParsingSnafu {
                format: DocumentType::Pdf,
            })
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Pdf
    }
}
