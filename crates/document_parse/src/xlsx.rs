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

use bytes::Bytes;
use calamine::{Reader, Xlsx, open_workbook_from_rs};
use snafu::ResultExt;
use std::io::Cursor;
use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{
    Document, DocumentParser, DocumentParserFactory, DocumentType, InternalParsingSnafu, Result,
};

pub struct XlsxParserFactory {}

impl DocumentParserFactory for XlsxParserFactory {
    fn create(&self, parser_options: &HashMap<String, String>) -> Result<Arc<dyn DocumentParser>> {
        Ok(Arc::new(XlsxParser::new(parser_options)))
    }

    fn default(&self) -> Arc<dyn DocumentParser> {
        Arc::new(XlsxParser::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
pub struct XlsxParser {}
impl XlsxParser {
    #[must_use]
    pub fn new(_parser_options: &HashMap<String, String>) -> Self {
        XlsxParser::default()
    }
}

impl DocumentParser for XlsxParser {
    fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>> {
        let cursor = Cursor::new(raw.clone());
        let mut wb: Xlsx<_> =
            open_workbook_from_rs(cursor)
                .boxed()
                .context(InternalParsingSnafu {
                    format: DocumentType::Xlsx,
                })?;
        let mut sheets: Vec<(String, Vec<Vec<String>>)> = Vec::new();
        let names: Vec<String> = wb.sheet_names();
        for name in names {
            let range = wb
                .worksheet_range(&name)
                .boxed()
                .context(InternalParsingSnafu {
                    format: DocumentType::Xlsx,
                })?;
            let mut rows: Vec<Vec<String>> = Vec::new();
            for row in range.rows() {
                rows.push(row.iter().map(ToString::to_string).collect());
            }
            sheets.push((name, rows));
        }
        Ok(Arc::new(XlsxDocument { sheets }))
    }
}

struct XlsxDocument {
    sheets: Vec<(String, Vec<Vec<String>>)>,
}

impl Document for XlsxDocument {
    fn as_flat_utf8(&self) -> Result<String> {
        let mut out = String::new();
        for (name, rows) in &self.sheets {
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            out.push_str("# ");
            out.push_str(name);
            out.push('\n');
            for row in rows {
                out.push_str(&row.join("\t"));
                out.push('\n');
            }
        }
        Ok(out)
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Xlsx
    }
}
