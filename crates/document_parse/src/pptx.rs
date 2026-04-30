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
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use snafu::ResultExt;
use std::io::{Cursor, Read};
use std::{any::Any, collections::HashMap, sync::Arc};
use zip::ZipArchive;

use crate::{
    Document, DocumentParser, DocumentParserFactory, DocumentType, InternalParsingSnafu, Result,
};

pub struct PptxParserFactory {}

impl DocumentParserFactory for PptxParserFactory {
    fn create(&self, parser_options: &HashMap<String, String>) -> Result<Arc<dyn DocumentParser>> {
        Ok(Arc::new(PptxParser::new(parser_options)))
    }

    fn default(&self) -> Arc<dyn DocumentParser> {
        Arc::new(PptxParser::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
pub struct PptxParser {}
impl PptxParser {
    #[must_use]
    pub fn new(_parser_options: &HashMap<String, String>) -> Self {
        PptxParser::default()
    }
}

impl DocumentParser for PptxParser {
    fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>> {
        let cursor = Cursor::new(raw.clone());
        let mut archive = ZipArchive::new(cursor)
            .boxed()
            .context(InternalParsingSnafu {
                format: DocumentType::Pptx,
            })?;

        // PPTX layout: slides live at `ppt/slides/slideN.xml`. Sort by
        // numeric suffix so the rendered text follows presentation order
        // rather than zip-entry order (which is alphanumeric and mis-orders
        // slide10 before slide2). `file_names()` reads the central directory
        // without setting up per-entry decompression, so the enumeration pass
        // is cheap.
        let mut slide_paths: Vec<String> = archive
            .file_names()
            .filter(|n| is_slide_path(n))
            .map(String::from)
            .collect();
        slide_paths.sort_by_key(|p| slide_index(p).unwrap_or(usize::MAX));

        let mut slides: Vec<String> = Vec::with_capacity(slide_paths.len());
        for path in slide_paths {
            let mut entry = archive
                .by_name(&path)
                .boxed()
                .context(InternalParsingSnafu {
                    format: DocumentType::Pptx,
                })?;
            let mut xml = String::new();
            entry
                .read_to_string(&mut xml)
                .boxed()
                .context(InternalParsingSnafu {
                    format: DocumentType::Pptx,
                })?;
            slides.push(extract_slide_text(&xml)?);
        }

        Ok(Arc::new(PptxDocument { slides }))
    }
}

fn is_slide_path(name: &str) -> bool {
    let Some(stripped) = name.strip_prefix("ppt/slides/slide") else {
        return false;
    };
    if stripped.contains("_rels") {
        return false;
    }
    std::path::Path::new(name)
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("xml"))
}

fn slide_index(path: &str) -> Option<usize> {
    let stripped = path.strip_prefix("ppt/slides/slide")?;
    let slide_path = std::path::Path::new(stripped);
    slide_path
        .extension()
        .filter(|ext| ext.eq_ignore_ascii_case("xml"))?;
    slide_path.file_stem()?.to_str()?.parse().ok()
}

/// Extract concatenated `<a:t>` text from a single slide's XML. Each text
/// run is separated by a space, each paragraph by a newline.
fn extract_slide_text(xml: &str) -> Result<String> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut out = String::new();
    let mut in_text = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.local_name().as_ref() == b"t" => {
                in_text = true;
                if !out.is_empty() && !out.ends_with('\n') {
                    out.push(' ');
                }
            }
            Ok(Event::End(ref e)) if e.local_name().as_ref() == b"t" => {
                in_text = false;
            }
            Ok(Event::End(ref e)) if e.local_name().as_ref() == b"p" => {
                out.push('\n');
            }
            Ok(Event::Text(t)) if in_text => {
                let decoded = t.decode().boxed().context(InternalParsingSnafu {
                    format: DocumentType::Pptx,
                })?;
                let unescaped = quick_xml::escape::unescape(&decoded).boxed().context(
                    InternalParsingSnafu {
                        format: DocumentType::Pptx,
                    },
                )?;
                out.push_str(&unescaped);
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => {
                return Err(crate::Error::InternalParsingError {
                    format: DocumentType::Pptx,
                    source: Box::new(e),
                });
            }
        }
        buf.clear();
    }
    Ok(out)
}

struct PptxDocument {
    slides: Vec<String>,
}

impl Document for PptxDocument {
    fn as_flat_utf8(&self) -> Result<String> {
        use std::fmt::Write as _;
        let mut out = String::new();
        for (i, text) in self.slides.iter().enumerate() {
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            let _ = writeln!(out, "# Slide {}", i + 1);
            out.push_str(text);
        }
        Ok(out)
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Pptx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slide_index_parses_numeric_suffix() {
        assert_eq!(slide_index("ppt/slides/slide1.xml"), Some(1));
        assert_eq!(slide_index("ppt/slides/slide12.xml"), Some(12));
        assert_eq!(slide_index("ppt/slides/_rels/slide1.xml.rels"), None);
        assert_eq!(slide_index("ppt/theme/theme1.xml"), None);
    }

    #[test]
    fn is_slide_path_excludes_rels() {
        assert!(is_slide_path("ppt/slides/slide1.xml"));
        assert!(!is_slide_path("ppt/slides/_rels/slide1.xml.rels"));
        assert!(!is_slide_path("ppt/slideLayouts/slideLayout1.xml"));
    }

    #[test]
    fn extract_slide_text_concatenates_runs() {
        let xml = r#"<?xml version="1.0"?>
            <sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">
                <a:p><a:r><a:t>Hello</a:t></a:r><a:r><a:t>world</a:t></a:r></a:p>
                <a:p><a:r><a:t>Second line</a:t></a:r></a:p>
            </sld>"#;
        let text = extract_slide_text(xml).expect("parse");
        assert!(text.contains("Hello"));
        assert!(text.contains("world"));
        assert!(text.contains("Second line"));
    }
}
