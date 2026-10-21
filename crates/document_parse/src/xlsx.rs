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
use calamine::{Data, DataRef, Reader, Xlsx, XlsxError, open_workbook_from_rs};
use snafu::ResultExt;
use std::fmt::Write as _;
use std::io::Cursor;
use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{
    Document, DocumentParser, DocumentParserFactory, DocumentType, InternalParsingSnafu, Result,
    archive::{Limits, ensure_text_within_limit, rebuild_package},
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

#[async_trait::async_trait]
impl DocumentParser for XlsxParser {
    /// Renders each worksheet as a `# {name}` line followed by one line per
    /// row of its used range, cells separated by tabs and empty cells as empty
    /// strings.
    ///
    /// Cells are streamed rather than loaded with `worksheet_range`, which
    /// allocates the whole `rows × columns` grid (two distant cells make it
    /// billions of slots) and copies a shared string into every cell that
    /// references it. Each sheet is read twice: once to measure the text it
    /// renders to, which is checked against the text limit before anything is
    /// rendered, and once to render it.
    async fn parse(&self, raw: &Bytes) -> Result<Arc<dyn Document>> {
        let raw = raw.clone();
        let doc = crate::parse_blocking(DocumentType::Xlsx, move || render_workbook(&raw)).await?;
        Ok(Arc::new(doc))
    }
}

fn render_workbook(raw: &Bytes) -> Result<XlsxDocument> {
    // calamine decompresses every part it reads without a size limit.
    let package = rebuild_package(raw, DocumentType::Xlsx, Limits::DEFAULT)?;
    let mut wb: Xlsx<_> = open_workbook_from_rs(Cursor::new(package))
        .boxed()
        .context(InternalParsingSnafu {
            format: DocumentType::Xlsx,
        })?;
    let mut text = String::new();
    for name in wb.sheet_names() {
        let separator = if text.is_empty() { "" } else { "\n\n" };
        let extent = measure_sheet(&mut wb, &name)?;
        let header_len = separator.len() + "# ".len() + name.len() + "\n".len();
        let sheet_len = extent.as_ref().map_or(0, SheetExtent::rendered_len);
        ensure_text_within_limit(
            (text.len() as u64)
                .saturating_add(header_len as u64)
                .saturating_add(sheet_len),
            DocumentType::Xlsx,
        )?;

        text.push_str(separator);
        text.push_str("# ");
        text.push_str(&name);
        text.push('\n');
        if let Some(extent) = extent {
            render_sheet(&mut wb, &name, &extent, &mut text)?;
        }
    }
    Ok(XlsxDocument { text })
}

fn parsing_error(source: impl std::error::Error + Send + Sync + 'static) -> crate::Error {
    crate::Error::InternalParsingError {
        format: DocumentType::Xlsx,
        source: Box::new(source),
    }
}

/// The used range of a worksheet's non-empty cells, and the total length of
/// their rendered values.
struct SheetExtent {
    first_row: u64,
    last_row: u64,
    first_col: u64,
    last_col: u64,
    values_len: u64,
}

impl SheetExtent {
    /// Every row renders one tab between each pair of adjacent cells and ends
    /// in a newline.
    fn rendered_len(&self) -> u64 {
        let rows = self.last_row - self.first_row + 1;
        let cols = self.last_col - self.first_col + 1;
        rows.saturating_mul(cols).saturating_add(self.values_len)
    }
}

/// Streams the non-empty cells of a worksheet in document order. Returns
/// `Ok(None)` for a sheet that is not a worksheet (a chart sheet), which
/// renders no rows.
fn for_each_cell(
    wb: &mut Xlsx<Cursor<Bytes>>,
    name: &str,
    mut visit: impl FnMut((u64, u64), &Data) -> Result<()>,
) -> Result<Option<()>> {
    let mut reader = match wb.worksheet_cells_reader(name) {
        Ok(reader) => reader,
        Err(XlsxError::NotAWorksheet(_)) => return Ok(None),
        Err(e) => return Err(parsing_error(e)),
    };
    while let Some(cell) = reader.next_cell().map_err(parsing_error)? {
        if matches!(cell.get_value(), DataRef::Empty) {
            continue;
        }
        let (row, col) = cell.get_position();
        visit(
            (u64::from(row), u64::from(col)),
            &Data::from(cell.get_value().clone()),
        )?;
    }
    Ok(Some(()))
}

fn measure_sheet(wb: &mut Xlsx<Cursor<Bytes>>, name: &str) -> Result<Option<SheetExtent>> {
    let mut extent: Option<SheetExtent> = None;
    let mut previous: Option<(u64, u64)> = None;
    let mut value = String::new();
    let visited = for_each_cell(wb, name, |position, data| {
        // Worksheets list rows in ascending order and the cells of a row in
        // ascending column order; rendering relies on it to stream.
        if previous.is_some_and(|previous| position <= previous) {
            return Err(parsing_error(std::io::Error::other(format!(
                "worksheet '{name}' lists cell (row {}, column {}) out of order",
                position.0 + 1,
                position.1 + 1
            ))));
        }
        previous = Some(position);
        value.clear();
        write!(value, "{data}").map_err(parsing_error)?;

        let (row, col) = position;
        let extent = extent.get_or_insert(SheetExtent {
            first_row: row,
            last_row: row,
            first_col: col,
            last_col: col,
            values_len: 0,
        });
        extent.last_row = row;
        extent.first_col = extent.first_col.min(col);
        extent.last_col = extent.last_col.max(col);
        extent.values_len = extent.values_len.saturating_add(value.len() as u64);
        // Fail as soon as the sheet alone is over the limit, rather than after
        // measuring all of it.
        ensure_text_within_limit(extent.rendered_len(), DocumentType::Xlsx)
    })?;
    Ok(visited.and(extent))
}

fn render_sheet(
    wb: &mut Xlsx<Cursor<Bytes>>,
    name: &str,
    extent: &SheetExtent,
    out: &mut String,
) -> Result<()> {
    let start = out.len();
    out.reserve(usize::try_from(extent.rendered_len()).map_err(parsing_error)?);

    // The next cell slot to render: no separator or value has been written for
    // it yet.
    let mut next = (extent.first_row, extent.first_col);
    let advance_to = |out: &mut String, next: &mut (u64, u64), target: (u64, u64)| {
        while next.0 < target.0 {
            for col in next.1..=extent.last_col {
                if col > extent.first_col {
                    out.push('\t');
                }
            }
            out.push('\n');
            *next = (next.0 + 1, extent.first_col);
        }
        for col in next.1..target.1 {
            if col > extent.first_col {
                out.push('\t');
            }
        }
        next.1 = next.1.max(target.1);
    };

    for_each_cell(wb, name, |position, data| {
        advance_to(out, &mut next, position);
        if position.1 > extent.first_col {
            out.push('\t');
        }
        write!(out, "{data}").map_err(parsing_error)?;
        next = (position.0, position.1 + 1);
        Ok(())
    })?;
    advance_to(out, &mut next, (extent.last_row + 1, extent.first_col));

    // Both passes read the same cells, so a mismatch is a bug here, never a
    // property of the file; fail rather than return misaligned text.
    let rendered = (out.len() - start) as u64;
    if rendered != extent.rendered_len() {
        return Err(parsing_error(std::io::Error::other(format!(
            "worksheet '{name}' rendered to {rendered} bytes, expected {}",
            extent.rendered_len()
        ))));
    }
    Ok(())
}

struct XlsxDocument {
    text: String,
}

impl Document for XlsxDocument {
    fn as_flat_utf8(&self) -> Result<String> {
        Ok(self.text.clone())
    }

    fn type_(&self) -> DocumentType {
        DocumentType::Xlsx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::test_support::{Part, deflated_package};

    const WORKSHEET_PREFIX: &[u8] = br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>"#;
    const WORKSHEET_SUFFIX: &[u8] = b"</sheetData></worksheet>";

    fn worksheet(rows: &str) -> Vec<u8> {
        [WORKSHEET_PREFIX, rows.as_bytes(), WORKSHEET_SUFFIX].concat()
    }

    fn shared_strings(strings: &[&str]) -> Vec<u8> {
        let mut items = String::new();
        for s in strings {
            let _ = write!(items, "<si><t>{s}</t></si>");
        }
        format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">{items}</sst>"#
        )
        .into_bytes()
    }

    /// A minimal hand-built `.xlsx` with one worksheet per entry of `sheets`,
    /// named in order, plus any `extra` parts (such as `xl/sharedStrings.xml`).
    fn workbook(sheets: Vec<(&str, Part<'_>)>, extra: Vec<(&str, Part<'_>)>) -> Bytes {
        let mut sheet_entries = String::new();
        let mut relationships = String::new();
        let mut paths = Vec::new();
        for (index, (name, _)) in sheets.iter().enumerate() {
            let id = index + 1;
            let _ = write!(
                sheet_entries,
                r#"<sheet name="{name}" sheetId="{id}" r:id="rId{id}"/>"#
            );
            let _ = write!(
                relationships,
                r#"<Relationship Id="rId{id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{id}.xml"/>"#
            );
            paths.push(format!("xl/worksheets/sheet{id}.xml"));
        }
        let workbook_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>{sheet_entries}</sheets></workbook>"#
        );
        let rels_xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{relationships}</Relationships>"#
        );

        let mut parts: Vec<(&str, Part<'_>)> = vec![
            ("xl/workbook.xml", Part::Bytes(workbook_xml.as_bytes())),
            (
                "xl/_rels/workbook.xml.rels",
                Part::Bytes(rels_xml.as_bytes()),
            ),
        ];
        for (path, (_, part)) in paths.iter().zip(sheets) {
            parts.push((path.as_str(), part));
        }
        parts.extend(extra);
        deflated_package(&parts)
    }

    async fn parse(raw: &Bytes) -> Result<String> {
        XlsxParser::default().parse(raw).await?.as_flat_utf8()
    }

    /// The rendering this parser produced when it loaded each sheet with
    /// calamine's `worksheet_range`: the reference the streaming renderer must
    /// reproduce byte for byte on any workbook small enough to load that way.
    fn reference_rendering(raw: &Bytes) -> String {
        let mut wb: Xlsx<_> =
            open_workbook_from_rs(Cursor::new(raw.clone())).expect("reference opens workbook");
        let mut out = String::new();
        for name in wb.sheet_names() {
            let range = wb.worksheet_range(&name).expect("reference reads sheet");
            if !out.is_empty() {
                out.push_str("\n\n");
            }
            out.push_str("# ");
            out.push_str(&name);
            out.push('\n');
            for row in range.rows() {
                let cells: Vec<String> = row.iter().map(ToString::to_string).collect();
                out.push_str(&cells.join("\t"));
                out.push('\n');
            }
        }
        out
    }

    fn too_large_reason(result: Result<String>) -> String {
        match result {
            Err(crate::Error::DocumentTooLarge {
                format: DocumentType::Xlsx,
                reason,
            }) => reason,
            Err(other) => panic!("expected an XLSX size-limit error, got: {other}"),
            Ok(text) => panic!(
                "expected an XLSX size-limit error, but {} bytes of text were extracted",
                text.len()
            ),
        }
    }

    /// Differential test: the streaming renderer must reproduce, byte for byte,
    /// what the dense `worksheet_range` rendering produced before — including a
    /// sheet with leading empty rows and columns, interior gap rows, an empty
    /// cell, and an empty shared string.
    #[tokio::test]
    async fn renders_exactly_as_the_dense_range_did() {
        let strings = shared_strings(&["alpha", "beta", ""]);
        let contiguous = worksheet(
            r#"<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1"><v>1.5</v></c><c r="C1" t="b"><v>1</v></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>inline</t></is></c><c r="B2"><v>42</v></c><c r="C2" t="s"><v>1</v></c></row>"#,
        );
        let sparse = worksheet(
            r#"<row r="3"><c r="C3" t="s"><v>1</v></c><c r="D3"/><c r="F3"><v>7</v></c></row><row r="6"><c r="E6" t="s"><v>2</v></c></row><row r="7"><c r="B7"><v>-3</v></c></row>"#,
        );
        let raw = workbook(
            vec![
                ("Contiguous", Part::Bytes(&contiguous)),
                ("Sparse", Part::Bytes(&sparse)),
            ],
            vec![("xl/sharedStrings.xml", Part::Bytes(&strings))],
        );

        let text = parse(&raw).await.expect("workbook within limits parses");
        assert_eq!(text, reference_rendering(&raw));
        // A sheet's last row ends in a newline and sheets are joined by a blank
        // line, so three newlines fall between the two sheets.
        assert_eq!(
            text,
            "# Contiguous\nalpha\t1.5\ttrue\ninline\t42\tbeta\n\n\n\
             # Sparse\n\tbeta\t\t\t7\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n-3\t\t\t\t\n"
        );
    }

    /// Cells without `r` attributes take their position from document order, and
    /// an empty worksheet renders only its header line.
    #[tokio::test]
    async fn renders_positional_cells_and_an_empty_sheet() {
        let positional =
            worksheet(r"<row><c><v>1</v></c><c><v>2</v></c></row><row><c/><c><v>4</v></c></row>");
        let empty = worksheet("");
        let raw = workbook(
            vec![
                ("Positional", Part::Bytes(&positional)),
                ("Empty", Part::Bytes(&empty)),
            ],
            vec![],
        );

        let text = parse(&raw).await.expect("workbook within limits parses");
        assert_eq!(text, "# Positional\n1\t2\n\t4\n\n\n# Empty\n");
    }

    #[tokio::test]
    async fn rejects_cells_listed_out_of_order() {
        let sheet = worksheet(
            r#"<row r="2"><c r="A2"><v>1</v></c></row><row r="1"><c r="A1"><v>2</v></c></row>"#,
        );
        let raw = workbook(vec![("S", Part::Bytes(&sheet))], vec![]);
        let err = parse(&raw)
            .await
            .expect_err("out-of-order rows are rejected");
        assert_eq!(
            err.to_string(),
            "Internal parsing XLSX document. Error: Custom { kind: Other, error: \"worksheet 'S' \
             lists cell (row 1, column 1) out of order\" }"
        );
    }

    /// Regression test for a decompression bomb: a worksheet part that inflates
    /// past the per-entry limit is rejected before calamine reads it.
    #[tokio::test]
    async fn rejects_a_worksheet_that_decompresses_past_the_entry_limit() {
        let prefix = [
            WORKSHEET_PREFIX,
            br#"<row r="1"><c r="A1" t="inlineStr"><is><t>"#,
        ]
        .concat();
        let suffix = [b"</t></is></c></row>".as_slice(), WORKSHEET_SUFFIX].concat();
        let raw = workbook(
            vec![(
                "Bomb",
                Part::Repeated {
                    prefix: &prefix,
                    byte: b'x',
                    len: usize::try_from(crate::archive::MAX_ENTRY_BYTES).expect("fits usize"),
                    suffix: &suffix,
                },
            )],
            vec![],
        );
        assert!(
            raw.len() < 1024 * 1024,
            "the bomb should be small on the wire, was {} bytes",
            raw.len()
        );
        assert_eq!(
            too_large_reason(parse(&raw).await),
            "archive entry 'xl/worksheets/sheet1.xml' decompresses to more than 64 MiB, the \
             per-entry limit"
        );
    }

    /// Two cells at opposite corners of a sheet span 2^34 cell slots. Loading
    /// them as a dense range allocates every slot; streaming them measures the
    /// text first and rejects it without allocating.
    #[tokio::test]
    async fn rejects_a_used_range_that_renders_past_the_text_limit() {
        let sheet = worksheet(
            r#"<row r="1"><c r="A1"><v>1</v></c></row><row r="1048576"><c r="XFD1048576"><v>2</v></c></row>"#,
        );
        let raw = workbook(vec![("Corners", Part::Bytes(&sheet))], vec![]);
        assert_eq!(
            too_large_reason(parse(&raw).await),
            "the extracted text exceeds 64 MiB, the per-document limit"
        );
    }

    /// One shared string, within every decompression limit, referenced by
    /// enough cells that the rendered text exceeds the text limit.
    #[tokio::test]
    async fn rejects_a_shared_string_repeated_past_the_text_limit() {
        let strings_prefix = br#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>"#;
        let strings_suffix = b"</t></si></sst>";
        let mut cells = String::new();
        for row in 1..=65 {
            let _ = write!(
                cells,
                r#"<row r="{row}"><c r="A{row}" t="s"><v>0</v></c></row>"#
            );
        }
        let sheet = worksheet(&cells);
        let raw = workbook(
            vec![("Repeated", Part::Bytes(&sheet))],
            vec![(
                "xl/sharedStrings.xml",
                Part::Repeated {
                    prefix: strings_prefix,
                    byte: b'x',
                    len: 1024 * 1024,
                    suffix: strings_suffix,
                },
            )],
        );
        assert_eq!(
            too_large_reason(parse(&raw).await),
            "the extracted text exceeds 64 MiB, the per-document limit"
        );
    }
}
