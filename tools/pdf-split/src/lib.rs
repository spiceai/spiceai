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

//! Split a multi-page PDF into one single-page PDF per page.
//!
//! Page identity must survive Spice ingestion: the document/`file:` connector
//! flattens a whole PDF into a single `content` string per file, so page
//! boundaries are lost once the runtime parses the file. Splitting each source
//! PDF into one file per page — with the page number in the file name — makes
//! the page the unit of ingestion, so page-level ground truth (for example
//! `FinanceBench` evidence pages) stays intact.
//!
//! Output file names are zero-indexed (`p0000.pdf`, `p0001.pdf`, …) to match
//! `FinanceBench`'s zero-indexed `evidence_page_num`.

use std::path::{Path, PathBuf};

use lopdf::Document;
use snafu::{ResultExt, Snafu};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to load PDF {path}: {source}", path = path.display()))]
    LoadPdf { path: PathBuf, source: lopdf::Error },

    #[snafu(display("Failed to create output directory {path}: {source}", path = path.display()))]
    CreateOutputDir {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to read directory {path}: {source}", path = path.display()))]
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "PDF {path} has no pages. Confirm the file is a valid PDF and is not empty.",
        path = path.display()
    ))]
    NoPages { path: PathBuf },

    #[snafu(display("Failed to save page {page} of {path}: {source}", path = path.display()))]
    SavePage {
        path: PathBuf,
        page: usize,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Split a single PDF into one single-page PDF per page, written to `out_dir`.
///
/// For each page the source document is cloned and every other page deleted,
/// leaving a one-page document that is saved as `<out_dir>/pNNNN.pdf` with a
/// zero-indexed, zero-padded page number. Returns the written paths in page
/// order.
///
/// The split is idempotent: if `out_dir` already holds exactly one `pNNNN.pdf`
/// per source page, the existing files are returned without re-writing them.
///
/// # Errors
///
/// Returns an error if the input cannot be opened as a PDF, the output
/// directory cannot be created, or a page cannot be saved.
pub fn split_pdf(input: &Path, out_dir: &Path) -> Result<Vec<PathBuf>> {
    let doc = Document::load(input).context(LoadPdfSnafu { path: input })?;

    // `get_pages()` keys are 1-indexed page numbers in document order.
    let page_numbers: Vec<u32> = doc.get_pages().keys().copied().collect();
    snafu::ensure!(!page_numbers.is_empty(), NoPagesSnafu { path: input });

    std::fs::create_dir_all(out_dir).context(CreateOutputDirSnafu { path: out_dir })?;

    let expected: Vec<PathBuf> = (0..page_numbers.len())
        .map(|idx| out_dir.join(page_file_name(idx)))
        .collect();

    if expected.iter().all(|p| p.is_file()) && count_page_pdfs(out_dir)? == page_numbers.len() {
        return Ok(expected);
    }

    let mut written = Vec::with_capacity(page_numbers.len());
    for (idx, keep) in page_numbers.iter().enumerate() {
        let mut page_doc = doc.clone();
        let to_delete: Vec<u32> = page_numbers
            .iter()
            .filter(|n| *n != keep)
            .copied()
            .collect();
        page_doc.delete_pages(&to_delete);
        page_doc.prune_objects();
        page_doc.renumber_objects();
        page_doc.compress();

        let out_path = out_dir.join(page_file_name(idx));
        page_doc.save(&out_path).context(SavePageSnafu {
            path: input,
            page: idx,
        })?;
        written.push(out_path);
    }

    Ok(written)
}

/// Split every `*.pdf` in `input_dir` (non-recursively) into `<out_dir>/<stem>/pNNNN.pdf`.
///
/// # Errors
///
/// Returns an error if `input_dir` cannot be read or any contained PDF fails to
/// split (see [`split_pdf`]).
pub fn split_pdf_dir(input_dir: &Path, out_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut inputs: Vec<PathBuf> = std::fs::read_dir(input_dir)
        .context(ReadDirSnafu { path: input_dir })?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| is_pdf(path))
        .collect();
    inputs.sort();

    let mut written = Vec::new();
    for input in inputs {
        let stem = input
            .file_stem()
            .map_or_else(|| PathBuf::from("document"), PathBuf::from);
        let doc_out = out_dir.join(stem);
        written.extend(split_pdf(&input, &doc_out)?);
    }

    Ok(written)
}

/// Zero-indexed, zero-padded page file name for page `idx`, e.g. `p0000.pdf`.
#[must_use]
pub fn page_file_name(idx: usize) -> String {
    format!("p{idx:04}.pdf")
}

fn is_pdf(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("pdf"))
}

/// Count files in `dir` whose names match the `pNNNN.pdf` page pattern.
fn count_page_pdfs(dir: &Path) -> Result<usize> {
    let count = std::fs::read_dir(dir)
        .context(ReadDirSnafu { path: dir })?
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_name().to_str().is_some_and(is_page_pdf_name))
        .count();
    Ok(count)
}

/// True for names of the form `pNNNN.pdf` where `NNNN` is 4+ ASCII digits.
fn is_page_pdf_name(name: &str) -> bool {
    let Some(digits) = name.strip_prefix('p').and_then(|s| s.strip_suffix(".pdf")) else {
        return false;
    };
    digits.len() >= 4 && digits.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lopdf::dictionary;
    use lopdf::{Document, Object, Stream};

    /// Build a PDF with `n` blank pages for testing.
    fn build_pdf(n: usize) -> Document {
        let mut doc = Document::with_version("1.5");
        let pages_id = doc.new_object_id();

        let mut kids = Vec::with_capacity(n);
        for _ in 0..n {
            let content_id = doc.add_object(Stream::new(dictionary! {}, Vec::new()));
            let leaf_id = doc.add_object(dictionary! {
                "Type" => "Page",
                "Parent" => pages_id,
                "Contents" => content_id,
                "MediaBox" => vec![0.into(), 0.into(), 595.into(), 842.into()],
            });
            kids.push(leaf_id.into());
        }

        let pages = dictionary! {
            "Type" => "Pages",
            "Kids" => kids,
            "Count" => i64::try_from(n).expect("page count fits in i64"),
        };
        doc.objects.insert(pages_id, Object::Dictionary(pages));

        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        doc.trailer.set("Root", catalog_id);
        doc
    }

    #[test]
    fn splits_into_one_file_per_page() {
        let tmp = std::env::temp_dir().join(format!("pdf-split-test-{}", std::process::id()));
        let src = tmp.join("three-page.pdf");
        let out = tmp.join("out");
        std::fs::create_dir_all(&tmp).expect("create temp dir");

        let mut doc = build_pdf(3);
        doc.save(&src).expect("save source pdf");

        let outputs = split_pdf(&src, &out).expect("split pdf");
        assert_eq!(outputs.len(), 3, "one output per page");

        for (idx, path) in outputs.iter().enumerate() {
            assert_eq!(
                path.file_name().and_then(|s| s.to_str()),
                Some(page_file_name(idx).as_str()),
                "zero-indexed page file name",
            );
            let split = Document::load(path).expect("load split page");
            assert_eq!(split.get_pages().len(), 1, "each output has one page");
        }

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn split_is_idempotent() {
        let tmp = std::env::temp_dir().join(format!("pdf-split-idem-{}", std::process::id()));
        let src = tmp.join("two-page.pdf");
        let out = tmp.join("out");
        std::fs::create_dir_all(&tmp).expect("create temp dir");

        let mut doc = build_pdf(2);
        doc.save(&src).expect("save source pdf");

        let first = split_pdf(&src, &out).expect("first split");
        let first_mtimes: Vec<_> = first
            .iter()
            .map(|p| {
                std::fs::metadata(p)
                    .and_then(|m| m.modified())
                    .expect("mtime")
            })
            .collect();

        let second = split_pdf(&src, &out).expect("second split");
        assert_eq!(first, second, "same outputs on re-run");
        let second_mtimes: Vec<_> = second
            .iter()
            .map(|p| {
                std::fs::metadata(p)
                    .and_then(|m| m.modified())
                    .expect("mtime")
            })
            .collect();
        assert_eq!(first_mtimes, second_mtimes, "files not re-written");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn page_file_name_is_zero_padded() {
        assert_eq!(page_file_name(0), "p0000.pdf");
        assert_eq!(page_file_name(42), "p0042.pdf");
        assert_eq!(page_file_name(1234), "p1234.pdf");
    }
}
