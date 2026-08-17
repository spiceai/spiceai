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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use lopdf::{Document, Object, ObjectId};
use snafu::{OptionExt, ResultExt, Snafu};

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

    #[snafu(display("Failed to read {path}: {source}", path = path.display()))]
    ReadInput {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to remove stale page file {path}: {source}", path = path.display()))]
    RemoveStalePage {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write split digest {path}: {source}", path = path.display()))]
    SaveDigest {
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

    #[snafu(display("Page {page} missing from page tree of {path}", path = path.display()))]
    PageMissing { path: PathBuf, page: usize },

    #[snafu(display("Failed to trim page {page} of {path}: {source}", path = path.display()))]
    TrimPage {
        path: PathBuf,
        page: usize,
        source: lopdf::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Split a single PDF into one single-page PDF per page, written to `out_dir`.
///
/// For each page the source document is cloned and its page tree trimmed to
/// that single page, leaving a one-page document that is saved as
/// `<out_dir>/pNNNN.pdf` with a zero-indexed, zero-padded page number. Returns
/// the written paths in page order.
///
/// The split is idempotent: if `out_dir` already holds exactly one `pNNNN.pdf`
/// per source page, written from a source with the same content digest, the
/// existing files are returned without re-writing them. Otherwise `out_dir` is
/// cleared of prior page outputs first, so a source whose page count has
/// shrunk since the last run doesn't leave stale trailing page files behind.
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

    let digest = source_digest(input)?;
    let digest_path = out_dir.join(DIGEST_FILE_NAME);

    if expected.iter().all(|p| p.is_file())
        && count_page_pdfs(out_dir)? == page_numbers.len()
        && std::fs::read_to_string(&digest_path).ok().as_deref() == Some(digest.as_str())
    {
        return Ok(expected);
    }

    clear_page_pdfs(out_dir)?;

    // `keep_page_id` is the leaf `Page` object for each 1-indexed page number.
    let pages: BTreeMap<u32, ObjectId> = doc.get_pages();

    let mut written = Vec::with_capacity(page_numbers.len());
    for (idx, page_number) in page_numbers.iter().enumerate() {
        let keep_page_id = *pages.get(page_number).context(PageMissingSnafu {
            path: input,
            page: idx,
        })?;

        // Clone the whole document — this preserves every inherited page-tree
        // attribute (Resources, MediaBox, Rotate, …) for the kept page. Then,
        // instead of deleting the other N-1 pages (each deletion traverses the
        // whole object graph, making a full split O(pages^2 * objects)), trim
        // the page tree to just the kept page's ancestor chain and prune the
        // now-unreferenced objects in a single pass.
        let mut page_doc = doc.clone();
        retain_single_page(&mut page_doc, keep_page_id).context(TrimPageSnafu {
            path: input,
            page: idx,
        })?;
        page_doc.prune_objects();

        let out_path = out_dir.join(page_file_name(idx));
        page_doc.save(&out_path).context(SavePageSnafu {
            path: input,
            page: idx,
        })?;
        written.push(out_path);
    }

    std::fs::write(&digest_path, &digest).context(SaveDigestSnafu { path: digest_path })?;

    Ok(written)
}

/// Content digest of `input`, used to detect a same-name/same-page-count
/// source whose bytes have actually changed since the last split.
fn source_digest(input: &Path) -> Result<String> {
    let bytes = std::fs::read(input).context(ReadInputSnafu { path: input })?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

/// Sidecar file recording the source digest a directory was last split from.
const DIGEST_FILE_NAME: &str = ".source.blake3";

/// Remove every existing `pNNNN.pdf` file in `dir`.
///
/// Called before re-splitting so trailing page files from a previous, larger
/// split (e.g. `p0002.pdf` when the source now has only two pages) don't
/// survive alongside the freshly written, smaller set.
fn clear_page_pdfs(dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir).context(ReadDirSnafu { path: dir })? {
        let entry = entry.context(ReadDirSnafu { path: dir })?;
        if entry.file_name().to_str().is_some_and(is_page_pdf_name) {
            let path = entry.path();
            std::fs::remove_file(&path).context(RemoveStalePageSnafu { path })?;
        }
    }
    Ok(())
}

/// Trim `doc`'s page tree so `keep_page_id` is the only reachable page.
///
/// Walks the kept page's `Parent` chain to the page-tree root and, at every
/// intermediate `Pages` node, replaces `Kids` with just the child on that chain
/// and sets `Count` to 1. The ancestor chain itself is left intact, so every
/// attribute the kept page inherits (`Resources`, `MediaBox`, `Rotate`, …) is
/// preserved. Callers should follow with `prune_objects` to drop the sibling
/// pages and subtrees this leaves unreferenced.
fn retain_single_page(doc: &mut Document, keep_page_id: ObjectId) -> Result<(), lopdf::Error> {
    strip_cross_page_catalog_refs(doc);

    let mut child = keep_page_id;
    // Bound the walk by the object count so a malformed `Parent` cycle cannot
    // loop forever.
    let max_depth = doc.objects.len() + 1;
    for _ in 0..max_depth {
        let parent = doc
            .get_object(child)
            .and_then(Object::as_dict)?
            .get(b"Parent")
            .and_then(Object::as_reference);
        let Ok(parent_id) = parent else {
            // Reached the page-tree root (a `Pages` node has no `Parent`).
            return Ok(());
        };
        let parent_dict = doc
            .get_object_mut(parent_id)
            .and_then(Object::as_dict_mut)?;
        parent_dict.set("Kids", vec![Object::Reference(child)]);
        parent_dict.set("Count", 1);
        child = parent_id;
    }
    Err(lopdf::Error::ReferenceCycle(child))
}

/// Reduce the document catalog to just `Type` and `Pages`, dropping every other
/// entry that can reach pages other than the one being kept.
///
/// A document catalog holds many entries that transitively reference every page:
/// a tagged PDF's `/StructTreeRoot` accessibility tree, optional-content layers
/// (`/OCProperties`), named destinations, outlines, forms, an `/OpenAction`, and
/// vendor-specific keys (preflight/color profiles). Any one of them keeps every
/// page's content reachable, so `prune_objects` reclaims nothing and one split
/// page re-serializes the whole document. Rather than deny-list the known
/// offenders (fragile — vendors add their own keys), keep only what renders the
/// page tree: `Type` and the `Pages` reference. None of the dropped entries are
/// needed to extract a page's text; the kept page's own `/Contents` and
/// `/Resources` hang off the page object and are untouched.
fn strip_cross_page_catalog_refs(doc: &mut Document) {
    let Ok(catalog_id) = doc.trailer.get(b"Root").and_then(Object::as_reference) else {
        return;
    };
    if let Ok(catalog) = doc.get_object_mut(catalog_id).and_then(Object::as_dict_mut) {
        let pages = catalog.get(b"Pages").ok().cloned();
        let mut trimmed = lopdf::Dictionary::new();
        trimmed.set("Type", Object::Name(b"Catalog".to_vec()));
        if let Some(pages) = pages {
            trimmed.set("Pages", pages);
        }
        *catalog = trimmed;
    }
}

/// Split every `*.pdf` in `input_dir` (non-recursively) into `<out_dir>/<stem>/pNNNN.pdf`.
///
/// # Errors
///
/// Returns an error if `input_dir` cannot be read or any contained PDF fails to
/// split (see [`split_pdf`]).
pub fn split_pdf_dir(input_dir: &Path, out_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut inputs: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(input_dir).context(ReadDirSnafu { path: input_dir })? {
        let path = entry.context(ReadDirSnafu { path: input_dir })?.path();
        if is_pdf(&path) {
            inputs.push(path);
        }
    }
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
    let mut count = 0;
    for entry in std::fs::read_dir(dir).context(ReadDirSnafu { path: dir })? {
        let entry = entry.context(ReadDirSnafu { path: dir })?;
        if entry.file_name().to_str().is_some_and(is_page_pdf_name) {
            count += 1;
        }
    }
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
