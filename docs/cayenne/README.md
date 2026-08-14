# Cayenne doc

A breadth-first technical reference for the `cayenne` crate
([`crates/cayenne`](../../crates/cayenne)) — a Vortex-native lakehouse table
format / high-rate-CDC accelerator — rendered to a grayscale-printable PDF.

This doc lives in-tree so it is versioned alongside the code it describes.
Because the crate is now in the same repository, source-ground every claim
against `crates/cayenne` in this tree (see *Conventions* below). The current
baseline commit is recorded in the *Document changelog* at the end of
`cayenne.md` — that changelog is the authoritative record of which repository
revision each version of the doc reflects.

## Contents

| File | Role |
|---|---|
| `cayenne.md` | Source document. **This is the source of truth — edit this.** |
| `build_pdf.py` | Converter: extracts <code>```mermaid</code> blocks → kroki.io SVG → python-markdown → WeasyPrint → `cayenne.html` + `Cayenne.pdf`. The cover page, running footer, and landscape-figure CSS live here. |
| `gen_waterfall.py` | Generator for the committed `waterfall.svg` landscape "life of a change" waterfall figure. |
| `waterfall.svg` | The waterfall figure, **committed** and referenced from `cayenne.md` as `<img src="waterfall.svg">` so it renders in both the GitHub markdown view and the PDF. Regenerate with `gen_waterfall.py`. |
| `Cayenne.pdf` | Built output. **Not committed** (git-ignored) — CI builds it as a linkable artifact off `trunk`; build it locally to preview. |

Committed figures are referenced as separate `.svg` files (currently `waterfall.svg`)
rather than pasted inline, because GitHub's markdown renderer strips inline
`<svg>` markup — a referenced `.svg` renders in both the GitHub view and the
PDF. Mermaid diagrams stay as <code>```mermaid</code> fenced blocks: GitHub renders them
natively, and `build_pdf.py` rasterizes them via kroki for the PDF.

## Continuous integration

[`.github/workflows/cayenne_doc.yml`](../../.github/workflows/cayenne_doc.yml)
builds the PDF:

- **On pull requests** that touch `docs/cayenne/**`, it renders the PDF to
  verify the document still builds (mermaid blocks resolve, WeasyPrint
  succeeds) and uploads the result as a run artifact for review.
- **On push to `trunk` that touches `docs/cayenne/**`** (or the workflow file
  itself), and on manual dispatch, it builds and uploads `Cayenne.pdf` as a
  downloadable, linkable artifact of the current `trunk`.

## Prerequisites

- Python 3 with `markdown` and `weasyprint`
  (`pip install markdown weasyprint` — add `--break-system-packages`
  on Debian/Ubuntu system Pythons).
- WeasyPrint's native deps (Pango/Cairo/…):
  - **macOS**: `brew install pango` (pulls in cairo, gdk-pixbuf, libffi). If the
    import still can't find the libs on Apple Silicon, export
    `DYLD_FALLBACK_LIBRARY_PATH="$(brew --prefix)/lib"`.
  - **Debian/Ubuntu**: `sudo apt-get install libpango-1.0-0 libpangoft2-1.0-0`
    (see the CI workflow for the exact package list).
- Network access to `kroki.io` (renders the Mermaid diagrams to SVG).
- Optional, for visual verification: `pdftoppm` (poppler-utils) to rasterize
  pages, e.g. `pdftoppm -png -r 100 -f 12 -l 12 Cayenne.pdf page`.

## Building the PDF

```sh
python3 build_pdf.py
```

Produces `cayenne.html` (intermediate) and `Cayenne.pdf` in the working
directory. The script fetches each Mermaid block from kroki concurrently and
injects a high-contrast override stylesheet into every SVG so diagrams stay
legible in grayscale print.

## Regenerating the waterfall figure

The "life of a change" waterfall is **not** built from a Mermaid block — it's
a hand-built landscape SVG, generated to the committed `waterfall.svg`:

```sh
python3 gen_waterfall.py   # overwrites waterfall.svg (and a waterfall_ls.pdf preview if WeasyPrint is available)
```

`cayenne.md` references it as `<img src="waterfall.svg">` inside the
`<div class="landscape-fig">…</div>` block, so there is **no inline paste step**:
after editing `gen_waterfall.py`, just re-run it to overwrite `waterfall.svg`, then
rebuild the PDF (and commit the regenerated `waterfall.svg`).

## Conventions

- **Source-ground every claim** against `crates/cayenne` in this repository.
  Name the relevant type/function so it can be found in the tree.
  `metastore/sqlite.rs` DDL + `metastore::EXPECTED_TABLES` are authoritative
  for the on-disk schema. Distinguish "what the README says" from "what the
  code does" where they differ.
- **Grayscale-safe diagrams**: white node fills, dark solid borders
  (`#312e81`), near-black edges (`#1e293b`), dashed cluster/optional outlines
  (`#6366f1`), neutral-gray notes/brackets/axes (`#94a3b8`). Mermaid is
  rendered via kroki with `htmlLabels:false` — plain text + `<br/>` only, no
  `<b>`/`<i>`, and commas rather than semicolons in sequence-diagram text.
- **Changelog**: add a row only when reviewing a new merged PR or repo commit
  (Date | Reviewed commit | Changes), and keep it to **one or two sentences**
  naming what changed. The row indexes the history rather than retelling it —
  the reasoning and measurements live in the PR it points at, and anything a
  reader needs in order to use Cayenne belongs in the body of the document.
  **One row per merged PR**, not one per revision: revise the existing row
  while a change is still in review. A row is worth adding only for something
  a reader would act on — a new or renamed parameter, a schema change, a
  behavior or correctness change, or a structural revision to the document;
  skip internal refactors and behavior-identical perf work. Judge that against
  what the document is for: **how Cayenne sustains high-rate ingestion and
  low-latency reads on one table** — synchronization and locking, the write and
  compaction paths, tiering, visibility. Bookkeeping that only keeps the
  accounting honest (how a cache charges entries, where a byte budget is
  enforced) is described in the body beside the structure it governs and needs
  no row. Write the row from the state **after** the change lands — it is read
  once the change is in, so it never calls itself "pre-merge" — and cite a
  **merged PR number or a `trunk` commit**, never a pre-merge branch SHA, which
  is squashed away on merge.
- **Prose style**: minimal formatting, no over-bolding; breadth-first; honest
  about design alternatives and version accuracy.

## Keeping the doc current

Cayenne moves quickly. PRs that change `crates/cayenne` behavior, config
parameters, the metastore schema, or the CDC/compaction flows should update
`cayenne.md` in the same PR where practical, and add a *Document changelog*
row referencing the merged PR. See the note in the repository's agent
instructions (`CLAUDE.md` / `.github/copilot-instructions.md`).
