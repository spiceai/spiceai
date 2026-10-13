# Spice.ai in Action

Original technical manuscript, September 2026. The primary deliverable is **Spice.ai in Action.docx**. A PDF is included for fixed-layout reading. The book contains 36 chapters, seven appendices, 13 original technical diagrams, worked exercises, two local application capstones, an Enterprise deployment capstone, a glossary, a source register covering 50 website blog posts, and a linked topic index.

The Enterprise section is Chapters 30–36 and Appendix G, grounded in the supplied Enterprise, Kubernetes operator, and Cloud documentation repositories. Exact source identities and execution boundaries are in Appendices A and F.

`companion/` contains the fictional Northstar data and executable local examples, plus Enterprise integration templates. `evidence/` contains actual SQL rows, query plans, search and service results, version records, and offline operator validation. The packaged companion archive selects these records and excludes runtime caches, downloaded models, credentials, and the full third-party blog extracts.

`cover-copy.txt` supplies text for the O'RLY cover-generator form, including the humorous top line. The manuscript uses an editable typographic cover.

## Read or download

- [Word manuscript](<Spice.ai in Action.docx>)
- [PDF edition](<Spice.ai in Action.pdf>)
- [Companion examples and editable sources](<Spice.ai in Action - Companion and Sources.zip>)

## Rebuilding the document

`chapters/` contains the editable Markdown sources. `build_book.py` orders chapters by their displayed chapter number and appends the appendices and index. It requires Python with python-docx and Pandoc. `draw_figures.py` uses ReportLab, PyMuPDF, and the local Inter fonts to produce original PDF/SVG/PNG diagrams. Word was used to update the native contents field, paginate, and export the layout PDF.

The DOCX uses native headings, hyperlinks, editable tables and code, a native contents field, page numbers, and embedded diagrams. The body is Georgia, with Inter headings and Consolas code. Original source files are retained so later editions can update the technical material without editing PDF text.

The book preserves a reproduced Cayenne NULL-sensitive NOT IN discrepancy and marks its tested variant as not fully accepted. External-service and Enterprise deployment procedures are explicitly separated from executed local results. No production performance, cloud deployment, identity-provider integration, or cluster failover result is invented.
