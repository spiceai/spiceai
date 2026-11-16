# Cayenne doc

A breadth-first technical reference for the `cayenne` crate
([`crates/cayenne`](../../crates/cayenne)) — the Vortex-native engine that
powers Spice.ai, built for high-rate CDC ingestion and low-latency queries on the
same table.

This doc lives in-tree so it is versioned alongside the code it describes.
Because the crate is now in the same repository, source-ground every claim
against `crates/cayenne` in this tree (see *Conventions* below). The baseline
commit the doc was last checked against is recorded in *Scope and sourcing* at
the top of `cayenne.md`.

## Contents

| File | Role |
|---|---|
| `cayenne.md` | Source document. **This is the source of truth — edit this.** |
| `gen_waterfall.py` | Generator for the committed `waterfall.svg` landscape "life of a change" waterfall figure. |
| `waterfall.svg` | The waterfall figure, **committed** and referenced from `cayenne.md` as `<img src="waterfall.svg">`. Regenerate with `gen_waterfall.py`. |

Committed figures are referenced as separate `.svg` files (currently `waterfall.svg`)
rather than pasted inline, because GitHub's markdown renderer strips inline
`<svg>` markup. Mermaid diagrams stay as <code>```mermaid</code> fenced blocks,
which GitHub renders natively.

## Regenerating the waterfall figure

The "life of a change" waterfall is **not** built from a Mermaid block — it's
a hand-built landscape SVG, generated to the committed `waterfall.svg`:

```sh
python3 gen_waterfall.py   # overwrites waterfall.svg
```

`cayenne.md` references it as `<img src="waterfall.svg">`, so there is **no
inline paste step**: after editing `gen_waterfall.py`, re-run it and commit the
regenerated `waterfall.svg`.

## Conventions

- **Source-ground every claim** against `crates/cayenne` in this repository.
  Name the relevant type/function so it can be found in the tree.
  `metastore/sqlite.rs` DDL + `metastore::EXPECTED_TABLES` are authoritative
  for the on-disk schema. Distinguish "what the README says" from "what the
  code does" where they differ.
- **Grayscale-safe diagrams**: white node fills, dark solid borders
  (`#312e81`), near-black edges (`#1e293b`), dashed cluster/optional outlines
  (`#6366f1`), gray notes (`#e2e8f0` fill, `#475569` border), neutral-gray brackets/axes
  (`#94a3b8`). Diagram labels are plain text + `<br/>` only, no `<b>`/`<i>`,
  and commas rather than semicolons in sequence-diagram text.
- **Prose style**: minimal formatting, no over-bolding; breadth-first; honest
  about design alternatives and version accuracy.

## Keeping the doc current

Cayenne moves quickly. PRs that change `crates/cayenne` behavior, config
parameters, the metastore schema, or the CDC/compaction flows should update
`cayenne.md` in the same PR where practical. See the note in the repository's
agent instructions (`AGENTS.md`).
