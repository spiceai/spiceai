---
name: verify-cayenne-fts-restart
description: Verify Cayenne mode:file + default memory FTS still returns hits after a restart that reuses .spice/data (regression for #14618 / #14954 coverage on Cayenne).
---

# Verify Cayenne FTS survives restart

## When to use

After changing full-text index rebuild, Cayenne file-mode acceleration, or `Refresher::start` readiness, confirm a restart that keeps `.spice/data` still answers `text_search` / `POST /v1/search`.

## Steps

1. Use a `spiced` binary that contains the candidate fix. Do **not** let a script prepend `/home/box/.spice/bin` over it (Cartman's `restart_empty_fts.sh` does that and silently runs the installed release).
2. From a spicepod with `engine: cayenne`, `mode: file`, default (memory) FTS on a text column, and a small CSV:
   - `rm -rf .spice/data && mkdir -p .spice`
   - start spiced; wait for `/v1/ready`
   - `text_search` must return hits
   - SIGTERM; keep `.spice/data`; start again; wait for ready
   - `COUNT(*)` matches the corpus; `text_search` and `POST /v1/search` return the same hits (no empty HTTP 200)
3. Automated unit coverage: `cargo test -p runtime --test integration_models --features models cayenne_full_text_search_finds_persisted_rows_after_a_restart`

## Pass / fail

- Pass: after restart, FTS hits match the first load (same ids).
- Fail: `COUNT(*)` is full but FTS/`/v1/search` return `[]` while `/v1/datasets` still reports `search:supported`.
