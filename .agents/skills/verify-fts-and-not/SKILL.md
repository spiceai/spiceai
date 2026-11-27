---
name: verify-fts-and-not
description: Drive a real `spiced` over HTTP to prove `text_search` and `POST /v1/search` treat `AND NOT` the same as the `-` form. Use when changing full-text query parsing, `parse_user_query`, or the Tantivy BooleanQuery rewrite in `crates/search`.
---

# Verify FTS `AND NOT` against a running `spiced`

Tantivy parses `a AND NOT b` as `+a +(-b)`. A `BooleanQuery` of only `MustNot` clauses matches nothing ([quickwit-oss/tantivy#1980](https://github.com/quickwit-oss/tantivy/issues/1980)), so documented query syntax returns zero rows. `a -b` and `a AND -b` already parse as `+a -b` and are correct.

This skill is the pstack-style proof: the same `spiced` HTTP surface a user calls, not a unit test. Run it against a binary built from trunk and against the candidate binary, and keep both outputs.

A compile, `cargo test`, or an in-process runtime test is not this proof.

## Build

File connector, Cayenne, and `text_search` are always linked. `POST /v1/search` is only registered when `spiced` is built with the `models` feature (`if cfg!(feature = "models")` in `crates/runtime/src/http/routes.rs`); a `--no-default-features` binary still proves the bug through `text_search`, which shares `parse_user_query` with `/v1/search`.

```bash
CC=cc CXX=g++ cargo build -p spiced --no-default-features
```

The binary is `target/debug/spiced`. For a before/after, build trunk in a separate worktree (or a separate `CARGO_TARGET_DIR`) — do not switch this checkout off the candidate branch.

## Run

```bash
.agents/skills/verify-fts-and-not/helpers/verify-fts-and-not.sh target/debug/spiced
```

The helper:

1. Writes a six-row CSV (the same comments/titles as the `search` crate rewrite tests) and a Spicepod with Cayenne `mode: memory` plus `en_stem` FTS on `title` and `comment` under a fresh `mktemp -d` (override with `WORK_DIR`, which must be empty or new).
2. Refuses to run if anything already answers on `HTTP_ADDR`, then starts only this `spiced` on `HTTP_ADDR` (default `127.0.0.1:18493`) plus side ports, logging to `$WORK_DIR/spiced.log`, and records its pid.
3. Waits for `GET /v1/ready` to be `ready` (90s), then polls `text_search(docs, 'dolphins', comment)` until it returns 3 rows (index built) or 60s.
4. Prints `git_head`, binary identity, then one line per query: SQL `COUNT(*)` / ids from `text_search`. When `/v1/search` is linked (`models`), also prints its result length; a 404 is reported as `search skipped` rather than a failed run.
5. Prints a `compare` line per pair (`AND NOT` vs `-`). Trunk must show `AND NOT` count 0 and `MISMATCH`; the fix must show equal non-zero counts and `MATCH`. A leading `NOT x` / `-x` on trunk is Tantivy's `AllButQueryForbidden` falling through to bag-of-words (hits are the term, not the complement).
6. Stops the pid it started (never `pkill`) and removes the work directory it created unless `KEEP=1`. A `WORK_DIR` you pass is never removed.

To point it at an already-running instance, pass `--url http://host:port` instead of a binary; it then skips launch and teardown.

## Expected counts (6-row fixture, `en_stem`)

| query | expected rows (ids) |
|---|---|
| `dolphins AND NOT furiously` | 2 (`1,3`) — same as `dolphins -furiously` |
| `dolphins AND (NOT furiously)` | 2 (`1,3`) |
| `comment:(dolphins OR pearls) AND NOT furiously` | 3 (`1,3,5`) |
| `title:(spice OR runtime) AND NOT draft` | 3 (`1,3,6`) — the documented form |
| `NOT furiously` | 3 (`1,3,5`) — every document except the term |

On trunk, every `AND NOT` / leading `NOT` SQL count is `0`. The `-` form already returns the expected rows.

## Proof

Run the helper against the binary built from trunk and the one after the rewrite, and keep both outputs. Paste the `sql` / `search` / `compare` lines into the PR. A compile or the unit tests alone are not proof.
