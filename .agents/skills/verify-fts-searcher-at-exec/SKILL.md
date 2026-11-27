---
name: verify-fts-searcher-at-exec
description: Drive a real spiced over HTTP to prove text_search and POST /v1/search see the latest full-text index after a Cayenne full refresh (same SQL, reworded SQL, and the FTS search API). Use after changing FullTextSearchFieldIndex, make_stream, or the /v1/search full-text path.
---

# Verify FTS takes the searcher at execution

`FullTextSearchFieldIndex` must not keep a `tantivy::Searcher` from planning. After a full refresh that deletes, updates, and inserts rows, a repeated `text_search` query, a reworded query with the same term, and `POST /v1/search` must match the new index.

Unit coverage lives in `crates/search` (`reused_field_index_sees_refresh_for_{memory,file}_index`) and `crates/runtime/tests/acceleration/fts_refresh.rs`. This skill is the end-to-end check against a built `spiced`.

## Build

```bash
CC=gcc CXX=g++ LIBRARY_PATH=/usr/lib/gcc/x86_64-linux-gnu/13 cargo build -p spiced --no-default-features
```

File connector and Cayenne are always linked on Linux. Do not let a script prepend an installed `spiced` over the binary under test.

## Run

```bash
.agents/skills/verify-fts-searcher-at-exec/helpers/verify-fts-searcher-at-exec.sh target/debug/spiced
```

`INDEX_STORE=memory`, `INDEX_STORE=file`, or `INDEX_STORE=both` (default) selects the FTS store. The helper:

1. Seeds a 4-row CSV (`1 apple`, `2 banana`, `3 cherry`, `4 durian`) and a Cayenne `mode: file`, `refresh_mode: full`, `primary_key: id` dataset with `full_text_search` on `text`. SQL and search result caches are off so a refresh that does not invalidate them cannot hide a stale searcher.
2. Starts only this `spiced` on `HTTP_ADDR` (default `127.0.0.1:18424`) plus side ports, logging to `$WORK_DIR/spiced.log`.
3. Waits for `GET /v1/ready` to be `ready`.
4. Queries `text_search` for `banana` (same SQL) and `POST /v1/search`.
5. Rewrites the CSV (delete id 2, cherry→walrus, insert id 5 zephyrine), `POST /v1/datasets/docs/acceleration/refresh`, and polls until id 3 is `walrus`.
6. Repeats the same SQL, a reworded `WHERE true` query, `walrus` / `zephyrine` searches, and `/v1/search`.
7. Stops the pid it started (never `pkill`) and removes the work directory unless `KEEP=1`.

## Expected

| step | expected ids |
|---|---|
| before refresh, `banana` | `[2]` |
| after refresh, same SQL `banana` | `[]` |
| after refresh, reworded `banana` | `[]` |
| after refresh, `walrus` | `[3]` |
| after refresh, `zephyrine` | `[5]` |
| after refresh, `/v1/search` `banana` | no hit |
| after refresh, `/v1/search` `walrus` | id 3 |
| after refresh, `/v1/search` `zephyrine` | id 5 |

On `release/2.4` without #15424, after refresh the same SQL still returns banana `[2]`, and walrus/zephyrine miss. Run the helper against the binary built before the change and the one after, and keep both outputs.
