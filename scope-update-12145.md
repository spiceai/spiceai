### Implementation scope update

Implemented against the design above, with one deliberate deviation on Elasticsearch/S3 Vectors worth flagging for review:

**Delivered**

- **`IndexWriteMode { Append, Overwrite }`** threaded from the sink (derived from the `InsertOp` it already carries) through `on_write_start`/`on_write_failed`/`on_write_complete` on the `Index` trait, and forwarded through every wrapper (`Chunked*`, `Compound*`) and both sinks (`TableSink`, `MultiSink`). This is the missing abstraction the issue identified: an index can now tell a replace-everything full refresh from an add-to-existing append.
- **Full-text (tantivy) index** — fixed with a genuinely atomic swap: a full refresh stages a `delete_all_documents` into the existing deferred-commit window, so the single commit replaces the whole index at once (no empty/partial window; a failed refresh rolls the delete back with everything else). Regression tests included.
- **Elasticsearch** (vector + text) — fixed via a per-write generation stamped on each document (`spice_write_generation`) plus a `delete_by_query` purge of the previous generation in `on_write_complete`, run inside the existing `refresh_interval: -1` window so the new docs and the deletions publish together. Best-effort (a failed purge is logged, not fatal), collision-guarded against a source column of the same name, and covered by mock-client unit tests.

**Deferred (deliberately): S3 Vectors deletion**

S3 Vectors is wired to the new lifecycle but does **not** yet delete stale vectors on a full refresh — it logs that stale vectors may remain and links here. Reason: the safe approach (snapshot keys at write-start, delete `snapshot − written` on complete) is sound, but it is a non-trivial amount of AWS-SDK code (list pagination, `delete_vectors` batching, partition/spill handling) plus mock-client work, and this environment can't compile or run it. Shipping unverified deletion code against a live vector store risks removing live data — strictly worse than the current stale-data bug. I'd rather land the abstraction + the two backends whose fixes are verifiable and do S3 as a focused follow-up. Happy to take direction on whether to fold S3 into this PR or track it separately.
