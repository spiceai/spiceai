# Vortex Columnar Format

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

Vortex: Encoding-Efficient Columnar Storage for Hot Data

Columnar file formats face a fundamental tradeoff: compression ratio vs. decode speed. Parquet compresses well but decodes slowly. Arrow IPC decodes instantly but doesn't compress. Vortex offers a middle ground—encoding-efficient compression that decodes fast.

Parquet uses block compression (Snappy, Zstd, LZ4) on top of encoding. Reading requires: decompress then decode then convert to Arrow. That's three steps, and the block decompression is the bottleneck—it's entropy-bound and hard to parallelize.

Arrow IPC stores uncompressed Arrow buffers. Write is direct from Arrow to file. Read is direct from file to Arrow, with zero-copy possible. Compression ratio is 1.0x (no compression). Decode speed is instant.

Vortex skips block compression entirely. Instead, it uses more aggressive lightweight encodings that compress nearly as well but decode directly to Arrow via SIMD. One step instead of three. Compression ratio around 0.4x (vs Parquet's 0.3x), but 3-5x faster decode.

How encoding-efficient compression works:

Dictionary encoding: Low-cardinality strings stored once, referenced by integer index. Decode: single SIMD gather instruction per 8 values. A status column with 5 unique values compresses 64-bit string pointers to 3-bit indices—21x smaller.

Delta encoding: Sorted or nearly-sorted integers. Store differences instead of absolute values. If timestamps increment by ~1ms each, you're storing 10-bit deltas instead of 64-bit values. Decode: SIMD prefix sum.

Run-length encoding: Repeated values stored as (value, count) pairs. A column of 1M nulls becomes 1 pair. Decode: SIMD broadcast plus scatter.

Bitpacking: Small integers stored in their actual bit width. 12-bit values pack 5 per 64-bit word. Decode: SIMD shuffle plus mask. On AVX-512, that's 40 values per instruction.

Frame-of-reference: Clustered integers. Subtract the minimum, then bitpack residuals. Values 1000000-1000100 become 0-100, stored in 7 bits instead of 64.

ALP (Adaptive Lossless floating-point): Floats with limited precision stored as scaled integers. $99.99 stored as integer 9999 plus scale factor, not IEEE 754.

These encodings decode fast because they use simple operations (array lookup, addition, bit manipulation), SIMD-vectorizable (8-64 values per instruction), direct to Arrow buffers (no intermediate format), branch-free (predictable CPU pipeline), and cache-friendly (sequential access).

Vortex uses Btrblocks compression scheme by default. It samples data at write time and automatically selects the optimal encoding cascade. A single column might use: FOR then delta then bitpacking for maximum compression.

Cayenne: Lakehouse architecture on Vortex

Cayenne combines Vortex columnar storage with an embedded SQLite metadata layer. Write path: append new Vortex files (immutable, 128MB target size), atomic metadata commit in SQLite (WAL mode, under 1ms), no file rewrites, no lock contention. Read path: query SQLite for file list plus deletion vectors, read Vortex files directly (SIMD decode to Arrow), apply deletion vectors. Delete path: write deletion vector, register in metastore with sequence number, no data file modification.

Why SQLite plus Vortex instead of DuckDB: DuckDB stores everything in one file. At 50GB+, checkpoint operations can take 10+ seconds while blocking writes. Cayenne sidesteps this: new data means new files. No checkpointing. No rewriting. Memory overhead is minimal. Query speed is comparable within 5%. Startup is immediate with lazy loading.

When to use which: Parquet for cold data, archival, network transfer. Arrow IPC for hot in-memory data, IPC between processes. DuckDB for ad-hoc analysis, embedded under 50GB, single-user. Vortex/Cayenne for production acceleration, continuous ingestion, primary-key CRUD at scale.

The right format depends on your access patterns. Hot data needing compression and continuous writes? That's where Vortex fits.

---

## X (5 posts, 280 characters each)

Post 1:
Why we shifted from DuckDB to Cayenne/Vortex for acceleration: DuckDB single-file architecture means checkpoint blocking at 50GB+, write lock contention, memory overhead scaling with data. Cayenne uses multi-file architecture.

Post 2:
Vortex decodes faster than Parquet because it skips block compression. Parquet: decompress then decode then Arrow. Vortex: SIMD decode straight to Arrow. One step vs three. Block decompression is the bottleneck.

Post 3:
Vortex uses cascaded encodings. Timestamp column: FOR (subtract base) then Delta (store differences) then Bitpack (10-bit values). 64 bits becomes 10 bits. No Zstd needed. Domain-specific beats general-purpose.

Post 4:
Cayenne deletion without rewriting: Data file has records. Delete vector has IDs. Sequence numbers order operations. Delete only applies to data where seq less than delete_seq. New inserts have higher seq. No anti-deletion tracking.

Post 5:
The choice: Parquet compresses but decodes slow. Arrow IPC decodes instantly but no compression. DuckDB is great under 50GB. Vortex/Cayenne for hot data needing both compression and fast decode at scale. Right tool for right pattern.
