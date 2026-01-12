# Vortex Columnar Format

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

**Vortex: Encoding-Efficient Columnar Storage for Hot Data**

Columnar file formats face a fundamental tradeoff: compression ratio vs. decode speed. Parquet compresses well but decodes slowly. Arrow IPC decodes instantly but doesn't compress. Vortex offers a middle ground—encoding-efficient compression that decodes fast.

```
┌─────────────────────────────────────────────────────────────────┐
│              COLUMNAR FORMAT TRADEOFFS                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   PARQUET:                                                       │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │ Block compression (Snappy, Zstd, LZ4)                     │  │
│   │                                                           │  │
│   │ Write: Data → Encode → Compress block → File             │  │
│   │ Read:  File → Decompress block → Decode → Arrow          │  │
│   │                                                           │  │
│   │ Compression ratio: Excellent (~0.3x raw size)            │  │
│   │ Decode speed:      Slow (decompress + decode)             │  │
│   │ Best for:          Cold data, network transfer            │  │
│   └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│   ARROW IPC:                                                     │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │ Uncompressed Arrow buffers                                │  │
│   │                                                           │  │
│   │ Write: Arrow buffers → File (direct)                     │  │
│   │ Read:  File → Arrow buffers (zero-copy possible)         │  │
│   │                                                           │  │
│   │ Compression ratio: None (1.0x raw size)                  │  │
│   │ Decode speed:      Instant (zero decode)                  │  │
│   │ Best for:          Hot data, memory-mapped access         │  │
│   └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│   VORTEX:                                                        │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │ Encoding-efficient compression (per column type)         │  │
│   │                                                           │  │
│   │ Write: Data → Column-specific encoding → File            │  │
│   │ Read:  File → SIMD decode → Arrow buffers                │  │
│   │                                                           │  │
│   │ Compression ratio: Very good (~0.4x raw size)            │  │
│   │ Decode speed:      Fast (lightweight decode, SIMD)        │  │
│   │ Best for:          Hot data needing compression           │  │
│   └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**The key insight: Parquet's compression is *two-stage***

Parquet first encodes data (dictionary, RLE, delta), then applies block compression (Snappy, Zstd) on top. Reading requires: decompress → decode → convert to Arrow. That's three steps, and the block decompression is the bottleneck—it's entropy-bound and hard to parallelize.

Vortex skips block compression entirely. Instead, it uses *more aggressive* lightweight encodings that compress nearly as well but decode directly to Arrow via SIMD. One step instead of three.

**How encoding-efficient compression works:**

Instead of applying general-purpose compression (Snappy, Zstd) to entire blocks, Vortex uses specialized encodings for each column type:

**Dictionary encoding**: Low-cardinality strings (country codes, status values). Store unique values once, reference by integer index. Decode: single SIMD gather instruction per 8 values. Example: a `status` column with 5 unique values compresses 64-bit string pointers to 3-bit indices—21x smaller.

**Delta encoding**: Sorted or nearly-sorted integers (timestamps, auto-incrementing IDs). Store differences instead of absolute values. If your timestamps increment by ~1ms each, you're storing 10-bit deltas instead of 64-bit values. Decode: SIMD prefix sum (cumulative add).

**Run-length encoding (RLE)**: Repeated values (null runs, constant columns). Store (value, count) pairs. A column of 1M nulls becomes 1 pair. Decode: SIMD broadcast + scatter.

**Bitpacking**: Small integers. If values fit in 12 bits, store 12 bits—not 64. Vortex packs 5x 12-bit values into a single 64-bit word. Decode: SIMD shuffle + mask. On AVX-512, that's 40 values per instruction.

**Frame-of-reference (FOR)**: Clustered integers (IDs in a range). Subtract the minimum, then bitpack the residuals. Values 1000000-1000100 become 0-100, stored in 7 bits instead of 64.

**ALP (Adaptive Lossless floating-point)**: Floats with limited precision (prices, sensor readings). Detects the actual precision and encodes as scaled integers. $99.99 stored as integer 9999 + scale factor, not IEEE 754.

**Why these encodings decode fast:**

- Simple operations (array lookup, addition, bit manipulation)
- SIMD-vectorizable (process 8-64 values per instruction on modern CPUs)
- Direct to Arrow buffers (no intermediate format, no memory copies)
- Branch-free (predictable CPU pipeline, no misprediction stalls)
- Cache-friendly (sequential access patterns, no random jumps)

**Btrblocks: Automatic encoding selection**

Vortex uses the Btrblocks compression scheme by default. Rather than requiring you to specify encodings per column, it samples data at write time and automatically selects the optimal encoding cascade. A single column might use: FOR → delta → bitpacking for maximum compression.

The algorithm analyzes: cardinality (dictionary?), sortedness (delta?), value range (bitpacking?), run lengths (RLE?), and floating-point precision (ALP?). It tries combinations and picks the smallest output.

### Cayenne: Lakehouse architecture on Vortex

Vortex is a file format. Cayenne is a lakehouse built on Vortex—inspired by DuckLake but optimized for high-throughput continuous data ingestion:

```text
┌─────────────────────────────────────────────────────────────────┐
│                    CAYENNE ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────────────┐    ┌──────────────────────────────┐  │
│   │   SQLite Metastore   │    │      Vortex Data Lake        │  │
│   │   ────────────────   │    │      ─────────────────       │  │
│   │   • Table schemas    │    │   table_abc/                 │  │
│   │   • Primary keys     │    │     ├─ file_001.vortex       │  │
│   │   • File references  │    │     ├─ file_002.vortex       │  │
│   │   • Sequence numbers │    │     └─ file_003.vortex       │  │
│   │   • Deletion vectors │    │   table_xyz/                 │  │
│   │   • Vortex config    │    │     └─ file_001.vortex       │  │
│   └──────────────────────┘    └──────────────────────────────┘  │
│                                                                  │
│   WRITE PATH:                                                    │
│   1. Append new Vortex files (immutable, 128MB target size)      │
│   2. Atomic metadata commit in SQLite (WAL mode, <1ms)           │
│   3. No file rewrites, no lock contention on data files         │
│                                                                  │
│   READ PATH:                                                     │
│   1. Query SQLite for table's file list + deletion vectors      │
│   2. Read Vortex files directly (SIMD decode to Arrow)          │
│   3. Apply deletion vectors (position-based or key-based)       │
│   4. Footer + segment caching (128MB + 256MB defaults)           │
│                                                                  │
│   DELETE PATH:                                                   │
│   1. Write deletion vector (Arrow IPC format)                    │
│   2. Register in metastore with sequence number                  │
│   3. No data file modification—deletes are overlays              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

1. **Sequence numbers for ordering**: Every write operation gets a monotonically increasing sequence number. Deletion vectors only apply to data with `sequence < delete_sequence`. This enables upserts without anti-deletion tracking—insert after delete just works.

2. **128MB target file size**: Smaller files = better parallelism for DataFusion's parallel scan, more granular statistics for predicate pushdown, and faster compaction. Larger files = less metadata overhead. 128MB is the sweet spot.

3. **SQLite in WAL mode**: Write-ahead logging enables concurrent readers during writes. Busy timeout reduces lock contention. The metastore is tiny (KB-MB) even for TB-scale data lakes.

4. **Footer + segment caching**: Vortex file footers contain column statistics and encoding metadata. Caching them (128MB default) means repeated scans skip I/O. Segment cache (256MB) keeps hot column chunks in memory.

**Why SQLite + Vortex (vs. DuckDB):**

We invested heavily in DuckDB for local acceleration. It worked well—until deployments scaled:

| Challenge         | DuckDB                        | Cayenne/Vortex                     |
| ----------------- | ----------------------------- | ---------------------------------- |
| File scaling      | Single file, contention >50GB | Multiple files, horizontal         |
| Write concurrency | Lock contention               | New files + atomic commit          |
| Memory overhead   | In-memory structures          | Minimal (files are self-contained) |
| Query speed       | Excellent                     | Comparable (within 5%)             |
| CRUD operations   | Full SQL                      | Primary key deletes/upserts        |
| Startup time      | Load indexes into memory      | Immediate (lazy loading)           |

**The DuckDB pain point at scale:**

DuckDB stores everything in one file. At 50GB+, checkpoint operations (flushing WAL to main file) can take 10+ seconds while blocking all writes. Concurrent inserts contend on a single WAL. Memory overhead scales with data size for indexes and statistics.

Cayenne sidesteps this: new data = new files. No checkpointing. No rewriting. The metastore stays tiny. Memory usage is bounded by cache settings, not data size.

**When to use which:**

- **Parquet**: Cold data, archival, network transfer, interchange
- **Arrow IPC**: Hot data in memory, IPC between processes, no persistence needed
- **DuckDB**: Ad-hoc analysis, embedded <50GB, single-user, full SQL DML
- **Vortex/Cayenne**: Production acceleration, continuous ingestion, primary-key CRUD, scale

The right format depends on your access patterns. Hot data that needs compression and continuous writes? That's where Vortex fits.

---

## X (Twitter)

### Post 1: The shift from DuckDB

Why we're shifting investment from DuckDB to Cayenne/Vortex for acceleration:

DuckDB limitations at scale:

- Single-file architecture (>50GB = checkpoint blocking)
- Write lock contention (continuous ingestion + concurrent reads)
- Memory overhead (indexes + stats scale with data size)
- Startup time grows with data (must load metadata)

Cayenne architecture:

```text
┌─────────────────────────────────────────────────────────┐
│  SQLite Metastore    │    Vortex Data Lake              │
│  ─────────────────   │    ──────────────────            │
│  • table schemas     │    table_abc/                    │
│  • file refs         │      ├─ file_001.vortex         │
│  • sequence numbers  │      └─ file_002.vortex         │
│  • deletion vectors  │    table_xyz/                    │
│  • primary keys      │      └─ file_001.vortex         │
│                      │                                  │
│  Writes = new immutable files + atomic metadata commit  │
│  Deletes = overlay vectors, no rewrites                 │
│  Reads = parallel scan + deletion filtering             │
└─────────────────────────────────────────────────────────┘
```

DuckDB: still great for <50GB, ad-hoc, embedded.

Cayenne: production acceleration at scale, continuous ingestion, primary-key upserts.

The right tool changes as constraints change.

### Post 2: Why Vortex decodes faster than Parquet

Parquet's compression is two-stage:

1. Encode (dictionary, RLE, delta)
2. Block compress (Snappy/Zstd/LZ4)

Reading: decompress → decode → Arrow conversion. Three steps.

Block decompression is entropy-bound. Can't parallelize within a block. CPU-bound on a single core.

Vortex skips block compression. Uses *more aggressive* lightweight encodings instead:

- Dictionary: SIMD gather, 8 values per instruction
- Delta: SIMD prefix sum
- Bitpacking: 12-bit values → 5 per 64-bit word
- FOR: subtract min, then bitpack
- RLE: broadcast + scatter
- ALP: floats as scaled integers

One decode step. Direct to Arrow. Branch-free. Cache-friendly.

Result: ~0.4x compression (vs Parquet's ~0.3x), but 3-5x faster decode.

For hot data you query repeatedly, decode speed > compression ratio.

### Post 3: Encoding-efficient compression explained

Why does Vortex compress nearly as well as Parquet without block compression?

It uses *cascaded* encodings. A timestamp column might get:

FOR → Delta → Bitpacking

1. FOR: Subtract 1704067200 (Jan 1 2024)
2. Delta: Store differences (~1000ms each)
3. Bitpack: 10-bit values instead of 64-bit

64 bits → 10 bits = 6.4x compression. No Zstd needed.

A status column with 5 values:
Dictionary: 3-bit indices instead of 64-bit pointers = 21x

Vortex samples data at write time and picks the optimal cascade automatically (Btrblocks algorithm).

The key insight: *domain-specific* encodings beat *general-purpose* compression when you know your data patterns.

### Post 4: Deletion without rewriting

Lakehouse formats (Iceberg, Delta, Hudi) avoid rewriting files on delete. They use deletion vectors.

Cayenne's approach:

```text
Data file: [id=1, id=2, id=3, id=4, id=5]  seq=1
Delete:    [id=2, id=4]                     seq=2

Read: scan data → check delete → filter

Result: [id=1, id=3, id=5]
```

Sequence numbers enable upserts:

```text
Delete id=3                                 seq=3
Insert id=3 (new value)                     seq=4

Delete only applies to data where seq < 3
New insert has seq=4, so delete doesn't apply

Result: new id=3 value returned correctly
```

No anti-deletion tracking. No coordination. Just sequence ordering.

Stored as Arrow IPC. Merged with data at read time. Position-based or key-based depending on whether table has primary key.

### Post 5: Cache architecture

Vortex file structure:

```text
┌─────────────────────────────┐
│  Data segments (columns)    │ ← Segment cache (256MB default)
├─────────────────────────────┤
│  Footer                     │ ← Footer cache (128MB default)  
│  • Column statistics        │
│  • Encoding metadata        │
│  • Segment offsets          │
└─────────────────────────────┘
```

First query: read footer, decode stats, read needed segments.

Subsequent queries: footer cached, stats checked in-memory, skip segments via predicate pushdown.

`WHERE timestamp > '2024-01-01'` → check footer min/max → skip entire file if no match.

Unlike DuckDB (all indexes in memory), cache is bounded. 384MB default regardless of data size. Evicts LRU.

Production at 1TB with <500MB memory footprint.
