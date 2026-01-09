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

**How encoding-efficient compression works:**

Instead of applying general-purpose compression (Snappy, Zstd) to entire blocks, Vortex uses specialized encodings for each column type:

**Dictionary encoding**: Low-cardinality strings (country codes, status values). Store unique values once, reference by integer index. Decode: array lookup.

**Delta encoding**: Sorted or nearly-sorted integers (timestamps, auto-incrementing IDs). Store differences instead of absolute values. Decode: cumulative sum.

**Run-length encoding (RLE)**: Repeated values (null runs, constant columns). Store value + count. Decode: expand runs.

**Bitpacking**: Small integers. If values fit in 12 bits, store 12 bits—not 64. Decode: bit unpacking (SIMD-friendly).

**Why these encodings decode fast:**

- Simple operations (array lookup, addition, bit manipulation)
- SIMD-vectorizable (process multiple values per instruction)
- Direct to Arrow buffers (no intermediate format)

**Cayenne: Lakehouse architecture on Vortex**

Vortex is a file format. Cayenne is a lakehouse built on Vortex:

```
┌─────────────────────────────────────────────────────────────────┐
│                    CAYENNE ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────────────┐    ┌──────────────────────────────┐  │
│   │   SQLite Metastore   │    │      Vortex Data Lake        │  │
│   │   ────────────────   │    │      ─────────────────       │  │
│   │   • Table schemas    │    │   snapshot_v1/               │  │
│   │   • Snapshot history │    │     ├─ file_001.vortex       │  │
│   │   • File references  │    │     └─ file_002.vortex       │  │
│   │   • Statistics       │    │   snapshot_v2/               │  │
│   │   • Deletion vectors │    │     └─ file_003.vortex       │  │
│   └──────────────────────┘    └──────────────────────────────┘  │
│                                                                  │
│   WRITE PATH:                                                    │
│   1. Write new Vortex files (immutable)                          │
│   2. Atomic metadata commit in SQLite                           │
│   3. No file rewrites, no lock contention on data               │
│                                                                  │
│   READ PATH:                                                     │
│   1. Query SQLite for current snapshot's file list              │
│   2. Read Vortex files directly (SIMD decode to Arrow)          │
│   3. Snapshot isolation: readers see consistent state           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Why SQLite + Vortex (vs. DuckDB):**

We invested heavily in DuckDB for local acceleration. It worked well—until deployments scaled:

| Challenge         | DuckDB                        | Cayenne/Vortex                     |
| ----------------- | ----------------------------- | ---------------------------------- |
| File scaling      | Single file, contention >50GB | Multiple files, horizontal         |
| Write concurrency | Lock contention               | New files + atomic commit          |
| Memory overhead   | In-memory structures          | Minimal (files are self-contained) |
| Query speed       | Excellent                     | Comparable (within 5%)             |

**When to use which:**

- **Parquet**: Cold data, archival, network transfer
- **Arrow IPC**: Hot data in memory, IPC between processes
- **DuckDB**: Ad-hoc analysis, embedded <50GB, single-user
- **Vortex/Cayenne**: Production acceleration, continuous ingestion, scale

The right format depends on your access patterns. Hot data that needs compression and continuous writes? That's where Vortex fits.

---

## X

Why we're shifting investment from DuckDB to Cayenne/Vortex for acceleration:

DuckDB limitations at scale:
- Single-file architecture (>50GB = problems)
- Write lock contention (continuous ingestion + reads)
- Memory overhead (dense deployments struggle)

Cayenne architecture:
```
┌─────────────────────────────────────────────────────────┐
│  SQLite Metastore    │    Vortex Data Lake              │
│  ─────────────────   │    ──────────────────            │
│  • snapshots         │    snapshot_001/                 │
│  • schemas           │      ├─ file_001.vortex         │
│  • file refs         │      └─ file_002.vortex         │
│  • deletion vectors  │    snapshot_002/                 │
│                      │      └─ file_003.vortex         │
│                      │                                  │
│  Writes = new files + atomic metadata commit            │
│  No file rewrites. No lock contention.                  │
└─────────────────────────────────────────────────────────┘
```

Vortex encoding-efficient compression:
- Dictionary (low-cardinality strings)
- Delta (timestamps, IDs)
- RLE (repeated values)
- Bitpacking (small integers)

All decode directly to Arrow via SIMD.

Benchmarks (100GB TPC-H):

| Metric             | DuckDB   | Cayenne   |
| ------------------ | -------- | --------- |
| Query latency      | baseline | ≈ same    |
| Ingestion speed    | baseline | 3x faster |
| Memory usage       | baseline | 40% less  |
| Max practical size | ~100GB   | unlimited |

DuckDB: still great for <50GB, ad-hoc, embedded.
Cayenne: production acceleration at scale.

The right tool changes as constraints change.
