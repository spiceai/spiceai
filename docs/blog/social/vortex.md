# Vortex Columnar Format

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

🌪️ Vortex: The Bet on Encoding-Efficient Columnar Storage for Hot Data

After years building data infra with Apache Arrow, Parquet, and DuckDB, here's what we've learned about columnar formats and why we're investing in Vortex.

⚖️ THE PROBLEM: COMPRESSION VS. DECODE SPEED

Every columnar format makes a tradeoff. When your app queries data repeatedly (hot data), this becomes critical:

PARQUET uses block compression (Snappy, Zstd). Data is encoded, then compressed. To read, you decompress the entire block, then decode. Compression is excellent (~0.3x), but decode is slow. Great for cold data and archival.

ARROW IPC stores uncompressed Arrow buffers directly. Write is a memcpy. Read can be zero-copy via memory mapping. No compression (1.0x), but decode is instant. Ideal for IPC and memory-mapped access.

VORTEX sits in the middle. Instead of block compression, it uses encoding-efficient compression tailored to each column's type. Compression is good (~0.4x), decode is fast because encodings are designed for SIMD. The sweet spot for hot data needing both compression and fast repeated access.

➡️ Running 100 queries/sec against the same dataset? With Parquet, you decompress the same blocks 100 times. With Arrow, you use 3x storage. Vortex gives 80% of Parquet's compression with 10x faster decode.

🧠 ENCODING-EFFICIENT COMPRESSION

Vortex selects the best encoding per column:

• DICTIONARY: Low-cardinality strings (country codes, status values). Store once, reference by index. Decode is an array lookup.
• DELTA: Sorted integers (timestamps, IDs). Store differences. Decode is cumulative sum, trivially SIMD-vectorizable.
• RLE: Repeated values (null runs, booleans). Store run counts. Excellent for sparse data.
• BITPACKING: Small integers. If values fit in 12 bits, why store 64? SIMD-friendly unpacking.

All decode directly to Arrow. No intermediate format, no extra copies.

🔄 WHY WE'RE SHIFTING FROM DUCKDB

DuckDB is excellent. But at scale, we hit limits:

• FILE SCALING: Single-file architecture struggles >100GB. Cayenne uses multiple immutable files.
• WRITE CONCURRENCY: Lock contention with continuous ingestion. Cayenne's append-only design eliminates this.
• MEMORY: In-memory structures cause pressure in dense K8s deployments. Vortex files are self-contained; 1/3 memory usage.
• QUERY SPEED: Comparable, within 5% on TPC-H.

DuckDB remains right for <100GB and low concurrency. For production acceleration with continuous ingestion at scale, Cayenne/Vortex is our path forward.

🎯 WHEN TO USE WHAT

PARQUET: Cold data, archival, network transfer
ARROW IPC: In-memory, IPC, zero-copy
DUCKDB: <100GB, low concurrency
VORTEX/CAYENNE: Production acceleration, continuous ingestion, scale beyond 100GB

The right format depends on access patterns. Hot data needing compression + continuous writes? That's Vortex.

#datafusion #spiceai #data #arrow #parquet #duckdb 

---

## X

🌪️ Why Vortex for hot data?

Parquet: great compression, slow decode
Arrow: instant decode, no compression
Vortex: both—encoding-efficient compression with SIMD decode to Arrow

80% of Parquet's compression, 10x faster decode.

spiceai.org
