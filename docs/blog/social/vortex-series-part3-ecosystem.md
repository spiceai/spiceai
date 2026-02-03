# Vortex Deep Dive Part 3: Ecosystem & Adoption

> The companies, projects, and community driving Vortex forward

---

## 📚 Vortex at Spice AI Series

This is Part 3 of our 3-part deep dive into Vortex, following our [Vortex at Spice AI](../blog/engineering/vortex-at-spiceai.md) engineering post.

- [Part 1: The Research Behind Vortex](vortex-series-part1-research.md)
- [Part 2: Real-World Use Cases](vortex-series-part2-use-cases.md)
- **Part 3: Ecosystem & Adoption** *(You are here)*

---

## Linux Foundation Stewardship

In 2024, Vortex was donated to the Linux Foundation, joining the ranks of open-source data infrastructure projects with neutral governance. This move signals long-term commitment to open development and vendor-neutral stewardship.

Why this matters:

| Benefit                 | Impact                                           |
| ----------------------- | ------------------------------------------------ |
| **Neutral governance**  | No single vendor controls the roadmap            |
| **Long-term stability** | Foundation outlives any single company           |
| **Community trust**     | Contributors know their work won't be relicensed |
| **Enterprise adoption** | Legal teams trust LF governance model            |

## Spiral: The Company Behind Vortex

[Spiral](https://spiraldb.com/) (formerly Vortex Labs) is the primary commercial entity behind Vortex development. Founded by engineers from Google and Meta with deep expertise in columnar systems, Spiral offers:

- **Vortex Cloud** — Managed Vortex storage with enterprise features
- **Commercial support** — SLAs for production deployments
- **Training and consulting** — Help teams adopt Vortex effectively

The business model follows the proven open-core playbook: open-source the format, commercialize the managed service and enterprise features.

## Technology Stack

Vortex integrates with the modern data stack:

### Arrow Ecosystem

Vortex is Arrow-native by design. Data decompresses directly to Arrow arrays with zero intermediate copies. This enables seamless integration with:

- **DataFusion** — Query engine integration via `vortex-datafusion`
- **DuckDB** — Through Arrow interchange
- **Polars** — Native Rust + Arrow compatibility
- **PyArrow** — Python data science workflows

### Cloud Object Stores

Vortex files work with any object store:

- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO and S3-compatible stores

## Who's Using Vortex?

### NVIDIA

NVIDIA is an official backer of the Vortex Linux Foundation project. Their strategic interest centers on **GPU-direct decompression**—loading training data straight from object storage into GPU memory without CPU bottlenecks.

The Vortex repository has active CUDA development:

- `vortex-cuda` — CUDA integration crate
- `vortex-gpu` — GPU acceleration infrastructure
- `vortex-gpu-kernels` — CUDA kernel code generation

Implemented GPU kernels include Frame of Reference (FoR), BitPacking, and RLE decompression—with fused kernel composition for optimized decode paths. Integration with NVIDIA's RAPIDS cuDF is on the roadmap.

As Spiral notes: *"Modern GPUs can consume terabits per second, but legacy storage formats require CPUs to decompress data first. Vortex supports decoding data directly from S3 to GPU, skipping the CPU bottleneck entirely."*

### LangChain

LangChain uses Vortex internally for observability in LangSmith and their cloud products. When you're processing millions of LLM traces and spans, fast columnar decode matters. Vortex's compression ratios and query performance make it well-suited for the high-cardinality, append-heavy workloads typical of observability systems.

### DuckDB

[DuckDB announced native Vortex support](https://duckdb.org/2026/01/23/duckdb-vortex-extension) in January 2026, making Vortex a first-class citizen alongside Parquet, CSV, and JSON. The SpiralDB team built the extension, partnering with DuckDB Labs to deliver it as a core extension.

**Performance results** from TPC-H SF100 benchmarks:

- **18% faster** than Parquet v2 (geometric mean)
- **35% faster** than Parquet v1
- Significantly lower standard deviation across query runs

```sql
INSTALL vortex;
LOAD vortex;

SELECT * FROM read_vortex('my.vortex');

COPY (SELECT * FROM my_table)
TO 'output.vortex' (FORMAT vortex);
```

DuckDB highlights three key Vortex use cases:

1. **SQL analytics** — Late decompression and compute on compressed data reduces IO
2. **ML preprocessing** — Wide encoding support for audio, text, images, vectors
3. **AI model training** — Efficient GPU data transfer via FastLanes encoding

### Spice AI (Cayenne)

[Spice Cayenne](https://spice.ai/blog/introducing-spice-cayenne-data-accelerator) is our next-generation data accelerator built on Vortex for multi-terabyte, low-latency workloads. Cayenne combines Vortex columnar storage with an embedded SQLite metadata layer.

**Benchmark results** (TPC-H SF100, ClickBench):

- **1.4x faster** than DuckDB file mode on TPC-H
- **~3x lower memory usage** than DuckDB
- **14% faster** on ClickBench with 3.4x less memory

Vortex advantages in Cayenne:

- **100x faster random access** than Parquet
- **10–20x faster full scans**
- **5x faster writes**
- Zero-copy Arrow compatibility

```yaml
datasets:
  - from: s3://my-bucket/data/
    name: analytics_data
    acceleration:
      engine: cayenne
      mode: file
```

### Polar Signals

[Polar Signals switched from Parquet to Vortex](https://www.polarsignals.com/blog/posts/2025/11/25/interface-parquet-vortex) for their profiling database, achieving dramatic performance improvements:

- **70% average performance improvement** across all queries
- **10% better uncompressed storage size**
- Only **3% larger compressed size** vs. snappy-compressed Parquet

Their key insight: Parquet's conversion to Arrow dominated query CPU time. Vortex's design—late decompression, compute pushdown on compressed data, and zero-copy Arrow conversion—eliminated this bottleneck.

> "Finding Vortex was like finding a pair of nice-fitting shoes after walking around in a size too small for longer than we could remember." — Alfonso Subiotto Marqués, Polar Signals

### NVIDIA GPU Acceleration

Vortex includes native CUDA support through dedicated GPU crates (`vortex-cuda`, `vortex-gpu`, `vortex-gpu-kernels`). Recent development integrates [NVIDIA nvCOMP](https://developer.nvidia.com/nvcomp) for GPU-accelerated decompression, enabling:

- **Direct S3-to-GPU data transfer** — Skip CPU entirely for AI training pipelines
- **FastLanes GPU encoding** — SIMD-friendly integer compression optimized for GPU saturation
- **Zstd CUDA decompression** — Hardware-accelerated decompression via nvCOMP

This positions Vortex as a key format for GPU-intensive workloads, bridging storage and compute for AI/ML pipelines.

### Research Institutions

Academic groups evaluating next-generation columnar formats reference Vortex as a production-ready implementation of BtrBlocks and related research.

### Startups Building on Arrow

Early-stage companies in the Arrow ecosystem adopt Vortex when they need compressed columnar storage with fast decode. The zero-copy Arrow integration reduces engineering effort.

## Comparison to Alternatives

How does Vortex fit in the columnar format landscape?

| Format        | Strengths                        | Weaknesses                  |
| ------------- | -------------------------------- | --------------------------- |
| **Parquet**   | Universal support, battle-tested | Slower decode, complex spec |
| **Arrow IPC** | Zero-copy, fast                  | Minimal compression         |
| **ORC**       | Hive ecosystem, mature           | Java-centric, complex       |
| **Vortex**    | Fast decode, Arrow-native        | Newer, smaller ecosystem    |

Vortex occupies a unique position: more compressed than Arrow IPC, faster to decode than Parquet, simpler than Iceberg, and fully Arrow-native.

## The Roadmap

Based on public discussions and commits, Vortex development focuses on:

**Near-term:**

- Improved DataFusion integration
- Enhanced statistics for query optimization
- More encoding types (sparse, dictionary cascade)

**Medium-term:**

- Native Python SDK
- Streaming write support
- Cloud-native partitioning

**Long-term:**

- Learned compression (ML-based encoding selection)
- GPU decode acceleration (NVIDIA partnership)
- Cross-language specification
- cuDF integration (RAPIDS ecosystem)

## Getting Involved

Vortex is open-source under the Apache 2.0 license:

- **GitHub:** [github.com/vortex-data/vortex](https://github.com/vortex-data/vortex)
- **Documentation:** In-repo docs and examples
- **Community:** GitHub Discussions and issues

Contributions welcome in:

- New encoding implementations
- DataFusion integration improvements
- Benchmarking and performance optimization
- Documentation and examples

## Our Investment

At Spice AI, we maintain a fork of Vortex to align Arrow and DataFusion versions with our release cycle. We contribute improvements upstream and track the main project closely.

Our philosophy:

- **Minimal divergence** — Stay close to upstream
- **Upstream first** — Contribute improvements back
- **Collaborate** — Work with Spiral and the community

The Vortex project represents the future of columnar storage: research-grade algorithms in production-quality code, backed by neutral governance and commercial support.

---

## LinkedIn

The Vortex Ecosystem Is Ready

In August 2024, Vortex was donated to the Linux Foundation. Eighteen months later, the ecosystem has matured into something production-ready. Here's the full picture.

Linux Foundation Governance

Vortex now sits alongside projects like PyTorch and ONNX under the LF AI and Data Foundation. This matters for enterprise adoption: Neutral governance—no single vendor controls the roadmap. Long-term stability—foundation outlives any company. Enterprise trust—legal teams know what they're getting.

Spiral, the company behind Vortex (founded by ex-Google/Meta columnar systems engineers), provides commercial support while contributing to the open-source core. The proven open-core model aligns incentives—Spiral wins when Vortex adoption grows.

Why Vortex Exists

Parquet has been the columnar format for a decade. But "efficient storage" doesn't mean "efficient querying." Reading Parquet means decompressing blocks, then converting to Arrow for computation. That conversion dominates CPU time.

Vortex takes a different approach: late decompression and compute pushdown. Filter expressions execute directly on compressed data. When you do decompress, encodings like ALP (floats), FSST (strings), and FastLanes (integers) are SIMD-friendly and decode directly to Arrow with zero copies.

The result: 100x faster random access, 10-20x faster scans, Arrow-native from the ground up.

Ecosystem Integrations

The integration story is strong and growing: DataFusion—native TableProvider via vortex-datafusion. DuckDB—core extension with full read/write support. Polars—Rust plus Arrow compatibility. PyArrow—Python data science workflows. NVIDIA nvCOMP—GPU-accelerated decompression for AI/ML pipelines.

DuckDB's adoption as a core extension in January 2026 signals Vortex is ready for mainstream use.

Real-World Results

Early adopters are seeing consistent improvements across independent evaluations:

DuckDB benchmarks (TPC-H SF100): 18% faster than Parquet v2 (geometric mean), 35% faster than Parquet v1.

Polar Signals (profiling database): 70% query performance improvement, 10% better uncompressed storage size.

Spice AI Cayenne (data accelerator): 1.4x faster than DuckDB on TPC-H, approximately 3x lower memory usage.

These numbers come from three independent teams—this isn't one vendor's marketing.

The adopter list speaks volumes:

NVIDIA — Official LF project backer, actively developing CUDA kernels for GPU-direct decompression. Their vision: load training data straight from S3 to GPU memory, bypassing CPU bottlenecks entirely.

LangChain — Uses Vortex internally for LangSmith observability and their cloud products. Millions of LLM traces need fast columnar decode.

Spice AI — Vortex powers Cayenne, our data accelerator.

Plus academic groups, Arrow-ecosystem startups, and the growing community of contributors. We've built Spice Cayenne on Vortex and contribute improvements upstream. The combination of research-grade algorithms, production-quality code, and sustainable governance is rare.

---

## X (5 posts, 280 characters each)

Post 1:
Vortex ecosystem is production-ready: DuckDB core extension (18-35% faster than Parquet). NVIDIA nvCOMP GPU acceleration. Spice Cayenne (1.4x faster than DuckDB). Polar Signals (70% speedup). Linux Foundation governance.

Post 2:
Linux Foundation stewardship matters: neutral governance, no single vendor controls roadmap. Long-term stability. Enterprise legal teams trust LF model. Spiral provides commercial support while contributing to open-source core.

Post 3:
Why Vortex beats Parquet for hot data: late decompression, compute pushdown on compressed data. Filter expressions run directly on encoded columns. SIMD-friendly decode straight to Arrow. 100x faster random access.

Post 4:
Who's using Vortex: NVIDIA for GPU-direct decompression (S3 to GPU, skip CPU). LangChain for LangSmith observability (millions of LLM traces). DuckDB as core extension. Polar Signals for profiling database.

Post 5:
Vortex integrations: DataFusion native TableProvider. DuckDB full read/write. Polars via Rust plus Arrow. PyArrow for Python. NVIDIA nvCOMP for GPU. The Arrow-native columnar format ecosystem is ready.
