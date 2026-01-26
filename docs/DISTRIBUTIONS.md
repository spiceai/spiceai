# Spice Runtime Distributions

The Spice open source project provides multiple distribution variants to support different use cases and deployment scenarios.

> **Note:** The Spice runtime is **64-bit only**. 32-bit platforms are not supported.

> **Note:** Variant distributions (data, allocators, CUDA) are only available in **nightly images** for the open source project. All features and distributions are available in the [Spice Cloud Platform](https://spice.ai/pricing) and [Spice.ai Enterprise](https://spice.ai/pricing).

## Default Distribution

The default distribution includes all features including AI/ML model support. This is the recommended distribution for most users.

**Included Features:**

- All standard data connectors (PostgreSQL, MySQL, DuckDB, SQLite, ClickHouse, etc.)
- Embedded data accelerators (Spice Cayenne, DuckDB, SQLite)
- AI/ML model inference (LLMs, embeddings)
- Search capabilities (Vector and BM-25 Full-Text-Search)
- Default memory allocator (snmalloc)

> **Note:** The PostgreSQL data accelerator is only available in nightly builds. The PostgreSQL data connector is included in all distributions.

**Installation:**

```bash
curl https://install.spiceai.org | /bin/bash
```

**Docker:**

```bash
docker pull ghcr.io/spiceai/spiceai:latest
# or
docker pull spiceai/spiceai:latest
```

## Data Distribution

The data distribution excludes AI/ML model support, resulting in a smaller binary size and reduced attack surface. Use this when you only need data federation and acceleration capabilities without AI features.

> **Open Source:** Available in nightly builds only. **[Cloud Platform & Enterprise](https://spice.ai/pricing):** Production-ready data distribution available.

**Included Features:**

- All data connectors
- All data accelerators
- Default memory allocator (snmalloc)

**Excluded Features:**

- AI/ML model inference
- LLM support
- Embedding models

**Docker (Nightly):**

```bash
docker pull ghcr.io/spiceai/spiceai-nightly:latest-data
```

**Local Build:**

```bash
make install-data-only
```

## GPU-Accelerated Distributions

### Metal (macOS)

For macOS systems with Apple Silicon, the Metal distribution enables GPU-accelerated AI/ML inference.

**Included Features:**

- All default features
- Metal GPU acceleration for model inference

**Local Build:**

```bash
make install-metal
```

### CUDA (Linux)

For Linux systems with NVIDIA GPUs, CUDA distributions enable GPU-accelerated AI/ML inference. Multiple CUDA compute capability versions are available.

> **Open Source:** Available in nightly builds only. **[Cloud Platform & Enterprise](https://spice.ai/pricing):** Production-ready CUDA distribution available.

**Included Features:**

- All default features
- CUDA GPU acceleration for model inference

**Supported Compute Capabilities:**

- 80 (A100, A30)
- 86 (RTX 30xx, A40, A10)
- 87 (Jetson Orin)
- 89 (RTX 40xx, L40, L4)
- 90 (H100, H200)

**Docker (Nightly):**

```bash
docker pull ghcr.io/spiceai/spiceai-nightly:latest-cuda
```

**Local Build:**

```bash
CUDA_COMPUTE_CAP=89 make install-cuda
```

## Allocator Variants

Different memory allocators can significantly impact performance depending on workload characteristics.

> **Open Source:** Allocator variants are available in nightly builds only for testing and benchmarking. **[Cloud Platform & Enterprise](https://spice.ai/pricing):** Production-ready allocator variants available.

### snmalloc (Default)

The default allocator, optimized for concurrent workloads.

### jemalloc

Alternative allocator that may perform better for certain memory allocation patterns.

**Docker (Nightly):**

```bash
docker pull ghcr.io/spiceai/spiceai-nightly:latest-jemalloc
```

### mimalloc

Microsoft's mimalloc allocator, designed for performance and security.

**Docker (Nightly):**

```bash
docker pull ghcr.io/spiceai/spiceai-nightly:latest-mimalloc
```

### System Allocator

Uses the system's default allocator (glibc malloc on Linux).

**Docker (Nightly):**

```bash
docker pull ghcr.io/spiceai/spiceai-nightly:latest-sysalloc
```

## Platform Support

| Platform                      | Default | Data            | Metal | CUDA            |
| ----------------------------- | ------- | --------------- | ----- | --------------- |
| Linux x86_64                  | ✅       | Nightly         | ❌     | Nightly         |
| Linux aarch64                 | ✅       | Nightly         | ❌     | ❌               |
| macOS aarch64 (Apple Silicon) | ✅       | Nightly         | ✅     | ❌               |
| Windows (WSL)                 | ✅       | Nightly         | ❌     | Nightly         |
| Windows (Native)              | ❌       | Enterprise only | ❌     | Enterprise only |

> **Note:** Native Windows support for the Spice runtime is available with the [Spice Cloud Platform and Spice.ai Enterprise](https://spice.ai/pricing). Open source users on Windows should use Windows Subsystem for Linux (WSL).

## Choosing a Distribution

| Use Case                                | Recommended Distribution     |
| --------------------------------------- | ---------------------------- |
| General purpose with AI capabilities    | Default                      |
| Data federation only, minimal footprint | Data (nightly)               |
| macOS with GPU acceleration             | Metal                        |
| Linux with NVIDIA GPU                   | CUDA (nightly)               |
| Memory allocation benchmarking          | Allocator variants (nightly) |

## Additional Connectors

Some connectors require additional dependencies and are available with the [Spice Cloud Platform and Spice.ai Enterprise](https://spice.ai/pricing):

- **ODBC** - Connect to any ODBC-compatible data source
- **NFS** - Network File System support

These can be built locally for development and testing:

```bash
make install-odbc
make install-nfs
```

## Building Custom Distributions

You can build custom distributions with specific feature combinations:

```bash
# Build with specific features
SPICED_CUSTOM_FEATURES="duckdb,postgres,sqlite,models" make build-runtime

# Build with non-default features added to defaults
SPICED_NON_DEFAULT_FEATURES="odbc" make install
```

See the [Makefile](../Makefile) for all available build targets and options.
