# Spice Runtime Distributions

The Spice open source project provides multiple distribution variants to support different use cases and deployment scenarios.

> **Note:** The Spice runtime is **64-bit only**. 32-bit platforms are not supported.

> **Note:** Variant distributions (data, allocators, CUDA) are only available in **nightly images** for the open source project. All features and distributions are available in the [Spice Cloud Platform](https://spice.ai/pricing) and [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise).

## Supported Platforms & Hardware Requirements

| Platform | Architecture            | Minimum CPU Features                   | Build Prerequisites |
| -------- | ----------------------- | -------------------------------------- | ------------------- |
| Linux    | x86_64                  | AVX2, FMA, BMI1/2, LZCNT, POPCNT       | —                   |
| Linux    | aarch64 (arm64)         | NEON, FP16 (FEAT_FP16), FHM (FEAT_FHM) | `clang`, `lld`      |
| macOS    | aarch64 (Apple Silicon) | Native (build host)                    | —                   |
| Windows  | x86_64 (MSVC)           | —                                      | MSVC toolchain      |

> **Note:** Open source Windows support is CLI (`spice`) only. The runtime daemon (`spiced`) is not supported on Windows natively in open source builds — use WSL instead. Native Windows runtime support is available with managed enterprise deployments.

## Distribution Availability

| Distribution / Variant  | Open Source      | Spice Cloud | Enterprise |
| ----------------------- | ---------------- | ----------- | ---------- |
| Default (Data + AI)     | ✅                | ✅           | ✅          |
| Data-only               | Nightly only     | ✅           | ✅          |
| NAS (SMB + NFS)         | Nightly only     | ❌           | ✅          |
| Metal (macOS)           | ✅                | ✅           | ✅          |
| CUDA (Linux)            | Nightly only     | ✅           | ✅          |
| Allocator variants      | Nightly only     | ✅           | ✅          |
| ODBC connector          | Local build only | ✅           | ✅          |
| Acceleration snapshots  | Local build only | ✅           | ✅          |
| HTTP function servers   | Local build only | ✅           | ✅          |
| WASM user functions     | Local build only | ✅           | ✅          |
| OIDC Token Verification | ❌                | ✅           | ✅          |

## Default Distribution

The default distribution includes the standard data feature set (without AI/ML model inference). Use the [Models distribution](#models-distribution) when you also need LLM/embedding inference.

**Included Features:**

- All standard data connectors (PostgreSQL, MySQL, DuckDB, SQLite, ClickHouse, Elasticsearch, MongoDB, ScyllaDB, Oracle, etc.)
- Embedded data accelerators (Spice Cayenne, DuckDB, SQLite, Turso, PostgreSQL)
- Acceleration snapshots (`snapshots` feature)
- Search capabilities (Vector and BM-25 Full-Text-Search)
- Inline SQL user-defined scalar and table functions
- HTTP-backed function servers (`http-functions` feature)
- WebAssembly user-defined functions (`wasm-functions` feature; Rust source compilation additionally requires `wasm-functions-compile`)
- ODBC connector (`odbc` feature)
- SMB data connector
- Default memory allocator (snmalloc)

**Not included by default:**

- AI/ML model inference, LLMs, embedding models (`models` feature) — see the Models distribution.
- MCP support (`mcp` feature) — implied by `models`.
- NFS data connector (`nfs` feature) — see the NAS distribution.
- Metal / CUDA GPU acceleration — see the Metal and CUDA distributions.

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

The data distribution is the default Spice distribution; it excludes AI/ML model support, resulting in a smaller binary size and reduced attack surface. Use this when you only need data federation, acceleration, and search capabilities without AI features. As of the current Enterprise build, **Data is the default distribution** \u2014 `make install` (no extra flags) and the `default` Docker tag both produce a Data build.

> **Open Source:** Available in nightly builds only. **[Cloud Platform](https://spice.ai/pricing) & [Enterprise](https://docs.spice.ai/docs/enterprise):** Production-ready data distribution available.

**Included Features:**

- All data connectors (including Elasticsearch, MongoDB, ScyllaDB, Oracle, ODBC, etc.)
- All data accelerators (Cayenne, DuckDB, SQLite, Turso, PostgreSQL)
- Acceleration snapshots
- HTTP-backed function servers and WASM user functions
- Default memory allocator (snmalloc)

**Excluded Features:**

- AI/ML model inference
- LLM support
- Embedding models
- MCP support (implied by `models`)

**Docker (Nightly):**

```bash
docker pull ghcr.io/spiceai/spiceai-nightly:latest-data
```

**Local Build:**

```bash
make install-data-only   # equivalent to `make install` in the Enterprise build
```

## Models Distribution

The Models distribution adds AI/ML inference (LLMs, embeddings) on top of the Default (Data) distribution. It also enables MCP support.

**Included Features:**

- All Default (Data) features
- AI/ML model inference (LLMs, embeddings)
- MCP support (`mcp` feature; implied by `models`)

**Local Build:**

```bash
make install-models
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

> **Open Source:** Available in nightly builds only. **[Cloud Platform](https://spice.ai/pricing) & [Enterprise](https://docs.spice.ai/docs/enterprise):** Production-ready CUDA distribution available.

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

## NAS Distribution

The NAS (Network Attached Storage) distribution adds support for SMB and NFS data connectors, enabling federated queries against data stored on network file shares.

> **[Enterprise](https://docs.spice.ai/docs/enterprise):** The NAS distribution is available in nightly builds and with Spice.ai Enterprise.

**Included Features:**

- All default features
- SMB data connector
- NFS data connector

**Local Build:**

```bash
make install-nas
```

## Allocator Variants

Different memory allocators can significantly impact performance depending on workload characteristics.

> **Open Source:** Allocator variants are available in nightly builds only for testing and benchmarking. **[Cloud Platform](https://spice.ai/pricing) & [Enterprise](https://docs.spice.ai/docs/enterprise):** Production-ready allocator variants available.

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

| Platform                      | Default | Data            | NAS             | Metal | CUDA            |
| ----------------------------- | ------- | --------------- | --------------- | ----- | --------------- |
| Linux x86_64                  | ✅       | Nightly         | Nightly         | ❌     | Nightly         |
| Linux aarch64                 | ✅       | Nightly         | Nightly         | ❌     | ❌               |
| macOS aarch64 (Apple Silicon) | ✅       | Nightly         | Nightly         | ✅     | ❌               |
| Windows (WSL)                 | ✅       | Nightly         | Nightly         | ❌     | Nightly         |
| Windows (Native)              | ❌       | Enterprise only | Enterprise only | ❌     | Enterprise only |

> **Note:** Native Windows support for the Spice runtime is available with the [Spice Cloud Platform](https://spice.ai/pricing) and [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise). Open source users on Windows should use Windows Subsystem for Linux (WSL).

## Choosing a Distribution

| Use Case                                | Recommended Distribution     |
| --------------------------------------- | ---------------------------- |
| General purpose with AI capabilities    | Default                      |
| Data federation only, minimal footprint | Data (nightly)               |
| Network attached storage (SMB/NFS)      | NAS                          |
| macOS with GPU acceleration             | Metal                        |
| Linux with NVIDIA GPU                   | CUDA (nightly)               |
| Memory allocation benchmarking          | Allocator variants (nightly) |

## Additional Connectors

Some connectors require additional dependencies and are available with the [Spice Cloud Platform](https://spice.ai/pricing) and [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise):

- **ODBC** - Connect to any ODBC-compatible data source

These can be built locally for development and testing:

```bash
make install-odbc
```

## Enterprise Features

The following features are available with the [Spice Cloud Platform](https://spice.ai/pricing) and [Spice.ai Enterprise](https://docs.spice.ai/docs/enterprise). Open source builds can enable some of these features locally with the listed Cargo feature flags, but they are not included in the default open source distribution.

- **Acceleration Snapshots** - Bootstrap accelerated datasets from durable snapshot storage for fast cold starts, recovery after ephemeral storage loss, and controlled rollback to a previous acceleration state. Local open source builds can enable this with `snapshots`.
- **Globally Persisted HTTP Rate-Control State** - Persist request-rate state and expiring active instance heartbeats per origin from a background worker so multiple Spice instances or clusters coordinate a shared request budget. Local open source builds can enable this with `rate-control`.
- **Function Servers** - Run HTTP-backed user-defined scalar and table functions from `functions:` declarations. The default open source distribution supports inline SQL functions only; local builds can enable HTTP-backed function servers with `http-functions`.
- **WASM Functions** - Run sandboxed WebAssembly table functions from `functions:` declarations using Arrow IPC batches as the data ABI. Local open source builds can enable precompiled modules with `wasm-functions`; compiling Rust sources to WASM at startup additionally requires `wasm-functions-compile`.
- **OIDC Token Verification** - Validate identity tokens from enterprise providers (Okta, Azure AD, Auth0, Google, etc.) for secure access to Spice runtime endpoints.
- **Native Windows Runtime Support** - Run `spiced` natively on Windows in managed enterprise deployments. Open source users on Windows should use WSL.

### Linux arm64 Notes

- **FP16 (FEAT_FP16)** is required because the `gemm` matrix multiplication library (used by the Candle ML framework) contains half-precision ARM inline assembly that requires the `fullfp16` CPU feature. This is supported on AWS Graviton2+, Ampere Altra, Apple M-series (via Linux VM), and most ARMv8.2-A+ processors.
- **lld** is required as the linker because the spiced debug binary is large enough to exceed GNU ld's ±128 MiB branch range for `R_AARCH64_CALL26` relocations. lld automatically inserts range extension thunks.
- Install prerequisites on Ubuntu/Debian: `sudo apt-get install -y clang lld`

### Linux x86_64 Notes

- Release builds target AVX2+ for optimized SIMD performance, covering Intel Haswell (2013+) and AMD Excavator (2015+) processors, including all current AWS x86_64 instance families (C6/C7/C8).

## Building Custom Distributions

You can build custom distributions with specific feature combinations:

```bash
# Build with specific features
SPICED_CUSTOM_FEATURES="duckdb,postgres,sqlite,models" make build-runtime

# Build with HTTP-backed function servers
SPICED_NON_DEFAULT_FEATURES="http-functions" make install

# Build with precompiled WASM user functions
SPICED_NON_DEFAULT_FEATURES="wasm-functions" make install

# Build with Rust source-to-WASM compilation for user functions
SPICED_NON_DEFAULT_FEATURES="wasm-functions-compile" make install

# Build with acceleration snapshots
SPICED_NON_DEFAULT_FEATURES="snapshots" make install

# Build with globally persisted HTTP rate-control state
SPICED_NON_DEFAULT_FEATURES="rate-control" make install

# Build with non-default features added to defaults
SPICED_NON_DEFAULT_FEATURES="odbc" make install
```

See the [Makefile](../Makefile) for all available build targets and options.
