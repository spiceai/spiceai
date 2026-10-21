# SOP: Upgrading Text Embeddings Inference (TEI)

This document outlines the standard operating procedure for upgrading the `spiceai/text-embeddings-inference` fork to a new upstream version from `huggingface/text-embeddings-inference`.

## Overview

Spice uses a fork of TEI (`spiceai/text-embeddings-inference`) with custom patches for CUDA compatibility. When upgrading to a new upstream version, we need to:
1. Merge upstream changes into our fork
2. Maintain spiceai patches for auxiliary crates
3. Update spiceai/spiceai to use the new fork SHA
4. Handle any API changes

## ⚠️ Current Limitations (as of 2026-02-04)

### CUDA Auxiliary Crate Incompatibility

**The primary blocker for TEI upgrades is the CUDA auxiliary crates.**

TEI's CUDA support depends on several auxiliary crates:
- `candle-cublaslt` - CUBLASLt GEMM operations
- `candle-rotary` - Rotary position embeddings
- `candle-layer-norm` - Layer normalization
- `candle-index-select-cu` - Index selection

**Problem**: These crates are pinned to specific `cudarc` versions:
- Upstream TEI and spiceai forks use **cudarc 0.12.x** (via EricLBuehler/cudarc)
- candle 0.9.x uses **cudarc 0.19.x**
- These versions have incompatible APIs (`CudaView`, `DevicePtr` traits changed)

**Result**: Cannot upgrade TEI to candle 0.9.x without also updating all auxiliary crates.

### Upstream TEI Status

As of 2026-02-04, **upstream huggingface/text-embeddings-inference still uses candle 0.8**. They have not upgraded to candle 0.9.x, likely due to the same auxiliary crate compatibility issues.

## Prerequisites

- Local clone of `spiceai/text-embeddings-inference`
- Local clone of `spiceai/spiceai`
- Write access to both repositories
- (For CUDA) Local clones of auxiliary crate forks:
  - `spiceai/candle-cublaslt`
  - `spiceai/candle-rotary`
  - `spiceai/candle-layer-norm`

## Spiceai TEI Patches

### Current Patches (as of 2026-02-04)

| Patch | Description | Location | Status |
|-------|-------------|----------|--------|
| Public `download_safetensors` | Exposes function for spiceai embedding downloads | `backends/src/lib.rs` | Maintain |
| cudarc patch | Uses EricLBuehler/cudarc for CUDA compatibility | `Cargo.toml` [patch.crates-io] | Maintain |
| Auxiliary crate forks | Uses spiceai forks of candle-cublaslt, candle-rotary, candle-layer-norm | `Cargo.toml` workspace deps | Maintain |

### Auxiliary Crate Fork Details

| Crate | Spiceai Fork | SHA | cudarc Version |
|-------|--------------|-----|----------------|
| candle-cublaslt | spiceai/candle-cublaslt | `72be72013e9b9ba69a066aa1920b13255d43959b` | 0.17.6 |
| candle-rotary | spiceai/candle-rotary | `8a437f99881bd4803f61c7c16ed4367f7cab76fc` | 0.12.x |
| candle-layer-norm | spiceai/candle-layer-norm | `88b9ffdd7077fce11d3f6cb77bbe7093269631fe` | 0.12.x |

## Step 1: Check Upstream Changes

```bash
cd /path/to/text-embeddings-inference

# Add upstream remote if not already added
git remote add upstream https://github.com/huggingface/text-embeddings-inference.git
git fetch upstream

# Check upstream candle version
curl -s https://raw.githubusercontent.com/huggingface/text-embeddings-inference/main/Cargo.toml | grep "candle"

# Compare branches
git log --oneline upstream/main..origin/spiceai | head -20
```

**Decision point**: If upstream still uses candle 0.8, proceed with merge. If upstream upgraded candle, check auxiliary crate compatibility first.

## Step 2: Create Upgrade Branch

```bash
cd /path/to/text-embeddings-inference
git fetch upstream
git checkout -b spiceai-upgrade origin/spiceai
git merge upstream/main
```

## Step 3: Resolve Merge Conflicts

Common conflict areas:

### Cargo.toml - Workspace Dependencies

Keep spiceai's git dependencies for auxiliary crates:
```toml
[workspace.dependencies]
# Use git deps for candle auxiliary crates - crates.io versions are incompatible with our cudarc patches
candle-nn = { version = "*" }
candle-transformers = { version = "*" }
candle-flash-attn = { version = "*" }
candle-flash-attn-v1 = { git = "https://github.com/huggingface/candle-flash-attn-v1", rev = "..." }
candle-cublaslt = { git = "https://github.com/spiceai/candle-cublaslt", rev = "..." }
candle-rotary = { git = "https://github.com/spiceai/candle-rotary", rev = "..." }
candle-layer-norm = { git = "https://github.com/spiceai/candle-layer-norm", rev = "..." }
cudarc = { git = "https://github.com/EricLBuehler/cudarc", rev = "..." }

[patch.crates-io]
cudarc = { git = "https://github.com/EricLBuehler/cudarc", rev = "..." }
candle = { git = "https://github.com/huggingface/candle", rev = "...", package = "candle-core" }
candle-nn = { git = "https://github.com/huggingface/candle", rev = "...", package = "candle-nn" }
# ... other candle patches
```

### backends/src/lib.rs - download_safetensors

Keep function `pub`:
```rust
// Spiceai: Keep public for embedding downloads
pub async fn download_safetensors(api: Arc<ApiRepo>) -> Result<Vec<PathBuf>, ApiError> {
```

### backends/candle/Cargo.toml - CUDA features

Ensure CUDA feature includes all auxiliary crates:
```toml
[features]
cuda = ["candle/cuda", "candle-nn/cuda", "dep:candle-cublaslt", "dep:candle-layer-norm", "dep:candle-rotary", "dep:candle-index-select-cu"]
```

## Step 4: Verify Build

```bash
cd /path/to/text-embeddings-inference

# CPU build
cargo check -p text-embeddings-backend-candle

# Note: CUDA build requires CUDA toolkit and may fail due to auxiliary crate issues
# cargo check -p text-embeddings-backend-candle --features cuda
```

## Step 5: Push and Create PR

```bash
git push origin spiceai-upgrade

gh pr create --title "Merge upstream TEI changes" \
  --body "Merges latest upstream TEI while maintaining spiceai patches.

## Upstream changes
- [List key changes]

## Spiceai patches maintained
- Public download_safetensors
- cudarc patch for CUDA compatibility
- Auxiliary crate forks

## Testing
- [ ] CPU build passes
- [ ] CUDA build passes (if auxiliary crates compatible)"
```

## Step 6: Update spiceai/spiceai

```bash
cd /path/to/spiceai

# Get new SHA
NEW_SHA=$(gh api repos/spiceai/text-embeddings-inference/commits/spiceai-upgrade --jq '.sha')

# Update all TEI references in crates/llms/Cargo.toml
sed -i '' "s|text-embeddings-inference.git\", optional = true, rev = \"[a-f0-9]*\"|text-embeddings-inference.git\", optional = true, rev = \"$NEW_SHA\"|g" crates/llms/Cargo.toml
sed -i '' "s|text-embeddings-inference.git\", rev = \"[a-f0-9]*\"|text-embeddings-inference.git\", rev = \"$NEW_SHA\"|g" crates/llms/Cargo.toml
```

## Step 7: Verify spiceai Build

```bash
cargo check -p llms --features "local_embed"
```

## Upgrading to Candle 0.9+ (Future Work)

When upstream TEI or we decide to upgrade candle, the following steps are required:

### 1. Update Auxiliary Crates for cudarc 0.19

Each auxiliary crate needs updates for the new cudarc API:

**API Changes in cudarc 0.19:**
- `device_ptr()` now takes `&CudaStream` argument and returns tuple `(CUdeviceptr, SyncOnDrop)`
- `DevicePtr` and `DevicePtrMut` traits changed
- `CudaView` type changes
- `alloc()` return type wrapped differently

**Example fix for candle-cublaslt:**
```rust
// Old (cudarc 0.12/0.17):
let x_ptr = *x.device_ptr() as *const c_void;

// New (cudarc 0.19):
let stream = dev.stream();
let (x_ptr, _sync) = x.device_ptr(&stream);
let x_ptr = x_ptr as *const c_void;
```

### 2. Update Each Fork

For each auxiliary crate fork:
```bash
cd /path/to/candle-cublaslt  # or candle-rotary, candle-layer-norm
git checkout -b cudarc-0.19-upgrade

# Update Cargo.toml
# Change: cudarc = "0.17" -> cudarc = "0.19"

# Fix API incompatibilities in src/lib.rs
# - device_ptr() calls
# - DevicePtr/DevicePtrMut trait usage
# - alloc() calls

cargo check --features cuda
git commit -am "Upgrade to cudarc 0.19"
git push origin cudarc-0.19-upgrade
```

### 3. Update TEI to Use New Auxiliary Crates

```toml
# Cargo.toml
[workspace.dependencies]
candle-cublaslt = { git = "https://github.com/spiceai/candle-cublaslt", rev = "<new-sha>" }
candle-rotary = { git = "https://github.com/spiceai/candle-rotary", rev = "<new-sha>" }
candle-layer-norm = { git = "https://github.com/spiceai/candle-layer-norm", rev = "<new-sha>" }
cudarc = { git = "https://github.com/EricLBuehler/cudarc", rev = "<0.19-compatible-sha>" }

[patch.crates-io]
cudarc = { git = "https://github.com/EricLBuehler/cudarc", rev = "<0.19-compatible-sha>" }
```

### 4. Update candle Patches

Remove or update candle patches to use 0.9.x:
```toml
[patch.crates-io]
# May no longer need patches if using crates.io candle 0.9.x
# candle = { git = "...", rev = "..." }  # Remove if using crates.io
```

## Troubleshooting

### "multiple versions of crate `cudarc`"

**Cause**: Different dependencies use incompatible cudarc versions.

**Solution**: Ensure all CUDA-using crates use the same cudarc version via patches:
```toml
[patch.crates-io]
cudarc = { git = "https://github.com/EricLBuehler/cudarc", rev = "..." }
```

### "method `device_ptr` takes 1 argument but 0 were supplied"

**Cause**: cudarc 0.19 API change.

**Solution**: Update to new API:
```rust
// Add stream parameter
let stream = dev.stream();
let (ptr, _sync) = slice.device_ptr(&stream);
```

### "trait bound `CudaView<'_, T>: DevicePtr<_>` is not satisfied"

**Cause**: cudarc version mismatch between crates.

**Solution**: Verify all crates use same cudarc version. Check Cargo.lock for multiple cudarc entries.

### CPU build works but CUDA fails

**Cause**: Auxiliary crate incompatibility.

**Workaround**: Disable TEI CUDA temporarily:
```toml
# crates/llms/Cargo.toml
[features]
cuda = [
    # "tei_backend/cuda",  # Disabled until auxiliary crates updated
    # "tei_candle/cuda",
    "mistralrs-core/cuda",
    # ...
]
```

## Version History

| Date | Upstream Base | Spiceai SHA | Candle Version | Notes |
|------|---------------|-------------|----------------|-------|
| 2026-02-04 | main (cb9de7a) | `87f9f6fa5f7bdea46f1cf108e7fed97c354a6d89` | 0.8 | Attempted upgrade, reverted due to API issues |
| Current | 6457d4c | `58b44fbbbde55a6ea33c8d0b4b9504942405c481` | 0.8 | Working version in trunk |

## Related Issues

- [#8634 - Model dependency upgrades](https://github.com/spiceai/spiceai/issues/8634)
- [#9275 - Upgrade TEI to candle 0.9.2](https://github.com/spiceai/spiceai/issues/9275) - Blocked on auxiliary crates

## Related Documentation

- [spiceai/text-embeddings-inference](https://github.com/spiceai/text-embeddings-inference)
- [huggingface/text-embeddings-inference](https://github.com/huggingface/text-embeddings-inference)
- [SOP: Upgrading mistral.rs](./sop-upgrade-mistral-rs.md)
