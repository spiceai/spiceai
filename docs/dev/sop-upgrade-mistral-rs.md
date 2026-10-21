# SOP: Upgrading mistral.rs

This document outlines the standard operating procedure for upgrading the `spiceai/mistral.rs` fork to a new upstream version from `EricLBuehler/mistral.rs`.

## Overview

Spice uses a fork of mistral.rs (`spiceai/mistral.rs`) with custom patches. When upgrading to a new upstream version, we need to:
1. Rebase our patches onto the new upstream version
2. Update spiceai/spiceai to use the new fork SHA
3. Handle any API changes in the llms crate

## Prerequisites

- Local clone of `spiceai/mistral.rs`
- Local clone of `spiceai/spiceai`
- Write access to both repositories

## Step 1: Identify Current Spiceai Patches

Before upgrading, identify what patches spiceai maintains on top of upstream.

```bash
cd /path/to/mistral.rs

# Add upstream remote if not already added
git remote add upstream https://github.com/EricLBuehler/mistral.rs.git
git fetch upstream

# Find current spiceai branch base
git log --oneline spiceai -20

# Compare spiceai branch to its upstream base to see our patches
git log --oneline upstream/master..spiceai
```

### Current Spiceai Patches (as of 2026-02-04)

| Patch | Description | File(s) | Status |
|-------|-------------|---------|--------|
| Tool template warning | Warns when tools provided to non-tool-supporting templates | `mistralrs-core/src/pipeline/chat_template.rs` | Maintain |
| NaN panic with debug info | Panics with debug info when NaN detected in sampler | `mistralrs-core/src/sampler.rs` | Maintain |
| SIGTERM handling | Graceful shutdown during HF model downloads | Various | Track separately (#9279) |

## Step 2: Create Upgrade Branch

```bash
cd /path/to/mistral.rs
git fetch upstream
git checkout -b spiceai-upgrade upstream/v{NEW_VERSION}

# Example for v0.8.0
git checkout -b spiceai-upgrade upstream/v0.8.0
```

## Step 3: Cherry-pick or Reapply Patches

For each spiceai patch, either cherry-pick or manually reapply:

```bash
# Option A: Cherry-pick if commits are clean
git cherry-pick <commit-sha>

# Option B: Manually reapply if there are conflicts
# 1. View the original patch
git show <commit-sha>
# 2. Manually apply the changes to the new codebase
# 3. Commit with descriptive message
```

### Patch: Tool Template Warning

Location: `mistralrs-core/src/pipeline/chat_template.rs`

In the `apply` function, after checking `has_tools`, add:
```rust
if self.chat_template.is_some() && has_tools && !self.chat_template_supports_tools() {
    tracing::warn!(
        "Tools were provided but the chat template does not support tools. \
         Tools will be ignored."
    );
}
```

### Patch: NaN Panic with Debug Info

Location: `mistralrs-core/src/sampler.rs`

In the sampling logic where `logits` are processed, add NaN detection:
```rust
if logits.iter().any(|x| x.is_nan()) {
    panic!(
        "NaN detected in logits. Debug info: batch_size={}, vocab_size={}, \
         temperature={:?}, top_p={:?}",
        batch_size, vocab_size, temperature, top_p
    );
}
```

## Step 4: Verify Build

```bash
cd /path/to/mistral.rs
cargo check
cargo test
```

## Step 5: Push and Create PR

```bash
git push origin spiceai-upgrade

# Create PR in spiceai/mistral.rs
gh pr create --title "Upgrade to v{NEW_VERSION}" \
  --body "Rebases spiceai patches onto upstream v{NEW_VERSION}

## Changes from upstream
- [List key upstream changes]

## Spiceai patches maintained
- Tool template warning
- NaN panic with debug info

## Testing
- [ ] cargo check passes
- [ ] cargo test passes"
```

## Step 6: Update spiceai/spiceai

Once the mistral.rs PR is merged (or ready for testing):

```bash
cd /path/to/spiceai

# Get the new SHA
NEW_SHA=$(gh api repos/spiceai/mistral.rs/commits/spiceai-upgrade --jq '.sha')

# Update crates/llms/Cargo.toml
sed -i '' "s/rev = \"[a-f0-9]*\"/rev = \"$NEW_SHA\"/g" crates/llms/Cargo.toml
```

## Step 7: Handle API Changes

Check for breaking API changes in mistral.rs:

```bash
cargo check -p llms --features "local_llm,local_embed" 2>&1 | head -50
```

Common areas that change between versions:
- `MistralRs::new()` / `MistralRsBuilder` API
- `Request` / `RequestMessage` structures
- `Response` / `CompletionResponse` structures
- Model loading configuration
- Token sampling parameters

Update `crates/llms/src/chat/mistral.rs` as needed.

## Step 8: Update Workspace Dependencies

If mistral.rs changes its candle version, update workspace deps in `Cargo.toml`:

```toml
[workspace.dependencies]
candle-core = "X.Y.Z"  # Match mistral.rs version
candle-nn = "X.Y.Z"
candle-transformers = "X.Y.Z"
```

Check if candle patches in `[patch.crates-io]` are still needed:
- If mistral.rs uses crates.io candle: patches may be removable
- If mistral.rs uses git candle: patches may need updating

## Step 9: Test

```bash
# Build check
cargo check -p llms --features "local_llm,local_embed"

# Full build
make build-runtime

# Run local LLM tests if available
cargo test -p llms --features "local_llm"
```

## Step 10: Create PR and Document

Create PR in spiceai/spiceai with:
- Link to spiceai/mistral.rs PR
- List of API changes handled
- Any dependency changes (candle version, etc.)
- Testing performed

## Troubleshooting

### Multiple cudarc versions error

If you see "multiple versions of crate `cudarc`":
- Check what cudarc version mistral.rs uses vs other deps (TEI)
- May need to update `[patch.crates-io]` cudarc entry
- TEI and mistral.rs can use different cudarc versions if they use different candle versions

### Candle API incompatibilities

If candle APIs changed:
- Check candle changelog/migration guide
- Common changes: tensor operations, device handling, dtype conversions

### CUDA build failures

- Ensure cudarc patch is correct version
- Check CUDA feature flags in llms Cargo.toml
- Verify CUDA toolkit version compatibility

## Version History

| Date | Upstream Version | Spiceai SHA | Notes |
|------|------------------|-------------|-------|
| 2026-02-04 | v0.7.0 | `3282ff19ed945c27a979e99295313a3ddd912cfe` | Removed candle fork dependency |
| Previous | v0.6.0 | `cf3749e243ebdd2d9dfa1b895261147ff9ba64e3` | Used spiceai/candle fork |

## Related Documentation

- [spiceai/mistral.rs repository](https://github.com/spiceai/mistral.rs)
- [EricLBuehler/mistral.rs upstream](https://github.com/EricLBuehler/mistral.rs)
- [Issue #8634 - Model dependency upgrades](https://github.com/spiceai/spiceai/issues/8634)
- [Issue #9279 - SIGTERM handling](https://github.com/spiceai/spiceai/issues/9279)
