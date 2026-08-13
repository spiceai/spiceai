# Cache Benchmark Implementation Summary

## What Was Added

Enhanced the cache throughput benchmarks to comprehensively test all combinations of:

### 1. **Engine / policy pairs** (3, or 2 without `--features pingora`)

- `moka_lru`: Moka with Least Recently Used eviction
- `moka_tinylfu`: Moka with frequency-based admission
- `pingora_lru`: Pingora-LRU, sharded. Requires `--features pingora`

Named pairs rather than a cartesian product: Pingora has no TinyLFU admission
and builds an LRU instead, so a `pingora_tinylfu` arm would duplicate
`pingora_lru`.

### 2. **Hash Algorithms** (4 variants)

- `siphash`: Rust default (cryptographically secure, baseline)
- `ahash`: Fast non-cryptographic hash
- `xxh3`: xxHash3 64-bit ⚡ **FASTEST**
- `xxh64`: xxHash 64-bit

### 3. **Workload Patterns** (4 types)

- `concurrent_get`: 100% reads (pre-populated cache, ~3% hit rate)
- `concurrent_put`: 100% writes
- `concurrent_mixed_80_20`: 80% reads, 20% writes
- `hot_read_write`: read-through on a full cache, swept across 50/95/99% hit
  rates at 16 and 32 threads

### 4. **Thread Counts** (4 levels)

- 1, 4, 8, 16 threads

## Total Benchmark Combinations

Counts below are with `--features pingora`; without it the Pingora arm is not
generated and each LruCache figure drops by a third.

**LruCache throughput benchmarks:**

- 3 engine/policy pairs × 4 hash algos × 4 thread counts × 3 workloads = **144 configurations**

**`hot_read_write`:**

- 3 engine/policy pairs × 3 hit rates × 2 thread counts = **18 configurations**

**SimpleCache benchmarks:**

- 1 hash algo × 4 thread counts × 3 workloads = **12 configurations**

**Total: 174 configurations** (120 without `--features pingora`)

## Code Changes

### Modified Files

1. **`crates/cache/benches/cache_throughput.rs`**

   - Added `get_hash_builder` import from cache crate
   - Added `HashingAlgorithm` and `CachingPolicy` imports from spicepod
   - Created `all_hash_algorithms()` helper returning 4 hash algorithm variants
   - Created `all_engine_policy_pairs()` helper returning the engine/policy pairs
   - Updated `bench_lru_cache_concurrent_get()` to iterate over all combinations
   - Updated `bench_lru_cache_concurrent_put()` to iterate over all combinations
   - Updated `bench_lru_cache_concurrent_mixed()` to iterate over all combinations
   - Added `bench_lru_cache_hot_read_write()`, swept across hit rates
   - Benchmark naming: `{pair}_{hash}_{threads}threads` (e.g. `moka_lru_xxh3_8threads`),
     and `{pair}_{hit_rate}_{threads}threads` for `hot_read_write`

2. **`crates/cache/benches/README.md`** (NEW)

   - Comprehensive documentation of benchmark suite
   - Explanation of what each variant tests
   - Running instructions with filtering examples
   - Performance interpretation guide
   - Expected performance rankings

3. **`crates/cache/benches/SUMMARY.md`** (NEW - this file)
   - Overview of implementation
   - Summary of all combinations tested

## Results

Measured engine comparisons, including the Moka/Pingora crossover, live in
[README.md](README.md) under "Engine Comparison Results".

## Running the Benchmarks

### Quick Start

```bash
# Run all benchmarks
cargo bench -p cache --bench cache_throughput

# Run just LRU cache benchmarks
cargo bench -p cache --bench cache_throughput -- lru_cache

# Compare specific configurations (8 threads only)
cargo bench -p cache --bench cache_throughput -- '8threads$'

# Quick test (10 samples instead of default 100)
cargo bench -p cache --bench cache_throughput -- --sample-size 10
```

### Filtering by Component

```bash
# Specific engine / policy pair
cargo bench -p cache -- moka_lru
cargo bench -p cache -- moka_tinylfu
cargo bench -p cache --features pingora -- pingora_lru

# Specific hash algorithm
cargo bench -p cache -- xxh3
cargo bench -p cache -- ahash

# Specific thread count
cargo bench -p cache -- 8threads
```

## Performance Expectations

### Caching Policy Comparison

- **LRU**: Standard eviction policy, predictable performance
- **TinyLFU**: Better hit rates for frequency-skewed workloads

### Hash Algorithm Impact

- **xxh3/xxh64**: Fastest options, ~2x faster than siphash
- **ahash**: ~1.5x faster than siphash
- **siphash**: Slowest but cryptographically secure

### Thread Scaling

- Linear scaling up to ~8 threads
- Diminishing returns at 16+ threads

## Next Steps

1. **Run full benchmark suite** to collect baseline data
2. **Compare with previous implementation** (if benchmark history exists)
3. **Identify optimal configurations** for different workloads:
   - High-throughput: xxh3 hash algorithm
   - Security-sensitive: siphash
4. **Update documentation** with recommended configurations based on use case

## Notes

- Hash algorithm choice has minimal impact at high thread counts (bottleneck shifts to lock contention)
- Results may vary based on CPU architecture (arm64 vs amd64)
