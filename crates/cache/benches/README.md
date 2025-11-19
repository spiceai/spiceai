# Cache Throughput Benchmarks

This benchmark suite tests the performance of different cache implementations with various configurations.

## What's Being Tested

### Cache Implementations

- **SimpleCache**: Pingora-based simple cache (legacy, baseline)
- **LruCache**: Pluggable backend architecture supporting both Moka and Pingora

### Backend Engines (LruCache only)

- **Moka**: Stable, built-in TTL, eviction tracking, no race conditions
- **Pingora**: 2-3x faster, sharded architecture (16 shards), manual TTL with rare race condition

### Hash Algorithms

- **siphash**: Rust's default hasher (cryptographically secure, slower)
- **ahash**: Fast non-cryptographic hash (good balance)
- **xxh3**: xxHash3 64-bit (requires `xxhash` feature, very fast)
- **xxh32**: xxHash 32-bit (requires `xxhash` feature)
- **xxh64**: xxHash 64-bit (requires `xxhash` feature)
- **xxh128**: xxHash3 128-bit (requires `xxhash` feature)

### Encoding Variants

- **no_encoding**: Raw Arrow IPC format (no compression)
- **zstd**: Zstd compression level 3 (trades CPU for memory)

Note: Encoding affects RecordBatch caching but not raw key benchmarks. Raw key benchmarks include encoding variants for completeness but encoding has no effect on performance.

## Workload Patterns

- **concurrent_get**: Read-heavy workload (100% reads, pre-populated cache)
- **concurrent_put**: Write-heavy workload (100% writes)
- **concurrent_mixed_80_20**: Realistic workload (80% reads, 20% writes)

## Thread Counts

Tests scalability with: 1, 4, 8, 16, 32 threads

## Running Benchmarks

### All benchmarks (takes ~1-2 hours with all combinations)

```bash
cargo bench -p cache --bench cache_throughput
```

### Specific benchmark groups

```bash
# Just LRU cache benchmarks
cargo bench -p cache --bench cache_throughput -- lru_cache

# Just get operations
cargo bench -p cache --bench cache_throughput -- concurrent_get

# Specific backend
cargo bench -p cache --bench cache_throughput -- moka
cargo bench -p cache --bench cache_throughput -- pingora

# Specific hash algorithm
cargo bench -p cache --bench cache_throughput -- xxh3
cargo bench -p cache --bench cache_throughput -- ahash

# Specific encoding
cargo bench -p cache --bench cache_throughput -- zstd
cargo bench -p cache --bench cache_throughput -- no_encoding

# Specific thread count
cargo bench -p cache --bench cache_throughput -- 8threads
```

### Quick comparison (just a few key configs)

```bash
# Compare moka vs pingora with xxh3 and no encoding at 8 threads
cargo bench -p cache --bench cache_throughput -- 'lru.*xxh3_no_encoding_8threads'
```

### With xxhash feature enabled (recommended for best performance)

```bash
cargo bench -p cache --bench cache_throughput --features xxhash
```

## Understanding Results

Criterion outputs results like:

```
lru_cache_concurrent_get/moka_xxh3_no_encoding_8threads
                        time:   [125.43 ms 126.89 ms 128.52 ms]
                        thrpt:  [623.18 Kelem/s 631.31 Kelem/s 638.52 Kelem/s]
```

- **time**: Total time for 8 threads × 10,000 operations = 80,000 operations
- **thrpt**: Throughput in thousands of elements per second
- Higher throughput = better performance

### What to Look For

1. **Moka vs Pingora**: Expect Pingora to be 2-3x faster on throughput
2. **Hash algorithms**: xxh3 typically fastest, siphash slowest
3. **Thread scaling**: Should see throughput increase with thread count (diminishing returns after ~8-16 threads)
4. **Workload patterns**: Gets should be faster than puts, mixed should be between them

### Typical Performance Rankings (fastest to slowest)

1. Pingora + xxh3 + no_encoding
2. Pingora + ahash + no_encoding
3. Moka + xxh3 + no_encoding
4. Moka + ahash + no_encoding
5. Pingora + siphash + no_encoding
6. Moka + siphash + no_encoding

Zstd variants will have identical performance on raw key benchmarks (encoding only affects RecordBatch operations).

## Configuration Details

- `CACHE_WEIGHT`: 100,000 (cache capacity)
- `KEY_SPACE`: 100,000 (number of possible keys)
- `OPERATIONS_PER_THREAD`: 10,000
- Cache pre-population: 5,000 entries (for get/mixed benchmarks)
- TTL: 60 seconds
- Value size: 32 random alphanumeric characters per BenchValue

## Output

Results are saved to `target/criterion/` with HTML reports including:

- Performance history over time
- Statistical analysis (mean, median, std dev)
- Regression detection
- Comparison between runs
