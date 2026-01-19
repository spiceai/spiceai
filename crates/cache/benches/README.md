# Cache Throughput Benchmarks

This benchmark suite tests the performance of different cache implementations with various configurations.

## What's Being Tested

### Cache Implementations

- **SimpleCache**: Pingora-based simple cache (legacy, baseline)
- **LruCache**: Moka-based LRU cache with configurable caching policies

### Caching Policies (LruCache only)

- **LRU**: Standard Least Recently Used eviction policy
- **TinyLFU**: Frequency-based admission policy with better hit rates for some workloads

### Hash Algorithms (4 variants)

- **siphash**: Rust's default hasher (cryptographically secure, slower)
- **ahash**: Fast non-cryptographic hash (good balance)
- **xxh3**: xxHash3 64-bit (very fast)
- **xxh64**: xxHash 64-bit

## Workload Patterns

- **concurrent_get**: Read-heavy workload (100% reads, pre-populated cache)
- **concurrent_put**: Write-heavy workload (100% writes)
- **concurrent_mixed_80_20**: Realistic workload (80% reads, 20% writes)

## Thread Counts

Tests scalability with: 1, 4, 8, 16 threads

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

# Specific caching policy
cargo bench -p cache --bench cache_throughput -- lru
cargo bench -p cache --bench cache_throughput -- tinylfu

# Specific hash algorithm
cargo bench -p cache --bench cache_throughput -- xxh3
cargo bench -p cache --bench cache_throughput -- ahash

# Specific thread count
cargo bench -p cache --bench cache_throughput -- 8threads
```

### Quick comparison (just a few key configs)

```bash
# Compare LRU vs TinyLFU with xxh3 at 8 threads
cargo bench -p cache --bench cache_throughput -- 'xxh3_8threads'
```

### With xxhash feature enabled (recommended for best performance)

```bash
cargo bench -p cache --bench cache_throughput --features xxhash
```

## Understanding Results

Criterion outputs results like:

```text
lru_cache_concurrent_get/lru_xxh3_8threads
                        time:   [125.43 ms 126.89 ms 128.52 ms]
                        thrpt:  [623.18 Kelem/s 631.31 Kelem/s 638.52 Kelem/s]
```

- **time**: Total time for 8 threads × 10,000 operations = 80,000 operations
- **thrpt**: Throughput in thousands of elements per second
- Higher throughput = better performance

### What to Look For

1. **LRU vs TinyLFU**: Compare eviction policy performance under different workloads
2. **Hash algorithms**: xxh3 typically fastest, siphash slowest
3. **Thread scaling**: Should see throughput increase with thread count (diminishing returns after ~8-16 threads)
4. **Workload patterns**: Gets should be faster than puts, mixed should be between them

### Typical Performance Rankings (fastest to slowest)

1. xxh3 (fastest hash algorithm)
2. xxh64
3. ahash
4. siphash (slowest, but cryptographically secure)

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
