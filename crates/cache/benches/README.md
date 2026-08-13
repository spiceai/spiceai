# Cache Throughput Benchmarks

This benchmark suite tests the performance of different cache implementations with various configurations.

## What's Being Tested

### Cache Implementations

- **SimpleCache**: Pingora-based simple cache (legacy, baseline)
- **LruCache**: the results cache, run against both of its engines

### Engine / policy combinations (LruCache only)

Benchmarked as named pairs rather than a cartesian product:

- **moka_lru**: Moka with Least Recently Used eviction
- **moka_tinylfu**: Moka with frequency-based admission
- **pingora_lru**: Pingora-LRU, sharded. Requires `--features pingora`

There is no `pingora_tinylfu`: Pingora has no TinyLFU admission and builds an
LRU instead, so that arm would duplicate `pingora_lru`.

Without `--features pingora` the Pingora arm is not generated. With the feature
off, requesting the engine falls back to Moka, so including it anyway would
publish Moka numbers labelled `pingora`.

### Hash Algorithms (4 variants)

- **siphash**: Rust's default hasher (cryptographically secure, slower)
- **ahash**: Fast non-cryptographic hash (good balance)
- **xxh3**: xxHash3 64-bit (very fast)
- **xxh64**: xxHash 64-bit

## Workload Patterns

- **concurrent_get**: Read-heavy workload (100% reads, pre-populated cache)
- **concurrent_put**: Write-heavy workload (100% writes)
- **concurrent_mixed_80_20**: Realistic workload (80% reads, 20% writes)
- **invalidate_for_table**: Invalidating one table's entries — what an accelerated
  refresh triggers every cycle, once per dataset

### invalidate_for_table

Entries are spread over 8 tables and one is invalidated, so the share removed
stays fixed while the cache is sized at 50 / 1k / 10k / 50k entries. Cache size
is the axis because that is where the engines differ: Moka matches through its
own index, while Pingora has no closure-based API and walks every key.

Timing covers `invalidate_for_table` and `checkpoint()`. Moka's
`invalidate_entries_if` registers a predicate and applies it during later
maintenance, so timing the call alone would compare registering a predicate
against completing a scan. For Pingora it only settles the weight.

This benchmark is single-threaded and uses one hash algorithm: raw-key
operations bypass the hasher, so it cannot affect the measurement.

The engines cross over, so a single cache size does not tell you which is
faster. Apple M-series, `--sample-size 10`:

| entries | moka_lru | pingora_lru | ratio |
| ------: | -------: | ----------: | ----: |
|      50 |  30.4 µs |     17.7 µs | 0.58x |
|   1,000 |   178 µs |      237 µs | 1.33x |
|  10,000 |  1.13 ms |     2.38 ms | 2.10x |
|  50,000 |  5.39 ms |    18.44 ms | 3.42x |

Below roughly a thousand entries Pingora's scan is cheaper than Moka's predicate
registration and maintenance pass. Above it the walk dominates: 5x the entries
costs Moka 4.8x and Pingora 7.8x. Treat a shift in the crossover as the signal,
not any single row.

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

# Just the invalidation benchmark (needs the feature to include Pingora)
cargo bench -p cache --features pingora --bench cache_throughput -- invalidate_for_table

# Specific engine / policy pair
cargo bench -p cache --bench cache_throughput -- moka_lru
cargo bench -p cache --bench cache_throughput -- moka_tinylfu
cargo bench -p cache --features pingora --bench cache_throughput -- pingora_lru

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
lru_cache_concurrent_get/moka_lru_xxh3_8threads
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
