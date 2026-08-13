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
- **hot_read_write**: Full cache whose reads mostly hit, 95/5 and 80/20 read/write
  at 16 and 32 threads

### hot_read_write

The other `LruCache` benchmarks read a 100,000 key space holding at most 5,000
entries, so roughly 95% of their reads miss. That matters for the engine
comparison: a Pingora miss is one metadata lookup, while a hit removes the entry
and re-admits it, because `pingora-lru` has no `peek_value`. Reads that miss
never reach the path where the engines differ.

This benchmark reads a key space equal to the working set, so reads hit, and
sizes capacity at 80% of it, so writes evict and the cache runs full.

Parameters:

- **Threads**: 16 and 32, straddling the 16 metadata shards Pingora uses. At 16
  shard collisions are incidental; at 32 they are structural.
- **Mix**: 95/5 and 80/20 read/write. Writes rewrite a key already in the
  working set, as a refreshed result does, and still evict because the cache is
  full.

Uses one hash algorithm: raw-key operations bypass the hasher, so it cannot
affect the measurement.

Neither engine wins outright. Apple M-series, `--sample-size 10`, total time for
threads x 10,000 operations (lower is faster):

| case | moka_lru | moka_tinylfu | pingora_lru |
| ---: | ---: | ---: | ---: |
| 95/5, 16 threads | 34.6 ms | 32.8 ms | 41.9 ms |
| 95/5, 32 threads | 64.8 ms | 62.1 ms | 107.2 ms |
| 80/20, 16 threads | 80.6 ms | 82.2 ms | 50.2 ms |
| 80/20, 32 threads | 231.2 ms | 186.5 ms | 133.9 ms |

Read-heavy favours Moka, and the gap widens with threads. Doubling threads
doubles the work, so 2.0x is the break-even: Moka scales at 1.87x and Pingora at
2.56x. Pingora expresses a read as a remove plus re-admit, so reads contend on
the 16 shards in a way Moka's do not.

Write-heavy reverses it, with Pingora 38-42% faster at both thread counts.
TinyLFU also earns its keep here — it costs a little on read-heavy load and
saves 19% over LRU at 80/20 and 32 threads.

So the write fraction, not the engine, decides which is faster. Numbers are from
a laptop at reduced sample size, where 32 threads oversubscribes the cores:
adequate for the direction and the crossover, not for precise ratios.

## Thread Counts

Tests scalability with: 1, 4, 8, 16 threads. `hot_read_write` uses 16 and 32.

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

# Just the contention benchmark (needs the feature to include Pingora)
cargo bench -p cache --features pingora --bench cache_throughput -- hot_read_write

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

`hot_read_write` sizes itself separately, in entries rather than weight:

- `HOT_WORKING_SET`: 20,000 keys, all pre-populated and all read from
- `HOT_CACHE_WEIGHT`: capacity for 80% of them, so the cache runs full
- TTL: 10 minutes, so nothing expires mid-run

## Output

Results are saved to `target/criterion/` with HTML reports including:

- Performance history over time
- Statistical analysis (mean, median, std dev)
- Regression detection
- Comparison between runs
