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

### Hash Algorithms (4 variants)

- **siphash**: Rust's default hasher (cryptographically secure, slower)
- **ahash**: Fast non-cryptographic hash (good balance)
- **xxh3**: xxHash3 64-bit (very fast)
- **xxh64**: xxHash 64-bit

## Workload Patterns

- **concurrent_get**: Read-heavy workload (100% reads, pre-populated cache)
- **concurrent_put**: Write-heavy workload (100% writes)
- **concurrent_mixed_80_20**: Realistic workload (80% reads, 20% writes)
- **hot_read_write**: Read-through on a full cache, swept across 50/95/99% hit
  rates at 16 and 32 threads

### hot_read_write

Reads and writes are not independent knobs, because they are not independent in
the runtime: a miss executes the query and caches the result, so the write rate
is the miss rate. This benchmark reads, and refills on a miss, so hit rate is
the only knob and the write rate follows from it.

Hit rate is set by sizing: capacity is fixed at 16,000 entries and the working
set is `capacity / hit_rate`, since an LRU holding `C` of a `W`-key working set
hits about `C/W` under uniform access. The cache starts full. Threads are 16 and
32, straddling the 16 metadata shards Pingora uses.

## Engine Comparison Results

Apple M-series, `--sample-size 10`. Lower is faster.

### Pure reads (`concurrent_get`)

| threads | moka_lru | moka_tinylfu | pingora_lru | pingora vs moka_lru |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2.70 ms | 2.82 ms | 1.27 ms | 0.47x |
| 4 | 7.62 ms | 7.30 ms | 3.80 ms | 0.50x |
| 8 | 13.12 ms | 13.05 ms | 6.44 ms | 0.49x |
| 16 | 29.91 ms | 29.43 ms | 13.78 ms | 0.46x |

`concurrent_get` reads a 100,000 key space holding ~3,125 entries, so ~97% of
its reads miss. A Pingora miss is one metadata lookup, while a hit removes the
entry and re-admits it, so this measures the path where Pingora is cheapest.

### Read-through across hit rates (`hot_read_write`)

| hit rate | threads | moka_lru | moka_tinylfu | pingora_lru | pingora vs moka_lru |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 50% | 16 | 275.9 ms | 239.6 ms | 74.9 ms | 0.27x |
| 50% | 32 | 628.0 ms | 529.3 ms | 206.7 ms | 0.33x |
| 95% | 16 | 33.6 ms | 31.3 ms | 53.5 ms | 1.59x |
| 95% | 32 | 60.8 ms | 61.1 ms | 141.1 ms | 2.32x |
| 99% | 16 | 38.3 ms | 38.1 ms | 43.7 ms | 1.14x |
| 99% | 32 | 73.3 ms | 73.2 ms | 99.6 ms | 1.36x |

Hit rate decides the winner. At 50% every other operation is a refill, the run
is dominated by insert and eviction, and Pingora is 3-3.7x faster. At 95% and
99% the work is nearly all reads and Moka leads.

Pingora also scales worse with threads. Doubling threads doubles the work, so
2.0x is break-even: Moka runs 1.81-2.28x, Pingora 2.28-2.76x.

### Does cache size matter?

Largely no. A separate pure-read sweep over 1,000 / 5,000 / 30,000 entries put
the crossover between 80% and 95% at every capacity, and Pingora's timings were
within 3% of each other across the whole 30x range.

One exception, outside the range these benchmarks use: at 3% hits with 30,000
entries — a 1,000,000-key working set — Moka's pure-read time jumps 2.4x while
Pingora is unchanged. That corner scales capacity and key space together, so it
does not separate the two causes.

### Precision

Moka is far noisier than Pingora. Over five repetitions Moka varied up to 30%
between runs while Pingora stayed within 1-3%, most likely Moka's background
maintenance landing differently. Treat differences under roughly 15% against
Moka as unresolved at this sample size. Pingora's reproducibility is itself a
result, and arguably matters more for tail latency than the median either way.

The `C/W` sizing is an assumption in the code, checked once by hand: achieved
hit rates land within half a point of target (3.0 / 50.3 / 80.0 / 95.0 / 99.0)
and the resident set matches capacity exactly, for both engines.

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

- `HOT_RESIDENT_ENTRIES`: 16,000, the capacity the cache is built with
- `HOT_HIT_RATES`: 50 / 95 / 99%, each setting the working set to
  `HOT_RESIDENT_ENTRIES` divided by that rate
- TTL: 10 minutes, so nothing expires mid-run

## Output

Results are saved to `target/criterion/` with HTML reports including:

- Performance history over time
- Statistical analysis (mean, median, std dev)
- Regression detection
- Comparison between runs
