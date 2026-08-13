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

The other `LruCache` benchmarks read a 100,000 key space holding at most 5,000
entries, so roughly 95% of their reads miss. That matters for the engine
comparison: a Pingora miss is one metadata lookup, while a hit removes the entry
and re-admits it, because `pingora-lru` has no `peek_value`. Reads that miss
never reach the path where the engines differ.

Reads and writes are not independent knobs here, because they are not
independent in the runtime: a miss executes the query and caches the result, so
the write rate *is* the miss rate. Setting a read/write mix separately from a
hit rate describes states a results cache cannot be in. This benchmark reads,
and refills on a miss, so hit rate is the only knob and the write rate follows
from it.

Parameters:

- **Hit rate**: 50%, 95% and 99%. Under uniform access an LRU holding `C` of a
  `W`-key working set hits about `C/W`, so capacity is fixed at 16,000 entries
  and the working set is sized against it. 50% is the thrashing regime the older
  benchmarks sit in; 99% is where a results cache worth enabling runs.
- **Threads**: 16 and 32, straddling the 16 metadata shards Pingora uses. At 16
  shard collisions are incidental; at 32 they are structural.

The cache starts full: the whole working set is inserted, then evicted down to
capacity. One hash algorithm, since raw-key operations bypass the hasher.

Hit rate decides the winner, and it flips between 50% and 95%. Apple M-series,
`--sample-size 10`, total time for threads x 10,000 operations (lower is
faster):

| hit rate | threads | moka_lru | moka_tinylfu | pingora_lru | pingora vs moka_lru |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 50% | 16 | 275.9 ms | 239.6 ms | 74.9 ms | 0.27x |
| 50% | 32 | 628.0 ms | 529.3 ms | 206.7 ms | 0.33x |
| 95% | 16 | 33.6 ms | 31.3 ms | 53.5 ms | 1.59x |
| 95% | 32 | 60.8 ms | 61.1 ms | 141.1 ms | 2.32x |
| 99% | 16 | 38.3 ms | 38.1 ms | 43.7 ms | 1.14x |
| 99% | 32 | 73.3 ms | 73.2 ms | 99.6 ms | 1.36x |

At 50% every other operation is a refill, so the run is dominated by insert and
eviction, and Pingora is 3-3.7x faster. At 95% and 99% the work is nearly all
reads, and Moka leads by 1.14-2.32x because Pingora expresses a read as a remove
plus re-admit.

Pingora scales worse with threads at every hit rate. Doubling threads doubles
the work, so 2.0x is break-even:

| hit rate | moka_lru | pingora_lru |
| ---: | ---: | ---: |
| 50% | 2.28x | 2.76x |
| 95% | 1.81x | 2.64x |
| 99% | 1.91x | 2.28x |

TinyLFU earns its keep where admission control matters: 13% ahead of LRU at 50%,
7% at 95%, and level at 99%, where almost nothing is evicted.

Numbers are from a laptop at reduced sample size, where 32 threads oversubscribes
the cores.

Moka is far noisier than Pingora. A separate run measuring run-to-run spread
over five repetitions found Moka varying up to 30% between repetitions while
Pingora stayed within 1-3%, most likely Moka's background maintenance landing
differently. Treat differences under roughly 15% against Moka as unresolved at
this sample size: the 50% gaps are real, the 99% ones are directional. Pingora's
reproducibility is itself a result, and arguably matters more for tail latency
than the median either way.

The `C/W` sizing was checked separately against measured hit rates. Achieved
rates land within half a point of target (3.0 / 50.3 / 80.0 / 95.0 / 99.0) and
the resident set matches capacity exactly, for both engines. The crossover also
holds across a 30x capacity range - 1,000, 5,000 and 30,000 entries all put it
between 80% and 95% - so the single capacity used here does not bias it.

This benchmark asserts its hit rates through sizing rather than measuring them.
That was verified once by hand, as above, but it is an assumption in the code.

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
