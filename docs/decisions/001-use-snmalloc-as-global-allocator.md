# DR: Use snmalloc As Global Allocator

## Context

Spice performs federated, accelerated queries on large datasets, requiring frequent memory allocation and deallocation, leading to fragmentation.

Rust uses the system allocator by default, which may struggle in some Operating Systems under load, causing unbounded memory growth. Switching to a different allocator can help maintain a stable memory footprint in Spice.

## Assumptions

1. The chosen allocator resolves the issue of unbounded memory growth when Spice is under load.
2. The chosen allocator is as fast or faster than the system allocator.
3. Changing the allocator will not affect Spice’s stability.

## Options

1. [jemalloc/jemalloc](https://github.com/jemalloc/jemalloc)
2. [microsoft/mimalloc](https://github.com/microsoft/mimalloc)
3. [microsoft/snmalloc](https://github.com/Microsoft/snmalloc)

## First-Principles

- **Align to industry standards**

## Decision

Spice will use `snmalloc` as the default memory allocator on non-Windows systems.

**Why**:

- With Spice running in our production environment on Debian Bookworm, switching the global allocator to `snmalloc` fixes the issue of unbounded memory growth under the same workload that system allocator failed on.
- Benchmarks and resource monitoring show that using `snmalloc` in Spice with Arrow acceleration increases query throughput slightly and reduces memory usage by 10%-20% compared to `jemalloc` and `mimalloc`.
- DataFusion, Spice's core dependency, [recommends using `snmalloc-rs`](https://datafusion.apache.org/user-guide/example-usage.html) for optimized performance.
- `snmalloc` is actively maintained and supported by Microsoft.

**Why not**:

The [Rust RFC-1974](https://rust-lang.github.io/rfcs/1974-global-allocators.html#jemalloc) outlined reasons for not using `jemalloc` as the default allocator, which are also relevant to `snmalloc`.
To address this, Spice ensures compatibility with supported operating systems via end-to-end CI testing. Users needing alternative memory allocators can build custom versions from trunk with minimal changes by swapping out the `#[global_allocator]`.


## Consequences

- `snmalloc` is set as the `global_allocator` in `spiced` `main.rs` via the [snmalloc-rs](https://crates.io/crates/snmalloc-rs) crate.
- Instabilities may arise as explained in **Why not**, the decision should be revisited as Spice evolves and new discoveries are made in this area.
