# DR: Use snmalloc As Global Allocator

## Context

Spice performs federated and accelerated queries on large datasets, requiring intensive memory allocation and deallocation, which can result in memory fragmentation.

Rust uses system allocator by default which may struggle with this in some Operating Systems, leading to continual memory growth. Switching to a different allocator can help maintain a consistent memory footprint in Spice.

## Assumptions

1. The chosen allocator resolves the issue of continual memory growth in primary operating systems.
2. The chosen allocator is as performant as, if not more so than, the system allocator.
3. Spice's stability will not be impacted by the change of allocator in primary operating systems.

## Options

1. [jemalloc/jemalloc](https://github.com/jemalloc/jemalloc)
2. [microsoft/mimalloc](https://github.com/microsoft/mimalloc)
3. [microsoft/snmalloc](https://github.com/Microsoft/snmalloc)

## Decision

Spice will use snmalloc as the default allocator.

**Why**:

- snamlloc fixes the issue of continual memory growth under the same workload that system allocator couldn't handle. (debian bookworm)
- Benchmarks and resource monitoring indicate that using snmalloc in Spice yields slightly higher query throughput with Arrow acceleration and reduces 10%-20% memory usage comparing to jemalloc and mimalloc.
- DataFusion, Spice's core dependency, mentions using snmalloc-rs as [optimized configuration](https://datafusion.apache.org/user-guide/example-usage.html).
- snmalloc is actively maintained and supported by Microsoft.

**Why not**:

The [Rust RFC-1974](https://rust-lang.github.io/rfcs/1974-global-allocators.html#jemalloc) outlined the reason for not using jemalloc as the default allocator, which also have relevance to snmalloc.
To address this, Spice ensures compatibility with primary operating systems through end-to-end CI testing. Users requiring alternative memory allocators can easily build custom versions from the trunk with minimal changes on `#[global_allocator]`.


## Consequences

- Implement snamalloc as the `global_allocator` in `spiced` `main.rs` via the [snmalloc-rs](https://crates.io/crates/snmalloc-rs) crate.
