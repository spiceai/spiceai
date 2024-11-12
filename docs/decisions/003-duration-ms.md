# DR: Code Pattern For Obtaining Milliseconds-Based Duration

## Context

Spice.ai OSS uses the following common pattern to obtain and record telemetry metrics in floating-point milliseconds: 

```rust
duration.as_secs_f64() * 1000.0
```

```rust
pub fn track_query_execution_duration(duration: Duration, dimensions: &[KeyValue]) {
    QUERY_EXECUTION_DURATION.record(duration.as_secs_f64() * 1000.0, dimensions);
}
```

There is a newly introduced unstable Rust API `duration.as_millis_f64()`, which could simplify this pattern by directly providing millisecond precision as a floating-point number without extra multiplication: [Add as_millis_{f64,f32} helper functions for Duration](https://github.com/rust-lang/libs-team/issues/349)

```rust
#[unstable(feature = "duration_millis_float", issue = "122451")]
#[must_use]
#[inline]
#[rustc_const_unstable(feature = "duration_consts_float", issue = "72440")]
pub const fn as_millis_f64(&self) -> f64 {
    (self.secs as f64) * (MILLIS_PER_SEC as f64)
        + (self.nanos.0 as f64) / (NANOS_PER_MILLI as f64)
}
```

Switching to `as_millis_f64` could improve readability and reduce the need for multiplication.

## Assumptions

1. The chosen approach will enhance readability without affecting precision.
1. Changing the approach does not affect Spice’s stability.

## Options

1. Keep existing pattern: `as_secs_f64() * 1000.0`.
1. Switch to `as_millis_f64`.
1. Switch to `duration.as_millis() as f64` pattern.
1. Copying the underlying implementation of `as_millis_f64`

## First-Principles

- **Align to industry standards**
- **Developer experience first**

## Decision

Continue using existing pattern `as_secs_f64() * 1000.0` until `as_millis_f64` is stabilized.

**Why**:

- `as_millis_f64` alternative might affect Spice’s stability as the method is not fully supported and has not passed [stabilization review](https://std-dev-guide.rust-lang.org/development/stabilization.html).
- `duration.as_millis() as f64` alternative loses the sub-milliseconds precision
- Existing `.as_secs_f64() * 1000.0` is a widely recognized pattern with [extensive usage in Rust projects](https://github.com/search?q=%22.as_secs_f64%28%29+*+1000.0%22+language%3ARust+&type=code)
- As the existing pattern is used in multiple crates, copying the underlying implementation of `as_millis_f64` into a custom helper function will result in code duplication and maintenance overhead.

**Why not**:

- `as_millis_f64` has better readability and computationally more efficient.

## Consequences

- The Spice runtime retains an additional multiplication operation when obtaining millisecond-based duration.
- Adoption of `as_millis_f64` is deferred until its stabilized ([Tracking Issue for Duration::as_millis_{f64,f32}](https://github.com/rust-lang/rust/issues/122451)).

## Links

- [Add as_millis_{f64,f32} helper functions for Duration](https://github.com/rust-lang/libs-team/issues/349)
- [Tracking Issue for Duration::as_millis_{f64,f32}](https://github.com/rust-lang/rust/issues/122451)