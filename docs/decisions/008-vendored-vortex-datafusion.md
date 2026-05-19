# DR-008: Vendor `vortex-datafusion` for Cayenne

## Status

Accepted

## Context

Cayenne is Spice's accelerated table engine, built on top of [Vortex](https://github.com/spiceai/vortex) files for storage and Apache DataFusion for query execution. Cayenne requires a DataFusion `FileFormat` / `FileSource` adapter for Vortex files. Upstream [`vortex-datafusion`](https://github.com/spiceai/vortex/tree/main/vortex-datafusion) provides a baseline adapter, but it does not currently expose the extension points and correctness guarantees Cayenne needs:

1. **Position-delete awareness**: Cayenne maintains external delete metadata. Without per-file access plans and statistics adjustment hooks, DataFusion can optimize from stale exact file statistics and return wrong results.
2. **DataFusion fork compatibility**: Spice tracks a pinned DataFusion fork (see [`Cargo.toml`](../../Cargo.toml)). Upstream `vortex-datafusion`'s API direction and version targets do not match Spice's DataFusion surface.
3. **Cayenne-specific scan performance**: Vendored options for projection pushdown, scan concurrency, footer/segment caching, and dynamic filter handling are required to meet Cayenne's accelerated query targets (TPC-H/TPC-DS).
4. **Spice-wide hardening**: The crate must comply with Spice's lint, error-handling, async-blocking, and data-correctness rules (`docs/dev/style_guide.md`, `docs/dev/error_handling.md`, [.github/copilot-instructions.md](../../.github/copilot-instructions.md)).

Doing this work upstream first would block Cayenne on external review cycles and on a DataFusion API direction that does not currently match Spice's fork. Carrying patches against an external crate would create the same long-term maintenance cost as vendoring, without the freedom to land Spice-specific behavior.

Related decisions:

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md) — same vendoring pattern for an external framework Spice depends on.

## Design Principles

* **Data correctness is non-negotiable**: Cayenne queries must return exact results even when files have external position deletes. Statistics, predicate pushdown, and projection handling must preserve SQL semantics.
* **Developer experience first**: Cayenne users should not need to configure or wire deletion handling, segment caching, or footer caching. The vendored adapter exposes Spice-shaped defaults.
* **Spice-aligned engineering rules**: Vendored code follows Spice's error handling, logging, async, and lint policies even when upstream code does not.
* **Minimize divergence**: Keep the crate close enough to upstream that future upstreaming or migration is feasible.
* **Extensibility**: Cayenne integrates through public traits (`VortexAccessPlanProvider`, `ExpressionConvertor`) rather than ad-hoc patches.
* **Industry standards**: Continue to use DataFusion's `FileFormat` / `FileSource` / `FileOpener` extension points and Vortex's public scan/session APIs.

## Decision

Vendor `vortex-datafusion` into the Spice workspace as the [`crates/vortex`](../../crates/vortex) crate (package name `vortex-datafusion`). Cayenne and the Spice runtime depend on this vendored crate instead of upstream `vortex-datafusion`. The vendored crate remains tracked against the Spice Vortex fork and the Spice DataFusion fork.

The vendored crate carries the following Spice-specific behavior on top of the upstream baseline:

### 1. Cayenne deletion and access-plan extension points

* New trait `VortexAccessPlanProvider` and value type `VortexAccessPlan` in [`crates/vortex/src/persistent/access_plan.rs`](../../crates/vortex/src/persistent/access_plan.rs).
* The `VortexFormat` accepts an access-plan provider via `with_access_plan_provider`. During scan setup, per-`PartitionedFile` access plans are attached and applied to Vortex's `ScanBuilder` (`Selection`), so Cayenne's external position deletes are enforced at the Vortex scan layer.
* The same provider can adjust the `Statistics` derived from the Vortex file footer (`adjust_statistics`). This is a **data-correctness** requirement: DataFusion is allowed to answer some aggregates exactly from `Statistics`, so deletion-aware statistics correction must be available at the file-format level.

### 2. Dynamic filter, `IN`-list, and pushdown handling

* `ExpressionConvertor` is a public, fallible trait used to convert DataFusion `PhysicalExpr`s to Vortex `Expression`s. `make_vortex_predicate` returns errors instead of silently dropping predicates that DataFusion already considers pushed.
* `split_vortex_pushdown_conjuncts` (in [`crates/vortex/src/persistent/opener.rs`](../../crates/vortex/src/persistent/opener.rs)) unwraps `DynamicFilterPhysicalExpr`s into their current expression and splits the conjunction so Vortex receives the pushable fragments (e.g. `IN`/range), while unsupported dynamic pieces (e.g. hash-table probes) are skipped at open time rather than failing the whole scan.
* Decimal-to-floating-point pushdown is rejected as unsafe: it can change comparison semantics, so we degrade to post-scan filtering instead of risking incorrect results.

### 3. Schema evolution and projection rewriting

* The Vortex opener uses DataFusion's `PhysicalExprAdapter` to rewrite filters and projections from the unified table schema to each file's physical schema, and simplifies expressions against the file's actual columns. This supports Cayenne's evolving schemas and partition-column substitution.
* `ProcessedProjection` splits a projection into a scan-time Vortex expression and a leftover DataFusion projection so that projection pushdown is optional and post-scan projection is always sound.

### 4. Caching and reader sharing

* `SharedSegmentCache` in [`crates/vortex/src/persistent/segment_cache.rs`](../../crates/vortex/src/persistent/segment_cache.rs) is a path-keyed bounded segment cache (sized via `VortexTableOptions.segment_cache_size_bytes`). Vortex segment ids are file-local, so the cache key must include both file path and segment id.
* `CachedVortexMetadata` integrates with DataFusion's `FileMetadataCache` to avoid reparsing Vortex footers across schema inference, statistics inference, and scans.
* Layout readers and natural split ranges are shared across partitions within a single scan via `DashMap`, so each layout is opened once per file per query.

### 5. Pruning, ranges, and limits

* `PrunableStream` re-checks DataFusion's `FilePruner` once dynamic filters resolve, ending the stream early when a file becomes prunable. Pruning errors surface as stream errors rather than silent skips.
* File byte ranges are aligned to Vortex natural split ranges before being passed to `ScanBuilder.with_row_range`, so range scans never read across split boundaries.

### 6. Configuration surface (`VortexTableOptions`)

The vendored crate exposes Spice-shaped options on the DataFusion `config_namespace!`:

* `footer_initial_read_size_bytes` — bounded footer prefetch.
* `target_file_size_mb` — size-based file splitting for non-partitioned writes.
* `projection_pushdown` — opt-in pushdown of projection expressions.
* `scan_concurrency` — `auto`/`off`/explicit intra-file Vortex scan concurrency, derived from `DataFusion` target partitions and planned file count in `auto` mode.
* `segment_cache_size_bytes` — capacity of the shared segment cache.

These are surfaced through Cayenne's accelerator configuration (e.g. `cayenne_footer_cache_mb`, `cayenne_segment_cache_mb`).

### 7. Error handling and operational hardening

* No `todo!()` / `unimplemented!()` / `expect()` / `unwrap()` on non-test, reachable paths. Unsupported scalar conversions and unsupported `ScalarValue` variants return typed errors so callers can degrade gracefully (consistent with Spice's data-correctness rule of failing safely).
* Single-line error messages and `tracing` (not `log`) per Spice logging guidelines.
* No `#[allow(...)]` suppressions in production code; uses `#[expect(...)]` with reasons where unavoidable.
* No blocking operations in async paths (`SpawnedTask` for footer/stats inference).

### 8. Test and lint policy

* Named Insta snapshots only (snapshots in `crates/vortex/src/persistent/snapshots/`).
* Test-only `expect(...)`s are scoped via `#[cfg(test)]` attributes on the crate, so production clippy denials (`unwrap_used`, `expect_used`, `clone_on_ref_ptr`, `pedantic`) apply.
* Tests cover: persistent scans, dynamic filter conversion (including `IN`-list pruning expression generation), decimal pushdown safety, access-plan application, file-pruning behavior, and round-trip scalar conversion.

## Consequences

### Positive

* Cayenne queries are data-correct in the presence of position deletes, including for exact-from-statistics aggregate plans.
* Cayenne can land scan-side performance work (dynamic filter pruning, segment cache, footer cache, projection pushdown, scan concurrency) without waiting on upstream API decisions.
* The vendored crate compiles cleanly against Spice's DataFusion fork and Spice's Vortex fork, and complies with Spice's lint, error-handling, async, and snapshot policies.
* Cayenne integrates through stable public traits (`VortexAccessPlanProvider`, `ExpressionConvertor`) instead of internal patches, which keeps the integration boundary explicit.

### Negative / Costs

* Spice now owns a non-trivial DataFusion file-format adapter and must keep it building against an evolving Vortex and an evolving DataFusion fork.
* Divergence from upstream `vortex-datafusion` will accrue over time, which makes future upstreaming (or replacement with an upstream adapter) more expensive.
* Bug fixes and features from upstream `vortex-datafusion` must be manually evaluated and ported.

### Risks and Mitigations

* **Risk**: Internal divergence makes it harder to track upstream Vortex correctness fixes.
  * **Mitigation**: Keep module structure aligned with upstream; prefer trait-based extension over invasive edits; review Vortex/DataFusion bumps explicitly when upgrading.
* **Risk**: A future Vortex/DataFusion upgrade breaks vendored behavior silently.
  * **Mitigation**: Snapshot tests, dynamic-filter and pushdown unit tests, and Cayenne TPC benchmarks (`testoperator run bench`) guard the integration.
* **Risk**: Wrapper/decorator traits added in either DataFusion or Vortex acquire defaulted no-op behavior that silently breaks vendored hooks.
  * **Mitigation**: Follow the trait-evolution checklist in [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md) (the "Trait Evolution & Wrapper Delegation" section) when adding or changing trait methods that wrappers must forward.

## Exit Criteria

The vendored crate should be re-evaluated when **all** of the following hold:

1. Upstream `vortex-datafusion` (or its successor) exposes equivalent extension points for per-file access plans and footer-statistics adjustment.
2. Upstream's DataFusion target matches the Spice DataFusion fork closely enough that Cayenne can adopt the upstream adapter without reintroducing the data-correctness gaps above.
3. Upstream's pushdown, schema-adaptation, and dynamic-filter handling either match Cayenne's behavior or are made pluggable.

When those conditions are met, the vendored crate can be replaced with a thin Cayenne-side adapter over upstream `vortex-datafusion`, and this DR superseded.

## References

* [`crates/vortex`](../../crates/vortex) — vendored crate (package name `vortex-datafusion`).
* PR #10933 — initial vendoring.
* [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md) — Spice engineering rules the vendored crate must follow.
* [`docs/dev/style_guide.md`](../dev/style_guide.md), [`docs/dev/error_handling.md`](../dev/error_handling.md).
