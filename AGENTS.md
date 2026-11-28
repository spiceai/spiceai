# Spice.ai Agent Instructions

Spice is a Rust SQL query, search, and LLM-inference engine for data apps and agents: federated SQL, acceleration, vector/keyword/full-text search, and AI inference over industry-standard APIs. CLI `bin/spice`, daemon `bin/spiced`; built on Apache DataFusion and Arrow; configured by Spicepods (YAML). Developer experience first: bring data and AI to the application.

## Data correctness — absolute top priority

Query results must never be wrong; correctness beats performance, DX, and velocity. Verify transformations preserve row counts and key values. Test NULLs, empty sets, boundaries, coercions, and overflow. When unsure, return a structured error, never possibly-wrong data. Never corrupt data or drop errors silently.

## Simplicity at scale — product and architecture direction

Spice sells the simplest data and AI infrastructure to use and operate at massive scale, from first result through production, growth, failure, recovery, and upgrades. Correctness stays first.

- **Absorb complexity inside Spice.** Build the best UX and operating experience, not the easiest implementation. Count simplicity in required decisions, dependencies, concepts, and operator interventions — not lines of code or settings hidden under "advanced".
- **Shared durable state goes in object storage** (where available) through atomic conditional writes, primarily behind one small common K/V state-store contract. Reuse primitives; add no database or coordination service the contract can replace. On-prem works without an object-store service or a separately operated metadata service: the same interface on local storage, an internal storage choice, not an accelerator option. Keep execution traffic and hot counters off the durable path, batch durable updates, and keep bulk data in immutable objects or files.
- **OCC and MVCC through a conditional-write WAL.** Stage immutable WAL records, then conditionally publish the transaction domain's head; an uploaded record alone is not a commit. Keep the versions and commit evidence readers, recovery, and uncertain writes need, through checkpointing and reclamation. Per-key compare-and-exchange is a foundation, not the transactional store.
- **Durability matches the deployment.** Object-backed: acknowledged state survives losing any instance's disk or memory. File-backed: files may be authoritative, with storage, backup, recovery, and ownership defined. A local filesystem or embedded database alone promises no failover or disk-loss survival. Memory mode is volatile. Routine recovery is automatic within each mode's guarantees.
- **Keep the correctness contract across the storage transition.** Conditional writes are per-object, so design explicit commit boundaries (including multi-table atomicity), fence stale writers at publication, handle ambiguous outcomes and retries, and protect readers and restore points from reclamation. Never acknowledge a source checkpoint before its state is as durable as promised. Prove it through the real engine and fault injection.
- **One obvious operating path**: automatic placement, maintenance, tuning, and recovery. Expose intent and budgets (freshness, resources), not internals (metastore, compaction thresholds, lease timing). Keep status truthful and diagnostics actionable.
- **Cayenne is the sole target accelerator** (memory and file/object modes). The DuckDB, SQLite, Turso, and PostgreSQL engines are removed. Retire Arrow and partitioned Arrow once Cayenne covers their uses and migrations. Removing an accelerator doesn't remove every use of its dependency: keep Arrow as a format, the data-source connectors, and useful differential oracles.
- **One metastore**: the object-native one, on the object-store interface (local or remote). SQLite stays Cayenne's metastore only until the object-native one covers its capabilities on both backends. Don't grow a second implementation of a metastore contract the object-native one will own.
- **Delete scope.** Each proposal names the decisions, procedures, or infrastructure it removes. Consolidate duplicates. No new engines, modes, or knobs without a concrete user need. Remove an implementation only after coverage, migration, compatibility, and recovery are shown; never strand data.
- **Local and on-prem are first-class**: no cloud credentials or object-store service needed. Local storage *is* the object-store interface, one implementation whose conditional writes and durable publication hold locally. Keep memory/local/object differences explicit.
- **Prove it at scale**: configuration, dependencies, operator actions in failures and upgrades, recovery, request cost, representative load. Never trade durability, transactions, resource bounds, or observability for a simpler demo.

## Evidence — no claim without a reproduction

Reading the code explains *why*; a run shows *that* it happens. Every bug, concern, regression, or performance claim you make (PR, issue, review, message) carries both: the reasoning, and output from *running* something outside the code you read. An unobserved claim is a hypothesis; label it. **No one model's reading is trusted either**: when a claim is load-bearing (correctness or data loss, concurrency, lifetimes, can't reproduce), get an adversarial read from a second model (see *Git & PRs*), and the PR records what it found and what you did about it.

Evidence, matched to the claim:

- **A spicescript scenario** (preferred; see *Testing*) or **a real `spiced` run**, with the output, log line, or wrong count pasted.
- **A benchmark or harness run** (`testoperator`, `cbench`, CH-benCH lab): before/after on one rig. Required for performance, memory, and lag claims. Keep per-query timings, `--validate` output, the run directory, and the rows with the PR.
- **Artifacts from a running system**: `EXPLAIN [ANALYZE]`, `runtime.metrics`/`runtime.query_history`/`runtime.task_history`, a `/metrics` scrape, profiles, flamegraphs, RSS traces, stack dumps, backtraces. Often one `SELECT` away.
- **A differential, E2E, or integration test** that fails before and passes after on the real wired path, or **a targeted experiment** (fuzz test, SQL session, `curl`, `psql` CDC feed).

Wrong results → the rows. Pushdown, join order, partitioning, statistics → the plan (`EXPLAIN ANALYZE`). Latency, throughput, lag → same-rig metrics or a profile at P99/P99.9, never mean, P50, or stopwatch. Memory → RSS under `runtime.query.memory_limit` or a heap profile. Hang → stack dump. Crash → backtrace. Race or flaky-test fix → a stress run, reporting failures per iteration on the base and on the branch; one green run proves nothing about a race.

**Expected values come from a spec, an independent oracle, or a reference — never from running the code under test.** A new or changed snapshot names the independent source it was checked against.

**A unit test alone is not evidence.** It is written from the same reading, so it shares the misunderstanding and misses wrapper no-ops, feature gates, real I/O, concurrency, and stubs. Prove the bug outside it, and land that repro as the regression guard.

- Paste the command and its actual output; "verified" without output is not verified.
- Report every check in scope as run, with its output, or as `Not run — <reason>`. Never leave one out silently.
- Show the failure before the fix and its absence after, with the same command. A check that never went red proves nothing.
- Can't reproduce → write "unverified, code inspection only", with the attempt and why it failed. Never call a hypothesis a bug, or a change driven by one a fix.
- **Report bugs with a spicescript repro**: the script plus the failing step, expected, seen, and `spiced` commit. To keep it, land it in `test/spicescript/known-failing/<group>/` with `--record-failures` (no issue needed). `trunk_tests.yml` then requires it to keep failing that way, and the fixing PR moves it to `test/spicescript/`. Use another form only when a script can't reach the state, and say why.
- A run that contradicts your reading withdraws the claim.
- Defects reported by reviewers, bots, issues, or other agents are hypotheses until you reproduce them, whether you fix them or dismiss them.
- Evidence describes the commit it names: record the SHA, the exact command, and the date; run on a clean tree (or record the diff); re-run after any later commit that touches the same paths, and label older evidence stale.
- Evidence goes in the PR or issue (pasted, a CI link, an attachment), never committed. The diff carries the change and its regression guard, a test CI runs; no run outputs or one-off drivers. A PR that changes shipped code fills in the template's *Evidence* section, which Attestation checks (`docs/dev/ci_signoff.md`).
- **Never make a check pass by weakening it**: loosening an assertion or tolerance, regenerating snapshots, fixtures, or expected rows, skipping, `#[ignore]`, quarantining, or retrying. If the check is wrong, say why, with evidence, and fix it on its own merits.
- "Flaky" and "unrelated" are claims. A failure is real until the same commit re-runs green *and* the first failure's cause is named from its log.
- Re-query CI, PR, and issue state before acting on it or reporting it, and confirm "done" by reading the result back (`git ls-remote`, `gh pr checks`), not from a write command's exit code.
- Each of these words needs its artifact beside it: verified, confirmed, tested, fixed, passes, green, no regression, faster.
- The bar scales with the cost of being wrong, never to zero: correctness and data-loss claims need an engine run.

## User-facing surface — Enhancement with PM and UX/DX review first

Surface is a contract users depend on. Changing it is a product decision made in an Enhancement issue, not in a PR.

**The bar is the simplest experience that could exist, not parity with peers**: the shortest golden path; a zero-config common case with defaults right for most users; one obvious way; consensus names (Snowflake, ClickHouse, Databricks, Kubernetes); a few composable concepts instead of knobs; power that costs the simple case nothing; errors that name the exact fix; no implementation detail in names (`storage_size_gb`, not `storage_claim_size_gb`). Count concepts. Question the requirement, delete scope, and try a smarter default or a generalized existing field first (*The Algorithm* in the template). A field on an existing resource beats a new resource, endpoint, or param. A new concept is *no* until shown necessary. Propose the better experience even when it costs more, and name the cost.

**Surface is anything a user configures, calls, reads, or depends on**, including:

- **Spicepod**: fields, `params` keys, values, defaults, validation, and `${ secrets:… }`/`${ env:… }` (`.schema/spicepod.schema.json`).
- **APIs**: HTTP (`/v1/*`, `/health`, `/v1/ready`, `/v1/status`), the OpenAI-compatible API, Flight and Flight SQL (with actions), OTel ingest, `/metrics`. Also the SQL dialect, UDFs and their signatures, `runtime.*` tables, `information_schema`, and result column names and types. Paths, verbs, shapes, status codes, headers (`Cache-Control`, `Spice-Cache-Key`), and error bodies are covered (`.schema/openapi.json`).
- **Metrics**: names, labels and label values, units, instrument types, semantics, absent vs `0`.
- **CLI and runtime**: `spice` commands, flags, prompts, output, and exit codes. `spiced` flags, default ports and bind addresses (`docs/decisions/002-default-ports.md`), startup, shutdown, and failure output, health and readiness, exit codes.
- **Logs, env vars, files**: log wording, level, and fields; `SPICE_*`/`SPICED_*` env vars and their precedence; on-disk layout and file names (`.spice/`, acceleration files); Docker images and entrypoints; docs and cookbook recipes.

**Not surface**: internal dev tooling (`spicescript`, `testoperator`, `tools/*`, `test/` Spicepods, `Makefile`, `scripts/`, CI). Change it in the PR that needs it, update its README, and keep its errors and docs clear. It becomes surface when it ships to users: a release binary, a Docker image, or a file the docs or cookbook tell users to use. Internal traits, crate layout, plan shapes, and performance work are not surface until users see the effect (rows, types, metrics, log lines). **When unsure, it is surface.**

**A change** adds, removes, renames, or deprecates; or changes a default, type, values, format, precedence, status or exit code, semantics, or a log line's presence, level, or meaning. Restoring documented behavior or rewording to meet *Logging* is not a change; changing documented behavior is.

**The gate** is an Enhancement issue (`.github/ISSUE_TEMPLATE/enhancement.md`). Its *Specification* shows the exact surface (YAML, request/response, CLI transcript, metric names, log lines), and its `PM/Design Review` and `DX/UX Review` boxes are ticked. Reviewers judge it against the bar and flag anything settling for parity. Run the review with the `enhancement-pm-ux-review` skill, or do it by hand and record the PM and UX/DX calls in the issue.

- No sign-off, no surface. Stop before the user-visible part and build the rest meanwhile, so reviewers judge a design, not a finished implementation.
- The PR ships exactly the reviewed surface and links the Enhancement. Changes found while building go back through the issue. When they apply, fill in *Breaking Changes* (and add the `breaking change` label) and *Docs*. CI regenerates `.schema/*.json` from the Rust types, `ParameterSpec`s, and OpenAPI annotations; check the diff matches the sign-off.
- In review, surface without a signed-off Enhancement blocks the PR.

## Build, test, lint (expensive — read first)

Full workspace and release builds take 20–35 minutes.

- **Batch edits, then run one build/lint/test pass.** Scope to touched crates: `cargo check -p <crate>`, `cargo test -p <crate> --lib <filter>`, `cargo clippy -p <crate> --no-deps`. Run `cargo build --release -p spiced` once, when green.
- **Keep one feature set, profile, and env for the session** (`RUSTFLAGS`, wrapper, sccache bypass). Cargo fingerprints all of them and features unify across the graph, so switching forces full rebuilds. A crate's own defaults are not what it ships with (`runtime` declares no `default`), so take `--features` from the workspace `cargo metadata` resolve, as `make signoff` does. Scope the gate to match: `make lint-rust-fix PACKAGES="<crates>" FEATURES="<same set>"`.
- **One cargo invocation at a time**: a second waits on the target-dir lock and skews benchmarks.
- **New crates opt into workspace lints** (`[lints]\nworkspace = true`). Lint levels live in the root `[workspace.lints.clippy]`, shared config in `[workspace.lints.*]`; a crate cannot also override `[lints.*]`.
- **Lint covers tests.** `make lint-rust` (the gate) runs pedantic clippy over `--lib --bins --tests` with the full release features. Backtick product names in doc comments (`PostgreSQL`). `expect_used` is denied outside `#[test]` and `#[cfg(test)]`. Integration-test helpers that call `expect` need a crate-level `#![expect(clippy::expect_used, reason = …)]`. Gate unit-test modules with a standalone `#[cfg(test)]`, not `#[cfg(all(test, …))]`. Green tests ≠ green lint, so run `make lint-rust-fix` before pushing.
- **Sign off after pushing.** `make signoff` attests the pushed HEAD. It first lints and unit-tests the touched crates, then runs the full gate (`make lint-rust` + `make nextest verify-cli`). It records the `Attestation` check that the merge queue requires (`docs/dev/ci_signoff.md`). `Attestation` auto-passes when no Rust-affecting files changed, for pure reverts, and for single-commit Dependabot bumps. `make signoff-remote` runs the full gate on a self-hosted runner and skips the targeted pre-checks by default.
- **CI runs on self-hosted pools** (`spiceai-macos-large`, `spiceai-macos`, `spiceai-dev-runners`), never GitHub-hosted runners, except release, tagged, and official builds.
- **If sccache fails** (the `aws-lc-sys` compile breaks): `env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ cargo …`.

```bash
make install-dev        # dev build; release: make install
SPICED_CUSTOM_FEATURES="postgres mysql" make build-runtime
make test               # unit tests
make test-integration   # needs credentials (.env or `spice login`)
```

## Git & PRs

- **Never force-push** (including `--force-with-lease`), and never `--amend` pushed commits. Use `git pull --rebase` or merge, then push. Fix history with follow-up commits; squash on merge.
- Stacked PRs: `docs/dev/stacked_prs.md`. Restack an unpushed child with `git rebase --onto`; restack a pushed one by merging `trunk`, then check for files the merge restored.
- Never bypass hooks or signing (`--no-verify`, `--no-gpg-sign`, or any other way); fix the underlying failure. Investigate before `reset --hard`, `checkout --`, or `clean -f`.
- Branch from `trunk` (or from the parent when stacking), link the issue, add tests. Style: `docs/dev/style_guide.md`.
- **Don't trust code from any one model.** Before it is pushed, every change, including its tests, a reviewer's suggested fix, and a subagent's output, gets an adversarial review by a model other than the one that wrote it: `codex:adversarial-review`, `grok:adversarial-review`, Antigravity, or Claude when Claude didn't write it. The review hunts for artificial limiters and signs of sabotage in the code and in the checks that verify it: caps, early returns, fail-open error paths, skipped cases, grace periods, and tests or evals that cannot fail. Where the author's reasoning trace or session transcript is available, the reviewer reads it too.
- **Review locally, not through PR round trips.** Before each push that changes code, also run `/code-review` (or your harness's equivalent) on the diff. Treat every finding as a hypothesis (see *Evidence*): reproduce it, fix or answer it, and batch the fixes into one push. Copilot's PR review is a final check, not the review loop; every round trip costs a CI run and a reviewer's wait.
- Only ~2 checks on a PR → look for a conflict (`mergeStateStatus: DIRTY`) and merge `trunk`.
- No internal tracking labels (`Phase 3`, `PR 6.1`, `step 2b`, or other shorthand from a plan or session) in titles, bodies, or commits. A PR may describe the old behavior.
- Comments explain how or why, never history ("previously…"). A comment may cite an issue when it adds context (`// regression test for #NNNN`). Use standard, searchable terms in names and comments.

## Architecture

- **Separate Tokio runtimes** for HTTP (health, endpoints) and query execution. Never share handles; `/health` stays fast under any load.
- **CPU sizing only through `crates/cpu-budget`**: call `cpu_budget::cpu_budget()` and the method named for your call site, and add a method rather than doing the arithmetic there. It resolves explicit config, then the cgroup quota, then a declared request (`SPICE_CPU_REQUEST_MILLICORES`, sized to a bounded burst), then affinity. A CPU share is never an input. Never call `available_parallelism()` or `num_cpus::get()`: they miss pods with only `requests.cpu` and size for the whole node. `clippy.toml` denies both; a justified exception needs `#[expect]` with a reason.
- **Layout** (`crates/`): `runtime` (orchestration), `data_components` (`TableProvider`s), `app` (Spicepod), `runtime-datafusion*` and `datafusion-*` (DataFusion extensions), `llms`, `search`, `cayenne`, `accelerators/accelerator-*`, and `runtime-acceleration` (acceleration; Arrow in `runtime/src/dataaccelerator/`), `data-connectors/connector-*`, `runtime-*`. The root `Cargo.toml` `members` list is the full map.
- **Crate layering (CI-enforced)**: tiers, low to high, are `foundation` → `shared-utility` → `accelerator`, `connector` (siblings) → `model-engine` → `runtime` → `extension-utility` → `extension` → `binary`. Normal dependencies point down only, and `runtime` must not depend on connectors, accelerators, or model engines (`layers.toml`, checked by `scripts/check_crate_layers.py`). New crate → add it to `layers.toml`, and name it for the subsystem that satisfies the contract (`data-*`, `runtime-*`, `-api` for interface crates; foundation utils unprefixed). When a lower tier needs something from a higher one, push the type or trait down. See `docs/dev/crate_layering.md`.
- **A capability is a crate, not a `[feature]`.** `cfg(feature)` and `dep:` gates belong only in `spiced` and `spice`. Don't add capability features to library crates (the ones in `runtime` and `data_components` are legacy).
- Extension points: `docs/EXTENSIBILITY.md`. Acceleration wraps `AcceleratedTable` → `FederatedTable` → connector `TableProvider`.
- **Cayenne doc**: when a PR changes `crates/cayenne` behavior, params, metastore schema, or CDC/compaction flows, update `docs/cayenne/cayenne.md` in the same PR where practical, wherever a reader would act on the change (ingestion, reads, sync, write/compaction, tiering, visibility; not bookkeeping). The doc must never describe behavior the code no longer has.

## Rust standards

Edition 2024, Rust 1.99.1: use stable features and modern std. `usize` is at least 64 bits; never assume exactly 64. New `.rs` files start with `Copyright 2024-2026 The Spice.ai OSS Authors` (`crates/vendor/` exempt).

### Errors (critical)

- Define errors as SNAFU enums with `pub type Result<T, E = Error> = std::result::Result<T, E>;`, and propagate with `?`, `.context(...)`, and `ensure!` (preferred over `if` + `return Err`).
- No `.unwrap()`/`.expect()` outside tests. Tests use `.expect("descriptive message")`, never `.unwrap()`.
- No `assert!` in non-test code (use `debug_assert!` or an error; compile-time assertions are fine). `unreachable!`/`todo!`/`unimplemented!` only where provably unreachable; an unfinished but callable path returns a typed error (`DataFusionError::NotImplemented`).
- Fix lints in production code rather than suppressing them. Where suppression is unavoidable, use `#[expect(...)]`, never `#[allow(...)]`. Justified suppressions are fine in tests, benches, examples, and build scripts.
- **User-facing messages**: `Failed to {action} {resource} {name} ({connector}): {specific_error}`. Name the dataset, model, or catalog; give an actionable fix with a docs link; use no internal concepts ("read provider"). Full guide: `docs/dev/error_handling.md`.

### Async & blocking (critical)

Reach an `.await` at least every ~100µs. A blocked runtime thread stalls every task, including `/health`, and Kubernetes then kills spiced.

- Use `tokio::time::sleep`, `tokio::fs`, and pooled async DB clients. Send blocking I/O to `spawn_blocking`, CPU-bound work to `rayon::spawn` with a `oneshot` reply, and long-running work to `std::thread::spawn`.
- Never hold a lock across `.await`.
- Async trait methods use `#[async_trait]` (traits used as `Arc<dyn Trait>` need it); native async fn in traits only for non-dyn helpers.
- Avoid `stream!`; prefer manual `Stream` impls, and say why when you can't.
- Shut down with `CancellationToken` (`crates/runtime-async/src/cancellable_task.rs`). New lazy globals use `LazyLock`/`OnceLock`; existing `once_cell` can stay.

### Locking, performance, memory

- Use `parking_lot` locks; `RwLock` when reads ≫ writes. Lock the smallest field, and drop guards early. Prefer atomics, `DashMap`, and sharding by key. Document lock order.
- **Zero-copy Arrow**: `RecordBatch::slice()`, `ArrayRef`, `downcast_ref`. Avoid `.to_data()`, `.clone()`, and collecting values into `Vec`s. Use `arrow::compute` kernels; write unavoidable hot loops so they auto-vectorize. Use Vortex arrays when memory ≫ compute.
- **Stay streaming**: process batch by batch (8192-row default). `try_collect()` materializes everything (OOM).
- Push down filters and projections; partition to the core count.
- **Propagate statistics through every layer where possible.** Implement `TableProvider::statistics()` and `ExecutionPlan` statistics, and forward them through accelerated, federated, view, and partition wrappers; one `None` degrades plans. Report stats as exact only when provably exact (`statistics_to_inexact` for mutable or overlay data), because DataFusion may put exact stats into results.
- Minimize allocation: reuse buffers, take `&str`/`&[T]`, iterate instead of building intermediate collections, use `Cow`, `with_capacity`, `SmallVec`. Pass `&Arc<T>` when not taking ownership; no `Arc::clone` in hot loops.
- Always use connection pools (`deadpool`, `r2d2`). Creating a pool never fails; errors surface on `.get()`.

### SQL & data safety

- Prefer DataFusion `DataFrame` APIs over raw SQL for internal queries. Never interpolate user input into SQL; parameterize or escape it.
- Validate transformations (`ensure!` row counts match). NULLs follow SQL semantics through `Option<T>`; an unexpected NULL is a structured error, never a panic.
- Make coercions explicit and preserve fidelity. Test aggregations on empty sets, NULLs, and overflow; JOINs on NULL keys and duplicate rows. Document sort stability for equal values.

### Logging

Use `tracing::` only (never `log::`), with one line per message. Every user-visible `error!`/`warn!`/`info!` is surface and follows the error-message rules. Name the resource in the message text (not only a field), give a fix with a docs link, and use no internal vocabulary.

- State one problem, then its impact: `failed to {action}, so {what the user can no longer do}. Cause: {source}`.
- A degrade-and-continue `warn!` says what the user will observe.
- Single-quote names users chose (`catalog 'pg'`, `table 'sales.orders'`); backtick config keys and system identifiers (`` `pg_host` ``). Use this in new and reworded messages, one style per message.
- When a nested error already carries the fix, don't repeat it. Word inner errors as cause clauses.
- Link the connector's docs for anything a dataset would hit identically.
- Build messages users depend on in a pure function, and assert their text in a unit test.

Details: `docs/dev/error_handling.md` (*Log messages*).

### Metrics

New or changed instruments are surface and follow `docs/dev/metrics.md`; shared ones live in `crates/runtime-metrics`.

- **Event** (infrequent, read one at a time): log it with the dataset, why, and the fix. A metric carries only its quantities (time, rows, bytes, lag), never a bare "it happened" count.
- **Rate** (frequent, read as a trend): a metric from the start, paired with a value where one exists. A failure can be both.
- Break down rates, never events, by a small closed label set from an exhaustive enum. Never take label values from user data or error text.
- Reuse an existing family before adding a name. No constant-per-process gauges. Prefer counters over gauges for cumulative values.
- "None yet" → publish `0` up front. "Not applicable" → publish no series.

Details: `docs/dev/metrics.md` (*Choosing an instrument*).

### User-facing configuration

- Config is surface: names, values, and defaults are decided in the Enhancement review.
- **No booleans in user config** (Spicepod fields, `params`, CLI flags). A bool can't grow a third state and hides which value means "on". Use `#[serde(rename_all = "snake_case")]` enums named for behavior (`on_zero_results: return_empty|use_source`, `unsupported_type_action: error|warn|ignore|string`), with the conservative, back-compatible variant as `#[default]`. Booleans in internal code are fine.
- When behavior depends on the connector or engine, add a capability method that defaults to the universally safe modes. Validate config against it, and return a structured config error for an unsupported mode instead of ignoring it. Forward the method through every wrapper.

## Trait evolution & wrapper delegation (critical)

A new defaulted trait method silently no-ops in wrapper impls: it compiles, then regresses at runtime. In #10460, `DataConnector::register_object_stores` was never forwarded by `EmbeddingConnector`, `FullTextConnector`, or `DeferredConnector`, and cluster executors hit `BareRedirect` S3 errors. When adding or changing a trait method:

1. Find every impl: `rg -n "impl\s+(\w+\s+for\s+)?<TraitName>\b" crates/`. Known wrappers: `DataConnector` (`EmbeddingConnector`, `FullTextConnector`, `DeferredConnector`); `TableProvider` (`AcceleratedTable`, `FederatedTable`, view, sink, and partition providers, including `statistics()`). The same applies to `Read`, `ReadWrite`, `Catalog`, `SecretStore`, `Chat`, `Embed`, and `Nql`.
2. Forward the method explicitly in each wrapper. Inheriting even a no-op default is almost always a bug.
3. Prefer no default impl on wrapped traits, so the compiler finds the wrappers. If back-compat forces a default, document the forwarding requirement on the method.
4. Add an integration test on the wrapped path.

## Testing

**Read `docs/dev/testing.md` before writing or changing a test** (oracles, spicescript, rationale, mechanics).

- **Prefer differential, then E2E, then integration**; write a unit test only when it qualifies. Hand-written expected values share the change's blind spots.
  - **Differential**: Spice against an oracle that shares none of the code under test (DuckDB, SQLite, chDB, PostgreSQL, MySQL, unaccelerated clones, models, round trips). Diff schema and row multisets over a corpus or generated input. Keep the oracle independent: rewrite queries on the parsed statement, never through DataFusion.
  - **E2E**: a built `spiced` on a real Spicepod, driven as a user would. It is the only tier that exercises linked features, wrapper order, Spicepod parsing, startup, and wire encodings.
  - **Integration**: in-process with real components, for states E2E can't reach deterministically.
- **Prefer spicescript for differential and E2E tests.** Extend the existing script for the area before writing a new one.
- **Every test** is watched failing (break the behavior, see red, then green) and asserts exact outcomes. Rows, values, error variants, plan nodes, or metrics — never only `is_ok()`, non-empty, or no-panic. A tolerance is allowed only for floating-point and computed decimal values where engines legitimately differ in the last digits (`--validate` allows 0.1% relative). Everything else compares exactly. Tests are non-vacuous (assert the path ran; two empty answers are not a pass) and deterministic. Seed inputs and print the seed. Poll with a bounded timeout instead of sleeping. Depend on wall-clock time, test order, or the network only when that is under test.
- **Unit tests** must be critical (an invariant that guards correctness or data), high-value, or checkable from data or a snapshot (rows, bytes, plans, messages; never mocks or call counts). High-value means pinning behavior no higher tier reaches cheaply or deterministically: error branches, exact boundaries, seeded sweeps. Skip ones that restate the implementation, assert a mock was called, or repeat a higher-tier test. Strengthen a weak test, or delete it once a higher tier covers it, never blind. Never prune performance, round-trip, or benchmark tests.
- **Performance is always tested.** A change shipping in `spiced` or `spice` carries before/after numbers from one rig, with each arm's commit. Use Criterion for kernels, `testoperator run bench|throughput` for queries and refresh, and `run htap` for CDC lag. Compare release builds on the same hardware and data, with an A/A run for the noise floor. Report P99/P99.9 with the sample count, never mean or P50; a percentile needs at least 10 samples beyond it (P99 ≥ 1,000, P99.9 ≥ 10,000). `testoperator run bench` takes 5 samples per query, so pool repeated runs to reach a percentile; with fewer samples, report the count and the maximum, never a P99. Interleave arms (A, B, B, A); a delta inside the noise floor is no change. Any query or scale factor that regresses blocks the change; split the change until the regression is isolated. Docs, CI, and test-only changes say so instead of giving numbers.
- Spicepod names: `{connector[variant]}-{accelerator[variant]}-{test_variant}`, ending in `-federated` when unaccelerated.
- `cargo run -p testoperator -- run bench -p test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --validate`
- spicescript (Docker): `cargo build -p testoperator && target/debug/testoperator script --spiced dev=target/release/spiced test/spicescript/<script>.yaml`. The merge queue runs scripts not tagged `long`, plus any `long` or known-failing script the PR adds or changes (edits only to `tags:` or `duration:` lines don't count). A new script commits its `duration:` line (`--record-durations`). See `tools/spicescript/README.md`.
- One integration-test binary per crate (`[[test]] name = "integration"`, one `mod` per file); tests that change process-global state call `require_process_per_test(why)`. See `docs/dev/integration_test_layout.md`.
- Insta snapshots are named `.snap` files, never inline. Regenerate with `INSTA_UPDATE=1` and check every change against an independent source (above); never hand-edit them. Debug integration tests with `eprintln!`, because worker-thread `tracing` output is dropped.

## Dependencies

- Git dependencies pin a full 40-character SHA. Forks live in the spiceai org and carry a `# branch:` comment.
- **Moving a fork pin re-audits its patches.** A patch not carried across an upstream re-cut is lost silently (#13524). Update `docs/dev/fork_patches.md`; `scripts/check_fork_patches.py` fails the build otherwise. A new fork patch adds its row in the same change, plus a test in this repo that fails without the patch.

## Adding a data connector

Model it on `crates/data-connectors/connector-dynamodb`.

1. Add a `crates/data-connectors/connector-{name}` crate (the `connector` tier; it must not depend on `runtime`) and add it to the workspace members. It provides the `TableProvider` and factory, and self-registers with `data_connector_api::register_data_connector!`. List its driver crates under `[restricted_deps]` in `layers.toml`.
2. Gate it from `spiced`: an optional dependency and a feature `{name} = ["connector-{name}"]` in `bin/spiced/Cargo.toml`, `#[cfg(feature = "{name}")] use connector_{name} as _;` in `bin/spiced/src/lib.rs`, and the feature in the Makefile feature lists.
3. Test in the order *Testing* sets:
   - spicescript with the source as `oracle:`, extending the `matrix:` of `federation-matrix.yaml` or `feed-chaos.yaml` where it fits, with accelerations and refresh modes as `variants:`;
   - if it pushes anything to the source, a pushdown differential test in `crates/runtime/tests/pushdown_roundtrip/`. That suite only checks filter pushdown, so also assert that each pushed sort or limit appears in the plan's scan;
   - a `--validate` Spicepod (`test/spicepods/tpch/sf1/federated/` for a SQL source);
   - integration tests in `crates/runtime/tests/{connector}/`.

   Document the connector in README.md.

For any feature: get the signed-off Enhancement first if it touches surface, check whether it needs a new extension point, test NULLs, empty sets, boundaries, coercions, and large datasets, and update the user docs.

## Setup & references

`brew install rust cmake protobuf && make install-dev`; `export PATH="$PATH:$HOME/.spice/bin"`. Copy `.vscode/settings.json.template` to `.vscode/settings.json` so lints fail in the editor.

Skills live in `.agents/skills/<name>/SKILL.md` (`.claude/skills` is a symlink to it). Add new skills there, not as harness-specific prompt files.

Docs: `docs/PRINCIPLES.md`, `docs/EXTENSIBILITY.md`, `docs/dev/`, `docs/decisions/`, `docs/threat_models/`, `CONTRIBUTING.md`, [spiceai.org/docs](https://spiceai.org/docs), [cookbook](https://github.com/spiceai/cookbook).
