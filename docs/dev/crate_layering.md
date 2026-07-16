# Crate layering

Spice is a large Cargo workspace (~120 member crates). To keep compile times,
ownership, and reasoning tractable, crates are organized into **tiers**: a crate
may depend only on crates in its own tier or a lower one. Upward dependencies are
forbidden and enforced in CI.

This document is the **rules** reference: (1) the tiers and the downward-only
rule, (2) how crates are **named**, (3) the feature/crate and granularity rules,
(4) how the rules are **enforced**, and (5) the **target** layering we are moving
toward and the method for getting there. The ordered, PR-by-PR execution is
tracked separately (a working plan, not part of these rules).

> **Guiding principle: dependencies flow downward only.** If a low-tier crate
> needs something from a higher tier, the shared piece is in the wrong crate —
> push the *type/trait* down, never the dependency up.

---

## The tiers (enforced today)

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  binary        bin/spiced, bin/spice, tools/*                          │  stitch everything
  ├──────────────────────────────────────────────────────────────────────┤
  │  extension     connector-* (~30), spice-cloud, tpc-extension           │  plug-ins that
  │                                                                        │  register with the runtime
  ├──────────────────────────────────────────────────────────────────────┤
  │  runtime       runtime                                                 │  orchestration daemon
  ├──────────────────────────────────────────────────────────────────────┤
  │  domain        data_components, cayenne, llms, search, app, cache,     │  composable libraries
  │                runtime-datafusion, runtime-search, runtime-cluster,    │  the runtime is built from
  │                runtime-acceleration, runtime-auth, workers…            │
  ├──────────────────────────────────────────────────────────────────────┤
  │  foundation    util, arrow_tools, arrow_sql_gen, spicepod, telemetry,  │  near-dependency-free
  │                db_connection_pool, token_provider, flight_client …     │  utilities + primitives
  └──────────────────────────────────────────────────────────────────────┘
         ▲ every arrow points DOWN — no crate depends on a higher tier ▲
```

| Tier | Purpose | May depend on |
|------|---------|---------------|
| **foundation** | Leaf utilities, wire formats, config parsing, primitives. Ideally reusable outside Spice. Little or no internal dependency. | foundation |
| **domain** | The libraries the daemon is assembled from — data-plane building blocks (`data_components`), the accelerator (`cayenne`), inference (`llms`), search, the `runtime-*` feature crates. | foundation, domain |
| **runtime** | The `runtime` crate: orchestration, component lifecycle, HTTP/Flight servers, and (today) the connector/accelerator/catalog trait definitions + registries. | foundation, domain |
| **extension** | Plug-ins that *register with* the runtime: the ~30 `connector-*` crates, `spice-cloud`, `tpc-extension`. Thin — a factory + wiring over a `domain` impl. | foundation, domain, runtime |
| **binary** | The `spiced`/`spice` binaries and `tools/*` — link the whole graph. | anything |

The **target** adds two lower tiers — `interface` (the `*-api` trait crates) and
`data` (the `data-<source>` crates, below `runtime`) — as the migration proceeds;
see [Target layering](#target-layering-and-how-we-get-there). `layers.toml`'s
`order` grows to match.

The authoritative, machine-readable assignment is [`layers.toml`](../../layers.toml);
`scripts/check_crate_layers.py` validates the whole workspace against it (see
[Enforcement](#enforcement)).

---

## Crate naming

A crate's **prefix names the subsystem it belongs to.** For a crate that defines a
contract (an `-api` crate), the owning subsystem is **whoever is obligated to
*satisfy* the contract, not whoever consumes it.** The litmus that resolves every
case:

> **"Where does the code that *satisfies* this contract live?"** — that's the prefix.

- **`data-*`** — the **data plane**: concrete data sources *and* the contracts they
  are obligated to implement.
  - interfaces: `data-connector-api`, `data-cdc-api`, `data-catalog-api` — a
    `data-postgres` *satisfies* these; the runtime only calls them.
  - concrete: `data-postgres`, `data-mysql`, … (the high-frequency prefix).
- **`runtime-*`** — the **engine**: its implementation sub-crates *and* the service
  contracts it is obligated to implement.
  - interfaces: `runtime-checkpoint-api` (the accelerator satisfies it; a connector
    *calls* it), later `runtime-secrets-api`, `runtime-object-store-api`.
  - concrete/impl: `runtime-query`, `runtime-serving`, `runtime-acceleration`,
    `runtime-cluster`, … and runtime-owned shared *types* like
    `runtime-component-api` (`Dataset`/`Catalog`/`RefreshMode`).
- **`datafusion-*`** — existing domain prefix for DataFusion extensions
  (`datafusion-ddl`, `datafusion-dml`, …). Keep it.
- **foundation utilities — no capability prefix** (`util`, `arrow_tools`,
  `spicepod`). They are generic; a prefix would over-claim ownership.

**Why not make everything `runtime-`?** "Part of runtime" conflates *consumed by*
the runtime with *owned by* the runtime. The runtime consumes the connector API,
but data crates satisfy it; naming it `runtime-connector-api` would label the data
plane's obligation as the runtime's and quietly reinforce the monolith we are
dismantling. **Ownership = who implements.** CDC deliberately straddles both
prefixes — `data-cdc-api` (source produces changes; data satisfies) +
`runtime-checkpoint-api` (runtime persists offsets; runtime satisfies) — which is
the interface/impl seam, correctly labeled on each side.

**Conventions.** New crate *names* use hyphens (`data-postgres`); the underscore in
`data_components` is legacy. Interface crates take the `-api` suffix. Only mint a
*new* subsystem prefix (`search-*`, `inference-*`) when that subsystem grows a real
interface/impl split worth grouping — do not pre-create.

---

## A feature is a crate, not a `[feature]`

Companion principle to the layering: **prefer a crate per optional capability over a
cargo `[feature]` inside a shared crate.** Capability/connector/behavior toggles
(`#[cfg(feature = "postgres")]`, `dep:` gates that fork behavior) belong **only in
the "stitch" binaries** (`spiced`/`spice`), which decide *which* crates to link.
Library crates below them (`runtime`, `data_components`, `llms`, …) carry **no
capability features**.

Why:

- **Parallelism & incrementality.** A cargo feature is a graph-wide axis: cargo
  re-fingerprints and recompiles every crate a feature flows through when the set
  changes. A capability that is its *own crate* is compiled once, cached, and
  linked or not — no re-fingerprint of dependents.
- **Layering clarity.** "Does Postgres support X?" is answered by reading
  `data-postgres`, not by tracing a `postgres` feature through
  `spiced -> runtime -> data_components`.
- **No feature-unification surprises.** Workspace feature unification turns on a
  library feature for *everyone* the moment one crate enables it; crate selection
  has no such coupling.

**The legitimate exception.** An *additive, optional-dependency* feature that only
gates `dep:` and **never `cfg`-forks behavior** (e.g. a `serde` or `arrow`
integration on a foundation type crate) is acceptable when a whole crate is
overkill. Two hard rules regardless:

- **Features must be additive and unifiable** — never mutually exclusive. Because
  unification can enable them all at once, a feature that changes behavior (rather
  than merely adds an optional dep) is a *correctness* trap, not just a style one.
- New crates use `default-features = false` on heavy deps and inherit shared deps
  via `foo.workspace = true`.

**Target end state:** the only place a `postgres`/`mysql`/`odbc` feature exists is
`spiced`, where `postgres = ["dep:data-postgres"]` selects the connector; `runtime`
has zero features. *Current gap:* `runtime` declares 54 features, `data_components`
30, `spiced` 58 — connector capabilities threaded through the graph. Collapsing
them is the same work as dissolving `data_components` into `data-<source>` crates:
once a capability *is* a crate, its library-crate feature has nothing left to gate.
Do not add new capability features to library crates.

---

## When to make a crate (vs. a module)

We are about to mint many crates; be deliberate. **Make a crate when it is:**

1. an **interface seam** (a trait implemented on one side, consumed on the other) —
   *always* worth a crate, because it inverts a dependency and enables parallel
   compilation;
2. a **parallel-compile / incrementality** win (a big, cohesive chunk that today
   serializes behind unrelated code);
3. **independently reusable**; or
4. a **layering boundary worth enforcing** (the guard can then hold the line).

Otherwise it is a **module**. Each crate costs a link/codegen unit, a `Cargo.toml`,
and a public-API surface to maintain — so go **granular on interfaces** (an `-api`
crate per concern; merging later is trivial, splitting is the hard part), but do
**not** shatter implementation code into micro-crates. Group impl by subsystem
(`runtime-query`, `data-postgres`), not by file.

---

## Enforcement

### The workspace layer check (regression guard)

```bash
python3 scripts/check_crate_layers.py            # fail on any upward normal edge
python3 scripts/check_crate_layers.py --list      # print every crate's tier
python3 scripts/check_crate_layers.py --mermaid   # tier-level DAG
```

Reads `layers.toml` + `cargo metadata --no-deps` (no compilation) and exits
non-zero on the first upward *normal* dependency. Wired into `make lint-rust`
(fail-fast, before clippy) so a PR that adds an upward edge fails before merge.
Only `kind = "normal"` edges are checked; dev- and build-dependencies are exempt
— they never ship in the library graph, so they cannot create a real cycle (this
is why `runtime` may dev-depend on every `connector-*` for its integration
tests). Requires Python 3.11+ (stdlib `tomllib`).

Because the manifest encodes *what is true today*, the check is a **ratchet**: it
cannot force an improvement, but it prevents backsliding, and it tightens as crates
move down.

### New-crate checklist

Every new crate must:

- add `[lints]\nworkspace = true` (else it is silently **under-linted** — scoped
  clippy and rust-analyzer won't apply the pedantic/`unwrap_used` levels);
- carry the copyright header (`Copyright 2024-2026 The Spice.ai OSS Authors`);
- be added to workspace `members` in the root `Cargo.toml` **and** to `layers.toml`;
- use a hyphenated name (and the `-api` suffix if it is an interface crate);
- inherit shared deps via `dep.workspace = true`.

### How to add or tighten a rule

1. **New crate?** Add one line to `layers.toml` at the tier its dependency closure
   allows; run the check.
2. **Splitting a crate?** Add the new crate(s), run the check, delete the old entry.
3. **Introducing the `interface`/`data` tiers?** Insert `interface` into `order`
   just above `foundation` (the `*-api` crates), and `data` just above it (the
   `data-<source>` crates, which drop below `runtime` once their `runtime` dep is
   gone). Move crates one at a time; the check flags the moment an edge points up.
4. **Stricter same-tier policy?** The check allows same-tier edges; to forbid them,
   extend the script to reject `rank(dep) == rank(src)` for chosen tiers, or split
   the tier.

Keep `layers.toml` and this document in sync; the `.toml` is what CI reads.

### `cargo-crate-split` (planning the splits)

[`cargo-crate-split`](https://github.com/zenide/cargo-crate-split) decides *where to
cut* a monolith crate: it parses with `syn`, builds the module reference graph,
finds SCCs (Tarjan), and computes the cheapest edge set to cut (weighted
feedback-arc-set) so the result is an acyclic set of sub-crates.

```bash
cargo crate-split analyze crates/runtime --json          # ranked cut list
cargo crate-split analyze crates/data_components --mermaid
```

Its `--respect-order` + `--check` guard one crate's *internal* module graph;
`check_crate_layers.py` is the *workspace* analogue across already-split crates. Use
crate-split to plan a split, then add the resulting crates to `layers.toml`. Caveat
from its docs: static analysis can miss edges behind glob re-exports/type inference
("false negatives possible; false positives not") — treat the cut list as a strong
prior, then verify with a build.

---

## Target layering, and how we get there

The intuition we are aiming for is a clean bottom-up stack:

```
  foundation -> interface (*-api) -> data (connectors) -> runtime -> binary
```

The workspace does **not** match that yet, in one important way:

> **Connectors currently sit ABOVE the runtime, not below it.** Every `connector-*`
> crate has a *normal* dependency on `runtime`, because the `DataConnector` /
> `DataConnectorFactory` traits, the connector registry, and the acceleration glue
> live inside `crates/runtime` (`runtime/src/dataconnector/mod.rs`). A connector
> depends on `runtime` just to implement the trait it exists to implement.

So the enforced tiers put connectors in `extension` (above `runtime`) — the truth
today. The target **inverts** that edge:

```
  TODAY (serial: extensions build          TARGET (data-<source> & runtime-* impl
   AFTER the whole runtime monolith)         crates build IN PARALLEL)
  ───────────────────────────────────      ─────────────────────────────────────────
  binary                                    binary  ── the only crate that sees both
    │                                          │       interfaces AND concrete impls;
  extension (connector-*)                      │       selects + wires extensions
    │        depends on ▼                 ┌────┴─────────────┐
  runtime  ◄── the monolith            runtime-* impl      data-<source>
    │                                  (runtime-query,      (data-postgres, …;
  domain                                runtime-serving,     absorb data_components/<src>)
    │                                   runtime-accel, …)        │ implement / use
  foundation                                 │ use / implement   │
                                             └──► interface ◄─────┘
                                        data-connector-api · data-cdc-api ·
                                        data-catalog-api · runtime-checkpoint-api · …
                                             │
                                        foundation
```

The target has two sibling subtrees — `runtime-*` impl crates and `data-<source>`
crates — sharing only the small **`-api`** interface crates. Because the data crates
no longer depend on `runtime`, cargo compiles the runtime and every source crate
**concurrently** instead of serially. That, plus breaking up the monolith, is the
whole point.

**Granular interface crates.** Stand up a **separate `-api` crate per concern**
(`data-connector-api`, `data-cdc-api`, `data-catalog-api`, `runtime-checkpoint-api`,
…) rather than one omnibus `data-api`. Merging crates later is trivial; splitting is
the hard part we are trying to stop repeating. Each is traits + tiny types only, and
lives *below* everyone who depends on it.

Three moves, in order (the priority is set by the [measured
baseline](#measured-compile-time-baseline)):

1. **Extract the `-api` interface crates** below `runtime` (per the naming rule).
   This inverts the ~30 `connector-* -> runtime` edges into `data-<source> -> *-api`.
2. **Split `runtime`** along its module seams into `runtime-*` impl crates that *use*
   the interfaces (`runtime-query`, `runtime-serving`, `runtime-acceleration`, …).
   This is where the compile-time win is.
3. **Dissolve `data_components`** into the `data-<source>` crates (each merges
   `connector-<source>` + `data_components/<source>` + any `<source>-utils`,
   implements the shapes it supports, depends on the `-api` crates). Only genuinely
   cross-source glue moves *down* into an `-api` or foundation crate — never sideways
   between sources.

### Method: inverting one seam

Each seam (connector, catalog, checkpoint, …) follows the same mechanical, reviewable
steps — **one seam per PR**:

1. **Name the seam** — the trait(s) one side implements and the other consumes.
2. **Create the `-api` crate** (per the [naming rule](#crate-naming)) containing
   *only* the traits + the tiny DTO/error types they reference. Keep deps minimal
   (ideally foundation-only) — heavy deps here defeat the parallelism.
3. **Move the definitions in**; have the old crate `pub use` them for the migration
   so call sites don't churn.
4. **Repoint consumers**: both sides depend *down* on the `-api` crate; the
   implementing side drops its dependency on the old monolith.
5. **Lift the wiring to the binary** — or a distributed-slice registry
   (`linkme`/`inventory`) in the `-api` crate. **The binary is the only crate allowed
   to see both an interface and its concrete impls.**
6. **Retier in `layers.toml`** and run the guard: the flagged upward edges flip to
   green, proving the inversion landed. Delete the temporary `pub use`.

### Patterns that recur across seams

- **Re-export / facade discipline.** The migration `pub use` shim is *temporary*.
  Standing rule: re-export types that are genuinely part of *your* public API; do not
  re-export a lower crate's types as a convenience — it hides the real dependency and
  defeats the layering signal.
- **Error types are cross-crate contracts.** Per-crate SNAFU enums; an `-api` crate
  defines its own minimal error type. Do not leak a lower layer's concrete error into
  your public API unless intended — box/source-chain it. (This bites: today
  `DataConnectorError` → `ConnectorComponent` → `Arc<Dataset>`/`Arc<Catalog>`, which
  drags heavy runtime types along and must be decoupled before the error can move.)
- **Interface traits are `dyn`-safe.** They are used as `Arc<dyn Trait>`, so keep
  them object-safe and `#[async_trait]` (see CLAUDE.md → Async & blocking). The
  *trait-evolution & wrapper-delegation* rule (CLAUDE.md) applies with force here: a
  new method on an interface trait must be **forwarded through every wrapper** impl
  (`AcceleratedTable`, `FederatedTable`, …), never left as a defaulted no-op.

### The three data-plane "shapes"

A source has up to three interfaces and often wears several hats (Postgres is all
three); each is a separate `-api` crate a `data-<source>` implements as applicable:

- **Query / federation** (`data-connector-api`) — `DataConnector` + DataFusion
  `TableProvider` with filter/projection push-down. (Every source.)
- **CDC / replication** (`data-cdc-api`) — the `changes` refresh mode; a
  stream-of-mutations shape (`data_components/src/cdc.rs`, `postgres_replication`,
  `mysql_replication`, `debezium`).
- **Catalog** (`data-catalog-api`) — discover datasets/schemas rather than serve
  rows (`runtime/src/catalogconnector`, `unity_catalog`, iceberg/glue).

### Worked example: interface vs. implementation dependency (CDC checkpoints)

`cargo-crate-split` flags `dataconnector -> dataaccelerator` (the CDC connectors
`dynamodb`/`kafka`/`debezium` importing `spice_sys::{DynamoDBSys, KafkaSys, …}`) as a
*hard* edge. It looks fundamental but is an **implementation** dependency, not an
interface one — telling the two apart is the core skill of this migration:

- **What the connector needs** is a *checkpoint/offset store*: "load my saved offsets
  for this dataset, persist new ones" — expressible entirely over data-layer types
  (`KafkaMetadata`/`KafkaOffset` already live in `data_components::kafka`).
- **What it imports** is `KafkaSys`, whose only tie to the accelerator is a
  `match AccelerationConnection::DuckDB(pool) => …` engine dispatch — pure impl.
- **The code already knows this**: `kafka.rs` defines a local
  `trait SidecarOffsetStore` and `impl`s it for `KafkaSys` — the interface was
  recognized, just left next to a concrete import.

> **Interface-vs-impl test:** could the caller take a `dyn Trait` and never name the
> concrete type or its heavy deps? If yes, it's an implementation dependency — invert
> it. If the caller genuinely needs the concrete type's data/layout, it's an
> interface dependency and the *type* must move down.

So the seam splits cleanly, and by the naming rule the two halves land under
different prefixes:

- **`runtime-checkpoint-api`** — the `CheckpointStore` trait (+ small metadata types).
  The **accelerator** satisfies it → `runtime-` prefix. Connectors *call* it.
- The connector-specific serialization (`KafkaMetadata`/offsets) moves *into*
  `data-kafka`, which persists through the generic store and so **names no accelerator
  engine and pulls in zero engine drivers**.

The generic checkpointer already exists as
`runtime_acceleration::dataset_checkpoint::DatasetCheckpointer` — but in
`runtime-acceleration` (too high for a `data`-tier connector to depend on). It must be
**extracted down** into `runtime-checkpoint-api`; the accelerator keeps the impl and
depends on the new crate. The rule that decides *where the trait lives* every time:
**the shared crate must be below everyone who depends on it** — else it is the upward
edge the guard rejects.

*Trade-off to decide when this lands:* today's offsets are a typed, relationally
queryable table; a generic keyed-blob store loses per-column queryability and needs a
migration for existing accelerator sidecar tables — acceptable for connector-internal
checkpoints, but call it out (data-correctness sensitive).

The lesson generalizes twice: most "hard" edges `cargo-crate-split` reports are
implementation leaks that dependency injection dissolves — and once inverted, push the
source-specific remainder all the way into the `data-<source>` crate, leaving only a
generic, reusable capability behind.

---

## Measured compile-time baseline

Measured once to set the priority — `cargo build -p spiced`, dev profile, default
features, cold target (macOS). Reproduce with `cargo build -p spiced --timings` and
read `target/cargo-timings/*.html`.

**Cold build — critical path ≈ 1835s (~30.6 min):**

| Stage (serial spine) | wall | % |
|---|---:|---:|
| `data_components` lib | 171s | 9% |
| `runtime` lib (**one un-parallelizable unit, compiles largely alone**) | **1127s** | **61%** |
| 30 connectors (parallel; only start after `runtime`'s rmeta) | 74s | 4% |
| `spiced` lib + final link | 60s | 3% |
| 3rd-party deps | rest | ~23% |

**Incremental (real one-line edit → `cargo build -p spiced`):** editing a connector
or `spiced` ≈ **8s** (rmeta pipelining already makes the static relink cheap); editing
**`runtime`** ≈ **52s**, rebuilding `runtime` + **all 30 connectors** + `spiced`
(because every connector depends on `runtime`).

**Priority follows directly:**

1. **Splitting `runtime` is the dominant win** — 61% of the cold build as a single
   un-parallelizable unit; only carving it into `runtime-*` crates parallelizes it.
2. **Inverting connectors off `runtime`** is a ~4% clean-build win but the real
   incremental fan-out fix (no more rebuilding 30 connectors on a `runtime` edit) and
   the **prerequisite** for the split and for dynamic linking.
3. **Dev dynamic linking is lowest priority** — the static relink is already ~8s
   incremental; the win only compounds after the split. Keep release fully static.

**Priority: split `runtime` ≫ invert connectors off `runtime` ≫ dev dynamic linking.**

### Later: dev-only dynamic linking

Once extensions depend only on `-api` crates, they become natural dynamic-linking
units in dev builds. Cheapest flavor: `-C prefer-dynamic` + `crate-type = ["dylib"]`
on the extension crates, **dev profile only** (Rust ABI is stable within one
toolchain; release stays fully static). The payoff is link time — measure the
incremental-relink delta (edit one connector → rebuild `spiced`) before/after to size
it. A true plugin loader (`libloading` + a C-ABI registration entry point) is heavier
and only worth it if runtime plug-ability becomes a product requirement.

---

## See also

- [`docs/EXTENSIBILITY.md`](../EXTENSIBILITY.md) — the extension points (Data
  Connector, Accelerator, Catalog, Secret Store, Model, Embedding). The `-api`
  interface crates *are* these extension points; keep the two aligned.
- **CLAUDE.md** — the *trait evolution & wrapper delegation* rule (forward new trait
  methods through every wrapper) and the async/`dyn`-safety rules that interface
  traits must satisfy.

## FAQ

**Why is `runtime` allowed to depend on `connector-*` (a higher tier)?** Only as a
**dev-dependency** for integration tests; the check ignores dev/build deps — they
never ship.

**Why not `cargo-deny`?** Its bans are global (ban crate X everywhere), not
directional (tier N may not depend on tier > N). `check_crate_layers.py` expresses the
directional rule directly and reads the same `cargo metadata`.

**Does the check slow builds?** No — `cargo metadata --no-deps` compiles nothing.
