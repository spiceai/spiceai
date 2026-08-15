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
  │  binary            bin/spiced, bin/spice, tools/*                      │  stitch everything
  ├──────────────────────────────────────────────────────────────────────┤
  │  extension         spice-cloud, tpc-extension, connector-databricks,   │  plug-ins that
  │                    connector-glue, connector-spiceai                   │  still need the runtime
  ├──────────────────────────────────────────────────────────────────────┤
  │  extension-utility (target slot; empty today)                          │  connector-only building
  │                                                                        │  blocks — runtime can't use
  ├──────────────────────────────────────────────────────────────────────┤
  │  runtime           runtime, runtime-table                              │  orchestration daemon
  ├──────────────────────────────────────────────────────────────────────┤
  │  connector         connector-* (30)                                    │  data sources — compile
  │                                                                        │  in parallel with runtime
  ├──────────────────────────────────────────────────────────────────────┤
  │  shared-utility    data_components, cayenne, llms, search, app, cache, │  always-shipped libraries
  │                    data-connector-api, runtime-datafusion, workers…    │  runtime is built from
  ├──────────────────────────────────────────────────────────────────────┤
  │  foundation        util, arrow_tools, arrow_sql_gen, spicepod,         │  near-dependency-free
  │                    db_connection_pool, token_provider, flight_client … │  utilities + primitives
  └──────────────────────────────────────────────────────────────────────┘
         ▲ every arrow points DOWN — no crate depends on a higher tier ▲
```

| Tier | Purpose | May depend on |
|------|---------|---------------|
| **foundation** | Leaf utilities, wire formats, config parsing, primitives. Ideally reusable outside Spice. Little or no internal dependency. | foundation |
| **shared-utility** | The **always-shipped** shared libraries the runtime is built on — the accelerator (`cayenne`), inference (`llms`), search, the `runtime-*` support crates. Sits *below* `runtime`, so `runtime` may build on it. *Optional* / connector-specific building blocks do **not** belong here (see [Shared building blocks vs. extension-only code](#shared-building-blocks-vs-extension-only-code)); `data_components` sits here today only because it is still an undivided monolith. | foundation, shared-utility |
| **connector** | The `connector-*` data-source crates. They implement the `DataConnector` contract, which lives in `data-connector-api` *below* `runtime`, so a connector names the contract and never the orchestrator. Being below `runtime` is what lets all 30 compile **in parallel with** it rather than behind it. `runtime` may **not** depend on one — see the `forbid` rule below. | foundation, shared-utility, connector |
| **runtime** | The `runtime` crate and `runtime-table`: orchestration, component lifecycle, HTTP/Flight servers, and the accelerator/catalog trait definitions + registries. May **not** depend on `extension-utility` or on `connector`. | foundation, shared-utility |
| **extension-utility** | Connector-specific building blocks that only *extensions* depend on — never `runtime`. It is a low-level *building block*, so it must itself depend **only** on `foundation`/`shared-utility` (at most the `runtime-*-api` interface crates, which sit low) — **not on the `runtime` crate**; pulling in the orchestrator would defeat the point. Sits *above* `runtime` so an accidental `runtime → connector-utility` edge is caught as upward. **Empty today** (a target slot): every such crate — `pgwire-replication`, the `elasticsearch`/`dynamodb-streams`/`smb`/`libnfs` clients, `s3_vectors` — is still pulled in by the `data_components` monolith or `runtime` itself, so it can't move up yet. Populates as the monolith dissolves. | foundation, shared-utility (+ `runtime-*-api`) |
| **extension** | Optional plug-ins that genuinely need the orchestrator: `spice-cloud`, `tpc-extension`, **plus the connector-specific building blocks only extensions use** (`extension-utility`). Three connectors are still here — `connector-glue` and `connector-spiceai` are registration shims over connector bodies that still live inside `runtime`, and `connector-databricks` reaches `runtime::catalogconnector::databricks` + `runtime::token_providers::databricks`. Each joins `connector` when its remaining `runtime` reference is evacuated. | foundation, shared-utility, connector, runtime, extension-utility |
| **binary** | The `spiced`/`spice` binaries and `tools/*` — link the whole graph. | anything |

The two utility tiers encode a single rule: **`runtime-*` may depend only on
`shared-utility`, while extensions may depend on both.** `shared-utility` is what
we always ship and the runtime builds on; `extension-utility` is connector-only
code the runtime must never pull in.

**`runtime` must not depend on a `connector` either.** The linear order permits
it — `connector` sits below `runtime` — but that edge is exactly what the
inversion removed: one such dependency puts every connector back on `runtime`'s
prerequisite path and the parallelism is gone. A second **`forbid`** rule
(`["runtime", "connector"]`) rejects it. The connectors are linked only by the
binaries and by `tools/spicepodschema`, which sit at the top.

A corollary the guard cannot see: **`runtime` must not publicly re-export the
connector contract.** `runtime::dataconnector`'s `pub use data_connector_api::*`
is `pub(crate)` for that reason — a public re-export is a second path to every
contract item, and a connector reaching one through `runtime` re-acquires the
orchestrator dependency while the guard sees only a legal `connector -> runtime`
dev-dep or nothing at all.

**`extension-utility` must not depend on `runtime` either.** It is a low-level
building block for extensions, not a consumer of the orchestrator: its only
dependencies should be `foundation`/`shared-utility` and — at most — the
`runtime-*-api` *interface* crates (which live low, below `runtime`, not the
`runtime` crate itself). The linear tier order enforces only one direction of
this mutual "no edge between `runtime` and `extension-utility`" — `runtime →
extension-utility` is caught as upward — so the reverse is enforced explicitly by
a **`forbid`** rule in [`layers.toml`](../../layers.toml)
(`forbid = [["extension-utility", "runtime"]]`), which rejects that edge even
though it points "downward". It becomes structural in the
[target](#target-layering-and-how-we-get-there) anyway: once `runtime` is split,
there is no monolithic `runtime` crate for a building block to depend on — only
the low `runtime-*-api` crates remain.

The **target** also adds two lower tiers — `interface` (the `*-api` trait crates)
and `data` (the `data-<source>` crates, below `runtime`) — as the migration
proceeds; see [Target layering](#target-layering-and-how-we-get-there).
`layers.toml`'s `order` grows to match.

The authoritative, machine-readable assignment is [`layers.toml`](../../layers.toml);
`scripts/check_crate_layers.py` validates the whole workspace against it (see
[Enforcement](#enforcement)).

### Shared building blocks vs. extension-only code

This is exactly what the **`shared-utility`** and **`extension-utility`** tiers
encode. The dividing line is **"does the always-shipped runtime actually build on
this?"** — not "is it a library?". Two kinds of building block look similar but
belong in different tiers:

- **`shared-utility`** — used by `runtime` itself and/or many crates: `util`,
  `arrow_tools`, `db_connection_pool`, `spicepod`, the `datafusion-*` extensions,
  `cayenne`. These are pieces we *always* ship; they sit below `runtime` so it can
  build on them. (`foundation` is the same idea, one level lower: near-leaf.)
- **`extension-utility`** — a building block a few *extensions* share but
  `runtime` itself never uses (a single-connector building block should instead
  live *inside* that connector — see the preference order below). Candidates:
  `pgwire-replication` (the PostgreSQL logical-replication wire protocol), the
  `elasticsearch` / `dynamodb-streams` / `smb` / `libnfs` clients, `s3_vectors`,
  the per-source halves of `data_components`. "Optional" ⇒ it sits *above*
  `runtime` (which therefore cannot depend on it) and *below* `extension`, rather
  than inflating the always-shipped graph with code only plug-ins touch.

> **Litmus:** if the only crates that depend on X are connectors/extensions, X is
> extension-tier code — even when it looks like a generic utility. **Prefer
> folding it *into* the one extension that needs it** (`extension`); reach for
> `extension-utility` only when *two or more* extensions genuinely share it.

**Target direction — push connector-specific code UP toward its extension.** This
is the mirror image of the guiding principle (push *shared* types *down*). The
preference order, most-common first:

1. **Into the extension crate itself** (`extension`) — a building block a single
   connector uses belongs *inside* that connector. E.g. `pgwire-replication` folds
   into the `data-postgres` crate rather than sitting in `crates/vendor` as a
   foundation dependency. This is the common case.
2. **Into `extension-utility`** — only the *limited* case where a few unrelated
   extensions share a building block that `runtime` must never touch. Shared, but
   still above `runtime`.
3. **Stays in `shared-utility`/`foundation`** — only if `runtime` (or something
   `runtime` always links) actually uses it.

Both **`data_components` and `runtime`** feed this over time: as the monolith is
dissolved and `runtime` is split, connector-specific pieces move out to the
extension crates (mostly), or to `extension-utility` (occasionally) — never left
in the always-shipped graph. Doing so shrinks what `runtime` transitively pulls
in, sharpens the always-shipped-vs-optional line, and — once the [connector
inversion](#target-layering-and-how-we-get-there) lands — lets a source and its
private utilities compile as one parallel unit.

**`extension-utility` is empty today.** Every candidate above is still a
dependency of the `data_components` monolith or of `runtime` directly, so moving
it up would be an upward edge the guard rejects. The tier is a defined,
enforced *slot* that populates as the monolith and `runtime` are split — it does
not force any move now.

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
- **`datafusion-*`** — existing subsystem prefix for DataFusion extensions
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
non-zero on any upward *normal* dependency, plus any edge listed in `layers.toml`'s
`forbid` (specific `[from_tier, to_tier]` pairs the linear order can't express,
e.g. `extension-utility -> runtime` and `runtime -> connector` — rejected even
though they point down). Wired
into `make lint-rust` (fail-fast, before clippy) so a PR that adds a bad edge fails
before merge. Only `kind = "normal"` edges are checked; dev- and build-dependencies
are exempt — they never ship in the library graph, so they cannot create a real
cycle (this is why `runtime` may dev-depend on every `connector-*` for its
integration tests, and why each `connector-*` may dev-depend on `runtime` for
the `Dataset`/`Runtime` its own unit tests build). Requires Python 3.11+ (stdlib `tomllib`).

Because the manifest encodes *what is true today*, the check is a **ratchet**: it
cannot force an improvement, but it prevents backsliding, and it tightens as crates
move down.

### Restricted dependencies (`restricted_deps`)

The tier rule only catches **upward** edges. A driver/format crate (`clickhouse-rs`,
`tokio-postgres`, `aws-sdk-dynamodb`, …) sits at or below `foundation`, so a
normal-dependency on it from *anywhere* is a legal **downward** edge — the tier check
structurally cannot enforce "only `connector-clickhouse` may depend on `clickhouse-rs`".
The optional `[restricted_deps]` table in `layers.toml` closes that gap with an
orthogonal, ownership-based rule:

```toml
[restricted_deps]
clickhouse-rs = ["connector-clickhouse"]   # only this crate may normal-dep clickhouse-rs
```

A normal-dependency edge to a restricted crate from any crate **not** in its list is a
violation (dev/build deps exempt, like the tier rule). The key may be an external crate;
every listed crate must be a real workspace member (a typo fails as a config error rather
than silently disabling the rule). This is the machine-checkable form of "a feature is a
crate" + "connector-specific code lives in its extension": it makes each source
extraction a **ratchet** — add one entry per source once its driver is fully evacuated
into the corresponding connector (adding an entry before the evacuation is complete
fails, which is the point). The entry count tracks migration progress alongside the
shrinking `runtime`/`data_components` feature counts.

### New-crate checklist

Every new crate must:

- add `[lints]\nworkspace = true` (else it is silently **under-linted** — scoped
  clippy and rust-analyzer won't apply the pedantic/`unwrap_used` levels);
- carry the copyright header (`Copyright 2024-2026 The Spice.ai OSS Authors`);
- be added to workspace `members` in the root `Cargo.toml`, and be **covered by
  `layers.toml`** — either it falls under an existing `[[rules]]` path prefix (or
  the `default_tier`) and you've confirmed that tier is right, or you add an
  explicit `[override]` entry. The guard rejects `[override]` keys that don't
  name a real crate, so a typo fails rather than silently defaulting;
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
4. **Forbidding a specific edge the linear order can't express?** Add a
   `[from_tier, to_tier]` pair to `forbid` in `layers.toml` (e.g.
   `["extension-utility", "runtime"]`). It is rejected even when it points
   downward — use this for sibling tiers that must not cross-depend.
5. **Stricter same-tier policy?** The check allows same-tier edges; to forbid them,
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

**The connector edge is inverted.** The `DataConnector` / `DataConnectorFactory`
traits, the link-time registry, the `ConnectorParams`/`ConnectorContext` a
connector is handed, and the listing-table connector all live in
`data-connector-api`, *below* `runtime`. 30 of the 33 `connector-*` crates
therefore have no `runtime` dependency at all and compile concurrently with it;
three still do, for reasons listed in the tier table.

What is left is the other half of the target: splitting `runtime` itself, and
dissolving `data_components` into the source crates.

```
  TARGET (data-<source> & runtime-* impl crates build IN PARALLEL)
  ─────────────────────────────────────────
   binary  ── the only crate that sees both
      │       interfaces AND concrete impls;
      │       selects + wires extensions
 ┌────┴─────────────┐
 runtime-* impl   data-<source>
 (runtime-query,   (data-postgres, …;
  runtime-serving,  absorb data_components/<src>)
  runtime-accel, …)      │ implement / use
      │ use / implement  │
      └──► interface ◄────┘
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
   This inverted the `connector-* -> runtime` edges into `connector-* -> *-api`;
   `data-connector-api` (contract + `listing` + connector parameters) and
   `data-connector-types` (the vocabulary its building blocks share) are done.
2. **Split `runtime`** along its module seams into `runtime-*` impl crates that *use*
   the interfaces (`runtime-query`, `runtime-serving`, `runtime-acceleration`, …).
   This is where the compile-time win is.
3. **Dissolve `data_components`** into the `data-<source>` crates (each merges
   `connector-<source>` + `data_components/<source>` + any `<source>-utils`,
   implements the shapes it supports, depends on the `-api` crates). Only genuinely
   cross-source glue moves *down* into an `-api` or foundation crate — never sideways
   between sources. The complement (see [Shared building blocks vs.
   extension-only code](#shared-building-blocks-vs-extension-only-code)): a
   building block only this source uses moves *up* into it — e.g.
   `pgwire-replication` folds into `data-postgres` rather than staying a foundation
   dependency.

> **In-flight example — PostgreSQL (temporary `shared-utility` override).**
> `connector-postgres-common` lives under `crates/data-connectors/` (which the
> path rule places in `connector`) but is pinned to `shared-utility` via a
> `[override]` in `layers.toml`. It is a leaf helper — its workspace-internal dep
> closure is empty — holding the PostgreSQL catalog/CDC-support queries
> (`list_schemas`/`list_tables`/`primary_key_columns`/`check_cdc_prerequisites`),
> which `data_components::postgres::provider` re-exports so `runtime` reaches them
> without a direct `connector-*` dependency. The override keeps the
> `data_components -> connector-postgres-common` edge a legal *same-tier* dep in
> the interim. Under move #3 the goal is to fold **all** PostgreSQL functionality —
> `data_components::postgres`, `connector-postgres-common`, and
> `runtime::catalogconnector::postgres_accelerated` (the CDC/catalog-acceleration
> glue) — into a single `connector-postgres` (`data-postgres`) crate that owns the
> CDC mechanisms. Once `runtime` no longer reaches these queries (even transitively
> via the re-export), drop the override so the crate returns to `extension`.

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
