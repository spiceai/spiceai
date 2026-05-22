# Spice OSS vs. Enterprise: Distribution Principles

This document defines the principles that guide what belongs in the **open-source Spice runtime** (OSS) vs. the **Spice Enterprise** distribution. It is the canonical decision guide for contributors and reviewers when proposing a new feature, deciding where it lives, and writing its decision record.

These principles complement, and never override, the [first principles](../PRINCIPLES.md) — most importantly that **data correctness is non-negotiable**, and that Spice is **secure by default** and **developer-experience first** in *both* distributions.

## Goal

The OSS runtime should be the best single-process SQL/search/inference engine a developer can run on a laptop, a VM, or a single container. The Enterprise distribution layers on the capabilities required to run that engine *as production infrastructure for an organization*: across many nodes, under regulatory scrutiny, with central identity, durable state, and the operational guarantees production fleets demand. Single-node performance — vectorized execution, zero-copy paths, efficient acceleration — keeps improving in OSS so the single-node ceiling stays as high as possible; scale-out across nodes is where Enterprise begins.

A developer who only ever needs OSS should never feel like they are using a "lite" product. An organization adopting Enterprise should never feel like they are running a different product.

## The Core Heuristic

> **OSS is single-node. Enterprise is multi-node, organizational, or high-support.**

Most decisions reduce to: *does this feature only make sense when more than one Spice process, more than one user, or more than one team is involved — or does it carry a support and maintenance burden that only makes sense under a commercial relationship?* If yes, it is almost certainly Enterprise. The three drivers — **multi-node**, **organizational**, and **high-support** — frequently overlap but are independent: a feature that satisfies any one of them is a candidate for Enterprise.

Scale, on its own, is *not* a driver. A change that lifts the single-node ceiling (faster execution, better caching, tighter memory use) belongs in OSS — every OSS user benefits. A change that requires more than one node to be correct or useful is already covered by the multi-node driver. Fleet-level operational tooling — capacity-planning tools, hardened high-throughput defaults, certified builds, long-term support — is *organizational/operational* tooling that exists to run Spice as production infrastructure for a company, and falls under the organizational driver.

| Concern             | OSS (single-node)                                                                   | Enterprise (multi-node / organizational)                                                            |
| ------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Query execution     | Single-process DataFusion                                                           | Distributed scheduler/executor (Ballista-based, HA), scale-out for datasets that exceed one node    |
| Concurrency         | Per-process concurrency limits                                                      | Cluster-wide concurrency, work stealing, fair scheduling across tenants                             |
| Rate control        | Per-process limits and queueing                                                     | Global cluster-wide rate control and quotas                                                         |
| Acceleration        | Local accelerated tables, single-engine                                             | Sharded/partitioned acceleration across executors, coordinated refresh                              |
| Authentication      | API keys, basic/local auth                                                          | OIDC / SSO, federated identity, short-lived tokens                                                  |
| Authorization       | Static config, dataset-level toggles                                                | Cedar policy engine, role/attribute-based access, central policies                                  |
| Secrets             | Env, file, keyring                                                                  | Same, plus enterprise secret stores (Vault, cloud KMS bridges)                                      |
| Telemetry           | Local logs, metrics, traces                                                         | Same, plus organizational observability (cluster-wide metrics, fleet traces)                        |
| State / recovery    | Local files, ephemeral acceleration                                                 | Object-store-backed acceleration snapshotting and recovery                                          |
| Deployment          | `spice run`, single container                                                       | Helm chart, multi-node cluster mode, HA schedulers, autoscaling                                     |
| Cluster security    | N/A (one process)                                                                   | mTLS between components, signed cluster membership                                                  |
| Capacity planning   | Local profiling and metrics                                                         | Fleet-level capacity-planning tools, hardened high-throughput defaults                              |
| Connectors          | Common open systems (Postgres, MySQL, S3, Snowflake, DuckDB, SQLite, Parquet, etc.) | High-support / specialized / legacy systems (e.g. Elasticsearch, ScyllaDB, Oracle, SAP, SharePoint) |
| SDKs                | Baseline clients for standard APIs                                                  | Enterprise SDK helpers for Enterprise-only features (e.g. OIDC / SSO, fleet administration)         |
| Operational support | Community                                                                           | SLAs, certified builds, long-term support                                                           |

## Principles

### 1. Single-node belongs in OSS. Multi-node belongs in Enterprise.

If a feature only has meaning when two or more Spice processes coordinate, it is Enterprise. Distributed query, HA schedulers, executor registries, cluster mTLS, and global rate control all sit here.

The single-node version of the same concern stays in OSS so that developers get a complete, useful product without a license. *Per-process* rate control, *per-process* query execution, and *per-process* caching are OSS.

### 2. Organizational features belong in Enterprise.

Features that exist to serve an *organization* rather than an *application* are Enterprise:

- Identity federation (OIDC, SAML, SSO).
- Centralized authorization policy (Cedar, ABAC/RBAC across teams).
- Quota and chargeback across tenants.
- Central admin/observability surfaces (org-wide dashboards, fleet config).
- Fleet-level operational tooling that only makes sense for an organization running Spice as production infrastructure: capacity-planning tools, hardened high-throughput defaults, certified builds, long-term support.

OSS may still expose the *primitives* these features build on (e.g., a generic `auth` extension point) so that the OSS runtime is composable and so Enterprise is implemented as an extension, not a fork.

Enterprise SDK support follows the same line as runtime features. Baseline SDKs and client libraries for standard Spice APIs remain OSS. SDK helpers that exist only to use Enterprise-only capabilities — for example native OIDC / SSO helpers in `spicepy`, fleet administration clients, or policy-management helpers — belong with the corresponding Enterprise feature so the runtime and SDK surfaces stay aligned.

### 3. Operational guarantees that require coordination belong in Enterprise.

If a guarantee depends on durable, off-host, or coordinated state across nodes or restarts — e.g., object-store-backed snapshots of accelerated state, cluster-wide failover, exactly-once semantics across executors — it is Enterprise.

Best-effort, local equivalents (a single-node checkpoint, a process-local retry) may live in OSS where they are genuinely useful on their own.

### 4. Single-node performance is an OSS investment, not an Enterprise driver.

Making Spice faster, smaller, or more efficient on a single node belongs in OSS — every OSS user benefits, and the single-node ceiling is what the rest of the product is built on. Vectorized execution, zero-copy paths, SIMD, efficient acceleration, better caching, and tighter memory use are OSS work.

When a workload genuinely exceeds what one Spice process can serve — datasets larger than one node's memory or local disk, throughput beyond a single CPU's reach, concurrency across hundreds of clients, sharded acceleration, autoscaling under bursty traffic — the answer is to coordinate across nodes, which falls under principle #1 (multi-node). Scale alone is not an independent reason to put a feature in Enterprise.

### 5. High-support, specialized, or legacy systems belong in Enterprise.

Some features — most often connectors — carry a support and maintenance burden that only makes sense under a commercial relationship. This includes:

- **Complex systems** with large surface areas and frequent breaking changes (e.g. Elasticsearch).
- **Specialized systems** with narrow user bases but deep integration requirements (e.g. ScyllaDB, vendor-specific analytic engines).
- **Legacy / proprietary enterprise systems** with non-trivial certification, licensing, or protocol surfaces (e.g. Oracle, SAP, SharePoint, mainframe gateways).

These ship as Enterprise connectors / extensions because keeping them correct and current requires sustained engineering investment, vendor relationships, and a support channel — commitments that don't fit a community-maintained model. Common, well-documented, broadly-used systems (Postgres, MySQL, S3, Snowflake, DuckDB, SQLite, Parquet, etc.) remain OSS.

The test is not "is this connector enterprise-y?" but "can this realistically be maintained at the quality bar Spice promises without a commercial commitment behind it?" When the answer is no, it belongs in Enterprise; when the answer is yes, it belongs in OSS even if the target system is large or commercial.

### 6. The OSS surface must remain whole and self-sufficient.

Enterprise must never be the *only* way to get a correct or secure answer. Specifically:

- **Correctness lives in OSS.** Query semantics, type handling, and data-correctness invariants are never gated.
- **Baseline security lives in OSS.** TLS to data sources, secrets handling, API-key auth, and safe defaults are OSS. Enterprise can *strengthen* security (mTLS between cluster nodes, OIDC, central authorization policy) but never *replace* OSS security with a paywall.
- **Extension points live in OSS.** Data connectors, accelerators, models, embeddings, secret stores, and policy hooks are extension points in OSS so the community can build, and so Enterprise features plug in cleanly.

### 7. Enterprise features extend OSS; they do not fork it.

Every Enterprise feature should be implemented as an extension, plugin, or additional crate that depends on OSS — not a parallel implementation. This keeps OSS honest (its extension points must be real) and keeps Enterprise maintainable (no divergent core).

When an Enterprise feature reveals a missing OSS extension point, the extension point is added to OSS first, then Enterprise consumes it.

### 8. Industry standards on both sides of the line.

Both distributions align to industry standards rather than inventing protocols. OSS speaks Arrow Flight, Postgres wire, OpenTelemetry, Parquet, OpenAI-compatible APIs. Enterprise speaks OIDC, Cedar policy, S3/object-store conditional writes, mTLS with X.509. A user who knows the standard knows Spice.

### 9. Default to OSS. Promote to Enterprise with evidence.

When the placement of a feature is ambiguous, default to OSS. Move it to Enterprise only when at least one of the following is clearly true:

1. The feature requires multi-node coordination to be correct or useful.
2. The feature exists to serve an organization rather than an application (including fleet-level operational tooling such as capacity planning, hardened high-throughput defaults, certified builds, and long-term support).
3. The feature requires operational guarantees (HA, durable coordinated state, off-host backup/recovery) that a single process cannot provide.
4. The feature carries a support and maintenance burden that only makes sense under a commercial relationship — typically connectors to complex (e.g. Elasticsearch), specialized (e.g. ScyllaDB), or legacy / proprietary (e.g. Oracle, SAP, SharePoint) systems.

If none of these hold, the feature is OSS. "It only matters at scale" is *not* on this list: scale that helps a single node belongs in OSS, and scale that requires more than one node is already #1.

### 10. Naming and crate boundaries make the line visible.

OSS crates live under the public workspace and have no dependency on Enterprise crates. Enterprise capabilities live in dedicated crates (e.g., `runtime-cluster`) or in subsystems within shared crates (e.g., `runtime-policy`, the OIDC backend in `runtime-auth`, and the snapshot module in `runtime-acceleration`) that depend *down* on OSS. The mechanism that enforces the line varies by subsystem: some are gated at compile time with Cargo features (e.g., `runtime-acceleration`'s `snapshots` feature, which surfaces the `SNAPSHOTS_ENTERPRISE_ONLY_MESSAGE` runtime gating message in OSS builds); others are always compiled but are inert until configured, with the runtime emitting a clear `"included in the Enterprise distribution"` message when an Enterprise-only feature is configured in an OSS build. New Enterprise subsystems should prefer compile-time feature gating where practical, and fall back to configuration-time runtime gating where a feature flag would unnecessarily fragment the build matrix.

## Applying the Principles

When proposing a new feature:

1. State the smallest deployment in which the feature has meaning. If that deployment is a single Spice process used by one developer, it is OSS — even if performance work is involved. If the feature requires more than one Spice process, or only makes sense for an organization running Spice as production infrastructure (central identity, fleet operations, regulatory scope), it is Enterprise.
2. Identify the OSS extension point the feature plugs into. If none exists, propose one in OSS first.
3. Write a decision record explaining placement: under [`docs/decisions/`](../decisions/) for OSS decisions, or under [`docs/enterprise/decisions/`](./decisions/) for Enterprise decisions. The DR must explicitly cite which principle(s) place the feature in OSS or Enterprise.

## Reference Decision Records

The following decision records apply these principles to specific features. OSS decision records live under [`docs/decisions/`](../decisions/) and are referenced as `OSS DR-N`; Enterprise decision records live under [`docs/enterprise/decisions/`](./decisions/) and are referenced as `DR-N`.

OSS decision records that motivate Enterprise features:

- [OSS DR-004: Use Apache Ballista as Spice's distributed query framework](../decisions/004-distributed-query-framework.md) — multi-node query execution (Enterprise).
- [OSS DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](../decisions/005-ballista-extensions.md) — cluster integration (Enterprise).
- [OSS DR-006: High Availability Distributed Query with Active/Active Schedulers](../decisions/006-ha-distributed-query.md) — HA scheduler (Enterprise).
- [OSS DR-007: mTLS for Distributed Query Cluster Communication](../decisions/007-cluster-mtls.md) — cluster-internal mutual TLS (Enterprise).

Enterprise decision records:

- [DR-001: OIDC Authentication for Federated Identity](./decisions/001-oidc-authentication.md) — federated identity, implemented in `runtime-auth` (Enterprise).
- [DR-002: Cedar-Based Authorization Policy Engine](./decisions/002-policy-authorization.md) — central authorization, implemented in `runtime-policy` (Enterprise).
- [DR-003: Acceleration Snapshotting and Recovery](./decisions/003-snapshotting.md) — object-store-backed acceleration snapshots, implemented in `runtime-acceleration` (Enterprise).
- [DR-004: Global Cluster-Wide Rate Control](./decisions/004-global-rate-control.md) — cross-node quotas, proposed (OSS `runtime-rate-control` provides per-process limits today).
