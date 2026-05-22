# DR-004: Global Cluster-Wide Rate Control (Proposed)

## Status

Proposed — not yet implemented. The OSS `runtime-rate-control` crate provides per-process rate control today; cluster-wide coordination is future work.

## Context

OSS Spice provides per-process rate control via the `runtime-rate-control` crate. It uses [`governor`](https://docs.rs/governor) for token-bucket quotas and a Tokio `Semaphore` for max-concurrent-request limits, with optional jitter and weighted-quota support. State is in-memory (`InMemoryState`, `NotKeyed`) — strictly local to one process.

This is the right abstraction for a single-node deployment. In a Spice Enterprise cluster (OSS DR-004, OSS DR-006), per-process limits are insufficient. Operators need to express limits *across the cluster*:

* "This tenant may run no more than 100 concurrent queries cluster-wide."
* "This model is rate-limited to 10k tokens/minute across all executors."
* "This dataset's expensive joins are capped at 4 concurrent across the cluster, regardless of which scheduler accepts them."
* "Per-principal (DR-001) quotas: this service account may consume at most N CPU-seconds/hour."

These cannot be enforced by per-process limits because the same logical tenant or principal can submit work to any scheduler in an HA cluster.

This places global rate control on the Enterprise side per [`enterprise/README.md`](../README.md): it requires multi-node coordination to be correct, and it serves an organization (tenants, principals, quotas) rather than an application.

This DR is **proposed**; implementation has not started. It is recorded now so that the OSS extension surface and the eventual Enterprise implementation are designed together.

Related decisions:

* [OSS DR-004: Use Apache Ballista as Spice's distributed query framework](../../decisions/004-distributed-query-framework.md)
* [OSS DR-006: High Availability Distributed Query with Active/Active Schedulers](../../decisions/006-ha-distributed-query.md)
* [DR-001: OIDC Authentication for Federated Identity](./001-oidc-authentication.md)
* [DR-002: Cedar-Based Authorization Policy Engine](./002-policy-authorization.md)

## Assumptions

1. The OSS `runtime-rate-control` crate remains the local enforcement point on every node; global rate control composes with it (the local cap always applies; the global cap is an additional constraint).
2. Globally-coordinated decisions cannot afford a network round-trip on every query for a busy cluster; the coordinator must support a leasing/reservation model.
3. The `AuthPrincipal` (DR-001) is available on every request; tenant identity is derived from claims when OIDC is configured, and from API-key metadata otherwise.
4. Object-store conditional writes are the cluster's existing coordination primitive (OSS DR-006); a global rate-control design that builds on them avoids new dependencies.
5. Strict, exact-count enforcement is *not* the goal — practical fairness and an effective ceiling are. Best-effort overshoot of a small constant under partition is acceptable.

## Options

### Coordination substrate

1. **Object-store leases with conditional writes** — extend OSS DR-006's pattern. Each scheduler periodically reconciles its local consumption against a shared ledger; leases authorize a chunk of capacity for a TTL.
2. **External coordination service** (Redis, etcd, a token-bucket service) — strong semantics, but adds infrastructure that OSS DR-006 deliberately avoided.
3. **Gossip / CRDT** — eventually consistent counters across schedulers. Lightweight, but error bounds are hard to reason about for hard quotas.

### Granularity

1. **Per-tenant** — by claim from the principal.
2. **Per-principal** — finer-grained, useful for service accounts.
3. **Per-resource** — per-dataset, per-model, per-tool.
4. **All of the above**, expressible as a small set of orthogonal limit dimensions composed in policy.

### Enforcement model

1. **Lease-based** — schedulers acquire a lease for a quantum of capacity (e.g., 5 query slots for 10s) and enforce locally against the lease until it expires or is renewed.
2. **Per-request approval** — every request consults the coordinator. Strongest semantics, worst tail latency.
3. **Hybrid** — lease-based for high-volume dimensions; per-request approval for rare expensive operations.

### Failure mode

1. **Fail closed** — if the coordinator is unavailable, deny requests beyond a configured local fallback cap.
2. **Fail open with local cap** — if the coordinator is unavailable, fall back to a conservative per-process cap.
3. **Configurable per dimension** — operators choose per limit.

## First-Principles

* **Secure by default**: Rate limits, when configured, cannot be silently bypassed by a partition; default failure mode is conservative.
* **Object-store native**: Use the same coordination primitive as OSS DR-006 rather than introducing a new dependency.
* **Simplicity**: Lease-based coordination keeps the hot path local; the coordinator is consulted on lease boundaries, not per request.
* **First-class extensibility**: Global rate control is expected to plug into a generalized rate-controller surface in `runtime-rate-control`; today that surface is concrete (`RateControllerBuilder` + `governor`-backed limiter), and a trait abstraction will be introduced as part of this work.

## Proposed Decision

Spice Enterprise will provide global, cluster-wide rate control with the following design (subject to revision when implementation begins):

1. **Lease-based coordination over object store** — schedulers acquire short-lived leases for a quantum of each rate-controlled dimension. The shared ledger is updated via conditional writes (OSS DR-006). The hot path is local enforcement against the lease.
2. **Composable limit dimensions** — per-tenant, per-principal, per-resource, per-action; configured declaratively. The local OSS per-process cap always applies in addition.
3. **Hybrid enforcement** — lease-based for high-volume dimensions; per-request approval reserved for narrowly-scoped expensive operations.
4. **Configurable failure mode** per dimension, defaulting to a conservative local fallback when the coordinator is unreachable.
5. **OSS extension point**: introduce a `RateController` trait in `runtime-rate-control` that the existing `governor`-based controller implements. The Enterprise crate provides the coordinated implementation behind the same trait.
6. **Observability**: lease grants, denials, and quota exhaustion are emitted as metrics; operators can attribute quota consumption to a tenant/principal.

### Why

* Reusing object-store conditional writes (OSS DR-006) avoids introducing Redis or etcd as a hard cluster dependency, consistent with [`enterprise/README.md`](../README.md) principle #7 (extend, don't fork) and OSS DR-006's posture.
* Lease-based coordination removes per-request coordinator latency from the query path while keeping ceilings effective; the worst-case overshoot is bounded by the lease quantum, which is configurable.
* Preserving the OSS per-process limit unchanged — and introducing a trait abstraction *before* the Enterprise impl ships — keeps the line between OSS and Enterprise visible at compile time.

## Consequences

* The OSS `runtime-rate-control` surface needs a small refactor (introduce the `RateController` trait) before the Enterprise impl can plug in. This refactor lands in OSS first, by the principles in [`enterprise/README.md`](../README.md).
* Lease quantum is an operator-tunable knob with a documented overshoot bound; documentation must make this explicit so operators do not assume exact-count semantics.
* Quota consumption is tied to the principal (DR-001); deployments without OIDC fall back to a coarser tenant model derived from API keys.
* Until this DR is implemented, global limits cannot be expressed; operators relying on cross-node ceilings must over-provision local limits or accept that the per-process cap multiplied by node count is the effective ceiling.
