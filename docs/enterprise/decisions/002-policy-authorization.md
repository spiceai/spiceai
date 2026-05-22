# DR-002: Cedar-Based Authorization Policy Engine

## Status

Accepted (implemented in `runtime-policy`)

## Context

OSS Spice exposes coarse-grained authorization controls: datasets can be enabled or disabled, endpoints can require an API key, and connectors enforce their own credentials. This is sufficient for a single-application deployment but does not meet the needs of an organization running Spice as shared infrastructure, where access to specific datasets, models, tools, and endpoints must be governed centrally and consistently.

Spice Enterprise needs an authorization layer that can:

* Express access decisions as central, version-controlled policies rather than per-deployment config.
* Operate on rich attributes from the authenticated `AuthPrincipal` (DR-001) — user_id, org_id, groups, roles, claims — and on resource attributes (dataset name, model, tool, endpoint).
* Make decisions in microseconds inside the request path so authorization does not become a bottleneck.
* Hot-reload policies as they change without restarting the runtime.

The `runtime-policy` crate provides this engine using Cedar.

Related decisions:

* [DR-001: OIDC Authentication for Federated Identity](./001-oidc-authentication.md)

## Assumptions

1. Authentication produces an `AuthPrincipal` (DR-001); authorization is a pure function of `(Principal, Action, Resource, Context)`.
2. Policies are authored centrally (Git, IaC) and rolled out to Spice; per-request policy authoring is out of scope.
3. Decisions must be local — no per-request callouts to a remote PDP — so policies live with the runtime and are reloaded on change.
4. The resource model covers datasets, models, tools, and endpoints. Future resources can be added without breaking existing policies.

## Options

### Policy language

1. **[Cedar](https://www.cedarpolicy.com/)** — purpose-built authorization language, formal semantics, strong tooling, used by AWS Verified Permissions.
2. **[Open Policy Agent / Rego](https://www.openpolicyagent.org/)** — popular, general-purpose, less ergonomic for fine-grained ABAC, typically deployed as a sidecar.
3. **Bespoke DSL** — full control, large surface to design and maintain.

### Decision evaluation

1. **In-process** — policies compiled into a decision engine inside the runtime. Microsecond latency, no extra deployment.
2. **Sidecar PDP** — policies evaluated by a separate process. Adds a hop per request and an operational dependency.

### Concurrency model

1. **`Arc<RwLock<PolicySet>>`** — multiple requests evaluate concurrently under a read lock; reloads briefly take the write lock to swap the set.
2. **Copy-on-evaluate** — clone the policy set per request. Simpler but wasteful at high QPS.

### Granularity

1. **Resource-level** (dataset, model, tool, endpoint) — coarse but matches typical access control.
2. **Row/column-level** with plan rewriting — required for true multi-tenant warehouses; significantly more implementation surface.

## First-Principles

* **Secure by default**: When the policy engine is enabled with no matching `permit`, the request is denied. Misconfiguration cannot silently grant access.
* **Data correctness is non-negotiable**: Resource-level decisions are enforced at the same boundary the resource is accessed, not as a post-hoc filter that can be bypassed.
* **Developer experience first**: Cedar has well-known editor tooling and formal semantics; misconfigurations surface at policy load time rather than at request time.
* **Align to industry standards**: Adopt Cedar rather than invent a DSL.
* **First-class extensibility**: New resource types are added to the `SpiceResource` enum and the Cedar schema; the engine API is unchanged.

## Decision

Spice Enterprise uses **Cedar** as its authorization policy engine, implemented in the `runtime-policy` crate:

* **Resource model**: `SpiceResource` enum with variants `Dataset`, `Model`, `Tool`, and `Endpoint`, mapping to Cedar entity types `Spice::Dataset`, `Spice::Model`, `Spice::Tool`, `Spice::Endpoint`.
* **Concurrency**: `Authorizer`, `PolicySet`, and `Schema` held behind an `Arc<RwLock<PolicyEngineInner>>` for concurrent evaluation and hot-reload.
* **Default deny**: when policy is enabled with no matching `permit`, the decision is `AuthzDecision::Deny { reasons }` carrying the policy IDs that contributed.
* **In-process evaluation**: no sidecar; decisions are evaluated locally on every request.
* **Principal source**: the `AuthPrincipal` produced by `runtime-auth` (API key or OIDC, DR-001) is mapped to a Cedar principal entity.

**Resource-level granularity** is the current scope. Row- and column-level enforcement via DataFusion plan rewriting is **not** implemented in this DR and is tracked as future work; until it ships, fine-grained data filtering must be expressed in the data layer (e.g., views) rather than in policy.

### Why

* Cedar's formal semantics make policies analyzable — overly-broad rules can be detected statically rather than at production request time.
* In-process evaluation keeps the request path under microseconds of authorization overhead, which would not hold for a sidecar PDP.
* The `Arc<RwLock<>>` pattern matches the read-mostly, occasionally-reload access pattern; reads do not block one another.
* Treating authorization as resource-typed (`Dataset`, `Model`, `Tool`, `Endpoint`) gives policies a stable vocabulary that survives schema and code changes inside Spice.

## Consequences

* Authorization gains a hard dependency on `runtime-auth`'s `AuthPrincipalRef`; deployments without authentication cannot meaningfully use the engine.
* Adding a new resource kind in Spice (e.g., a new tool family) requires extending `SpiceResource` and the Cedar schema together — these must remain in sync.
* Row/column-level policy is not yet available; customers requiring it use database views or per-dataset access toggles in the meantime.
* Hot-reload windows take the write lock briefly and may delay an in-flight evaluation by sub-millisecond; this is acceptable given how rarely policies reload in practice.
