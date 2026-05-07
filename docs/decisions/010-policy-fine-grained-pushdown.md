# DR-010: Fine-Grained Policy Enforcement via SQL Expression Push-Down

## Status

Proposed

## Context

DR-009 introduces a Cedar-based authorization engine (`runtime-policy`) that
makes a binary allow/deny decision per `(principal, action, resource)` triple.
Enforcement runs in `crates/runtime/src/datafusion/policy_enforcer.rs`: it walks
the DataFusion `LogicalPlan`, finds every `TableScan` and `Dml` node, and asks
the engine whether the caller may touch that dataset.

That model is sufficient for "can user X query dataset Y" but it cannot express
the policies real customers ask for:

* **Row-level**: a treating physician sees only rows where they appear in
  `patients.physician_id`; a sales rep sees only rows for their own region.
* **Column-level**: an analyst can query `customers` but never sees
  `ssn`, `dob`, or `email`; a support agent sees the last four digits of
  `card_number` only.
* **Tag-driven**: any column tagged `pii` is masked everywhere it appears,
  without per-table policy authoring.

Today an operator who needs any of this either creates curated views per role
(operationally heavy, easy to drift, and easy to bypass via a sibling table)
or restricts access at the dataset level and gives up on the use case. Both
violate the project's top principle: **data correctness is non-negotiable, and
includes never letting unauthorized data leave the engine**.

A portable policy-enforcement model demonstrates the shape of the answer:
extend Cedar with row-filter / column-mask actions, compile the policy down
to a query-engine expression, and push that expression into the table
provider so the engine cannot return forbidden rows or values even if a
downstream layer is buggy or malicious. Some implementations use Google CEL
as the expression language. For Spice we deliberately reject CEL and use
**pure SQL expressions** instead — this DR explains why and lays out the
design.

Related decisions:

* DR-008: OIDC Authentication for Federated Identity (supplies the
  `IdentityContext` consumed here).
* DR-009: Cedar-Based Authorization Policy Engine (the binary-decision base
  this DR extends).

## Assumptions

1. `runtime-policy` already evaluates `permit`/`forbid` policies against a
   `(User, Action, Resource)` triple using Cedar (DR-009). This DR extends
   that engine; it does not replace it.
2. All federated and accelerated tables in Spice are accessed through a
   DataFusion `TableProvider`. Both the `AcceleratedTable` and
   `FederatedTable` wrappers, and every connector's native `TableProvider`,
   sit on the same trait. (See the trait-evolution rules in
   `.github/copilot-instructions.md`.)
3. Policy authors can write SQL `WHERE`-style predicates and per-column SQL
   expressions. They are not expected to learn a second expression language.
4. The runtime is the trust boundary. Once data has left the runtime it is
   the caller's data; correctness obligations stop there. Inside the runtime,
   no code path may observe forbidden rows or columns — including tools, LLM
   inference, embeddings, cache layers, and acceleration refreshes.
5. Policies are small relative to data. Compiling a Cedar policy to a SQL
   expression for every query is fine; the expensive part is query execution,
   which DataFusion already optimizes.

## Options

### Expression language for filters and masks

1. **Pure SQL expressions, parsed by DataFusion's SQL parser.**
   The author writes a SQL boolean expression (for row filters) or a SQL
   scalar expression (for column masks). The engine parses it to a
   DataFusion `Expr` and injects it into the plan.
2. **Google CEL**, as in the talk. Compile CEL → DataFusion `Expr`.
3. **Cedar's own expression sub-language**, embedded in policy bodies via
   `when { ... }`. Translate Cedar expressions to DataFusion `Expr`.
4. **A bespoke Spice DSL.** Maximum flexibility, maximum cost.

### Where to enforce

1. **In a `LogicalPlan` rewrite immediately above each `TableScan`.**
   Add a `Filter` for row filters and a `Projection` that wraps each masked
   column for column masks. Every downstream node — joins, aggregations,
   tools, embeddings, acceleration refreshes — sees only the filtered/masked
   data because they are downstream of the rewrite.
2. **In each connector's `TableProvider::scan` via filter/projection
   push-down.** Connectors that support push-down get optimal performance;
   ones that don't fall back to the rewrite above the scan.
3. **As a `PhysicalPlan` rewrite.** Lower-level, much harder to reason about,
   loses the chance for connectors to push the filter further (e.g. into
   Postgres or Parquet predicate push-down).
4. **In the connector itself, opaquely.** The talk's `GovernTable` wrapper.
   Cleanest at one site but bypassable: any code path that accesses the
   underlying provider directly skips enforcement.

### Where to wire enforcement

1. **A new `PolicyTableProvider` wrapper in the runtime DataFusion layer,**
  layered around every registered public dataset provider when a policy
  engine is configured. Always-on for governed runtime query paths; callers
  cannot query a governed dataset through DataFusion without the policy
  wrapper participating.
2. **A logical plan optimizer rule registered in the DataFusion session.**
   Centralized but easier to forget on a code path that builds its own
   planner (tools, search, etc.).
3. **Both — the wrapper is the enforcement point of record, the optimizer
   rule is a defense-in-depth check that fails closed if it ever sees an
   unwrapped scan of a governed dataset.**

### Cedar action surface

1. **New `read` action that returns row-filter + column-mask context.**
   Replaces today's binary `query`. Cedar's `permit(...) when { ... }`
   conditions carry SQL fragments via annotations:

   ```cedar
   @row_filter("physician_id = current_user_id()")
   @target_table("patients")
   permit (
       principal in Spice::Role::"physician",
       action == Spice::Action::"read",
       resource == Spice::Dataset::"patients"
   );

    @mask_ssn("concat('XXX-XX-', right(ssn, 4))")
   permit (
       principal in Spice::Role::"support_agent",
       action == Spice::Action::"read",
       resource == Spice::Dataset::"customers"
   );
   ```

2. **Carry filters/masks in policy `context`** instead of annotations.
   Cleaner Cedar but harder to author and review.
3. **Tag-based policies** in addition to per-table policies. Datasets
   declare column tags (`pii`, `phi`, `card`); policies match on tag rather
   than column name. Strictly additive on top of (1).

### Identity available to SQL expressions

1. **A small set of SQL UDFs** (`current_user_id()`, `current_org_id()`,
   `current_user_has_role(text)`) that read from the request-scoped
   `IdentityContext` (DR-008). Stable, auditable, reviewable.
2. **Bind parameters** (`$user_id`, `$org_id`) substituted into the SQL
   text before parsing. Looks innocuous but invites injection bugs and
   makes the policy text non-portable.
3. **A magic table** (`spice.session.identity`) joined into every query.
   Powerful, but a join is too heavy for a hot per-row predicate.

## First-Principles

* **Data correctness is non-negotiable.** A user who is denied a column must
  not receive that column's value through any side channel — including
  error messages, query-plan EXPLAIN output, refresh logs, embedding
  inputs, LLM tool arguments, or cache keys.
* **Fail closed.** If the policy cannot be compiled, the schema cannot be
  resolved, or the wrapper cannot be applied, the query fails with a
  structured error rather than returning data. This is the in-doubt-fail-safely
  rule from the project principles.
* **Developer experience first.** Spice already speaks SQL; policy authors
  should not have to learn CEL or invent a parallel expression mini-language.
  SQL is what they already know, what they already test, and what they
  already EXPLAIN.
* **Composable from community-driven components.** Reuse Cedar for the
  authorization graph, DataFusion's SQL parser/planner for the expression
  language, and DataFusion's `TableProvider` filter/projection push-down for
  enforcement. No bespoke parser, no bespoke optimizer.
* **Trait evolution.** A new wrapper provider must forward every
  `TableProvider` method to the inner provider, including ones added later;
  this is the exact wrapper-delegation hazard called out in
  `.github/copilot-instructions.md`. The plan accounts for it (see
  Consequences).

## Decision

We extend `runtime-policy` and the DataFusion integration with the
following design.

### 1. Cedar schema additions

Add a `read` action that may carry annotations:

* `@row_filter("<sql boolean expr>")` — a SQL predicate evaluated per row
  on the resource. May appear multiple times; results are AND-combined.
* `@column_mask("<column>=<sql scalar expr>")`,
  `@column_mask_<column>("<sql scalar expr>")`, or
  `@mask_<column>("<sql scalar expr>")` — replaces a single column with
  the given SQL expression. May appear multiple times for different
  columns. Conflicting masks for the same column are rejected as an
  ambiguity error.
* `@column_mask_tag("<tag>=<sql scalar expr>")`,
  `@column_mask_tag_<tag>("<sql scalar expr>")`, or
  `@mask_tag_<tag>("<sql scalar expr>")` — applies the mask to every
  column whose field metadata contains the tag. Tag masks compose with
  column masks, and conflicts are rejected.
* `@target_table("<table>")` — optional disambiguation when the resource
  selector is broader than one dataset.

`query`, `insert`, `update`, `delete`, `ddl`, `invoke`, `execute`, `access`
are unchanged and remain binary. `read` supersedes `query` for
data-returning paths once enabled; `query` continues to work as a coarse
gate until callers migrate.

A `forbid` policy with `@row_filter` is rejected at compile time —
forbidding "rows where X" is encoded as `permit ... when X is false`,
because Cedar's `forbid` is a hard veto and cannot be selectively softened
by a row predicate without breaking the order-independence guarantee.

### 2. New module `runtime-policy::compile`

Given a `(principal, dataset, schema)` triple, return:

```rust
pub struct AccessPlan {
    pub allowed: bool,
    pub row_filters: Vec<String>,         // SQL boolean expr text
  pub column_masks: Vec<ColumnMask>,    // column-targeted SQL masks
  pub tag_masks: Vec<TagMask>,          // tag-targeted SQL masks
}
```

The compiler walks the matching policies, collects every annotation, and
returns the `AccessPlan`. The output is intentionally just SQL text;
turning text into DataFusion `Expr` happens in the runtime crate so
`runtime-policy` stays free of a DataFusion dependency. Column existence,
tag expansion, SQL parsing, type checking, and conflict detection happen
against the concrete registered table schema and fail closed.

### 3. New wrapper `PolicyTableProvider` in the runtime DataFusion layer

Wraps any inner `TableProvider` registered with the runtime when the policy
engine is configured. On `scan`:

1. Reads the request-scoped principal from `runtime-request-context`.
2. Calls `PolicyEngine::evaluate_read_access(...)` for this dataset.
3. If denied by a matching read policy, returns `DataFusionError::Plan`
   with the structured denial message. The query never sees the inner
   provider.
4. Parses row filters, column masks, and tag-expanded masks to DataFusion
   `Expr` values against the inner table's schema.
5. Builds a secured logical scan over the inner provider, adding parsed row
   filters to the scan filters so connectors that support push-down still
   get the predicate at the source.
6. Adds a projection that replaces each masked column with its mask
   expression. Schema-equivalence is checked before execution; the mask
   must produce the same data type as the original column or the query fails
   closed.
7. Delegates physical planning through DataFusion's session state and
   forwards every other `TableProvider` method (`schema`,
   `supports_filters_pushdown`, `statistics`, `insert_into`, `delete_from`,
   `update`, `truncate`, etc.) to the inner provider.

The wrapper is applied around both federated and accelerated dataset
providers at registration time. The acceleration service still refreshes
from the configured source using the runtime service identity; per-query
filtering and masking are applied at the public DataFusion table boundary.

### 4. Logical-plan defense-in-depth check

`policy_enforcer::authorize_query_plan` keeps walking the plan, but is
extended to assert that every `TableScan` of a governed dataset resolves
to a `PolicyTableProvider`. If it ever sees a raw connector provider for a
governed dataset, the query is denied with a "policy enforcement bypass
detected" error. This is the trait-wrapper-delegation safety net: if a
future code path forgets to wrap, queries fail loudly instead of silently
leaking data.

### 5. SQL UDFs for identity

Three UDFs registered in the DataFusion session, backed by
`runtime-request-context::auth_principal()`:

* `current_user_id() -> Utf8`
* `current_org_id() -> Utf8`
* `current_user_has_role(role: Utf8) -> Boolean`

These are the only blessed way for a policy SQL expression to reach the
caller's identity. They are request-scoped and registered as volatile UDFs,
so DataFusion evaluates them at execution time instead of constant-folding
identity-sensitive values into reusable plans.

### 6. Tag-based policies

Datasets declare column tags in the spicepod (`columns: { ssn: { tags:
[pii] } }`). A mask annotation may target a tag instead of a column name,
for example `@mask_tag_pii("'***'")` or
`@column_mask_tag("pii='***'")`. The runtime expands tag-targeted masks
against Arrow field metadata before parsing and type-checking the mask SQL.
This keeps per-table policy authoring optional.

### 7. Audit and observability

Every coarse authorization check and `AccessPlan` evaluation emits a
`policy_audit` task-history span with the principal, dataset, decision,
applied filters, and applied masks (filter/mask **text**, not data). On
denial, the span carries the denying policy IDs from Cedar's diagnostics and
the structured error message. Policy and audit task spans are always persisted
to `runtime.task_history` when task history is enabled, even when
`task_history.min_sql_duration` would filter out short query spans.

### Why these choices

* **Pure SQL over CEL.** Policy authors already write SQL; the masks they
  want (`right(card, 4)`, `case when org_id = current_org_id() then ssn
  else null end`) are SQL one-liners. Adding CEL doubles the surface area
  the team must support, doubles the parser the security team must audit,
  and forces a mapping layer (`CEL → DataFusion Expr`) that has its own
  bug surface. The talk's CEL mapper is impressive but not free; we get
  a cleaner story by reusing the SQL parser DataFusion already ships.
* **Wrapper, not just an optimizer rule.** Wrappers are dyn-trait objects
  every code path holds; a missing optimizer rule is invisible until a
  wrong query slips through. The optimizer rule is kept as a *check*, not
  as the enforcement point.
* **Cedar still owns the "who".** Cedar is good at the entity graph
  (users, roles, hierarchies, conditions) and bad at expressing
  `right(card, 4)`. SQL is good at the latter and bad at the former. We
  use each tool for what it is good at.
* **Annotations carry SQL text.** Cedar annotations are opaque strings to
  the Cedar evaluator, so smuggling SQL through them does not perturb
  Cedar's order-independence and correctness guarantees. Similar portable
  policy-enforcement designs validate the same pattern.
* **UDFs over bind parameters.** UDFs survive in EXPLAIN, are visible to
  the optimizer, and can be unit-tested. Bind-parameter substitution
  re-introduces the SQL-injection risk the project already calls out as a
  data-correctness hazard.

## Consequences

### What gets better

* Field-level guarantees are enforceable and auditable in pure SQL.
* Connectors with predicate push-down (Postgres, MySQL, DuckDB, Iceberg,
  Parquet, S3 Vectors) will push row filters all the way to the source —
  the unauthorized rows never enter the runtime.
* Policy authors write one expression language, the same one they EXPLAIN
  and test against the data.
* Defense in depth: wrapper enforces, optimizer rule double-checks,
  acceleration refresh has explicit per-dataset identity choice.

### What gets harder / what we accept

* Every existing wrapper of `TableProvider` (`AcceleratedTable`,
  `FederatedTable`, view providers, sink providers, embedding/full-text
  wrappers) must be audited so `PolicyTableProvider` composes cleanly.
  This is the wrapper-delegation hazard from the trait-evolution rule.
  Mitigation: the new defense-in-depth check fails closed for any scan
  that escapes the wrapper.
* Acceleration refresh has to pick an identity. We default to "refresh as
  the runtime service identity, filter per query"; per-user materialized
  refreshes are opt-in and out of scope for the first cut.
* Tools, LLM inference, embedding pipelines, and search must all read
  through the wrapped `TableProvider`, not a raw connector handle. Any
  helper today that bypasses the provider needs to be migrated.
* Column masks must preserve data type. Mask expressions whose result
  type differs from the masked column are rejected at compile time.
* Cedar `forbid` plus `@row_filter` is intentionally disallowed; authors
  who want "deny these rows" express it as a `permit ... when` with the
  inverted predicate.
* `query` action is preserved alongside `read` for one release cycle to
  give external policy authors time to migrate. Both are evaluated; the
  stricter outcome wins.

### Out of scope for this DR

* CEL support (deliberately rejected; revisit only if a concrete customer
  need surfaces that SQL cannot express).
* Cross-engine policy export (e.g. shipping the same policy to Snowflake
  or BigQuery). The architecture does not preclude it — the
  `AccessPlan` is just SQL — but actually wiring it is a separate DR.
* Differential privacy / aggregation guards. Out of scope; column masks
  reduce-leak surface but do not give DP guarantees.
* Per-cell encryption. Out of scope; column masks are deterministic
  scalar SQL, not crypto.
