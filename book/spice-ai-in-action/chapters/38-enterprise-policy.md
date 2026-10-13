# 31. Enterprise identity and Cedar authorization

The local Northstar service derives a tenant from a demonstration token. Enterprise identity replaces that small mapping with a governed identity system, and policy makes the allowed operations explicit at the runtime boundary. The central distinction remains simple: authentication establishes a principal; authorization decides what that principal can do. Neither should depend on an LLM's interpretation of a request.

![Figure 31.1. Identity, resource permission, and data access form an explicit decision.](figures/enterprise-policy.png)

## 31.1 Map the identity you intend to use

The inspected Enterprise configuration includes OIDC bearer authentication under `runtime.auth.oidc`. The supplied public documentation marks OIDC as preview. Treat issuer configuration, claim mapping, token validation, and provider key rotation as integration requirements for the exact runtime build you deploy.

An integration fragment for a fictional Northstar issuer is:

```yaml
runtime:
  auth:
    oidc:
      enabled: true
      issuer_url: https://identity.example.invalid/
      audience:
        - northstar-spice
      groups_claims:
        - groups
      claims:
        user_id: sub
        org_id: tenant_id
        roles:
          - roles
```

The issuer is a placeholder, not a running service. Configure it to match your provider's discovery document and issued tokens. An accepted audience should identify the intended application. The subject identifies the user; the mapped organization claim supplies tenant context; the configured groups and roles supply membership information. Keep a small documented token example with values redacted, and verify the mapping through the runtime.

Test a valid northern analyst, a valid southern analyst, an authenticated user without the analyst role, an expired token, a wrong audience, and a wrong issuer. Include a token without a tenant claim. Do not let a missing claim become a wildcard tenant. The acceptance result is the observed response and resulting identity, not merely successful login in the identity provider's console.

## 31.2 Use API keys for the identity they actually represent

The `api_key` configuration accepts string keys with optional `:ro` or `:rw` suffixes. A key without a suffix is read-only in the inspected implementation. Source keys through the configured secret mechanism rather than embedding real credentials in a Spicepod.

```yaml
runtime:
  auth:
    api_key:
      enabled: true
      keys:
        - ${ env:NORTHSTAR_READER_KEY }:ro
```

A read-only credential is still a credential. It can expose sensitive query results if authorization grants broad access. A read-write suffix does not make every connector support every DML operation; it describes an authentication permission level, while the data path has its own capabilities.

Combined OIDC and API-key authentication is documented. Decide which clients use which identity and how each maps into policy. Do not assume a service API key carries the same organization claim as a human bearer token. If you permit a service identity to act for many tenants, enforce the delegation boundary explicitly in the application and audit it.

## 31.3 Verify request-scoped identity through SQL

The Enterprise identity surface includes `current_user_id()`, `current_org_id()`, `current_user_has_role(...)`, and `session_property(...)`. During an integration lab, inspect only the identity fields necessary to verify the mapping:

```sql
SELECT current_user_id() AS user_id,
       current_org_id() AS tenant_id,
       current_user_has_role('analyst') AS analyst;
```

This query establishes what the runtime sees for that request. It does not establish that every subsequent query contains a tenant filter. A view whose definition includes `tenant_id = current_org_id()` can be useful, but access to the underlying table must also be governed. Otherwise the caller can simply query a different relation.

Identity affects caching and reuse. Include two principals running the same SQL text in the test set, then alternate them over reused client connections. The expected result follows the authenticated request. Retain identity, result rows, and cache-related observations without logging raw tokens.

## 31.4 Build the Cedar decision around a resource

Cedar evaluates a principal, action, resource, and context. Spice supplies entity types for users, roles, datasets, models, tools, and endpoint categories. A policy can permit an analyst to access the SQL endpoint while separately restricting which datasets that endpoint can query.

Use explicit default-deny behavior for the lab policy bundle:

```yaml
runtime:
  authorization:
    enabled: true
    default: deny
    provider: local
    policies:
      - name: analyst-sql-endpoint
        cedar: |
          permit(
            principal in Spice::Role::"analyst",
            action == Spice::Action::"access",
            resource == Spice::Endpoint::"sql"
          );
      - name: analyst-orders
        cedar: |
          @row_filter("tenant_id = current_org_id()")
          permit(
            principal in Spice::Role::"analyst",
            action == Spice::Action::"read",
            resource == Spice::Dataset::"orders"
          );
```

This fragment illustrates two different permissions. It is not an all-purpose Northstar policy. Queries involving customers, returns, articles, models, or tools need the corresponding reviewed permissions. A broad permit added to make one failing query work can undermine the intended boundary.

Dataset resource identity matters. The source distinguishes dataset names and their catalog/schema attributes. Test the exact registered relation names, including qualified names and views, rather than assuming an unqualified example matches every catalog. Capture the evaluated resource and policy decision in the integration record. The companion policy fragment supplies configuration text, not a completed OIDC or policy enforcement test.

## 31.5 Add row filters and column masks

The Enterprise policy implementation compiles annotations on `read` permits into an access plan. A row filter is a SQL Boolean expression. Column masks are SQL scalar expressions that must preserve an acceptable type. In the documented model, a `read` permit also authorizes the corresponding query access.

For example, Northstar may permit analysts to see their tenant's customer rows while replacing the customer name:

```cedar
@row_filter("tenant_id = current_org_id()")
@mask_customer_name("'REDACTED'")
permit(
  principal in Spice::Role::"analyst",
  action == Spice::Action::"read",
  resource == Spice::Dataset::"customers"
);
```

Tag-based masks provide another way to select columns: dataset column metadata carries tags, and a policy can address the tagged columns. Treat tagging as part of the schema contract. Adding a new sensitive column without its intended tag is a policy rollout problem even if the query still compiles.

A mask changes the values downstream operations see. Test grouping, joins, ordering, predicates, `SELECT *`, projections, and aliases against the masked relation. Replacing many names with one string can collapse groups. A NULL mask must have an appropriate type. A row filter must handle NULL tenant values deliberately. These are data semantics as well as security semantics.

Do not infer end-to-end enforcement from the existence of an annotation parser. Verify the paths your application exposes: HTTP SQL, Flight, views, search, model tools, and distributed execution where applicable. Capture returned rows and plans, and inspect downstream model or reranker inputs when those services receive data. The book does not claim that an unexecuted path has passed this review.

## 31.6 Publish policy as a versioned application artifact

The configuration supports local, operator, and cloud policy providers. Local policies can be inline or file-based. Remote provider settings describe how the runtime obtains updates. That runtime-side capability does not establish that an arbitrary operator deployment exposes a policy distribution endpoint; verify the provider service and its version before selecting it.

Keep policy changes reviewable with a principal/action/resource matrix. For each change, record an allowed case, a denied case, and the reason. Deploy the smallest bundle needed for the application, and retain the preceding accepted revision. Define how policy updates interact with in-flight requests, caches, and long-lived connections through real integration tests.

A policy outage also needs a product decision. Distinguish an empty valid policy set, an invalid policy document, and an unavailable provider. Default-deny is a baseline, but operational acceptance must show what the selected runtime actually does in each condition. Avoid broad emergency permits as a recovery mechanism; use a deliberately restricted and audited administrative path.

## 31.7 Review the whole route to an answer

Consider the support assistant. The user authenticates, reaches a search endpoint, retrieves authorized article text, invokes a model, and may call an order tool. Each step can touch a distinct resource type. Permission to invoke the model does not imply permission to query orders. Permission to query a dataset does not authorize an arbitrary external tool to receive its contents.

Northstar's release matrix should include the two shipping policies, an unsupported question, a user without the analyst role, and a cross-tenant order request. Preserve the stable article IDs from the earlier capstone. A correct policy decision is observable in the returned evidence and in the absence of unauthorized data along the selected path.

**Exercise.** Extend the two-rule example to a reviewed sales-only application. List the exact tables and views it needs, prohibit mutations, and test the same SQL under both tenants. Then design a separate policy bundle for the support assistant without granting it the sales application's entire resource set.

**Further reading.** See the supplied `enterprise/features/authentication.md` and `enterprise/features/policy.md`, together with the inspected `runtime-auth`, `runtime-policy`, and runtime policy-enforcement source. The public [Enterprise policy reference](https://docs.spice.ai/docs/enterprise/features/policy) provides the current documentation path.
