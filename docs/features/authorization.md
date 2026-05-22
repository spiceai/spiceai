# Authorization

Spice supports fine-grained authorization using [Cedar](https://www.cedarpolicy.com/) policies. Cedar is an open-source policy language created by AWS that evaluates `(principal, action, resource)` authorization requests against a set of permit/forbid policies.

Authorization builds on top of [authentication](authentication.md). Authentication establishes **who** is making the request; authorization determines **what** they are allowed to do.

When no authorization is configured, all authenticated requests are allowed (backward-compatible with existing behavior).

## Configuration

Authorization is configured under `runtime.authorization` in `spicepod.yml`.

### Minimal Example

```yaml
runtime:
  auth:
    oidc:
      issuer_url: https://accounts.google.com
      audience: [my-app]
      claims:
        roles:
          - "https://myapp.com/roles"

  authorization:
    enabled: true
    default: allow
    policies:
      - name: deny-pii-access
        cedar: |
          forbid(
            principal,
            action == Spice::Action::"query",
            resource == Spice::Dataset::"pii_table"
          ) unless {
            principal in Spice::Role::"compliance"
          };
```

This configuration allows all authenticated users to query any dataset **except** `pii_table`, which is restricted to users with the `compliance` role.

### Full Configuration Reference

```yaml
runtime:
  authorization:
    enabled: true             # Enable/disable authorization (default: true)
    default: allow            # "allow" or "deny" (default: allow)
    provider: local           # "local", "operator", or "cloud" (default: local)
    policies:
      - name: analysts-read-only
        cedar: |
          permit(
            principal in Spice::Role::"analyst",
            action == Spice::Action::"query",
            resource
          );
      - name: admin-full-access
        path: ./policies/admin.cedar
    operator:
      endpoint: https://operator.spice-system.svc:8443/v1/policies
      poll_interval: 30s
    cloud:
      poll_interval: 60s
```

| Field            | Type   | Required | Default | Description                                                                                |
| ---------------- | ------ | -------- | ------- | ------------------------------------------------------------------------------------------ |
| `enabled`        | bool   | No       | `true`  | Enable/disable authorization.                                                              |
| `default`        | string | No       | `allow` | Default decision when no policy matches: `allow` or `deny`.                                |
| `provider`       | string | No       | `local` | Policy source: `local` (inline/file), `operator` (K8s), or `cloud` (Spice Cloud).         |
| `policies`       | list   | No       | `[]`    | Cedar policy definitions (inline text or file references). Used with `provider: local`.    |
| `operator`       | object | No       | —       | K8s Operator provider configuration.                                                       |
| `cloud`          | object | No       | —       | Spice Cloud provider configuration.                                                        |

### Policy Definitions

Each policy entry supports inline Cedar text, a file reference, or both:

```yaml
policies:
  # Inline Cedar policy
  - name: allow-analysts
    cedar: |
      permit(
        principal in Spice::Role::"analyst",
        action == Spice::Action::"query",
        resource
      );

  # File reference (resolved relative to spicepod.yml)
  - name: production-rules
    path: ./policies/production.cedar

  # Both (inline + file are combined)
  - name: combined
    cedar: |
      forbid(principal, action, resource == Spice::Dataset::"secrets");
    path: ./policies/additional.cedar
```

| Field  | Type   | Required | Description                                          |
| ------ | ------ | -------- | ---------------------------------------------------- |
| `name` | string | Yes      | Human-readable name for the policy.                  |
| `cedar`| string | No       | Inline Cedar policy text.                            |
| `path` | string | No       | Path to a `.cedar` file relative to `spicepod.yml`.  |

### Default Behavior

The `default` field controls what happens when no Cedar policy matches a request:

- **`allow`** (default) — A built-in `permit(principal, action, resource)` is automatically prepended. Users incrementally add `forbid` policies to restrict access. Best for getting started and local development.
- **`deny`** — No default permit. Users must explicitly write `permit` policies for every allowed action. Best for production lockdown.

## Cedar Entity Model

Spice maps its concepts to Cedar entity types in the `Spice` namespace.

### Entity Types

| Cedar Type       | Spice Concept           | Entity ID Source                                    | Attributes                          |
| ---------------- | ----------------------- | --------------------------------------------------- | ----------------------------------- |
| `Spice::User`    | Authenticated principal | `IdentityContext.user_id` or `AuthPrincipal.username` | `org_id: String`                  |
| `Spice::Role`    | Role / group            | Role name from `IdentityContext.roles` or `groups()` | —                                  |
| `Spice::Dataset` | Registered dataset      | Dataset table name                                   | `catalog: String`, `schema: String`|
| `Spice::Model`   | LLM model               | Model name                                           | —                                  |
| `Spice::Tool`    | Tool (MCP / built-in)   | Tool name                                            | —                                  |
| `Spice::Endpoint`| API endpoint category   | Endpoint name (e.g. `"chat"`, `"sql"`)               | —                                  |

### Group Membership

Users are automatically placed in their roles via Cedar's parent relationship. If a user has roles `["analyst", "admin"]`, the Cedar entity `Spice::User::"alice"` is `in` both `Spice::Role::"analyst"` and `Spice::Role::"admin"`.

This means policies can target roles directly:

```cedar
// Any user in the "admin" role can do anything
permit(principal in Spice::Role::"admin", action, resource);
```

### Actions

| Cedar Action                 | Applies To        | Triggered By                              |
| ---------------------------- | ----------------- | ----------------------------------------- |
| `Spice::Action::"query"`    | `Spice::Dataset`  | `SELECT` / table scan in a SQL query      |
| `Spice::Action::"insert"`   | `Spice::Dataset`  | `INSERT INTO` via SQL or Flight DoPut     |
| `Spice::Action::"update"`   | `Spice::Dataset`  | `UPDATE` via SQL                          |
| `Spice::Action::"delete"`   | `Spice::Dataset`  | `DELETE` via SQL                          |
| `Spice::Action::"ddl"`      | `Spice::Dataset`  | `CREATE TABLE`, `DROP TABLE`, etc.        |
| `Spice::Action::"invoke"`   | `Spice::Model`    | Chat/inference API call (future)          |
| `Spice::Action::"execute"`  | `Spice::Tool`     | Tool execution (future)                   |
| `Spice::Action::"access"`   | `Spice::Endpoint` | HTTP/Flight/gRPC endpoint access (future) |

## Policy Examples

### Allow all analysts to query, restrict writes to admins

```cedar
// Analysts can query any dataset
permit(
  principal in Spice::Role::"analyst",
  action == Spice::Action::"query",
  resource
);

// Only admins can write
permit(
  principal in Spice::Role::"admin",
  action in [Spice::Action::"insert", Spice::Action::"update", Spice::Action::"delete"],
  resource
);
```

### Block access to a specific dataset

```cedar
// Allow everything by default (set default: allow in config)
// Then block a specific table:
forbid(
  principal,
  action == Spice::Action::"query",
  resource == Spice::Dataset::"salary_data"
) unless {
  principal in Spice::Role::"hr"
};
```

### Per-user dataset access

```cedar
// Allow user "alice" to query the finance dataset
permit(
  principal == Spice::User::"alice",
  action == Spice::Action::"query",
  resource == Spice::Dataset::"finance_reports"
);
```

### Deny-default with explicit allowlist

With `default: deny`:

```cedar
// Only these actions are allowed; everything else is denied
permit(
  principal in Spice::Role::"data-engineer",
  action in [
    Spice::Action::"query",
    Spice::Action::"insert",
    Spice::Action::"ddl"
  ],
  resource
);

permit(
  principal in Spice::Role::"viewer",
  action == Spice::Action::"query",
  resource
);
```

### Dataset attribute-based policies

```cedar
// Only allow queries on datasets in the "analytics" catalog
permit(
  principal in Spice::Role::"analyst",
  action == Spice::Action::"query",
  resource
) when {
  resource.catalog == "analytics"
};
```

## Enforcement Points

Cedar policies are evaluated at these points in the request lifecycle:

### SQL Queries

Every SQL query is checked after plan validation. The logical plan is walked to identify all tables referenced and the operation type (SELECT, INSERT, UPDATE, DELETE). Each (user, action, dataset) triple is evaluated against the Cedar policy set.

If any table access is denied, the entire query fails with an error:

```
Authorization denied: action 'query' on dataset 'pii_table' is not permitted for this user
```

This covers all SQL execution paths: HTTP `/v1/sql`, Flight SQL, and the `/v1/nsql` text-to-SQL endpoint.

### Flight DoPut

Flight DoPut (data ingestion) is checked at two levels:
1. **Group-based write check** — the existing `read_write` group check (backward compatible)
2. **Cedar SQL check** — when the DoPut executes DML internally, the Cedar SQL enforcement evaluates the `insert` action

### Arrow Flight SQL

Flight SQL statement execution (queries and DML) goes through the same SQL query path and is subject to the same Cedar policy evaluation.

## Policy Providers

### Local (default)

Reads policies from `spicepod.yml` inline definitions and local `.cedar` files. Policies are re-read when `spicepod.yml` changes (hot-reload via the pods watcher).

```yaml
authorization:
  provider: local
  policies:
    - name: my-policy
      cedar: "permit(principal, action, resource);"
    - name: file-policy
      path: ./policies/access.cedar
```

### K8s Operator

Polls the Spice K8s Operator API for policies at a configurable interval.

```yaml
authorization:
  provider: operator
  operator:
    endpoint: https://operator.spice-system.svc:8443/v1/policies
    poll_interval: 30s
```

| Field           | Type   | Default | Description                            |
| --------------- | ------ | ------- | -------------------------------------- |
| `endpoint`      | string | —       | Operator API endpoint for policies.    |
| `poll_interval` | string | `30s`   | How often to poll for policy updates.  |

> **Note**: The operator provider is not yet fully implemented. It currently returns an empty policy set.

### Spice Cloud

Fetches policies from the Spice Cloud Management API.

```yaml
authorization:
  provider: cloud
  cloud:
    poll_interval: 60s
```

| Field           | Type   | Default | Description                           |
| --------------- | ------ | ------- | ------------------------------------- |
| `poll_interval` | string | `60s`   | How often to poll for policy updates. |

> **Note**: The cloud provider is not yet fully implemented. It currently returns an empty policy set.

## Hot-Reload

Policy changes take effect without restarting the runtime:

- **Local provider**: When `spicepod.yml` or referenced `.cedar` files change on disk, the pods watcher triggers a policy reload.
- **Remote providers** (operator/cloud): Policies are polled at the configured interval and reloaded when changes are detected.

In-flight requests complete with the old policy set. The new policies apply to all subsequent requests.

## Interaction with Authentication

Authorization requires authentication to be configured. Without authentication, there is no principal identity to evaluate policies against.

| Auth State            | Authorization Behavior                                    |
| --------------------- | --------------------------------------------------------- |
| No auth configured    | Authorization is skipped (no principal to evaluate)       |
| Auth configured       | Cedar policies evaluate using the authenticated principal |
| Unauthenticated request | Rejected by auth layer before reaching authorization    |

The Cedar `Spice::User` entity is constructed from:
- **OIDC**: `IdentityContext` with `user_id` (from mapped claim), `org_id`, roles, and all JWT claims
- **API Key**: Username `"api_key_auth"` with groups `["read"]` or `["read_write"]`

## Security Considerations

- **Cedar's default-deny**: Cedar's native behavior denies all requests when no policy matches. The `default: allow` config adds a built-in permit-all policy to ease onboarding. Production deployments should use `default: deny` with explicit allowlists.
- **Policy validation**: Invalid Cedar syntax is rejected at load time with an error log. The runtime continues without the policy engine if policies fail to parse.
- **No policy bypass**: When a policy engine is active, all SQL queries pass through Cedar evaluation. Internal runtime queries (system tables, health checks) bypass authorization because they run without an auth principal.
- **Forbid wins**: Cedar's evaluation model means `forbid` policies always override `permit` policies. A single `forbid` rule cannot be overridden by any number of `permit` rules.
- **Immutable during evaluation**: The policy set is held behind a read-write lock. Policy reloads are atomic — a query either evaluates against the old set or the new set, never a mix.

## Troubleshooting

| Symptom                                    | Cause                                                      | Fix                                                                            |
| ------------------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------ |
| All queries denied after enabling authz    | `default: deny` with no `permit` policies                  | Add `permit` policies or switch to `default: allow`                            |
| `Authorization denied` on a specific table | A `forbid` policy matches the (user, action, dataset)      | Check policy set for matching `forbid` rules; verify user has required role    |
| `Failed to parse Cedar policies` in logs   | Syntax error in inline Cedar or `.cedar` file              | Validate Cedar syntax; check for missing semicolons or incorrect entity names  |
| Policy engine not initialized              | `enabled: false` or parse failure                          | Check logs for initialization errors; verify `enabled: true`                   |
| Policy changes not taking effect           | File watcher not detecting changes, or wrong file path     | Verify `.cedar` file path is correct; check that `spicepod.yml` was saved      |
| API key user denied unexpectedly           | Policy references `Spice::User::"api_key_auth"` literally | API keys share a single identity; use role-based policies (`Spice::Role`) instead |
