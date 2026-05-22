# Authentication

Spice supports authentication for all query endpoints (HTTP, Arrow Flight, gRPC) using **OIDC (OpenID Connect)** bearer tokens, **API keys**, or both simultaneously. When authentication is enabled, unauthenticated requests are rejected.

## Configuration

Authentication is configured under `runtime.auth` in `spicepod.yml`.

### OIDC

```yaml
runtime:
  auth:
    oidc:
      enabled: true  # default: true
      issuer_url: https://accounts.google.com
      audience:
        - my-spice-app
      groups_claims:  # default: ["groups"]
        - groups
      claims:
        user_id: sub  # default: "sub"
        org_id: "https://myapp.com/org_id"
        roles:
          - "https://myapp.com/roles"
```

| Field           | Type            | Required | Default      | Description                                                                                                                                         |
| --------------- | --------------- | -------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`       | bool            | No       | `true`       | Enable or disable OIDC authentication.                                                                                                              |
| `issuer_url`    | string          | Yes      | —            | The OIDC issuer URL. Used to fetch `{issuer_url}/.well-known/openid-configuration`. Spice reads the canonical `issuer` from the discovery document and uses it for `iss` validation — so the configured URL does not need to exactly match the `iss` claim in tokens (e.g. Entra v1 tokens have a different `iss` than the discovery URL). |
| `audience`      | list of strings | Yes      | —            | Accepted values for the JWT `aud` claim. The token must match at least one.                                                                         |
| `groups_claims` | list of strings | No       | `["groups"]` | JWT claim names to extract group memberships from. Values from matching claims are merged. Each configured claim must be a string array.            |
| `claims`        | object          | No       | see below    | Configurable claim mappings for extracting identity fields from JWT tokens. See [Claim Mappings](#claim-mappings).                                  |

#### Claim Mappings

The `claims` section maps JWT claim names to identity context fields exposed via SQL functions.

| Field     | Type            | Default | Description                                                                                                                                                                 |
| --------- | --------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `user_id` | string          | `"sub"` | JWT claim to use as the primary user identifier. Returned by `current_user_id()`. Falls back to `sub` if the mapped claim is absent.                                        |
| `org_id`  | string          | —       | JWT claim to extract the organization or tenant identifier from. Returned by `current_org_id()`.                                                                            |
| `roles`   | list of strings | `[]`    | JWT claim names to extract role memberships from. Roles from matching claims are merged. Each claim may be a string array or a single string. Returned by `current_role()`. |

All validated JWT claims (including custom claims) are accessible via `session_property(key)` regardless of claim mappings.

**JWKS handling**: On startup, Spice discovers the JWKS endpoint from the issuer's OpenID configuration and fetches the signing keys. Keys are refreshed every 5 minutes in the background. Startup retries up to 3 times with exponential backoff (1s, 2s, 4s).

**Supported algorithms**: RS256, RS384, RS512, ES256, ES384, PS256, PS384, PS512, EdDSA.

#### Provider Setup

Spice is a JWT validator for incoming bearer tokens. It does **not** redirect users to an identity provider, redeem authorization codes, or manage OAuth client secrets for your application. Your app or gateway obtains a bearer JWT from the provider and sends it to Spice, and Spice validates that token using the configured issuer and audience.

When setting up any provider:

1. Create the OAuth or OIDC application for the client that signs users in or requests tokens.
2. Decide which token Spice will receive.
3. Decode a sample token and copy its exact `iss`, `aud`, and claim names into `spicepod.yml`.
4. Configure provider-side claims for any identity data you want to use in Spice, such as groups, tenant, or roles.

For backend API authentication, prefer JWT access tokens minted for the Spice API. For Google sign-in, the token sent to Spice is typically a Google ID token.

##### Provider Cheat Sheet

| Provider           | Token Spice typically validates                                                                 | `issuer_url`                                                                                         | `audience`                                                                | Common claim mappings                                                      |
| ------------------ | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Microsoft Entra ID | Access token for the Spice API                                                                  | Matches the token's `iss`: `https://sts.windows.net/<tid>/` (v1) or `https://login.microsoftonline.com/<tid>/v2.0` (v2) | v1: resource URI (e.g. `api://<client-id>`); v2: bare client GUID. Decode a sample token to confirm. | `user_id: oid`, `org_id: tid`, `roles: [roles]`, `groups_claims: [groups]` |
| Google             | ID token                                                                                        | `https://accounts.google.com`                                                                        | The Google OAuth client ID                                                | `user_id: sub`, optional `org_id: hd`                                      |
| Okta               | JWT access token from an authorization server, or an ID token if that is what your client sends | Exact issuer for the Okta authorization server, commonly `https://<your-okta-domain>/oauth2/default` | Exact `aud` from the token, such as the auth server audience or client ID | `user_id: sub`, `roles: [roles]`, `groups_claims: [groups]`                |

If your decoded token shows different `iss`, `aud`, or claim names than the common values above, use the token's actual values.

##### Microsoft Entra ID

Use this pattern when Spice is acting as an API or backend protected by Microsoft-issued bearer tokens.

1. In the Microsoft Entra admin center, create an **App registration** for the Spice API.
2. If Spice is only validating API tokens, you do not need a redirect URI on the API app registration.
3. Under **Expose an API**, set an **Application ID URI** and add one or more scopes if delegated clients will call Spice.
4. Under **App roles**, create roles if you want Spice to read role data from the `roles` claim.
5. If you want group membership in tokens, configure group claims for the app. For large tenants, prefer app roles or restrict group claims to groups assigned to the application.
6. If your users authenticate through a separate frontend or client app, register that client app separately and grant it permission to the Spice API.
7. Assign users, groups, or app roles in **Enterprise applications** so the claims are actually emitted in tokens.

Entra issues either v1 or v2 access tokens depending on the app registration manifest. The token version affects both `iss` and `aud`. Spice handles both automatically — set `issuer_url` and `audience` to match the token version you receive. Decode a sample token to confirm the exact values.

**v1 tokens (default — `"requestedAccessTokenVersion": null` in manifest)**

- `iss`: `https://sts.windows.net/<tenant-id>/`
- `aud`: the resource URI, typically `api://<client-id>`

```yaml
runtime:
  auth:
    oidc:
      issuer_url: https://sts.windows.net/<tenant-id>/
      audience:
        - api://<client-id>
      groups_claims:
        - groups
      claims:
        user_id: oid
        org_id: tid
        roles:
          - roles
```

**v2 tokens (`"requestedAccessTokenVersion": 2` in manifest)**

- `iss`: `https://login.microsoftonline.com/<tenant-id>/v2.0`
- `aud`: bare client ID GUID

```yaml
runtime:
  auth:
    oidc:
      issuer_url: https://login.microsoftonline.com/<tenant-id>/v2.0
      audience:
        - <client-id>
      groups_claims:
        - groups
      claims:
        user_id: oid
        org_id: tid
        roles:
          - roles
```

To switch to v2 tokens, set `"requestedAccessTokenVersion": 2` in the app manifest (**App registrations → your app → Manifest**). See [access token claims reference](https://learn.microsoft.com/en-us/entra/identity-platform/access-token-claims-reference) and [configuring token version](https://learn.microsoft.com/en-us/entra/identity-platform/reference-app-manifest#accesstokenacceptedversion-attribute).

Notes:

- Entra emits tenant ID in the `tid` claim, which maps naturally to `current_org_id()`.
- App roles are usually easier to manage than group claims for application authorization.
- If you configure Entra to emit groups as role claims, those values arrive in `roles`, and Entra application roles will no longer appear in that same claim.

##### Google

Use this pattern when your users sign in with Google and your app forwards the Google-issued JWT to Spice.

1. In Google Cloud Console, create or select a project.
2. Configure the OAuth consent screen or branding for the project.
3. Create an **OAuth client ID** for the application that signs users in. Choose the client type that matches your app, such as Web, Desktop, Android, or iOS.
4. Add redirect URIs for your application. Spice itself does not need a redirect URI because Spice does not perform the browser login flow.
5. Request at least the `openid` scope, and usually `email` and `profile` if your app needs them.
6. Send the resulting Google **ID token** to Spice.

Recommended Spice mapping:

```yaml
runtime:
  auth:
    oidc:
      issuer_url: https://accounts.google.com
      audience:
        - <google-oauth-client-id>.apps.googleusercontent.com
      claims:
        user_id: sub
        org_id: hd  # optional; present for Google Workspace accounts
```

Notes:

- For Google-backed Spice auth, use the Google ID token, not a Google API access token.
- Google standard OIDC tokens do not include application roles or group memberships by default, so `claims.roles` and `groups_claims` are usually unnecessary.
- If you need Google Workspace tenant context, map `hd` to `claims.org_id` or read it with `session_property('hd')`.

##### Okta

Use this pattern when your application authenticates with Okta and forwards an Okta-issued JWT to Spice.

1. In the Okta Admin Console, go to **Applications** and create an **OIDC - OpenID Connect** app integration.
2. Choose the app type that matches your client, such as Web, SPA, or Native.
3. Enable the grant types your client actually uses. For browser and native apps, this is usually Authorization Code with PKCE. For service-to-service clients, this is commonly Client Credentials.
4. Add your application's sign-in redirect URIs. Spice does not need a redirect URI.
5. Decide which Okta authorization server will mint the token that Spice validates. The common default is `https://<your-okta-domain>/oauth2/default`, but many teams use a custom authorization server.
6. If you want group claims in ID tokens, configure them in the app integration. If you want claims in access tokens, configure them on the chosen authorization server under **Claims**.
7. Add any custom `roles` or `groups` claims to the exact token type Spice will receive.

Recommended Spice mapping for a custom authorization server:

```yaml
runtime:
  auth:
    oidc:
      issuer_url: https://<your-okta-domain>/oauth2/default
      audience:
        - api://default  # or the exact aud value from your token
      groups_claims:
        - groups
      claims:
        user_id: sub
        roles:
          - roles
```

Notes:

- If Spice receives an Okta access token, set `issuer_url` and `audience` from that access token, not from the app integration alone.
- If Spice receives an Okta ID token instead, the audience is usually the Okta client ID.
- Okta can emit custom claims into access tokens and ID tokens independently. Ensure the claim is enabled for the token type your client forwards to Spice.

##### Verify the Token Before Configuring Spice

Before finalizing `spicepod.yml`, acquire one token from your provider and inspect it.

1. Copy the token into a JWT debugger or decode it locally.
2. Set `issuer_url` to the provider's issuer base URL (e.g. `https://login.microsoftonline.com/<tenant-id>/v2.0`). Spice reads the canonical issuer from the discovery document automatically.
3. Set `audience` to include the token's exact `aud` value.
4. Map `claims.user_id`, `claims.org_id`, `claims.roles`, and `groups_claims` to the exact claim names you see in the token.
5. Re-authenticate after claim changes. Many providers only apply new claims to newly issued tokens.

### API Keys

```yaml
runtime:
  auth:
    api_key:  # also accepted as: api-key
      enabled: true  # default: true
      keys:
        - ReadOnly:
            key: ${secrets:ro_api_key}
        - ReadWrite:
            key: ${secrets:rw_api_key}
```

| Field     | Type | Required | Default | Description                            |
| --------- | ---- | -------- | ------- | -------------------------------------- |
| `enabled` | bool | No       | `true`  | Enable/disable API key authentication. |
| `keys`    | list | Yes      | —       | API keys with permission levels.       |

**Permission levels**:

- `ReadOnly` — Query access only (default if unspecified).
- `ReadWrite` — Query and write access (DML operations).

**String shorthand**: Keys can be specified as plain strings with optional suffixes:

```yaml
keys:
  - "my-api-key"       # ReadOnly (default)
  - "my-api-key:ro"    # ReadOnly (explicit)
  - "my-api-key:rw"    # ReadWrite
```

API key values support [secret replacement](https://docs.spiceai.org/components/secret-stores) (e.g. `${secrets:my_key}`). Empty key values are silently dropped with a warning to prevent accidental unauthenticated access.

Key comparison uses constant-time equality to prevent timing attacks.

### Combined Authentication

OIDC and API keys can be enabled simultaneously:

```yaml
runtime:
  auth:
    oidc:
      issuer_url: https://accounts.google.com
      audience: [my-app]
    api_key:
      keys:
        - ReadOnly:
            key: ${secrets:service_key}
```

When both are enabled, the authentication method is determined by the request header:

| Header Present                  | Method Used             |
| ------------------------------- | ----------------------- |
| `X-API-Key`                     | API key authentication  |
| `Authorization: Bearer <token>` | OIDC JWT authentication |
| Neither                         | Request denied          |

## Protocol-Specific Authentication

### HTTP

- **API key**: Send via `X-API-Key` header.
- **OIDC**: Send via `Authorization: Bearer <jwt>` header.
- **Failure responses**: `401 Unauthorized` with body `Unauthorized`. Internal errors return `500`.

```bash
# API key
curl -H "X-API-Key: my-key" http://localhost:8090/v1/sql -d "SELECT 1"

# OIDC bearer token
curl -H "Authorization: Bearer eyJ..." http://localhost:8090/v1/sql -d "SELECT 1"
```

### Arrow Flight / Flight SQL

Flight uses the [handshake protocol](https://arrow.apache.org/docs/format/Flight.html#authentication):

1. **Handshake**: Client sends `authorization` metadata with `Basic <base64(username:password)>`.
   - For API keys: `username` is ignored, `password` is the API key.
   - For OIDC: `username` is ignored, `password` is the JWT bearer token.
2. **Subsequent requests**: Use the token returned from handshake as `authorization: Bearer <token>`.

**Failure responses**: gRPC `UNAUTHENTICATED` status with messages like `Missing authorization header`, `Invalid authorization header`, or `Invalid credentials`.

### gRPC

- **API key**: Send via `x-api-key` metadata.
- **OIDC**: Send via `authorization` metadata with `Bearer <token>` prefix.
- **Failure responses**: gRPC `UNAUTHENTICATED` with `Invalid credentials`.

## Identity SQL Functions

Spice provides four SQL functions for accessing the authenticated caller's identity. All are volatile scalar functions evaluated per-query.

### `current_user_id()`

Returns the primary user identifier. Takes no arguments, returns `VARCHAR`.

```sql
SELECT current_user_id();
```

**Return values by authentication method**:

| Auth Method                        | `current_user_id()` returns                                           |
| ---------------------------------- | --------------------------------------------------------------------- |
| OIDC                               | Value of the mapped `user_id` claim (default: `sub`)                  |
| OIDC (custom mapping)              | Value of the configured `claims.user_id` claim, falling back to `sub` |
| API Key                            | `"api_key_auth"`                                                      |
| No auth configured                 | `"anonymous"`                                                         |
| Auth configured but no credentials | Request rejected (never reaches query)                                |

### `current_org_id()`

Returns the organization/tenant identifier extracted from the JWT claim configured in `claims.org_id`. Takes no arguments, returns `VARCHAR` (nullable).

```sql
SELECT current_org_id();
```

Returns `NULL` when:

- No `claims.org_id` is configured
- The configured claim is absent from the JWT
- Using API key authentication (API keys have no identity context)
- No authentication is configured

### `current_role()`

Returns comma-separated role memberships. Takes no arguments, returns `VARCHAR` (nullable).

```sql
SELECT current_role();
```

| Auth Method | `current_role()` returns                                              |
| ----------- | --------------------------------------------------------------------- |
| OIDC        | Comma-separated roles from `claims.roles` mapped claims               |
| API Key     | Permission level: `"read"` for ReadOnly, `"read_write"` for ReadWrite |
| No auth     | `NULL`                                                                |

For OIDC, role values come from the JWT claims configured in `claims.roles`. Multiple claim names can be specified; values are merged.

### `session_property(key)`

Returns any validated JWT claim value by name. Takes one `VARCHAR` argument (the claim name), returns `VARCHAR` (nullable).

```sql
SELECT session_property('email');
SELECT session_property('https://myapp.com/tenant_id');
```

Returns `NULL` when the claim is not present in the JWT or when using API key authentication. String claim values are returned as-is; numbers and booleans are stringified; arrays and objects are JSON-serialized.

### Row-Level Security with Views

`current_user_id()` enables view-based row-level access control. Create a view that filters rows by the caller's identity:

```yaml
datasets:
  - from: postgres:documents
    name: documents

views:
  - name: my_documents
    sql: SELECT * FROM documents WHERE owner = current_user_id()
```

Each query against `my_documents` dynamically filters to the caller's rows:

```sql
-- As user "alice" (OIDC sub=alice):
SELECT * FROM my_documents;
-- Returns only rows where owner = 'alice'

-- As user "bob" (OIDC sub=bob):
SELECT * FROM my_documents;
-- Returns only rows where owner = 'bob'

-- As anonymous (no auth principal):
SELECT * FROM my_documents;
-- Returns only rows where owner = 'anonymous' (typically none)
```

The filter is evaluated **per-query at execution time**, not when the view is created.

### Access Control Patterns

**Filter by user identity**:

```sql
SELECT * FROM orders WHERE customer_id = current_user_id()
```

**Multi-tenant isolation** (using `current_org_id()`):

```yaml
views:
  - name: tenant_data
    sql: SELECT * FROM all_data WHERE tenant_id = current_org_id()
```

**Role-based access**:

```sql
-- View that shows sensitive data only to admins
SELECT
  id, name,
  CASE WHEN current_role() LIKE '%admin%' THEN ssn ELSE '***' END AS ssn
FROM employees
```

**Custom claim filtering** (using `session_property()`):

```sql
-- Filter by a custom JWT claim
SELECT * FROM resources
WHERE department = session_property('department')
```

**Conditional columns**:

```sql
SELECT
  id,
  CASE WHEN owner = current_user_id() THEN email ELSE '***' END AS email
FROM users
```

## Caching Behavior

The SQL results cache interacts with authentication to prevent cross-user data leakage while maximizing cache performance.

### Non-Identity Queries

Queries that do **not** reference any identity function (`current_user_id()`, `current_org_id()`, `current_role()`, `session_property()`) are cached normally at the plan level, regardless of whether the caller is authenticated. Different authenticated users issuing the same query (e.g. `SELECT count(*) FROM orders`) share the same cache entry.

### Identity-Dependent Queries

Queries whose resolved logical plan references any identity function are cached **per-user**. The cache key is scoped by the caller's identity, so:

- User "alice" querying `SELECT * FROM my_documents` gets a cache entry scoped to alice.
- User "bob" querying the same view gets a separate cache entry.
- Cache hits only occur when the same user repeats the same query.

This scoping applies even when `current_user_id()` is referenced indirectly through a view — the runtime inspects the fully resolved logical plan, not the raw SQL text.

### SQL-Level Cache Keys

When using SQL-level cache keys (`cache_key_type: sql`), the raw SQL text cache is bypassed for authenticated users because the SQL text alone cannot reveal whether expanded views reference identity-dependent functions. The plan-level cache handles identity detection after full plan resolution.

### Stale-While-Revalidate (SWR)

SWR background revalidation is **disabled** for identity-scoped cache entries. Background revalidation tasks run without a user authentication context, so identity functions would resolve to `"anonymous"` / `NULL` and produce incorrect results. When an identity-scoped cache entry becomes stale, it is treated as a cache miss and re-executed in the user's request context.

### Client-Supplied Cache Keys

When using client-supplied cache keys (`Spice-Cache-Key` header), the key is automatically scoped by a fingerprint of the `Authorization` header. Different tokens produce different cache scopes, even with the same client-supplied key.

## Federation Pushdown

Identity functions (`current_user_id()`, `current_org_id()`, `current_role()`, `session_property()`) are **not pushed down** to federated data sources (e.g. PostgreSQL, MySQL, DuckDB accelerators). They are always evaluated inside the Spice runtime where the request authentication context is available. Pushing them to remote engines would lose identity semantics and could return incorrect data.

This is enforced by the function deny list in federation pushdown configuration.

## Accelerated Tables and Views

### Non-Accelerated Views (Recommended for RBAC)

Non-accelerated views with `current_user_id()` work correctly — the UDF is evaluated per-query with the caller's identity.

### Accelerated Views (Not Recommended for RBAC)

**Do not use identity functions in accelerated (materialized) views.** Accelerated views are refreshed by background tasks that do not carry end-user authentication context. During refresh, `current_user_id()` resolves to `"anonymous"`, `current_org_id()` and `session_property()` return `NULL`, and `current_role()` returns `NULL` — materializing incorrect or empty result sets.

```yaml
# !! DO NOT DO THIS !!
views:
  - name: my_documents
    sql: SELECT * FROM documents WHERE owner = current_user_id()
    acceleration:
      enabled: true  # Background refresh will see owner = 'anonymous'
```

Instead, accelerate the underlying dataset and use a non-accelerated view for per-user filtering:

```yaml
datasets:
  - from: postgres:documents
    name: documents
    acceleration:
      enabled: true

views:
  - name: my_documents
    sql: SELECT * FROM documents WHERE owner = current_user_id()
    # No acceleration — evaluates current_user_id() per-query
```

## Security Considerations

- **JWT validation**: Tokens are validated for signature, expiration (`exp`), issuer (`iss`), and audience (`aud`). Malformed, expired, or incorrectly signed tokens are rejected.
- **Key rotation**: JWKS keys are refreshed every 5 minutes, supporting seamless key rotation by the identity provider.
- **No auth bypass**: When authentication is configured, there is no fallback to unauthenticated access. Missing or invalid credentials are always rejected.
- **Cache isolation**: Identity-dependent query results are never shared across users. The cache key scoping is based on the resolved plan, not the SQL text, preventing leakage through view indirection.
- **Timing-safe comparison**: API key comparison uses constant-time equality (`subtle` crate) to prevent timing-based key extraction.
- **Secret support**: API key values support secret store references to avoid plaintext keys in configuration files.

## Troubleshooting

| Symptom                                      | Cause                                                        | Fix                                                                                 |
| -------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| `401 Unauthorized` on HTTP                   | Missing or invalid credentials                               | Check `X-API-Key` or `Authorization: Bearer` header                                 |
| `UNAUTHENTICATED` on Flight                  | Missing or invalid handshake                                 | Verify handshake sends `Basic` auth with credentials                                |
| `Failed to initialize OIDC auth` on startup  | Unreachable issuer or invalid OIDC config                    | Verify `issuer_url` is accessible and serves `/.well-known/openid-configuration`    |
| `JWKS discovery failed`                      | Network error fetching signing keys                          | Check connectivity to the issuer's JWKS endpoint                                    |
| `current_user_id()` returns `"anonymous"`    | No auth principal in context                                 | Ensure auth is configured and credentials are sent                                  |
| `current_user_id()` returns `"api_key_auth"` | Using API key auth                                           | API keys don't carry individual user identity; use OIDC for per-user identification |
| `current_org_id()` returns `NULL`            | No `claims.org_id` configured or claim absent from JWT       | Add `claims.org_id` mapping to OIDC config pointing to the correct JWT claim        |
| `session_property()` returns `NULL`          | Claim name not in JWT or using API key auth                  | Verify the claim exists in the JWT; `session_property()` only works with OIDC       |
| Accelerated view returns no rows             | Background refresh evaluates identity functions as anonymous | Remove acceleration from views using identity functions                             |
| Cache miss on every authenticated request    | Identity-scoped queries with SWR disabled                    | Expected behavior — identity-scoped entries don't use background revalidation       |
