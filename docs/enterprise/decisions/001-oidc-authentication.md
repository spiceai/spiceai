# DR-001: OIDC Authentication for Federated Identity

## Status

Accepted (implemented in `runtime-auth`)

## Context

OSS Spice supports per-process API-key authentication via the `runtime-auth` crate. API keys are sufficient for a single application or a single developer, but they do not scale to an organization that already has an identity provider (IdP), wants to issue short-lived credentials, or needs to carry identity claims into authorization (DR-002).

Spice Enterprise users typically operate Spice as shared infrastructure across many teams, with identities managed in Okta, Entra ID, Auth0, Google Workspace, or an internal OIDC provider. They expect to:

* Authenticate users and workloads against their existing IdP without provisioning per-user secrets in Spice.
* Receive short-lived, automatically rotated tokens rather than long-lived API keys.
* Carry identity claims (subject, groups, organization, roles, arbitrary claims) into Spice for downstream authorization (DR-002) and request-scoped session properties.

This places OIDC firmly on the Enterprise side of the line per [`enterprise/README.md`](../enterprise/README.md): it exists to serve an organization, not a single application, and depends on federated infrastructure that has no meaning in a single-node deployment.

Related decisions:

* [DR-002: Cedar-Based Authorization Policy Engine](./002-policy-authorization.md)

## Assumptions

1. Customers already operate a standards-compliant OIDC provider with an OIDC discovery document and JWKS endpoint; Spice does not need to be one.
2. Both human users (interactive Bearer tokens) and workloads (service-to-service JWTs) need to authenticate.
3. Tokens are JWTs signed by the IdP and verifiable via the IdP's published JWKS.
4. Spice must validate tokens locally, without a per-request callback to the IdP, to keep query latency low.
5. The OSS API-key auth path remains the default for single-node deployments and must continue to work alongside OIDC.

## Options

### Token validation

1. **Local JWT validation against cached JWKS** — fetch the IdP's JWKS via OIDC discovery, cache it, validate signatures locally on every request.
2. **Token introspection (RFC 7662)** — call the IdP on every request. Highest fidelity but adds a hop per query and a hard dependency on IdP availability.

### Identity propagation

1. **Request-scoped principal with rich identity context** — extract subject, groups, user_id, org_id, roles, and arbitrary claims into a `Principal` plus an `IdentityContext` carried via the request context for the lifetime of the request.
2. **Subject-only principal** — carry only the JWT subject; lose claims after authentication.

### Coexistence with API keys

1. **Composite auth** — a single auth layer dispatches between API key and OIDC by header (`X-API-Key` vs `Authorization: Bearer`) for HTTP, and tries API key first then OIDC for Flight/gRPC basic auth.
2. **Mutually exclusive** — pick API key *or* OIDC at deploy time.

### Algorithm acceptance

1. **Allowlist of asymmetric algorithms only** (RS*, ES*, PS*, EdDSA) — never trust the JWT header's `alg` directly.
2. **Trust JWT header** — accept whatever `alg` the token claims, including `none` or HS*. Known unsafe pattern.

## First-Principles

* **Secure by default**: Algorithms are allowlisted; the JWT header's `alg` is never trusted directly. The canonical issuer comes from the OIDC discovery document, not user config. Bearer tokens require TLS at the transport layer.
* **Developer experience first**: Configuration is declarative — issuer URL, audience, optional claim mappings.
* **Align to industry standards**: OIDC Core 1.0, OIDC Discovery 1.0, RFC 7519 (JWT), RFC 7517 (JWKS). No bespoke token formats.
* **Composable from community-driven components**: Use `jsonwebtoken` for JWT/JWKS rather than rolling our own crypto.
* **First-class extensibility**: OIDC plugs into the OSS `runtime-auth` crate's auth traits (`HttpAuth`, `GrpcAuth`, `FlightBasicAuth`); other backends (e.g. SAML, SPIFFE) follow the same shape.

## Decision

Spice Enterprise implements OIDC as an authentication backend in `runtime-auth/src/oidc/` with the following design:

1. **Local JWT validation** against a cached JWKS, with a background refresh task that keeps keys fresh and re-fetches on `kid` miss.
2. **Canonical-issuer enforcement**: the issuer used for `iss` validation comes from the OIDC discovery document; if the user-configured `issuer_url` differs (beyond a trailing slash), we warn and use the discovery value, matching OIDC Discovery §4.3.
3. **Algorithm allowlist**: RS256/384/512, ES256/384, PS256/384/512, EdDSA. Symmetric algorithms and `none` are rejected.
4. **Rich `IdentityContext`** carrying `user_id`, `org_id`, roles, and a flat map of extra JWT claims, available to downstream authorization (DR-002) and as request session properties. Groups are surfaced separately on the `OidcPrincipal` (via the `AuthPrincipal::groups()` accessor) rather than as a dedicated `IdentityContext` field.
5. **Configurable claim mappings**: `user_id` (default `sub`), `org_id`, role-bearing claim names, and group-bearing claim names — all declarative.
6. **Composite auth coexistence**: a `CompositeAuth` dispatches HTTP requests by header (`X-API-Key` → API key auth, `Authorization: Bearer` → OIDC) and Flight/gRPC by trying API key first (cheap string compare) then OIDC (crypto). Either or both can be configured.
7. **Single issuer per `OidcAuth` instance**; multiple issuers can be supported in the future by composing multiple `OidcAuth` instances behind the existing auth trait.
8. **Audience validation** is set on the prebuilt `Validation` config so audience checks happen on every token without re-parsing config per request.

Token introspection (RFC 7662) is **not** implemented and not in scope for this DR.

### Why

* Local JWKS validation matches industry practice and keeps query latency unaffected by IdP round-trips.
* Lifting identity into a typed `IdentityContext` rather than a bag of headers means downstream policy (DR-002) can reason about identity without re-parsing tokens.
* Implementing OIDC behind the `runtime-auth` traits keeps the OSS auth surface honest and lets API-key and OIDC auth coexist in the same deployment.
* Using the canonical issuer from the OIDC discovery document closes a class of misconfiguration vulnerabilities where a slightly-wrong configured `issuer_url` could accept tokens from an unintended issuer.
* Allowlisting asymmetric algorithms only is a defense against the well-known JWT alg-confusion class of attacks.

## Consequences

* OIDC ships with the runtime crate but is opt-in: `OidcAuth` is constructed only when the user configures an issuer.
* Authorization (DR-002) consumes the `AuthPrincipal` produced here; deployments without OIDC fall back to API-key principals.
* Multi-issuer support requires a small wrapper that holds multiple `OidcAuth` instances and dispatches by `iss` claim; this is not yet implemented.
* The JWKS background refresh task holds a `JoinHandle` that is aborted on drop, ensuring no leaked tasks if `OidcAuth` is reconstructed.
