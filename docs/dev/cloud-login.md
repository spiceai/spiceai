# `spice cloud login` — Internal Reference

Internal documentation for the `spice cloud login` command surface. Covers the
three login methods, how they resolve credentials, where tokens are stored,
and the contracts each method has with the Spice Cloud API.

> Audience: Spice CLI / Spice Cloud engineers. For end-user docs, see the
> public Spice Cloud auth docs. For which **organization** a credential serves
> and which org commands act on, see [`cloud-multi-org.md`](./cloud-multi-org.md).

## Overview

`spice cloud login` authenticates the local CLI against Spice Cloud and writes
a bearer token (and, when available, an app API key) to the local credential
store. All methods converge on the same post-login path: write
`SPICE_SPICEAI_TOKEN`, fetch `/api/spice-cli/auth`, and (if present) write
`SPICE_SPICEAI_API_KEY`.

There are three methods, addressed by Clap subcommands under
[`bin/spice/src/commands/cloud/mod.rs`](../../bin/spice/src/commands/cloud/mod.rs):

| Method       | Subcommand                                | Identity               | Browser?    | Automatable via env vars                                    |
| ------------ | ----------------------------------------- | ---------------------- | ----------- | ----------------------------------------------------------- |
| Subscription | `spice cloud login subscription`          | Human Spice Cloud user | Auto-opened | No (browser required)                                       |
| Subscription | `spice cloud login subscription --device` | Human Spice Cloud user | User-opened | No (browser required)                                       |
| PAT          | `spice cloud login pat`                   | Human (long-lived)     | None        | Yes (`SPICE_CLOUD_PAT`)                                     |
| API (OAuth)  | `spice cloud login api`                   | Service principal      | None        | Yes (`SPICE_CLOUD_CLIENT_ID` + `SPICE_CLOUD_CLIENT_SECRET`) |

Running `spice cloud login` with no subcommand opens an interactive chooser
(via `dialoguer::Select`) when stdin is a TTY, and otherwise fails with a
helpful message that lists the explicit subcommands. The chooser path is
implemented in `execute_login_with_chooser`.

### Design intent

- Keep parameters for different auth types **separate**. The methods do not
  share flags. Subscription only takes `--device`; PAT only takes `--token`;
  API only takes `--client-id` / `--client-secret`.
- Subscription is the **default** human flow. `--device` is the
  headless/manual fallback for SSH or environments where `system_open::that`
  cannot launch a browser. Both modes use the same OAuth authorization code
  flow with PKCE and a loopback redirect; they differ only in whether the CLI
  auto-opens the URL and accepts a pasted redirect address.
- PAT and API are **non-interactive**. They support env-only invocation for
  CI/headless use and never prompt when both stdin is non-TTY and the value
  is missing.
- `Debug` is implemented manually for any struct that holds secrets (`token`,
  `client_secret`, `access_token`) so they cannot leak via `{:?}` in error
  contexts. Do not derive `Debug` on `OAuthTokenRequest` or `OAuthTokenResponse`
  in the client crate.

All methods accept the global `--org <org>`, which states the organization the
credential is expected to serve. The CLI verifies that claim against the server
before storing the credential and files it under a per-org variable, so
authenticating against a second org never displaces the first. See
[`cloud-multi-org.md`](./cloud-multi-org.md) for the storage layout and the
verification rules.

## Credential storage

After a successful login, the subscription and PAT methods call
`save_token_and_print_login_result(token, signed_in, target)` which:

1. Constructs an authenticated `CloudClient` with the **freshly obtained
   token** (`CloudClient::with_token(token)`), bypassing the normal
   environment/keychain lookup. This avoids the case where a stale
   `SPICE_SPICEAI_TOKEN` in the process env or platform keychain wins over the
   token that was just acquired.
2. Calls `merge_auth_config("SPICEAI", &[("TOKEN", token)])` to persist
   `SPICE_SPICEAI_TOKEN` to `.env.local` (preferred) or `.env`.
3. Calls `get_auth_context()` against `/api/spice-cli/auth`. If the call
   succeeds and returns an `app_api_key`, writes
   `SPICE_SPICEAI_API_KEY` to the same env file.
4. On failure, writes the token anyway and prints a yellow warning. The login
   itself is considered successful (the token is saved) but subsequent
   authenticated commands may fail. This is intentional: token mint and token
   verification are separate API calls, and we should not silently discard a
   valid token because the second call hiccupped.
5. For a subscription sign-in, also writes the refresh token beside every copy
   of the access token (`SPICE_SPICEAI_REFRESH_TOKEN`,
   `SPICE_SPICEAI_REFRESH_TOKEN_<ORG>`) and the OAuth client ID
   (`SPICE_SPICEAI_CLIENT_ID`).

Logout (`spice cloud logout`) removes credentials from the env file: by default
(`--scope active`) the credential for the organization in effect, and with
`--scope all` every stored org credential plus `SPICE_SPICEAI_TOKEN`,
`SPICE_SPICEAI_REFRESH_TOKEN`, `SPICE_SPICEAI_API_KEY`,
`SPICE_SPICEAI_CLIENT_ID`, and the OAuth client pair. It does **not** revoke
the token server-side and does not currently clear the platform keychain entry
written by other CLI commands.

### Renewal

A subscription access token expires after one hour. Before every `spice cloud`
command except `login` and `logout`,
[`cloud/refresh.rs`](../../bin/spice/src/commands/cloud/refresh.rs) renews each
stored sign-in whose token has less than 15 minutes left, which covers the
default `deploy --wait`. It renews only a token whose `client_id` claim is the
stored `SPICE_SPICEAI_CLIENT_ID`: a refresh token presented with another
client ID revokes the sign-in. It sends the refresh token to `/api/oauth/token`
and writes the new access and refresh tokens to every variable in the same
store (keychain or env file) that held the old ones, changing only those lines
of the env file. Renewal is best effort; a failed renewal leaves the old token,
and the command reports it as rejected.

A token or API login removes the refresh tokens beside the access tokens it
replaces, so renewal never pairs a refresh token with another login's token.

Spice Cloud replaces the refresh token on every use and revokes the whole
sign-in when an old refresh token is presented again. Renewals therefore take
an exclusive lock on `~/.spice/cloud-refresh.lock` and read the tokens again
under it, so concurrent commands use a refresh token once. The sign-in itself
ends 30 days after it was granted; renewal does not extend it.

## Method 1 — Subscription Login

```bash
spice cloud login                          # chooser → Subscription
spice cloud login subscription             # auto-open browser (default)
spice cloud login subscription --device    # print URL, accept a pasted redirect
```

Default human flow: the OAuth 2.0 authorization code grant with PKCE (S256)
and a loopback redirect (RFC 8252), implemented in
[`login/session.rs`](../../bin/spice/src/commands/login/session.rs). Both
modes call `execute_login_device_flow(open_browser)` with
`open_browser = !args.device`:

1. Reads the client ID from `SPICE_SPICEAI_CLIENT_ID`. Without one, registers
   a public client (no secret) at `/api/oauth/register` with the redirect URI
   `http://127.0.0.1/callback` and the CLI scopes. The person who approves the
   first sign-in owns the client.
2. Listens on `127.0.0.1` on a random port, and makes a new PKCE verifier and
   `state` for this sign-in only.
3. Builds `{portal}/oauth/authorize` with `redirect_uri`
   `http://127.0.0.1:<port>/callback`, the scopes, `state`, and the S256
   challenge. The authorization server accepts any port on a registered
   loopback URI.
4. Opens the URL (default) or prints it (`--device`). With `--device`, or
   when stdin is a terminal, the CLI also reads a line from stdin: a browser on
   another machine fails to load the loopback redirect, and the person pastes
   that page's address.
5. Takes the first request to `/callback` (or the pasted address), then closes
   the listener. Other paths get a 404 and the wait continues. Refuses a
   response whose `state` differs, and an `error=` response. Gives up after
   5 minutes.
6. Exchanges the code with the verifier at `/api/oauth/token`, then runs the
   shared post-login flow.

The CLI requests exactly the scopes its calls use:
`apps:read apps:write apps:delete deployments:read deployments:write
secrets:read secrets:write instances:write`.

User-facing characteristics:

- The person picks one organization on the consent page. `/api/spice-cli/*`
  accepts the token for that organization only, so a person who works in
  several organizations signs in once per organization with `--org`.
- An unclaimed registration expires after 15 minutes, and a client approved
  by another account is refused in the browser. In both cases the CLI waits
  until its timeout; `spice cloud logout` clears the stored client ID so the
  next sign-in registers a new one.
- Cannot be automated with env vars — a human must approve in a browser.

## Method 2 — Personal Access Token (PAT)

```bash
spice cloud login pat --token <TOKEN>
SPICE_CLOUD_PAT=... spice cloud login pat
spice cloud login pat                    # interactive prompt
```

Long-lived token issued from the Spice Cloud dashboard, scoped to the user
account. The CLI does **not** mint PATs — they must be created in the
dashboard.

Implementation:

- `PatLoginArgs::token` is `Option<String>` with `env = "SPICE_CLOUD_PAT"`.
- `execute_login_pat` calls `resolve_string_or_prompt(secret = true)`:
  - If `--token` was provided or `SPICE_CLOUD_PAT` is set: use it.
  - If stdin is a TTY: prompt for the value with
    `dialoguer::Password` (input is hidden).
  - If stdin is not a TTY: fail with
    `"PAT is required. Provide --token or set SPICE_CLOUD_PAT."`
- The token is treated as already being a Spice Cloud bearer token. There is
  no exchange step; the token is written directly via the shared post-login
  flow.

Characteristics:

- Fully automatable via env var.
- Identifies as the **owning user**, so any cloud action runs with that user's
  permissions.
- Should be revocable from the dashboard. The CLI has no logout-side
  revocation today.

## Method 3 — API Login (OAuth client credentials)

```bash
spice cloud login api \
  --client-id <ID> \
  --client-secret <SECRET>

SPICE_CLOUD_CLIENT_ID=... \
SPICE_CLOUD_CLIENT_SECRET=... \
spice cloud login api

spice cloud login api                    # interactive prompts
```

OAuth 2.0 `client_credentials` grant, intended for **service principals /
machine identities**. Unlike PAT, an API client has its own identity and
scopes that are independent of any human user.

Implementation:

- `ApiLoginArgs` has `client_id` (env `SPICE_CLOUD_CLIENT_ID`) and
  `client_secret` (env `SPICE_CLOUD_CLIENT_SECRET`). Both use
  `resolve_string_or_prompt`; client_id uses `Input` (visible), client_secret
  uses `Password` (hidden).
- `CloudClient::exchange_client_credentials` POSTs to
  `{oauth_base_url}/api/oauth/token` with
  `{ client_id, client_secret, grant_type: "client_credentials" }`. The
  request type uses borrowed `&str` fields and a `&'static str` grant type
  so the secret is not unnecessarily cloned.
- The CLI wrapper (`bin/spice/src/commands/cloud/client.rs`) validates the
  response: `token_type` must be `Bearer` (case-insensitive). Anything else
  errors out with a clear message rather than silently using a non-bearer
  token with `bearer_auth`.
- The returned `access_token` is treated like any other Spice Cloud bearer
  token and feeds into the shared post-login flow.

Characteristics:

- Fully automatable via env vars; the canonical CI path.
- Identity is the OAuth client, not a user. `get_auth_context` may return
  service-principal-shaped fields (no `app_api_key`, etc.). The post-login
  path tolerates a missing `app_api_key`.
- The token has whatever lifetime the OAuth server assigns (typically short).
  We do not currently auto-refresh; on expiry the user re-runs `spice cloud
  login api`.

## OAuth host resolution

The non-API OAuth host serves `/oauth/authorize`, `/api/oauth/register`,
`/api/oauth/token`, and `/api/spice-cli/auth`. The data-plane API base URL
(used for `/v1/apps`, `/v1/regions`, etc.) is a separate host. When building
non-API OAuth URLs, the data-plane API base URL contains an `api` segment
that must be stripped.

`CloudClient::oauth_base_url` parses with `reqwest::Url` and rewrites the
host:

- A label named exactly `api` is removed (`api.example.com` →
  `example.com`).
- A label with an `-api` suffix has the suffix trimmed
  (`foo-api.example.com` → `foo.example.com`).
- If the host has no recognizable `api` segment, the original base URL is
  returned unchanged (this is what we want for local dev / custom
  self-hosted deployments, e.g. `https://localhost:8090`).

Tests covering these cases live in `crates/spice-cloud-client/src/client.rs`
under `#[cfg(test)] mod tests`. **Add a case here whenever you touch host
rewriting.**

`SPICE_CLOUD_API_URL` (read by the CLI in
`bin/spice/src/commands/cloud/client.rs::get_base_url`) lets operators point
the CLI at a non-default cluster. The OAuth base URL is derived from
whatever that env var resolves to.

`SPICE_CLOUD_API_HEADERS` and `SPICE_CLOUD_PORTAL_HEADERS` add headers to
every request to the Cloud API host and the portal host, for example a
deployment protection bypass on a test environment. The format is
comma-separated `name=value` pairs with percent-encoded values, as in
`OTEL_EXPORTER_OTLP_HEADERS`. The values are marked sensitive and are never
sent to any other host, such as a Cloud Connect `--endpoint`.

## Wire formats

### Browser sign-in

```http
POST {oauth_base}/api/oauth/register
Content-Type: application/json

{ "client_name": "Spice CLI", "redirect_uris": ["http://127.0.0.1/callback"],
  "grant_types": ["authorization_code", "refresh_token"],
  "response_types": ["code"], "token_endpoint_auth_method": "none",
  "scope": "<CLI scopes>" }

201 Created { "client_id": "...", ... }
```

```http
GET {oauth_base}/oauth/authorize?response_type=code&client_id=...
    &redirect_uri=http://127.0.0.1:<port>/callback&scope=...&state=...
    &code_challenge=...&code_challenge_method=S256

302 → http://127.0.0.1:<port>/callback?code=...&state=...
302 → http://127.0.0.1:<port>/callback?error=access_denied&state=...
```

```http
POST {oauth_base}/api/oauth/token
Content-Type: application/json

{ "grant_type": "authorization_code", "client_id": "...", "code": "...",
  "redirect_uri": "http://127.0.0.1:<port>/callback", "code_verifier": "..." }
{ "grant_type": "refresh_token", "client_id": "...", "refresh_token": "..." }

200 OK { "access_token": "...", "token_type": "Bearer", "expires_in": 3600,
         "scope": "...", "refresh_token": "..." }
400 { "error": "invalid_grant", "error_description": "..." }
```

### Client credentials

```http
POST {oauth_base}/api/oauth/token
Content-Type: application/json

{
  "client_id": "...",
  "client_secret": "...",
  "grant_type": "client_credentials"
}

200 OK
{ "access_token": "...", "token_type": "Bearer" }
```

The CLI requires `token_type` to equal `bearer` (case-insensitive). Other
token types fail loudly.

### Auth context (post-login probe)

```http
GET {oauth_base}/api/spice-cli/auth
Authorization: Bearer <token>

200 OK
{
  "username": "...",
  "email": "...",
  "org":  { "name": "..." },
  "app":  { "name": "...", "api_key": "..." }     # both optional
}
```

The CLI flattens this into the in-memory `AuthContext` struct
(`org_name`, `app_name`, `app_api_key`) via `AuthContextRaw::into()`. For
client credentials logins, `username`/`email` may reflect the service
principal rather than a person, and `app` may be absent.

## Adding a new login method

If we ever add another method (e.g. an SSO/SAML corporate login, a
short-lived OIDC token from CI, etc.):

1. Add a new variant to `LoginMethod` with its own args struct. Keep its
   flags scoped to its own struct — **do not** add flags to the shared
   `LoginArgs`.
2. If the args struct holds a secret, implement `Debug` manually with a
   redacted field.
3. If the args use a `help_heading`, give it a unique heading
   (e.g. `"SSO Login Options"`) so Clap groups it cleanly.
4. Implement an `execute_login_<name>` function that ends by calling
   `save_token_and_print_login_result(&token, None, target)`.
5. Register the method in `execute_login` and in
   `execute_login_with_chooser` (both the `items` array and the `match`).
6. If the method can be fully automated, ensure `resolve_string_or_prompt`
   (or equivalent) returns a clear error when running non-TTY without the
   required value.
7. Add unit tests for any new credential-exchange parsing in
   `crates/spice-cloud-client`. Don't trust the server's response shape.
8. Update this doc and the `cloud login --help` text.

## Common pitfalls

- **Stale tokens in the environment**: `CloudClient::new()` resolves tokens in
  priority order (process env → keychain → env file). After a fresh login,
  always use `CloudClient::with_token(&newly_minted_token)` for the
  immediate verification call so you do not exercise a stale token from a
  higher-priority source.
- **Logging secrets**: never derive `Debug` on a struct containing
  `client_secret`, `access_token`, or PATs. Use the manual redacted impls in
  this codebase as the reference.
- **OAuth host rewriting**: a raw `replace("api.", "")` is wrong for hosts
  like `dev-api.spice.ai` or `staging.api.spice.ai`. Always parse with
  `reqwest::Url` and operate on host labels.
- **Non-bearer tokens**: don't pass an opaque token to `bearer_auth` without
  validating `token_type`. Fail loudly if the server ever returns a
  non-bearer type — silently mis-authenticating is worse than a clear error.
- **Forgetting the chooser path is interactive-only**: `dialoguer::Select`
  hangs (or worse, panics) on a non-TTY stdin. The chooser must be gated on
  `IsTerminal` and emit a friendly error otherwise.

## Related code

- [bin/spice/src/commands/cloud/mod.rs](../../bin/spice/src/commands/cloud/mod.rs) — Login subcommands, chooser, prompts, post-login flow.
- [bin/spice/src/commands/cloud/client.rs](../../bin/spice/src/commands/cloud/client.rs) — CLI-side `CloudClient` wrapper, base URL resolution, token resolution, `Bearer` validation.
- [bin/spice/src/commands/login/session.rs](../../bin/spice/src/commands/login/session.rs) — Browser sign-in: registration, PKCE, loopback listener, code exchange, refresh grant.
- [bin/spice/src/commands/cloud/refresh.rs](../../bin/spice/src/commands/cloud/refresh.rs) — Renewal of stored browser sign-ins.
- [crates/spice-cloud-client/src/client.rs](../../crates/spice-cloud-client/src/client.rs) — HTTP client: client-credentials exchange, OAuth host rewriting, response handling.
- [crates/spice-cloud-client/src/types.rs](../../crates/spice-cloud-client/src/types.rs) — Wire types: `OAuthTokenRequest`, `OAuthTokenResponse`, `AuthContext`.
- [bin/spice/src/commands/login/mod.rs](../../bin/spice/src/commands/login/mod.rs) — Generic `spice login` (data-source credentials), shares `merge_auth_config`.
