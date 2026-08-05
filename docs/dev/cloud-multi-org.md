# `spice cloud` — Organization Context and Command Surface

Internal reference for how the CLI decides **which Spice Cloud organization a
command acts on**, where per-org credentials live, and which parts of the
multi-org experience still depend on Cloud API work.

> Audience: Spice CLI / Spice Cloud engineers. For the login methods themselves,
> see [`cloud-login.md`](./cloud-login.md).

## Why there is an org context at all

Spice Cloud binds a management token to **one organization at mint time**. A
person who belongs to both a personal org (`lukekim`) and a company org
(`spicehq`) therefore has two different credentials, and every management route
(`/v1/apps`, deployments, secrets, logs) answers for whichever org the presented
credential belongs to.

That makes "which org?" a first-class piece of CLI state rather than a detail of
the app name. The CLI models it in three layers:

| Layer | Where it lives | Scope |
| ----- | -------------- | ----- |
| App-qualified org | `<org>/<app>` argument | One command |
| Per-invocation override | `--org` | One command |
| Per-shell override | `SPICE_CLOUD_ORG` | The shell and its children |
| Linked app | `.spice/cloud.json` | One directory |
| Active org | `~/.spice/cloud-context.json` | Machine-wide, survives new shells |

**Prefer `SPICE_CLOUD_ORG` for scripts and CI.** A machine-wide active org is
shared mutable state: it is retroactive across already-running shells, invisible
at the call site, and on a shared CI runner concurrent jobs will fight over it.
`org use` is a convenience for interactive single-org work; the env var is
scoped to the process that set it, which is what makes it safe to compose.
`spice cloud link` writes `.spice/cloud.json` and adds `.spice` to the
repository's `.gitignore`, because a committed link file retargets every
teammate who does not share that org.

## Precedence

The standard CLI configuration ladder — flags beat the environment, which beats
project config, which beats user config ([clig.dev](https://clig.dev/#configuration)):

1. the org in an `<org>/<project>` argument (`--project spicehq/team-app`);
2. `--org`;
3. `SPICE_CLOUD_ORG`;
4. the org recorded by `spice cloud link` in `.spice/cloud.json` (directory);
5. the persisted active org from `spice cloud org use` (user);
6. the credential's own org — no org is sent, and the server decides.

**Two explicit signals are never silently ranked.** If an `<org>/<project>` argument
and `--org` name different orgs — or a linked directory disagrees with an
explicit `--org`/`SPICE_CLOUD_ORG` — the command fails with `org_conflict` and
names both. Levels 4 and 5 are standing defaults, so any explicit signal
overrides them without complaint.

That asymmetry is the whole point: a wrong-organization deploy cannot be undone
by re-reading the scrollback, and a warning is invisible in `--machine` output.
`gh` adopted the same rule after implicit selection confused users
([cli/cli#6777](https://github.com/cli/cli/discussions/6777)); the CLIs that
silently pick instead have documented wrong-target incidents.

Implemented in `resolve_app_target_with_source` and `ensure_orgs_agree` in
`bin/spice/src/commands/cloud/mod.rs`, with tests covering each case.

`spice cloud whoami` is the single source of truth for what a command will do:
it prints the org in effect and where that choice came from. Mutating commands
(`deploy`, `project delete`, `secrets set`) echo the same provenance before acting.

## Credential storage

Credentials keep the existing storage backends (process env → platform keychain
→ `.env.local`/`.env`), with one addition: an org-scoped credential is filed
under a **per-org variable name**.

| Credential | Variable |
| ---------- | -------- |
| Default management token | `SPICE_SPICEAI_TOKEN` |
| Default app API key | `SPICE_SPICEAI_API_KEY` |
| Org-scoped management token | `SPICE_SPICEAI_TOKEN_<ORG>` |
| Org-scoped app API key | `SPICE_SPICEAI_API_KEY_<ORG>` |

`<ORG>` is a **reversible** encoding: ASCII alphanumerics uppercase (org names
compare case-insensitively everywhere else), and every other legal character
becomes `_XX` with its hex code. So `spice-hq` → `SPICE_2DHQ`, `spice.hq` →
`SPICE_2EHQ`, `spice_hq` → `SPICE_5FHQ`. A lossy mapping would let one org's
credential overwrite another's and then be sent to the wrong organization;
`encode_org`/`decode_org` round-trip every name `validate_org_name` accepts, and
a test enforces it.

`org::org_token_var` derives the read name; `credential_key` derives the write
name for `merge_auth_config`. Tests assert they agree for both the token and the
API key — if they drift, a login appears to succeed and every later command
fails as unauthenticated.

**Lookup fails closed, but only against the wrong org.** The invariant is
*never use one organization's token for another* — not *never use the default
token*. Naming an org resolves in this order:

1. a credential stored for that org;
2. otherwise the default credential, **only when the server confirms it belongs
   to that same org**;
3. otherwise `org_credential_missing`, naming both the requested org and the
   one the default credential actually belongs to.

Step 2 is what keeps the ordinary single-credential path working: a user who
ran `spice cloud login` and then names their own org is not asked to
re-authenticate. A service-account credential has no user identity to check
against; its org is fixed by the OAuth client that issued it and the server
authorizes every request, so it is allowed through rather than blocked on a
check that cannot be performed.

`spice cloud login` also files the credential under **its own** org, so step 2's
verification round trip happens at most once per machine.

The app API key follows the same rule: naming an org reads only that org's
key. Only a command that names no org reads the default one.

`spice cloud login --org <org>` **verifies before storing**. Spice Cloud fixes a
token's org at mint time, so `--org` is a claim to check, not a setting to
apply:

- user token whose identity reports a different org → fails with `wrong_org`
  and says where to mint the right credential;
- service-account token (no user identity) → probed against
  `/api/spice-cli/auth?org_name=<org>`, which the server rejects for a
  non-member.

`spice cloud logout` defaults to `--scope active`, which discards only the
credential for the org in effect; `--scope all` discards every stored org
credential plus the default one.

## Wire format

Every authenticated request carries the org it acts on:

```http
GET /v1/apps
Authorization: Bearer <token>
X-Org-Name: spicehq
```

The header states intent. **Membership is enforced server-side on every
request** — selecting an org locally never grants access a credential does not
already have. A token minted for a single org simply ignores the header.

Org names are validated (`[A-Za-z0-9._-]`, ≤ 64 chars) before they reach a URL,
a header, or an environment variable name, so an unusable value fails locally
instead of being silently dropped from a request.

## Machine mode

`--machine` (or `-o json`) makes every listing and status JSON, and turns errors
into a structured envelope on **stderr**:

```json
{
  "status": "error",
  "error": {
    "code": "project_not_found",
    "message": "Project 'spicehq/team-app' was not found. …",
    "hint": "Run 'spice cloud orgs' to list your organizations, then 'spice cloud org use <org>'."
  }
}
```

`code` is a stable contract — scripts branch on it, so renaming one is a
breaking change. The set lives in `CloudErrorCode` (`bin/spice/src/error.rs`):

| Code | Meaning |
| ---- | ------- |
| `not_authenticated` | No credential available locally |
| `token_expired` | Credential rejected (401) |
| `forbidden` | Valid credential, action not permitted (403) — usually a missing role or scope |
| `org_not_found` | Named org does not exist or is invisible |
| `org_forbidden` | Not a member of the requested org — emitted **only** by the membership probe, never inferred from a 403 |
| `org_conflict` | Two explicit signals named different orgs |
| `org_credential_missing` | An org was named, but no credential is bound to it |
| `project_not_found` | No such project in the org being acted on |
| `wrong_org` | Project exists, but under a different visible org |
| `deploy_conflict` | A deployment is already in flight (409) |
| `deploy_failed` | Deployment reached a terminal failed status |
| `deploy_timeout` | `--wait` elapsed; the deployment is **still running** and may yet succeed |
| `not_found` / `conflict` / `api_error` / `invalid_request` | Generic fallbacks |

`deploy_failed` and `deploy_timeout` are deliberately distinct: a script gating
on exit code must be able to tell "it broke" from "I stopped watching."

**Exit codes**: authentication failures (`not_authenticated`, `token_expired`,
`org_credential_missing`) exit **4**, so automation can re-authenticate and
retry without parsing the message; every other failure exits 1.

Human-readable diagnostics go through `tracing`, which this CLI configures to
write to **stdout**. Machine mode is the contract for scripting.

## Command surface

One grammar, matching the rest of the CLI: a **plural noun lists**
(`spice cloud projects`, like `spice datasets`), a **singular noun manages one**
(`spice cloud project create`, like `spice dataset`). `spice cloud status` is
the aggregated health view, mirroring `spice status` for a local runtime.

Superseded spellings — `create`/`get`/`update`/`delete <noun>`, `inspect`,
`instance`, `apps`, `--app` — are hidden from help but still parse. Each warns
once and delegates to its replacement; the replacement holds the
implementation, because two implementations of one command is how they drift
apart. They are removable in a later release.

## Diagnosing a deploy without the portal

```bash
spice cloud org use spicehq
spice cloud deploy  --project spicehq/team-app --wait --timeout 10m
spice cloud status  --project spicehq/team-app          # the whole picture, one command
spice cloud status  --project spicehq/team-app --instance <name>
spice cloud deployments --project spicehq/team-app      # id, status, commit, error
spice cloud datasets    --project spicehq/team-app      # every dataset's load state
spice cloud logs --project spicehq/team-app --level error --tail 200
```

`deploy --wait` polls the deployment's real status with backoff (2 s → 15 s)
rather than sleeping a fixed interval, and exits non-zero with `deploy_failed`
on a terminal failure, so it can gate a script. Statuses the CLI does not
recognize are treated as **still running**: waiting longer is recoverable,
declaring an in-flight deploy finished is not.

`spice cloud status` and `spice cloud datasets` reach the project's data plane rather than the management
API, because the management API does not expose runtime state. The CLI resolves
the app's region and API key through the management API and then calls the
regional data endpoint. These commands therefore need an app API key to exist;
`P6` below would remove that hop.

### Instance targeting

An app runs one or more instances (replicas), and the data endpoint
load-balances across them. **Which instance answers is part of the question**:

| Invocation | Header | Answered by |
| ---------- | ------ | ----------- |
| `status --project org/project` | none | the project's general endpoint — the deployment as a whole |
| `status --project org/project --instance <name>` | `SCP-Target-Instance: <name>` | that one instance |

Unpinned is the default because an arbitrary replica is not a useful answer: a
healthy one reports Ready while a sick one beside it is why the app is down.
Pinning is how you find the sick one. Every command prints which scope answered,
so "Ready" is never ambiguous between one replica and the whole deployment.

`spice cloud status` enumerates instances from `GET /v1/spice_runtime`
(`status.podStatuses`), reporting each instance's status, phase, deployment, and
start time, plus a serving count.

Instance status uses the **same six-state vocabulary as the Spice Cloud portal**
— `deploying`, `loading`, `ready`, `unhealthy`, `terminating`, `failed` — with
the portal's resolution order: terminating, then failed, then deploying (spiced
not reporting yet), then ready/unhealthy, then loading. The portal documents its
derivation as the single source of truth and warns against re-deriving it
inline, because ad-hoc versions drifted and labelled the same pod state
differently per surface; `InstanceStatus::status` mirrors it deliberately and is
covered by tests. **If the portal's rule changes, change this with it.**
`loading` is a real phase in Spice — the runtime loads its initial datasets
before becoming ready — so it is reported as its own state, not as "not ready".

### Routes

The three commands use **different** routes, and this matters: `/v1/status`
reports connection endpoints only (`http`, `flight`, `metrics`,
`opentelemetry`) and never datasets. Dataset state comes from
`/v1/datasets?status=true` — the `status`, `error`, and `error_message` fields
are populated only when that query parameter is set. Instance inventory comes
from `/v1/spice_runtime`. All responses are status-checked before
deserializing, so an unauthorized or errored reply cannot decode to an empty
list and read as "this app has no datasets."

## What still depends on Cloud API work

The CLI is complete and degrades honestly, but three capabilities cannot be
finished client-side. Each has a defined client behavior today and lights up
without further CLI changes once the API lands.

| Need | Endpoint | CLI behavior until then |
| ---- | -------- | ----------------------- |
| Enumerate a user's orgs | `GET /v1/orgs` | A 404 is treated as "cannot enumerate", not "no orgs". `spice cloud orgs` falls back to the credential's own org plus any org with a stored credential, and prints a note explaining the listing is partial. `whoami` omits the org count. |
| One credential across orgs | `X-Org-Name` honored on management routes, with a membership check | The header is already sent on every request. Until it is honored, acting on a second org requires a credential minted for that org (an org-owned OAuth client works today via `spice cloud login api --org <org>`). |
| Instance status / logs via management API | e.g. instance health on `/v1/projects/{id}/…` | `spice cloud status` uses the data plane with the project's API key. |

`spice cloud orgs` marks each row's `CREDENTIAL` column `stored` when the org has
its own credential, which is how an operator can tell a fully-working org from
one that is merely known by name.

## Adding a command that acts on an app

1. Take `flag_org: Option<&str>` and resolve with
   `resolve_app_target(args.app.as_deref(), flag_org)` — never read `--app`
   directly, or the command will ignore `--org`, the link file, and the active
   org. If the command **mutates** anything, use
   `resolve_app_target_with_source` and call `announce_target` first, so the
   operator sees which org is about to be changed and why.
2. Build the client with `connect_for_target(&target)` so the request carries
   the target's org and uses that org's credential.
3. Pass `&AppTarget` to the client method. App-scoped `CloudClient` methods take
   a target rather than a string precisely so a caller cannot skip resolution.
4. Add the command to `apply_machine_cloud_mode` and `is_json_output` in
   `bin/spice/src/main.rs`, or `--machine` will silently not apply to it.
5. Report failures with `Error::cloud_with_hint` and a code from the table
   above; a bare message is not actionable for an agent.

## Related code

- [`bin/spice/src/commands/cloud/org.rs`](../../bin/spice/src/commands/cloud/org.rs) — active org, per-org credentials, org-name validation.
- [`bin/spice/src/commands/cloud/mod.rs`](../../bin/spice/src/commands/cloud/mod.rs) — command surface, precedence, deploy waiting, runtime inspection.
- [`bin/spice/src/commands/cloud/client.rs`](../../bin/spice/src/commands/cloud/client.rs) — `AppTarget`, app resolution, error-code mapping.
- [`crates/spice-cloud-client/src/client.rs`](../../crates/spice-cloud-client/src/client.rs) — `X-Org-Name`, `GET /v1/orgs`, org-scoped auth context.
- [`bin/spice/src/error.rs`](../../bin/spice/src/error.rs) — `CloudErrorCode`.
