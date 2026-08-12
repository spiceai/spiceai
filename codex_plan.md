# Codex plan

## Big goal

Codex talks to Spice.

Codex sends `POST /v1/responses`.
Spice accepts it.
Spice sends it to OpenAI.

## Two PRs

1. async-openai PR: <https://github.com/spiceai/async-openai/pull/41>
2. Spice PR: this branch.

First PR updates the async-openai fork.
Second PR makes Spice use it.

Do not merge Spice PR before async-openai PR merges.

## async-openai PR

Spice branch must get all of upstream `main`.
Then Spice-specific code must stay.

The rule:

```text
upstream -> async-openai main
async-openai main -> async-openai spiceai
async-openai spiceai -> Spice repo
```

PR #41 does the middle step.

It keeps Spice things:

- Azure API key auth.
- Azure Entra token auth.
- OpenAPI schema derives.
- Retry on rate limit.
- Read `Retry-After`.
- Do not spam logs on rate limit.
- Keep `OpenAIError::ApiError(ApiError)` shape. Spice uses this everywhere.
- Make WASM build work. Tokio `fs` must not go into WASM build.

Fork has upgrade instructions here:

```text
.agents/skills/upgrade-async-openai/
```

Use that skill next time. Do not skip the two steps above.

## Spice changes now

Spice points at async-openai PR #41 commit:

```text
605ffc7e9aa2963a977b69da882d022c2889dea0
```

This is temporary. After #41 merges, change this to the final `spiceai`
branch SHA.

Changes in Spice:

- `Cargo.toml` points at new async-openai.
- `Cargo.lock` updated.
- HTTP accepts compressed request bodies.
- Compressed body gets decoded before body-size limit check.
- Responses adapter knows new fields:
  - `namespace` on function calls.
  - `phase` on assistant messages.
  - `defer_loading` on tools.
- Function-call output does **not** have `namespace`. Do not add it.
- JSON schema is now required. Do not wrap it in `Some(...)`.

## Still need do

1. Fix compile errors from new async-openai types.
2. Add tests:
   - namespace tool;
   - gzip request to `/v1/responses`;
   - streaming tool call.
3. Test Spice.
4. After #41 merges, pin final async-openai `spiceai` SHA.
5. Make Codex plan auth go through Spice.

Do not run many Cargo commands at once. Cargo locks the build folder. Big builds are slow.

Fork tests that passed:

```bash
cargo check -p async-openai --features responses
cargo test -p async-openai --features response-types \
  --test responses_input_item_serde --test responses_output_to_input
cargo test -p async-openai --features responses --lib
```

WASM CI failed before. That was real. It was fixed in #41.
To test WASM locally:

```bash
rustup target add wasm32-unknown-unknown
cargo build -p async-openai --no-default-features \
  --features byot,middleware --target wasm32-unknown-unknown
```

## Run Codex with Spice now

### 1. Make Spice model

Make `spicepod.yaml`:

```yaml
models:
  - name: spice-codex
    from: openai:gpt-5.3-codex
    params:
      openai_api_key: ${ secrets:OPENAI_API_KEY }
      responses_api: enabled
```

Start Spice:

```bash
spiced
```

`spice-codex` is important. Codex must use the same name.

### 2. Make Codex config

Put this in `~/.codex/config.toml`:

```toml
model_provider = "spice"
model = "spice-codex"

[model_providers.spice]
name = "Local Spice"
base_url = "http://127.0.0.1:8090/v1"
wire_api = "responses"
env_key = "SPICE_CODEX_API_KEY"
request_max_retries = 4
stream_max_retries = 5
stream_idle_timeout_ms = 300000
```

If Spice has API-key auth:

```bash
export SPICE_CODEX_API_KEY='your Spice API key'
codex
```

If Spice has no API-key auth, remove this line:

```toml
env_key = "SPICE_CODEX_API_KEY"
```

### 3. Test Spice first

```bash
curl http://127.0.0.1:8090/v1/responses \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $SPICE_CODEX_API_KEY" \
  -d '{
    "model": "spice-codex",
    "input": "Reply with exactly: ready"
  }'
```

No Spice auth? Remove the `Authorization` line.

If curl says `ready`, run:

```bash
codex
```

This way needs an OpenAI API key in Spice. Codex does not need the key.

## Use Codex plan later

Codex plan has a bearer header. Codex sends it to a custom gateway.

Test saw these headers:

```text
authorization
chatgpt-account-id
originator
user-agent
accept
content-type
session-id
thread-id
x-client-request-id
x-codex-beta-features
x-codex-turn-metadata
x-codex-window-id
```

Spice does not pass these headers upstream now. Spice uses
`openai_api_key` now.

Future work:

```text
Codex plan header -> Spice -> Codex/ChatGPT upstream
```

Send all headers above. Do not send `host` or `content-length`. New HTTP
request makes those.

Do not log `authorization`.

This is not the normal OpenAI API key path. Do not send Codex plan bearer
header to `api.openai.com`.

## Before done

- #41 merged.
- Spice has final fork SHA.
- Spice builds/tests pass.
- Plain Responses request works.
- Streaming works.
- Namespace tool works.
- Compressed request works.
