---
name: verify-spice-search-cli
description: Drive a built `spice` binary against a local mock of POST /v1/search to prove the search CLI is scriptable — positional query, scoping flags in the request body, TTY-free JSON, `help` as a query term, and non-zero errors on stderr. Use when changing `bin/spice/src/commands/search.rs`, search help text, or the CLI search spinner.
---

# Verify `spice search` is scriptable

Library tests in `bin/spice/src/commands/search.rs` and `bin/spice/tests/cli_integration.rs` cover clap parsing, the request body, and the binary. This skill proves the same contract the way a user scripts it: a built `spice` binary, no TTY, piped JSON through `jq`, against a mock of `POST /v1/search`.

It does not start `spiced`. Search is an HTTP client; the mock is the runtime.

## Build

```bash
CC=cc CXX=c++ cargo build -p spice
```

## Run

```bash
.agents/skills/verify-spice-search-cli/helpers/verify-spice-search-cli.sh target/debug/spice
```

The helper:

1. Starts a Python mock of `POST /v1/search` on an ephemeral port (override with `SEARCH_MOCK_PORT`), logging each request body.
2. Runs each case below against that mock with stdout as a pipe (no TTY).
3. Stops the pid it started (never `pkill`) and removes the work directory unless `KEEP=1`.

## Expected

| case | expected |
|---|---|
| `spice search dolphins -o json` | exit 0, stdout is one JSON object (`results` array), no spinner bytes, `jq` can parse it |
| `spice search help -o json` | request body `text` is `help`, not the help screen |
| `spice search dolphins --datasets docs,wiki --additional-columns title,url --where "status = 'open'" --keywords mammal,cetacean --limit 5 -o json` | those fields are in the JSON body; no `model` field |
| `spice search -o json` with empty stdin | exit 2, message on stderr, empty stdout |
| `spice search dolphins -o json` against HTTP 500 | exit 1, message on stderr, empty stdout |
| `spice search dolphins --endpoint http://127.0.0.1:1 -o json` | exit 1, message on stderr |
| `spice search --help` | mentions `QUERY`, `--datasets`, `--where`, `--keywords`, `--additional-columns`, `--limit`; does not mention `--model`, `validate_cloud_region_usage`, or REPL-only scoping |
| `echo dolphins \| spice search -o json \| jq -e '.results'` | `jq` exit 0 |

## Proof

Run the helper against the binary built before the change and the one after, and keep both outputs. A compile or the unit tests alone are not proof.
