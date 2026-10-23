# Env secret store precedence and `file_path` isolation

## Intent

The last-declared secret store wins for `${ secrets:KEY }`. A store with `params.file_path` loads only that file and does not inherit project `.env` / `.env.local`. The default env store (no `file_path`) still lets the process environment beat `.env.local`, which beats `.env`.

## Launch

Working directory with:

```
.env            → SPICE_TEST_KEY=from_dotenv
.env.local      → SPICE_TEST_KEY=from_dotenv_local
custom.env      → SPICE_TEST_KEY=from_custom
```

Spicepod uses `runtime.auth.api-key.keys: ["${…}"]` so the accepted `X-API-Key` is the value that won. See the uploaded repro `03_secret_precedence_*.sh`.

## Doctor

`/health` must answer. `/v1/ready` is optional; auth is the signal.

## Drive

For each scenario, probe `from_process`, `from_dotenv_local`, `from_dotenv`, `from_custom` via `X-API-Key` against `GET /v1/datasets`. The first `200` is the winner.

Required outcomes after the fix:

- S3 `[env, custom]` + `${ secrets:spice_test_key }` → `from_custom`
- S5 same + `${ custom:SPICE_TEST_KEY }` → `from_custom`
- S6 same + process `SPICE_TEST_KEY=from_process` + `${ custom:… }` → `from_custom`
- S8 only `{from: env, name: custom, params: {file_path: custom.env}}` + `${ custom:… }` → `from_custom`

S2 (default store + process env, no `file_path`) remains `from_process`.

## Evidence

Print `$label => accepted key: $win` only. Never print a real secret.

## Cleanup

Kill the recorded `spiced` PID after each scenario.
