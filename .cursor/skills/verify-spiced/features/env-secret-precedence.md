# Env secret store precedence, `file_path` isolation, and default `.env` export

## Intent

The last-declared secret store wins for `${ secrets:KEY }`. A store with `params.file_path` loads only that file and does not export it. The default env store (no `file_path`) still lets the process environment beat `.env.local`, which beats `.env`.

Project `.env` / `.env.local` are exported into the process environment without overwrite **before** any store initializes, so AWS STS, Kubernetes, and other `std::env` readers see those values even when `env` is not first — or is absent (AWS-only / Kubernetes-only spicepods).

## Launch

Working directory with:

```
.env            → SPICE_TEST_KEY=from_dotenv
.env.local      → SPICE_TEST_KEY=from_dotenv_local
custom.env      → SPICE_TEST_KEY=from_custom
```

Spicepod uses `runtime.auth.api-key.keys: ["${…}"]` so the accepted `X-API-Key` is the value that won. See the uploaded repro `03_secret_precedence_*.sh`.

For the dotenv-export cases, also put dummy AWS-shaped markers in `.env` (never a real secret) and use a spicepod that lists `aws_secrets_manager` before `env`, `aws_secrets_manager` alone, or `kubernetes` alone.

## Doctor

`/health` must answer. `/v1/ready` is optional; auth is the signal.

## Drive

For each precedence scenario, probe `from_process`, `from_dotenv_local`, `from_dotenv`, `from_custom` via `X-API-Key` against `GET /v1/datasets`. The first `200` is the winner.

Required outcomes after the fix:

- S2 (default store + process env, no `file_path`) remains `from_process`
- S3 `[env, custom]` + `${ secrets:spice_test_key }` → `from_custom`
- S5 same + `${ custom:SPICE_TEST_KEY }` → `from_custom`
- S6 same + process `SPICE_TEST_KEY=from_process` + `${ custom:… }` → `from_custom`
- S8 only `{from: env, name: custom, params: {file_path: custom.env}}` + `${ custom:… }` → `from_custom`

Dotenv-export cases (dummy markers only). `/proc/PID/environ` is the exec-time snapshot and will not show a later `.env` export — do not use it as the signal.

- AWS-before-env / AWS-only: build `spiced` with `aws-secrets-manager`. STS `GetCallerIdentity` during store init must see the dummy `.env` access-key marker (`AKIAENVEXPORTTEST`). Class the log as `used-env-creds` (invalid dummy key) rather than `missing-creds`.
- Kubernetes-only: the store still initializes (and fails without a cluster) after the prelude export. Unit tests assert the dummy keys are in process env via `std::env::var`.
- file_path-only: the custom file stays private; the default `.env` is still exported for `std::env` readers.

## Evidence

Print `$label => accepted key: $win` only. Never print a real secret. For export cases, print `dummy_aws_key_in_log` / `sts_error_class` only.

## Cleanup

Kill the recorded `spiced` PID after each scenario.
