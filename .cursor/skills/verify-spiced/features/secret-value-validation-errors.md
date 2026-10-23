# Secret-derived values are not echoed in validation errors

## Intent

When a validated param (enum/bool/int/…) is built from a secret reference, the ERROR log and `GET /v1/datasets?status=true` `error_message` must name the param and the secret reference (for example `${ env:PG_PW }`). They must never contain the resolved value.

Use only the canary `CANARYPGPW7731`. Never a real secret.

## Launch

```
.env → PG_PW=CANARYPGPW7731
```

```yaml
secrets:
  - from: env
    name: env
datasets:
  - from: file:$CSV
    name: leak
    params:
      file_format: csv
      csv_has_header: ${ env:PG_PW }
```

## Doctor

`/health` must answer. The dataset is expected to be `Error`.

## Drive

1. `grep CANARYPGPW7731` the `spiced` log — must be empty after the fix.
2. `GET /v1/datasets?status=true` — `error_message` must not contain `CANARYPGPW7731`, and must contain `csv_has_header` and `${ env:PG_PW }` (spacing may match the spicepod).

## Evidence

Paste only the canary grep and the `error_message` field. On trunk the same command shows `Found CANARYPGPW7731`.

## Cleanup

Kill the recorded `spiced` PID. Delete the temp `.env`.
