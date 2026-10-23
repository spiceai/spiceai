# Secret-derived values are not echoed in validation errors

## Intent

When a validated param (enum/bool/int/…) is built from a secret reference, the ERROR log and `GET /v1/datasets?status=true` `error_message` must name the param and the secret reference (for example `${ env:PG_PW }`). They must never contain the resolved value.

Secret-derived typed-param parse failures must not surface parser text at all (`invalid value for \`<key>\` from ${store:key}; expected a valid value`). That covers a `FromStr` that trims or lowercases and would otherwise echo a normalized form (`Found secret123` from `" Secret123 "`), and a parser that echoes only a substring of a partial interpolation (`client_timeout: 1e${ env:SECRET }` → duration_parse `Exponent magnitude 1234`). When any part of a param value is secret-derived, never surface parser or echoed text.

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
3. For a partial interpolation (`client_timeout: 1e${ env:SECRET }` with `SECRET=1234`), the error must be the generic `invalid value for \`client_timeout\` from ${ env:SECRET }` form. It must not contain `1234`, `1e1234`, or `Exponent`.

## Evidence

Paste only the canary grep and the `error_message` field. On trunk the same command shows `Found CANARYPGPW7731`.

## Cleanup

Kill the recorded `spiced` PID. Delete the temp `.env`.
