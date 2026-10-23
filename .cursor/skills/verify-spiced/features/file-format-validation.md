# Unknown `file_format` is a configuration error

## Intent

A typo such as `cvs` or `parqet` must fail dataset setup. It must not become a 1-row document table (`location`, `content`) or a Ready dataset that then fails at query time. Document formats (`md`, `txt`, `pdf`, `docx`, `pptx`, `xlsx`) stay on the text/document path.

## Launch

A spicepod with a small CSV (a few rows is enough; do not generate 200k rows) and:

```yaml
datasets:
  - from: file:$CSV
    name: typo_cvs
    params: { file_format: cvs }
  - from: file:$CSV
    name: typo_parqet
    params: { file_format: parqet }
```

## Doctor

`/health` must answer. Datasets are expected to be `Error`, so `/v1/ready` may stay unready.

## Drive

`GET /v1/datasets?status=true`. Both datasets must be `Error`. The `error_message` must name the invalid format, list valid formats, and include a `Did you mean` hint (`csv` / `parquet`).

## Evidence

Paste the `error_message` text (no file contents). On trunk the same command shows `Ready` (or a query-time UTF-8 failure) — that is the before.

## Cleanup

Kill the recorded `spiced` PID.
