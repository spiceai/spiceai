# Unknown `file_format`: near-miss error, otherwise text-table fallthrough

## Intent

A near-miss typo such as `cvs` or `parqet` must fail dataset setup. It must not become a 1-row document table (`location`, `content`) or a Ready dataset that then fails at query time.

Any other unknown format (`log`, `xml`, `yaml`) keeps trunk's plain-text table fallthrough: the dataset goes Ready and the log has a WARN. Document formats (`md`, `txt`, `pdf`, `docx`, `pptx`, `xlsx`) stay on the text/document path.

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
  - from: file:$CSV
    name: fmt_log
    params: { file_format: log }
```

## Doctor

`/health` must answer. `typo_cvs` / `typo_parqet` are expected to be `Error`, so `/v1/ready` may stay unready. `fmt_log` must be `Ready`.

## Drive

`GET /v1/datasets?status=true`.

- `typo_cvs` and `typo_parqet` must be `Error`. The `error_message` must name the invalid format, list valid formats, and include a `Did you mean` hint (`csv` / `parquet`).
- `fmt_log` must be `Ready`. The `spiced` log must contain a WARN that names `file_format` `'log'` and says the dataset is loaded as a plain-text table.

## Evidence

Paste the `error_message` text (no file contents) and the `fmt_log` status plus the WARN line. On trunk the same command shows all three `Ready`.

## Cleanup

Kill the recorded `spiced` PID.
