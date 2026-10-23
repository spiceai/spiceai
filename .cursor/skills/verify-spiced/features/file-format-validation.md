# Unknown `file_format`: structured typo error, otherwise text-table fallthrough

## Intent

A near-miss typo of a structured listing format (`cvs`→`csv`, `parqet`/`parquett`→`parquet`, `jsno`→`json`) must fail dataset setup. It must not become a 1-row document table (`location`, `content`) or a Ready dataset that then fails at query time.

Common real text and source extensions (`tex`, `org`, `json5`, `jsonc`, `mdx`, `css`, `ts`, `psv`, and the rest of the allowlist) keep trunk's plain-text table path: the dataset goes Ready. Other unknown formats (`log`, `xml`, `yaml`) do the same, with a WARN. A document-format near-miss (`mardown`) also stays Ready; the WARN includes `Did you mean`.

`file_format: text` plus `file_extension: .tex` lists `*.tex` files. Document and text formats honor `file_extension`.

## Launch

A spicepod with a small CSV (a few rows is enough; do not generate 200k rows), a directory of one-line text files (`notes.tex`, `notes.org`, `notes.json5`, `notes.jsonc`, `notes.mdx`, `notes.css`, `notes.ts`, `notes.psv`), and:

```yaml
datasets:
  - from: file:$CSV
    name: typo_cvs
    params: { file_format: cvs }
  - from: file:$CSV
    name: typo_parqet
    params: { file_format: parqet }
  - from: file:$CSV
    name: typo_jsno
    params: { file_format: jsno }
  - from: file:$CSV
    name: typo_parquett
    params: { file_format: parquett }
  - from: file:$CSV
    name: fmt_log
    params: { file_format: log }
  - from: file:$DOCS
    name: fmt_tex
    params: { file_format: tex }
  - from: file:$DOCS
    name: fmt_org
    params: { file_format: org }
  - from: file:$DOCS
    name: fmt_json5
    params: { file_format: json5 }
  - from: file:$DOCS
    name: fmt_jsonc
    params: { file_format: jsonc }
  - from: file:$DOCS
    name: fmt_mdx
    params: { file_format: mdx }
  - from: file:$DOCS
    name: fmt_css
    params: { file_format: css }
  - from: file:$DOCS
    name: fmt_ts
    params: { file_format: ts }
  - from: file:$DOCS
    name: fmt_psv
    params: { file_format: psv }
  - from: file:$DOCS
    name: text_tex
    params: { file_format: text, file_extension: .tex }
```

## Doctor

`/health` must answer. The typo datasets are expected to be `Error`, so `/v1/ready` may stay unready. The `fmt_*` and `text_tex` datasets must be `Ready`.

## Drive

`GET /v1/datasets?status=true`.

- `typo_cvs`, `typo_parqet`, `typo_jsno`, and `typo_parquett` must be `Error`. The `error_message` must name the invalid format, list valid formats, and include a `Did you mean` hint (`csv` / `parquet` / `json` / `parquet`).
- `fmt_log` must be `Ready`. The `spiced` log must contain a WARN that names `file_format` `'log'` and says the dataset is loaded as a plain-text table.
- `fmt_tex`, `fmt_org`, `fmt_json5`, `fmt_jsonc`, `fmt_mdx`, `fmt_css`, `fmt_ts`, and `fmt_psv` must be `Ready`.
- `text_tex` must be `Ready`. A query over it must return the `notes.tex` object and must not return `notes.org` / `notes.mdx` / the other extensions.

## Evidence

Paste the typo `error_message` text (no file contents), the Ready statuses, the `fmt_log` WARN line, and the `text_tex` query rows (locations only). On trunk the typo datasets are also `Ready`.

## Cleanup

Kill the recorded `spiced` PID.
