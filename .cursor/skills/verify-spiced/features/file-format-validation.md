# Unknown `file_format`: listing-confirmed typo error, otherwise text-table fallthrough

## Intent

A near-miss of a structured listing format is a configuration error only when the listing confirms a typo: no object under the dataset path ends in `.{claimed}`, and at least one object ends in the suggested format's suffix (`cvs` over `.csv` files, `parqet` over `.parquet` files). That must fail dataset setup. It must not become a 1-row document table (`location`, `content`) or a Ready dataset that then fails at query time.

No allowlist is complete. Real text and source extensions near a structured format (`cs`, `csx`, `rc`, `jsonld`, and the rest of Codex r4) keep trunk's plain-text table path when matching files exist. `cvs` over a folder that only has `.cvs` files does the same. Other unknown formats (`log`, `xml`, `yaml`) stay Ready, with a WARN. A document-format near-miss (`mardown`) also stays Ready; the WARN includes `Did you mean`. When listing is unavailable, degrade to WARN and load as text. Never hard-error without listing evidence.

The allowlist (`tex`, `org`, `json5`, `jsonc`, `mdx`, `css`, `ts`, `psv`, …) is a fast path that skips the `Did you mean` hint.

`file_format: text` plus `file_extension: .tex` lists `*.tex` files. Document and text formats honor `file_extension`.

## Launch

A spicepod with a small CSV, a dummy `.parquet` file (suffix evidence only; the format check runs before parse), a directory of one-line text files (`notes.tex`, `notes.org`, `notes.json5`, `notes.jsonc`, `notes.mdx`, `notes.css`, `notes.ts`, `notes.psv`, `notes.cs`, `notes.csx`, `notes.rc`, `notes.jsonld`), a folder that contains only `notes.cvs`, and:

```yaml
datasets:
  - from: file:$CSV
    name: typo_cvs
    params: { file_format: cvs }
  - from: file:$PARQUET
    name: typo_parqet
    params: { file_format: parqet }
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
    name: fmt_cs
    params: { file_format: cs }
  - from: file:$DOCS
    name: fmt_csx
    params: { file_format: csx }
  - from: file:$DOCS
    name: fmt_rc
    params: { file_format: rc }
  - from: file:$DOCS
    name: fmt_jsonld
    params: { file_format: jsonld }
  - from: file:$CVS_ONLY
    name: fmt_cvs_only
    params: { file_format: cvs }
  - from: file:$DOCS
    name: text_tex
    params: { file_format: text, file_extension: .tex }
```

## Doctor

`/health` must answer. The typo datasets are expected to be `Error`, so `/v1/ready` may stay unready. The `fmt_*` and `text_tex` datasets must be `Ready`.

## Drive

`GET /v1/datasets?status=true`.

- `typo_cvs` and `typo_parqet` must be `Error`. The `error_message` must name the invalid format, list valid formats, and include a `Did you mean` hint (`csv` / `parquet`).
- `fmt_cs`, `fmt_csx`, `fmt_rc`, and `fmt_jsonld` must be `Ready` over matching files.
- `fmt_cvs_only` must be `Ready` over a folder that contains only `.cvs` files.
- `fmt_log` must be `Ready`. The `spiced` log must contain a WARN that names `file_format` `'log'` and says the dataset is loaded as a plain-text table.
- `fmt_tex`, `fmt_org`, `fmt_json5`, `fmt_jsonc`, `fmt_mdx`, `fmt_css`, `fmt_ts`, and `fmt_psv` must be `Ready`.
- `text_tex` must be `Ready`. A query over it must return the `notes.tex` object and must not return `notes.org` / `notes.mdx` / the other extensions.

## Evidence

Paste the typo `error_message` text (no file contents), the Ready statuses (including `fmt_cs` / `fmt_csx` / `fmt_rc` / `fmt_jsonld` / `fmt_cvs_only`), the `fmt_log` WARN line, and the `text_tex` query rows (locations only). On trunk the typo datasets are also `Ready`.

## Cleanup

Kill the recorded `spiced` PID.
