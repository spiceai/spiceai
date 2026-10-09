# Hugging Face Data Connector

The Hugging Face data connector queries and accelerates datasets hosted on the [Hugging Face Hub](https://huggingface.co/datasets): Parquet, CSV, TSV, JSON, JSONL and ORC files, public, gated or private.

```yaml
datasets:
  - from: hf://datasets/stanfordnlp/imdb/plain_text/
    name: imdb
```

```sql
SELECT label, count(*) FROM imdb GROUP BY label;
```

## Location

`from` uses the location syntax that `huggingface_hub`'s `HfFileSystem`, DuckDB and Polars share, so a path copied from a dataset card or a DuckDB query works unchanged:

```text
hf://datasets/<owner>/<dataset>[@<revision>][/<path>]
```

| Part         | Description                                                                                                                                                                                                                                 |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<owner>`    | The user or organization that owns the dataset.                                                                                                                                                                                             |
| `<dataset>`  | The dataset repository name.                                                                                                                                                                                                                |
| `<revision>` | Optional. A branch, tag or commit; defaults to `main`. A revision containing `/` is percent-encoded (`@refs%2Fconvert%2Fparquet`), except the Hub's own `refs/convert/<name>` and `refs/pr/<number>`. `@~parquet` reads `refs/convert/parquet`. |
| `<path>`     | Optional. A file, a folder (ending in `/`, or the whole repository), or a glob such as `data/train-*.parquet`.                                                                                                                               |

### Examples

```yaml
datasets:
  # A folder. The file format is inferred from its files.
  - from: hf://datasets/stanfordnlp/imdb/plain_text/
    name: imdb

  # One split by glob, pinned to an immutable commit.
  - from: hf://datasets/stanfordnlp/imdb@e6281661ce1c48d982bc483cf8a173c1bbeb5d31/plain_text/train-*.parquet
    name: imdb_train

  # A single file.
  - from: hf://datasets/stanfordnlp/imdb/plain_text/test-00000-of-00001.parquet
    name: imdb_test

  # The Hub's automatic Parquet conversion, available for every public dataset, including
  # datasets published as images, audio or WebDataset archives.
  - from: hf://datasets/nyu-mll/glue@~parquet/cola/train/
    name: cola_train
```

## File formats

The format comes from, in order:

1. `file_format` (or `file_extension`), when set.
2. The extension of the path in `from` (`.parquet`, `.csv`, `.jsonl.gz`, ...).
3. The files the location selects: when they hold exactly one data format (Parquet, CSV, TSV, JSON, JSONL or ORC, compressed or not), that format is used. Other files, such as `README.md` and `.gitattributes`, are ignored.

A folder holding more than one data format must be narrowed with a glob or `file_format`. All of the file-format parameters of the other object-store connectors (`csv_has_header`, `json_format`, `file_compression_type`, `hive_partitioning_enabled`, `schema_source_path`, ...) apply unchanged.

## Private and gated datasets

Set `hf_token` to a [User Access Token](https://huggingface.co/settings/tokens) of an account that can read the dataset. For a gated dataset, first accept its access conditions on the dataset's page with that account.

```yaml
datasets:
  - from: hf://datasets/my-org/my-private-dataset/data/
    name: private_data
    params:
      hf_token: ${ secrets:HF_TOKEN }
```

When `hf_token` is not set in the dataset, it is loaded from a secret named `hf_token` in the configured secret stores. Authenticated requests also get higher Hub rate limits.

## Parameters

| Parameter      | Default                  | Description                                                                                                                              |
| -------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `hf_token`     | none                     | A Hugging Face User Access Token, to read private and gated datasets.                                                                    |
| `hf_endpoint`  | `https://huggingface.co` | The Hub endpoint, to read through a mirror or proxy of the Hub. It must be `https://`; `http://` is accepted only for a loopback address. |
| `file_format`  | inferred                 | `parquet`, `csv`, `tsv`, `json`, `jsonl`, `ndjson`, `ldjson` or `orc`.                                                                   |

## Consistency

Every scan resolves the revision to a commit, then lists and reads files only at that commit. A query therefore never mixes two versions of a dataset, even if the dataset is updated while it runs. A branch is resolved again at most every 10 seconds; a commit never changes.

The schema is resolved when the dataset is registered, and files at later commits are read against it, as for the other object-store connectors. An accelerated dataset picks up new commits of its branch on each refresh.

CSV and TSV files are read by column position. Every CSV or TSV file a dataset selects must therefore have the dataset's columns in the same order. Registration fails if one does not, and so does a scan of a later commit whose files changed their columns. In either case the error names the file and its columns.

The `_location` metadata column, when enabled, holds the commit-pinned location of each row's file. For a public dataset read without `hf_token`, that is `hf://datasets/<owner>/<dataset>@<commit>/<path>`, which DuckDB and `HfFileSystem` can read directly. A dataset read with a token, or through `hf_endpoint`, has its own host instead, `hf://datasets.<key>/...`, so no two datasets with different credentials share a client. The key comes from the endpoint and the dataset's name, never from the token.

## Performance

- Parquet files are read in byte ranges, so a query reads only the row groups and columns it needs.
- A large file is resolved once to its CDN location, and later reads of the file go straight to the CDN.
- File listings are cached per commit, since a commit cannot change.
- For repeated or interactive queries, [accelerate](https://spiceai.org/docs/components/data-accelerators) the dataset. It is then read once per refresh rather than once per query.

## Limitations

- Datasets are read-only.
- Only dataset repositories can be read; `hf://models/...` and `hf://spaces/...` are not supported.
- Unstructured files (text, PDF, images) cannot be read as documents. `@~parquet` reads the Hub's Parquet conversion of image and audio datasets.
- `has_metadata_table` is not supported.
