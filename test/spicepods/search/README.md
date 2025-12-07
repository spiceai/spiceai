# Vector Search Test Spicepods

## Naming

Test spicepod names should be formatted according to the following template:

```console
{embedding-model-provider[variant]}-{accelerator/indexer[variant]}-{test variant}
```

`[variant]` refers to the specific information about the embedding model or indexer setup. For example:

* `openai[text-embedding-3-small]` - an OpenAI `text-embedding-3-small` embedding model with chunking enabled.
* `duckdb[file]` - a DuckDB accelerator using file-mode acceleration

Variants can be nested, up to 2 levels. For example, `openai[text-embedding-3-small[chunking]]` is embedding model with enabled chunking configuration.

`{test variant}` refers to additional configuration information, for example, `hybrid` indicating that full text search is enabled

Do not include test dataset information in the `{test variant}`. This information is supplied as a query metric dimension/attribute.

Examples of full spicepod names:

* `openai[text-embedding-3-small]-arrow` - an OpenAI `text-embedding-3-small` embedding model with Arrow acceleration.
* `openai[text-embedding-3-small[chunking]]-duckdb[file]` - an OpenAI `text-embedding-3-small` embedding model with chunking enabled, using DuckDB file-mode acceleration.
* `openai[text-embedding-3-small]-s3_vectors` - an OpenAI `text-embedding-3-small` embedding model with AWS S3-based vector storage.
* `huggingface[all-minilm-l6-v2]-arrow-hybrid_limit_2000` - a HuggingFace `all-MiniLM-L6-v2` embedding model with Arrow acceleration and hybrid search (vector + full-text search) enabled, with a test corpus data limit of 2000 records.
* `openai[text-embedding-3-small]-federated[postgres]` - an OpenAI `text-embedding-3-small` embedding model using precomputed embeddings stored in the source PostgreSQL dataset, performing direct search against the source without additional acceleration.
