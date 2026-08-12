# Vector Search Test Spicepods

## Naming

Test spicepod names should be formatted according to the following template:

```console
{embedding-model-provider[variant]}-{accelerator/indexer[variant]}-{test variant}
```

`[variant]` refers to the specific information about the embedding model or indexer setup. For example:

* `model2vec[potion-multilingual-128M]` - a Model2Vec `potion-multilingual-128M` embedding model.
* `duckdb[file]` - a DuckDB accelerator using file-mode acceleration

Variants can be nested, up to 2 levels. For example, `model2vec[potion-multilingual-128M[chunking]]` is an embedding model with enabled chunking configuration.

`{test variant}` refers to additional configuration information, for example, `hybrid` indicating that full text search is enabled

Do not include test dataset information in the `{test variant}`. This information is supplied as a query metric dimension/attribute.

Examples of full spicepod names:

* `huggingface[all-minilm-l6-v2]-arrow` - a HuggingFace `all-MiniLM-L6-v2` embedding model with Arrow acceleration.
* `model2vec[potion-multilingual-128M[chunking]]-duckdb[file]` - a Model2Vec `potion-multilingual-128M` embedding model with chunking enabled, using DuckDB file-mode acceleration.
* `huggingface[all-minilm-l6-v2]-arrow-hybrid_limit_2000` - a HuggingFace `all-MiniLM-L6-v2` embedding model with Arrow acceleration and hybrid search (vector + full-text search) enabled, with a test corpus data limit of 2000 records.
