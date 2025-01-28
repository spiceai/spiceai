# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to [file a new Issue](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) if you see a bug and let us know on Discord.

## v1.1 (Feb 2025)

- Enterprise Search and Retrieval improvements
  - [#3006](https://github.com/spiceai/spiceai/issues/3006) Search with keyword filtering
  - [#3016](https://github.com/spiceai/spiceai/issues/3016) Hybrid-Search (DB + Vector)
  - [#3015](https://github.com/spiceai/spiceai/issues/3015) DataFusion Search
- [#3318](https://github.com/spiceai/spiceai/issues/3318) FinanceBench in CI
- Model Providers: OpenAI, HuggingFace, File (Stable)
- Model Providers: Anthropic, Azure OpenAI, Grok (Beta), DeepSeek (Alpha)
- Data Accelerators: PostgreSQL (Stable)

## v1.2 (Mar 2025)

- AWS Glue Catalog Connector (Alpha)
- [#3018](https://github.com/spiceai/spiceai/issues/3018) Results caching for embeddings and search

## v1.3 (Q1 2025)

- Ingestion
  - PostgreSQL write
  - Delta Lake write
  - Iceberg write

## v2 (H2 2025)

- Policy
  - Security
  - Data Access
- Extensible Middleware
- AuthN Providers

## Features being considered

- Data Connectors

  - S3 Tables
  - ElasticSearch
  - MotherDuck
  - BigQuery
  - Kafka
  - Google Docs

- APIs

  - GraphQL API
  - Key/Value API
    - SlateDB data accelerator
    - RocksDB data accelerator
  - PostgreSQL API

- AI
  - Vision
