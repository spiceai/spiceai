# Spice.ai OSS Roadmap

Describes the Spice roadmap, updated based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us)!

## Known bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Feel free to [file a new Issue](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) if you see a bug and let us know on Discord.

## v1.1 (Apr 2025)

- DataFusion v44
- DuckDB v1.2.x
- [#4910](https://github.com/spiceai/spiceai/issues/4910) Parameterized queries
- [#3318](https://github.com/spiceai/spiceai/issues/3318) AI/LLM benchmarks in CI (FinanceBench)
- MCP Server and Client support

## v1.2 (Apr 2025)

- Model Providers: HuggingFace, File (Release Candidate)
- Enterprise Search and Retrieval improvements
  - [#3006](https://github.com/spiceai/spiceai/issues/3006) Search with keyword filtering
- [#3320](https://github.com/spiceai/spiceai/issues/3320) Eval datasets
- Spice Cloud Data Connector (Stable)
- Orchestration and workers

## v1.3 (Apr 2025)

- Ingestion
  - Iceberg write
- [#3018](https://github.com/spiceai/spiceai/issues/3018) Results caching for embeddings and search
- [#3016](https://github.com/spiceai/spiceai/issues/3016) Hybrid-Search (DB + Vector)
  - [#3015](https://github.com/spiceai/spiceai/issues/3015) DataFusion Search
- AWS Glue Catalog Connector (Alpha)
- Data Accelerators: PostgreSQL (Stable)
- Spice Cloud Catalog Connector (Beta)

## v2 (H2 2025)

- Ingestion
  - PostgreSQL write
  - Delta Lake write
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
