# Spice.ai OSS Roadmap

This roadmap outlines the planned and proposed features for Spice.ai OSS. It is updated regularly based on community and customer feedback.

If you have a feature request or suggestion, please [get in touch](https://github.com/spiceai/spiceai#-connect-with-us) or [file an issue](https://github.com/spiceai/spiceai/issues/new/choose)!

---

## Known Bugs

- See [Bugs](https://github.com/spiceai/spiceai/labels/bug). Please [file a new Issue](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) if you encounter a bug, and let us know on Discord.

---

## Release Timeline

### v1.3 (May 2025)

**Focus:** Cloud Integration

- DataFusion v46
- DuckDB v1.2.x
- Orchestration and workers
- AWS Glue Catalog Connector (Beta)
- Ingestion
  - Spice Cloud write
- Spice Cloud Catalog Connector (Beta)

### v1.4 (June 2025)

**Focus:** Search

- DataFusion v47
- Hash Partitioning
- [#3018](https://github.com/spiceai/spiceai/issues/3018) Results caching for embeddings and search
- [#3016](https://github.com/spiceai/spiceai/issues/3016) Hybrid-Search (DB + Vector)
  - [#3015](https://github.com/spiceai/spiceai/issues/3015) DataFusion Search
- Model Providers: HuggingFace, File (Release Candidate)
- Data Accelerators: PostgreSQL (Stable)

### v1.5 (July 2025)

**Focus:** Ingestion

- Ingestion
  - Delta Lake write
  - Iceberg write
  - PostgreSQL write

### v2 (H2 2025)

- Policy
  - Security
  - Data Access
- Extensible Middleware
- AuthN Providers

---

## Features Under Consideration

### Data Connectors

- S3 Tables
- ElasticSearch
- MotherDuck
- BigQuery
- Kafka
- Google Docs

### APIs

- GraphQL API
- Key/Value API
  - SlateDB data accelerator
  - RocksDB data accelerator
- PostgreSQL API

### AI & Analytics

- Vision (Image/Video)
- Advanced Embeddings
- Custom Model Integration

### Other

- Improved Observability & Monitoring
- Enhanced CLI & SDKs
- More Data Accelerators

---

_This roadmap is subject to change based on feedback and priorities. Thank you for being part of the Spice community!_
