# Spice.ai Open Source Roadmap

This roadmap details the planned features and priorities for Spice.ai Open Source, aligning with the mission to empower developers to build decision-making, data-driven AI applications. It is regularly refined based on community feedback, customer needs, and strategic goals.

To propose features or report issues, please [file an issue](https://github.com/spiceai/spiceai/issues/new/choose) or connect with us on [Discord](https://github.com/spiceai/spiceai#-connect-with-us). Your input drives our direction.

---

## Known Issues

- Track active bugs on [GitHub](https://github.com/spiceai/spiceai/labels/bug).
- Report new bugs via [this issue template](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) and share details on Discord for faster resolution.

---

## Release Timeline

### [v1.7 (September 2025)](https://github.com/spiceai/spiceai/milestone/75)

**Focus:** Real-Time Write-Through Caching - Streamlined data and embedding caching with real-time ingestion.

- **Query**:
  - Regex support for DuckDB accelerator/connector.
- **Search**:
  - Amazon S3 Vectors cross-index query.
  - Amazon S3 Vectors chunking support.
  - Reciprocal Rank Fusion (RRF) UDTF
- **Caching**:
  - Real-time write-through caching/acceleration for data and embeddings.
  - xxHash (XXH3) results-caching algorithm support.
- **Ingestion**:
  - Real-time data consumption via Apache Kafka.
  - Improved Debezium/Change Data Capture (CDC) mechanisms.
- **DataFusion**: Upgrade to v49.

### [v1.8 (October 2025)](https://github.com/spiceai/spiceai/milestone/77)

**Focus:** Resource Management & Expanded Write Support - Fine-grained resource control and broader database compatibility.

- **Resource Management**: Finer-grained runtime-wide control for optimized performance.
- **Hash Partitioning**: Improved query distribution and performance.
- **Write Support**:
  - Write-through support to Apache Iceberg.
  - MySQL write support.
  - PostgreSQL write support (expanded).
  - Spice Cloud write support (expanded).
- **DuckDB**: Upgrade to v1.4.
- **DataFusion**: Upgrade to v50.

### v1.9 (November 2025)

**Focus:** Hybrid Search Enhancements - Advanced search capabilities for improved relevance and performance.

- **Hybrid Search**:
  - Boosting and re-ranking for enhanced search results.
- **DuckLake**: Initial support for DuckDB-based lakehouse architectures.
- **DataFusion**: Upgrade to v51.

### v1.10 (December 2025)

**Focus:** Extensibility - Flexible middleware for custom integrations.

- **Extensible Middleware**:
  - Pluggable extensions for dynamic customization.

### v2.0 (H1 2026)

**Focus:** Enterprise Policy & Governance - Robust security and compliance features.

- **Policy Engine**:
  - Role-based security for fine-grained access control.
  - Data access policies to enforce compliance and governance.

---

## Features Under Consideration

These are prioritized based on community demand and strategic alignment. Share your feedback to influence their inclusion.

### Data Connectors

- **Delta Lake**: Write support for transactional data lakes.
- **Elasticsearch**: Integration for search and analytics workloads.
- **MotherDuck**: Hybrid cloud DuckDB connector.
- **BigQuery**: Support for Google Cloud's data warehouse.
- **Google Docs**: Experimental connector for collaborative data sources.

### APIs

- **Key/Value API**:
  - SlateDB data accelerator for low-latency storage.
- **PostgreSQL API**: Native compatibility with PostgreSQL clients.

### AI & Analytics

- **Vision Processing**: Support for image and video.
- **Custom Model Integration**: Framework for user-defined ML models.

### Platform Enhancements

- **Observability**: Comprehensive metrics, logging, and tracing.
- **CLI & SDKs**: Streamlined developer experience with improved tooling.
- **Data Accelerators**: Additional engines for specialized workloads.

---

This roadmap is dynamic and evolves with community input and market needs. Thank you for contributing to the Spice.ai ecosystem!
