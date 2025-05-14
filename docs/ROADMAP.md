# Spice.ai Open Source Roadmap

This roadmap details the planned features and priorities for Spice.ai Open Source, aligning with the mission to empower developers to build decision-making, data-driven AI applications. It is regularly refined based on community feedback, customer needs, and strategic goals.

To propose features or report issues, please [file an issue](https://github.com/spiceai/spiceai/issues/new/choose) or connect with us on [Discord](https://github.com/spiceai/spiceai#-connect-with-us). Your input drives our direction.

---

## Known Issues

- Track active bugs on [GitHub](https://github.com/spiceai/spiceai/labels/bug).
- Report new bugs via [this issue template](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) and share details on Discord for faster resolution.

---

## Release Timeline

### v1.3 (May 2025)

**Focus:** Cloud Integration

- Upgrade to DataFusion v46.
- **Ingestion**:
  - Support write operations to Spice Cloud.
- **Spice Cloud Catalog Connector** (Beta): Unified access to cloud-based datasets.

### v1.4 (June 2025)

**Focus:** Search

- Upgrade to DataFusion v47.
- Upgrade to DuckDB v1.2.x.
- [#3018](https://github.com/spiceai/spiceai/issues/3018) Results caching for embeddings and search queries.
- [#3016](https://github.com/spiceai/spiceai/issues/3016) Hybrid-Search (Database + Vector):
  - [#3015](https://github.com/spiceai/spiceai/issues/3015) DataFusion-powered search integration.
- **Catalog Connectors**
  - AWS Glue Catalog Connector (Beta).

### v1.5 (July 2025)

**Focus:** Advanced Ingestion - Expanded write capabilities for modern data lakehouse architectures.

- Hash partitioning for improved query distribution and performance.
- **Ingestion**:
  - Delta Lake write support for transactional data lakes.
  - Apache Iceberg write support for scalable table formats.
  - PostgreSQL write support for relational database integration.

### v1.6 (August 2025)

- **Model Providers**:
  - HuggingFace (Release Candidate).
  - File-based models (Release Candidate).

### v2.0 (H2 2025)

**Focus:** Enterprise Security, Extensibility, and Scalability

- **Policy Engine**:
  - Role-based security for fine-grained access control.
  - Data access policies to enforce compliance and governance.
- **Extensible Middleware**:
  - Pluggable extensions.

---

## Features Under Consideration

These are prioritized based on community demand and strategic alignment. Share your feedback to influence their inclusion.

### Data Connectors

- **S3 Tables**: Direct querying of S3-Table based datasets.
- **Elasticsearch**: Integration for search and analytics workloads.
- **MotherDuck**: Hybrid cloud DuckDB connector.
- **BigQuery**: Support for Google Cloud's data warehouse.
- **Kafka**: Real-time streaming data ingestion.
- **Google Docs**: Experimental connector for collaborative data sources.

### APIs

- **GraphQL API**: Flexible, query-driven data access.
- **Key/Value API**:
  - SlateDB data accelerator for low-latency storage.
  - RocksDB data accelerator for embedded key-value performance.
- **PostgreSQL API**: Native compatibility with PostgreSQL clients.

### AI & Analytics

- **Vision Processing**: Support for image and video.
- **Advanced Embeddings**: Enhanced support for vector-based AI models.
- **Custom Model Integration**: Framework for user-defined ML models.

### Platform Enhancements

- **Observability**: Comprehensive metrics, logging, and tracing.
- **CLI & SDKs**: Streamlined developer experience with improved tooling.
- **Data Accelerators**: Additional engines for specialized workloads.

---

This roadmap is dynamic and evolves with community input and market needs. Thank you for contributing to the Spice.ai ecosystem!
