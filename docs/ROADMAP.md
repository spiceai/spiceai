# Spice.ai Open Source Roadmap

This roadmap details the planned features and priorities for Spice.ai Open Source, aligning with the mission to empower developers to build decision-making, data-driven AI applications. It is regularly refined based on community feedback, customer needs, and strategic goals.

To propose features or report issues, please [file an issue](https://github.com/spiceai/spiceai/issues/new/choose) or connect with us on [Slack](https://spiceai.org/slack). Your input drives our direction.

---

## Known Issues

- Track active bugs on [GitHub](https://github.com/spiceai/spiceai/labels/bug).
- Report new bugs via [this issue template](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) and share details on Slack for faster resolution.

---

## Release Timeline

### [v2.2](https://github.com/spiceai/spiceai/milestone/99) (July 2026)

**Focus:** Schema Registry, Distributed Search & Event Processing.

**DataFusion:** v55

- **Schema Registry (Initial)**: Versioning and backward compatibility checks.
- **Distributed Search (Alpha)**: Federated vector and full-text search across multiple nodes, with FTS indexes available in distributed query mode.
- **Webhooks & Event Notifications**: Push-based data change alerts for downstream consumers.
- **Write-Back Acceleration**: Eventually-consistent write-back, with full DML (UPDATE/DELETE) and `spice refresh`/`refresh_check_interval` on write-through accelerated tables.

### [v2.3](https://github.com/spiceai/spiceai/milestone/100) (September 2026)

**Focus:** Enterprise Security, Compliance, & Governance.

**DataFusion:** v56

- **Audit Logging**: Persistent, immutable query and access logs for compliance.
- **Resource Quotas**: Per-user/tenant query limits and throttling.
- **Actions (Drasi-based)**: Reactive event-driven actions triggered by data changes.
- **Distributed Cayenne Catalog**: Cayenne catalog with full distributed query and acceleration support.
- **Distributed Search Scale-Out**: Search query partitioning and relative score fusion across distributed nodes.

### [v2.4](https://github.com/spiceai/spiceai/milestone/101) (October 2026)

**Focus:** Extensibility & Plugin Architecture.

**DataFusion:** v57

- **Extensible Middleware**: Pluggable extensions for dynamic customization.
- **Search at 100B+ Row Scale**: Vector and full-text search benchmarked and tuned for hundred-billion-row deployments, including S3 Vectors throughput improvements.
- **Unified Connector Rate Control**: Extend the runtime-wide rate-control surface from HTTP connectors to database and file/object-store connectors for consistent per-origin concurrency and request-rate limits.

### [v2.5](https://github.com/spiceai/spiceai/milestone/102) (November 2026)

**Focus:** Encryption.

**DataFusion:** v58

- **Customer-Managed Keys (BYOK)**: Encryption key management for sensitive workloads.
- **Data-at-Rest Encryption**: Encrypted storage for accelerated datasets.

---

## Features Under Consideration

These are prioritized based on community demand and strategic alignment. Share your feedback to influence their inclusion.

### Data Connectors

- **Delta Lake**: Write support for transactional data lakes.
- **Google Docs**: Experimental connector for collaborative data sources.

### APIs

- **Key/Value API**:
  - SlateDB data accelerator for low-latency storage.
- **PostgreSQL API**: Native compatibility with PostgreSQL clients.

### AI & Analytics

- **Vision Processing**: Support for image and video.
- **Custom ML Model Integration**: Framework for user-defined ML models.
- **Model Versioning & A/B Testing**: Canary deployments and version management for models.
- **Hallucination Detection**: Fact-checking LLM responses against source data.

### Search & Retrieval

- **Faceted Search**: Native facet buckets and counts returned directly in the search API response — beyond today's `GROUP BY` over the `vector_search`/`text_search` SQL functions — for filter-sidebar enterprise search UX.

### Data Platform

- **Data Lineage**: Track data provenance and transformations across the pipeline.

---

This roadmap is dynamic and evolves with community input and market needs. Thank you for contributing to the Spice.ai ecosystem!
