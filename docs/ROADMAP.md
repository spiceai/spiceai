# Spice.ai Open Source Roadmap

This roadmap details the planned features and priorities for Spice.ai Open Source, aligning with the mission to empower developers to build decision-making, data-driven AI applications. It is regularly refined based on community feedback, customer needs, and strategic goals.

To propose features or report issues, please [file an issue](https://github.com/spiceai/spiceai/issues/new/choose) or connect with us on [Slack](https://spiceai.org/slack). Your input drives our direction.

---

## Known Issues

- Track active bugs on [GitHub](https://github.com/spiceai/spiceai/labels/bug).
- Report new bugs via [this issue template](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) and share details on Slack for faster resolution.

---

## Release Timeline

### v2.0 (April 2026)

**Focus:** Production-Grade HA.

- **Spice Cayenne (GA)**: Production-ready columnar storage format for accelerated datasets.
- **Multi-Active HA (GA)**: Production-ready multi-node deployment with zero-downtime failover.
- **Distributed Query (GA)**: Stable multi-node query execution for large-scale workloads.
- **Accelerated Dataset Distribution**: Replicate accelerated datasets across executor nodes.

### v2.1 (H2 2026)

**Focus:** Schema Management, Search, and Policy.

- **Elasticsearch**: Integration for search and analytics workloads.
- **Distributed Search (Alpha)**: Federated vector and full-text search across multiple nodes.
- **Schema Registry (Initial)**: Versioning and backward compatibility checks.
- **Schema Evolution**: Safe, non-breaking schema changes for accelerated datasets (add/drop/rename columns, type widening) with automatic migration.
- **Hybrid Search Ranking**: Configurable fusion strategies (Reciprocal Rank Fusion (RRF) weights, learned ranking).
- **Policy Engine (Cedar-based) Beta**:
  - Role-based security for fine-grained access control.
  - Data access policies to enforce compliance and governance.

### v2.2 (H2 2026)

**Focus:** Reactive Actions & Event Processing.

- **Webhooks & Event Notifications**: Push-based data change alerts for downstream consumers.
- **Actions (Drasi-based)**: Reactive event-driven actions triggered by data changes.

### v2.3 (H2 2026)

**Focus:** Enterprise Security, Compliance, & Governance.

- **Data Masking & Anonymization**: Dynamic PII redaction at query time.
- **Audit Logging**: Persistent, immutable query and access logs for compliance.
- **Resource Quotas**: Per-user/tenant query limits and throttling.

### v2.4 (2027)

**Focus:** Extensibility & Plugin Architecture.

- **Extensible Middleware**: Pluggable extensions for dynamic customization.
- **Custom Tool Providers**: User-defined MCP tool implementations.

### v2.5 (2027)

**Focus:** Encryption.

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

- **Faceted Search**: Aggregations, filters, and counts for enterprise search UX.

### Data Platform

- **Data Lineage**: Track data provenance and transformations across the pipeline.

---

This roadmap is dynamic and evolves with community input and market needs. Thank you for contributing to the Spice.ai ecosystem!
