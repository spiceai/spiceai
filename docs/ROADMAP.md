# Spice.ai Open Source Roadmap

This roadmap details the planned features and priorities for Spice.ai Open Source, aligning with the mission to empower developers to build decision-making, data-driven AI applications. It is regularly refined based on community feedback, customer needs, and strategic goals.

To propose features or report issues, please [file an issue](https://github.com/spiceai/spiceai/issues/new/choose) or connect with us on [Slack](https://spiceai.org/slack). Your input drives our direction.

---

## Known Issues

- Track active bugs on [GitHub](https://github.com/spiceai/spiceai/labels/bug).
- Report new bugs via [this issue template](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) and share details on Slack for faster resolution.

---

## Release Timeline

### v1.11 (Jan 2026)

**Focus:** Distributed Query Foundation.

- **Spice Cayenne (Beta)**: High-performance columnar storage format for accelerated datasets.
- **Distributed Query (Beta)**: Multi-node query execution with Apache Ballista integration.
- **Active-Active HA (Preview)**: Multi-node active-active deployment with automatic failover.

### v1.12 (Feb 2026)

**Focus:** Distributed Query & Search Maturity.

- **Spice Cayenne (RC)**: High-performance columnar storage format for accelerated datasets.
- **Distributed Query (RC)**: Multi-node query execution with Apache Ballista integration.
- **Distributed Search (Alpha)**: Federated vector and full-text search across multiple nodes.
- **Accelerated Dataset Distribution**: Replicate accelerated datasets across executor nodes.
- **DataFusion**: Upgrade to v51.

### v2.0 (March 2026)

**Focus:** Production-Grade HA.

- **Spice Cayenne (GA)**: Production-ready columnar storage format for accelerated datasets.
- **Active-Active HA (GA)**: Production-ready multi-node deployment with zero-downtime failover.
- **Distributed Query (GA)**: Stable multi-node query execution for large-scale workloads.
- **Backup & Point-in-Time Recovery**: Snapshot restore for accelerated datasets.

### v2.1 (H2 2026)

**Focus:** Schema Management & Search.

- **Schema Registry (Initial)**: Versioning and backward compatibility checks.
- **Hybrid Search Ranking**: Configurable fusion strategies (Reciprocal Rank Fusion (RRF) weights, learned ranking).

### v2.2 (H2 2026)

**Focus:** Reactive Actions & Event Processing.

- **Actions (Drasi-based)**: Reactive event-driven actions triggered by data changes.
- **Webhooks & Event Notifications**: Push-based data change alerts for downstream consumers.

### v2.3 (H2 2026)

**Focus:** Enterprise Security & Compliance.

- **Data Masking & Anonymization**: Dynamic PII redaction at query time.
- **OIDC Token Verification**: Validate identity tokens from enterprise providers (Okta, Azure AD, etc.).

### v2.4 (2027)

**Focus:** Governance.

- **Policy Engine (Cedar-based)**:
  - Role-based security for fine-grained access control.
  - Data access policies to enforce compliance and governance.
- **Audit Logging**: Persistent, immutable query and access logs for compliance.
- **Resource Quotas**: Per-user/tenant query limits and throttling.

### v2.5 (2027)

**Focus:** Extensibility & Plugin Architecture.

- **Extensible Middleware**: Pluggable extensions for dynamic customization.
- **Custom Tool Providers**: User-defined MCP tool implementations.

### v2.6 (2027)

**Focus:** Encryption.

- **Customer-Managed Keys (BYOK)**: Encryption key management for sensitive workloads.
- **Data-at-Rest Encryption**: Encrypted storage for accelerated datasets.

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
- **Model Versioning & A/B Testing**: Canary deployments and version management for models.
- **Hallucination Detection**: Fact-checking LLM responses against source data.

### Search & Retrieval

- **Faceted Search**: Aggregations, filters, and counts for enterprise search UX.

### Data Platform

- **Data Lineage**: Track data provenance and transformations across the pipeline.
- **Schema Registry & Evolution**: Versioning and backward compatibility checks.

### Platform Enhancements

- **Observability**: Comprehensive metrics, logging, and tracing.
- **CLI & SDKs**: Streamlined developer experience with improved tooling.
- **Data Accelerators**: Additional engines for specialized workloads.

---

This roadmap is dynamic and evolves with community input and market needs. Thank you for contributing to the Spice.ai ecosystem!
