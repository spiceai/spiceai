# Spice.ai Open Source Roadmap

This roadmap details the planned features and priorities for Spice.ai Open Source, aligning with the mission to empower developers to build decision-making, data-driven AI applications. It is regularly refined based on community feedback, customer needs, and strategic goals.

To propose features or report issues, please [file an issue](https://github.com/spiceai/spiceai/issues/new/choose) or connect with us on [Slack](https://spiceai.org/slack). Your input drives our direction.

---

## Known Issues

- Track active bugs on [GitHub](https://github.com/spiceai/spiceai/labels/bug).
- Report new bugs via [this issue template](https://github.com/spiceai/spiceai/issues/new?template=bug_report.md) and share details on Slack for faster resolution.

---

## Release Timeline

### [v2.4](https://github.com/spiceai/spiceai/releases/tag/v2.4.0) (Released October 2026)

**Focus:** Spice Cayenne by default, ingestion, caching, and AI decisions.

**DataFusion:** v55

- **Spice Cayenne by Default**: Datasets and views without `acceleration.engine` use Cayenne (except on native Windows), with automatic primary-key handling, persistent secondary indexes, and adaptive tuning (preview). Other accelerators are deprecated and scheduled for removal in v2.5.
- **Faster Cayenne Lookups and Refreshes**: About 2x faster uncached primary-key lookups, and steady query latency during full refreshes.
- **AI Decisions in SQL and HTTP**: SQL decision functions and an OpenAI-compatible `POST /v1/decisions` API.
- **S3 Event-Driven Refresh**: `refresh_mode: changes` for accelerated S3 datasets via SQS event notifications.
- **SQL Results-Cache Warmup** and a single shared `runtime.state` location for state kept across restarts and instances.
- **New Connectors and Formats**: Hugging Face datasets, ORC, and Apache Iceberg tables without a catalog.
- **Query Engine Upgrades**: Apache DataFusion v55, Arrow v59.3, Ballista v55, Vortex v0.86, and Iceberg v0.11.

See the [v2.4.0 release notes](https://github.com/spiceai/spiceai/releases/tag/v2.4.0) for details and breaking changes. Fixes found after release ship in v2.4.x patch releases.

### [v2.5](https://github.com/spiceai/spiceai/milestone/102) (December 2026)

**Focus:** Cayenne as the only accelerator, lakehouse catalog, and secure by default.

- **Cayenne-Only Acceleration** ([#15492](https://github.com/spiceai/spiceai/issues/15492)): Remove the non-Cayenne acceleration engines (`arrow`, `duckdb`, `sqlite`, `turso`, `postgres`) and the DuckDB vector engine, and stop bundling DuckDB ([#15684](https://github.com/spiceai/spiceai/issues/15684), [#15683](https://github.com/spiceai/spiceai/issues/15683), [#15685](https://github.com/spiceai/spiceai/issues/15685)). Removal is gated on correctness: no open urgent Cayenne correctness issues and a sustained period of green soak and benchmark runs. If the gate is not met, removal moves to the next release.
- **Cayenne Correctness**: Durability and correctness fixes for restarts, CDC, partitioning, and write-back ([#15211](https://github.com/spiceai/spiceai/issues/15211), [#13368](https://github.com/spiceai/spiceai/issues/13368), [#13913](https://github.com/spiceai/spiceai/issues/13913), [#14809](https://github.com/spiceai/spiceai/issues/14809), [#15020](https://github.com/spiceai/spiceai/issues/15020), [#14565](https://github.com/spiceai/spiceai/issues/14565)).
- **Iceberg REST Catalog Conformance**: Iceberg REST catalog APIs that standard Iceberg clients can use end to end ([#14587](https://github.com/spiceai/spiceai/issues/14587), [#14477](https://github.com/spiceai/spiceai/issues/14477), [#14478](https://github.com/spiceai/spiceai/issues/14478)).
- **Write-Through UPDATE/DELETE DML**: UPDATE and DELETE on write-through accelerated tables, and recreation on restart ([#10121](https://github.com/spiceai/spiceai/issues/10121), [#10122](https://github.com/spiceai/spiceai/issues/10122), [#10120](https://github.com/spiceai/spiceai/issues/10120)).
- **Secrets Rotation**: Rotate delivered secrets and API keys without a restart ([#12869](https://github.com/spiceai/spiceai/issues/12869)).
- **BYOC Runtime Support**: Process CPU time metrics, including through Cloud Connect ([#12888](https://github.com/spiceai/spiceai/issues/12888), [#12911](https://github.com/spiceai/spiceai/issues/12911)), and dataset listing for self-hosted Cloud Connect instances ([#14058](https://github.com/spiceai/spiceai/issues/14058)).

### [v3.0](https://github.com/spiceai/spiceai/milestone/105) (Q1 2027)

Release candidates are tracked in [v3.0.0-rc.1](https://github.com/spiceai/spiceai/milestone/103) and [v3.0.0-rc.2](https://github.com/spiceai/spiceai/milestone/114). Scope is being planned. Leading candidates:

- **Secure by Default**: Governed access for agents with policy-based authorization (Cedar), starting with design; encryption (customer-managed keys and data-at-rest) to follow.
- **Lakehouse APIs**: A DataFrame API and Python SDK, and SQL DDL/DML for datasets, catalogs, and acceleration ([#15562](https://github.com/spiceai/spiceai/issues/15562)).
- **Unified Connector Rate Control** ([#10670](https://github.com/spiceai/spiceai/issues/10670)): Extend rate control from HTTP connectors to database and file/object-store connectors.
- **Local Models**: Local GGUF models with llama.cpp and a single Hugging Face Hub client ([#15395](https://github.com/spiceai/spiceai/issues/15395), [#15394](https://github.com/spiceai/spiceai/issues/15394), [#15272](https://github.com/spiceai/spiceai/issues/15272)).
- **OTLP Logs** ([#15202](https://github.com/spiceai/spiceai/issues/15202)): Store and query OpenTelemetry logs.

Tell us which matter most to you.

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

- **Distributed Search**: Federated vector and full-text search across multiple nodes ([#13138](https://github.com/spiceai/spiceai/issues/13138)).
- **Search at 100B+ Row Scale**: Vector and full-text search tuned for hundred-billion-row deployments.
- **Faceted Search**: Native facet buckets and counts returned directly in the search API response — beyond today's `GROUP BY` over the `vector_search`/`text_search` SQL functions — for filter-sidebar enterprise search UX.

### Data Platform

- **Schema Registry**: Versioning and backward compatibility checks.
- **Audit Logging**: Persistent, immutable query and access logs for compliance.
- **Resource Quotas**: Per-user/tenant query limits and throttling.
- **Extensible Middleware**: Pluggable extensions for dynamic customization.
- **Graph Queries**: Cypher/GQL support.
- **Data Lineage**: Track data provenance and transformations across the pipeline.

---

This roadmap is dynamic and evolves with community input and market needs. Thank you for contributing to the Spice.ai ecosystem!
