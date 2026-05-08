# Spice.ai Project Docs

The project docs for contributors and community. For user documentation of the Spice.ai platform, see [spiceai.org/docs](https://spiceai.org/docs).

## Core Docs

- [Principles](PRINCIPLES.md)
- [Roadmap](ROADMAP.md)
- [Distributions](DISTRIBUTIONS.md)
- [Extensibility](EXTENSIBILITY.md)

## Contributing

- [CONTRIBUTING.md](../CONTRIBUTING.md)
- [Release Process](RELEASE.md)

## Developer Notes

- [Cosmos DB](dev/cosmosdb.md)
- [Error Handling](dev/error_handling.md)
- [LLM Prompt Caching](dev/llm-prompt-caching.md)
- [Metrics](dev/metrics.md)
- [Snapshot Tests](dev/snapshot_tests.md)
- [Rust Style Guide](dev/style_guide.md)

## Standard Operating Procedures

- [Upgrade mistral.rs](dev/sop-upgrade-mistral-rs.md)
- [Upgrade Text Embeddings Inference](dev/sop-upgrade-tei.md)

## Criteria

- [Criteria Principles](criteria/PRINCIPLES.md)
- [Criteria Definitions](criteria/definitions.md)

### Accelerators

- [Alpha](criteria/accelerators/alpha.md)
- [Beta](criteria/accelerators/beta.md)
- [Release Candidate](criteria/accelerators/rc.md)
- [Stable](criteria/accelerators/stable.md)

### Catalogs

- [Alpha](criteria/catalogs/alpha.md)
- [Beta](criteria/catalogs/beta.md)
- [Release Candidate](criteria/catalogs/rc.md)
- [Stable](criteria/catalogs/stable.md)

### Connectors

- [Alpha](criteria/connectors/alpha.md)
- [Beta](criteria/connectors/beta.md)
- [Release Candidate](criteria/connectors/rc.md)
- [Stable](criteria/connectors/stable.md)

### Embeddings

- [Alpha](criteria/embeddings/alpha.md)
- [Beta](criteria/embeddings/beta.md)
- [Release Candidate](criteria/embeddings/rc.md)
- [Stable](criteria/embeddings/stable.md)

### Features

- [Overview](criteria/features/README.md)
- [Alpha](criteria/features/alpha.md)
- [Beta](criteria/features/beta.md)
- [Release Candidate](criteria/features/rc.md)
- [Stable](criteria/features/stable.md)

### Models

- [Grading](criteria/models/grading.md)
- [Alpha](criteria/models/alpha.md)
- [Beta](criteria/models/beta.md)
- [Release Candidate](criteria/models/rc.md)
- [Stable](criteria/models/stable.md)

## Architecture Decisions

- [001: Use snmalloc as Global Allocator](decisions/001-use-snmalloc-as-global-allocator.md)
- [002: Default Ports](decisions/002-default-ports.md)
- [003: Duration Milliseconds](decisions/003-duration-ms.md)
- [004: Distributed Query Framework](decisions/004-distributed-query-framework.md)
- [005: Ballista Extensions](decisions/005-ballista-extensions.md)
- [006: High-Availability Distributed Query](decisions/006-ha-distributed-query.md)
- [007: Cluster mTLS](decisions/007-cluster-mtls.md)

## Examples

- [HTTP Refresh SQL Example](examples/http_refresh_sql_example.md)
- [Turso Acceleration Example](examples/turso_acceleration_example.md)

## Feature Notes

- [Databricks Resilience](features/databricks-resilience.md)
- [DuckDB Index Scan Settings](features/duckdb_index_scan_settings.md)
- [GCS Connector](features/gcs-connector.md)
- [Git Connector](features/git-connector.md)
- [Postgres Replication](features/postgres-replication.md)
- [Schema Decomposition](features/schema-decomposition.md)

## Threat Models

- [v1.9.2](threat_models/v1.9.2.md)
- [v2.0.0](threat_models/v2.0.0.md)
- [v0.17.4-beta JSON](threat_models/v0.17.4-beta.json)
- [v1.9.1 JSON](threat_models/v1.9.1.json)

## Release Notes

Release notes are stored in [release_notes](release_notes/). Use the series directories for older releases:

- [Alpha](release_notes/alpha/)
- [Beta](release_notes/beta/)
- [Release Candidate](release_notes/rc/)
- [v1.0](release_notes/v1.0/)
- [v1.1](release_notes/v1.1/)
- [v1.2](release_notes/v1.2/)
- [v1.3](release_notes/v1.3/)
- [v1.4](release_notes/v1.4/)
- [v1.5](release_notes/v1.5/)
- [v1.6](release_notes/v1.6/)
- [v1.7](release_notes/v1.7/)
- [v1.8](release_notes/v1.8/)
- [v1.9](release_notes/v1.9/)
- [v1.10](release_notes/v1.10/)

Recent v1.11 and v2.0 release notes are at the top level of [release_notes](release_notes/).
