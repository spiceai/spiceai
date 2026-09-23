# Appendix F. Sources and the blog reading register

This manuscript uses primary project material: the runtime source, OSS documentation, cookbook, and website blog archive. Links below identify the public reading path; the local commit identities preserve which material informed this edition. Public pages can evolve after publication.

## F.1 Pinned repositories

- Runtime: [spiceai/spiceai at the inspected commit](https://github.com/spiceai/spiceai/tree/16c436f7b8a76ce161a07c0288277ced0ada4a07).
- Documentation: [spiceai/docs](https://github.com/spiceai/docs), local checkout commit `e30ef3d3dd84f7ba32c1eb32fc7f7f5d5dc6f375`.
- Cookbook: [spiceai/cookbook](https://github.com/spiceai/cookbook), local checkout commit `a76a26add6545c7edf986791e23966eafcb09a7b`.
- Website: user-supplied `~/dev/website`, commit `f1e0f9be59753b7e088a1bba6d57dc209650a6bd`; blog source directory `content/pages/blog/`.

The blog inventory contains 50 posts. Each is classified below as technical input, release or case-study context, historical background, or outside the technical scope. The reading process used the full archive inventory, extracted text and headings, and targeted review of the technical sections relevant to the manuscript. It does not treat every historical snippet as a current configuration recipe.

## F.2 Core documentation and recipes

| Topic | Primary documentation | Companion recipe family |
|---|---|---|
| First project | [Getting started](https://spiceai.org/docs/getting-started) | `file/` |
| Configuration | [Spicepod reference](https://spiceai.org/docs/reference/spicepod) | Complete recipe Spicepods |
| Source access | [Data connectors](https://spiceai.org/docs/components/data-connectors) | `postgres/connector/`, `s3/`, `databricks/`, `snowflake/` |
| Acceleration | [Data accelerators](https://spiceai.org/docs/components/data-accelerators) | `arrow/`, `sqlite/accelerator/`, `cayenne/` |
| Refresh | [Data acceleration](https://spiceai.org/docs/features/data-acceleration) | `acceleration/data-refresh/`, `retention/` |
| Caches | [Caching](https://spiceai.org/docs/features/caching) | Cache and acceleration examples |
| PostgreSQL changes | [PostgreSQL connector](https://spiceai.org/docs/components/data-connectors/postgres) | `postgres/cdc/` |
| Other change streams | Connector-specific references | `mysql/cdc/`, `mongodb/change-streams/`, `dynamodb/streams/`, `cdc-debezium/` |
| Lakehouse | [Iceberg](https://spiceai.org/docs/components/data-connectors/iceberg) and [Delta Lake](https://spiceai.org/docs/components/data-connectors/delta-lake) | `glue/`, `delta-lake/` |
| SQL clients | [API reference](https://spiceai.org/docs/api) | `clients/adbc/`, `openai_sdk/` |
| Retrieval | [Search](https://spiceai.org/docs/features/search) and [SQL search](https://spiceai.org/docs/reference/sql/search) | `full-text-search/`, `vectors/s3/` |
| Embeddings | [Embedding components](https://spiceai.org/docs/components/embeddings) | Model and search recipes |
| Models | [Large language models](https://spiceai.org/docs/features/large-language-models) | `models/openai/`, `models/filesystem/`, `text-to-sql/` |
| Tools | [MCP](https://spiceai.org/docs/features/large-language-models/mcp) | `mcp/`, `mcp-server/` |
| Security | [Authentication](https://spiceai.org/docs/api/auth) | `api_key/`, `mtls/` |
| Deployment | [Docker](https://spiceai.org/docs/deployment/docker) | `docker/` |
| Distributed work | [Distributed query](https://spiceai.org/docs/features/distributed-query) | `distributed/`, `async-queries/` |
| Observability | [Monitoring](https://spiceai.org/docs/monitoring) | Runtime metrics and traces |

## F.3 Blog-to-chapter register

Dates are the publication dates recorded in the supplied website checkout. A post's release availability, maturity, pricing, and limits remain historical unless separately verified for the target deployment. Case-study performance claims are attributed to those publications and are not adopted as measurements in this book.

| Publication date | Post | Use in this edition |
|---|---|---|
| 2021-11-05 | [Making Apps That Learn And Adapt](https://spice.ai/blog/making-apps-that-learn-and-adapt) | Historical background; Chapter 29 only |
| 2021-11-15 | [Teaching Apps how to Learn with Spicepods](https://spice.ai/blog/teaching-apps-how-to-learn-with-spicepods) | Historical background; Chapter 29 only |
| 2021-11-18 | [Spice.ai's approach to Time-Series AI](https://spice.ai/blog/spiceais-approach-to-time-series-ai) | Historical background; Chapter 29 only |
| 2021-12-02 | [Spicepods: From Zero to Hero](https://spice.ai/blog/spicepods-from-zero-to-hero) | Historical background; Chapter 29 only |
| 2021-12-05 | [AI needs AI-ready data](https://spice.ai/blog/ai-needs-ai-ready-data) | Historical background; Chapter 29 only |
| 2021-12-30 | [A New Class of Applications That Learn and Adapt](https://spice.ai/blog/a-new-class-of-applications-that-learn-and-adapt) | Historical background; Chapter 29 only |
| 2022-01-04 | [What Data Informs AI-driven Decision Making?](https://spice.ai/blog/what-data-informs-ai-driven-decision-making) | Historical background; Chapter 29 only |
| 2023-10-25 | [Spice.ai is now generally available!](https://spice.ai/blog/spice-ai-is-now-generally-available) | Historical background; Chapter 29 only |
| 2023-12-05 | [Spice Firecache — Cloud-Scale DuckDB](https://spice.ai/blog/spice-firecache) | Historical background; Chapter 29 only |
| 2024-03-05 | [Spice AI achieves SOC 2 Type II compliance](https://spice.ai/blog/spice-ai-achieves-soc-2-type-ii-compliance) | Security context; no deployment certification inferred |
| 2024-03-14 | [Interviewing at Spice AI](https://spice.ai/blog/interviewing-at-spice-ai) | Hiring; outside the technical scope |
| 2024-03-28 | [Adding Spice - The Next Generation of Spice.ai OSS](https://spice.ai/blog/adding-spice-the-next-generation-of-spice-ai-oss) | 1, 29; modern runtime lineage |
| 2024-04-01 | [Spice OSS, rebuilt in Rust](https://spice.ai/blog/spice-oss-rebuilt-in-rust) | 1, 29; runtime lineage |
| 2024-05-23 | [On Writing](https://spice.ai/blog/on-writing) | Editorial context |
| 2024-07-04 | [Spice AI Announces Contribution of TableProviders for PostgreSQL, MySQL, DuckDB, and SQLite to the Apache DataFusion Project](https://spice.ai/blog/contribution-of-tableproviders-to-datafusion) | 5, 24, 29; connector interfaces |
| 2024-10-27 | [The Spice.ai for GitHub Copilot Extension is now available!](https://spice.ai/blog/spice-ai-for-github-copilot-extension-now-available) | Historical background; Chapter 29 only |
| 2025-01-22 | [Announcing Spice.ai Open Source 1.0-stable: A Portable Compute Engine for Data-Grounded AI - Now Ready for Production](https://spice.ai/blog/announcing-spice-ai-open-source-1-0-stable) | 1, 19, 29; stable-era context |
| 2025-06-10 | [Announcing Our Partnership with Databricks!](https://spice.ai/blog/databricks-partnership) | 6, 7, 28; integration context |
| 2025-07-16 | [Spice.ai Now Supports Amazon S3 Vectors For Vector Search at Petabyte Scale!](https://spice.ai/blog/amazon-s3-vectors) | 14, 15, 28; vector service boundary |
| 2025-07-31 | [Getting started with Amazon S3 Vectors and Spice](https://spice.ai/blog/getting-started-with-amazon-s3-vectors-and-spice) | 14, 15, 28; keys, vectors, lifecycle |
| 2025-09-15 | [Faster, Simpler Dashboards with Spice and Power BI](https://spice.ai/blog/spice-and-power-bi) | 13, 28; BI client lifecycle |
| 2025-09-17 | [Basis Set Ventures Deploys Spice.ai to Power Natural Language Queries and Mitigate Hallucinations](https://spice.ai/blog/basis-set-ventures-deploys-spice-ai) | 16, 17, 27; changing entity data |
| 2025-09-22 | [True Hybrid Search: Vector, Full-Text, and SQL in One Runtime](https://spice.ai/blog/true-hybrid-search) | 14–16; combined retrieval signals |
| 2025-09-24 | [Spice Cloud v1.7.0: DataFusion v49, Full-Text Search Updates & More](https://spice.ai/blog/announcing-spice-cloud-v1-7-0) | 28; dated Cloud release and migration context |
| 2025-10-06 | [Making Object Storage Operational for Real-Time and AI Workloads](https://spice.ai/blog/making-object-storage-operational) | 7, 27, 28; serving object-stored data |
| 2025-10-08 | [Spice Cloud v1.8.0: Iceberg Write Support, Acceleration Snapshots & More](https://spice.ai/blog/spice-cloud-v1-8-0-iceberg-writes) | 28; dated Cloud release and migration context |
| 2025-10-14 | [Getting Started with Spice.ai SQL Query Federation & Acceleration](https://spice.ai/blog/spice-sql-query-federation-acceleration) | 5, 8, 9; execution placement |
| 2025-10-23 | [Real-Time Hybrid Search Using RRF: A Hands-On Guide with Spice](https://spice.ai/blog/real-time-hybrid-search-using-rrf) | 15; candidate fusion |
| 2025-11-18 | [Write to Apache Iceberg Tables with SQL in Spice](https://spice.ai/blog/write-to-apache-iceberg-tables-with-sql) | 7, 28, 29; versioned write capabilities |
| 2025-11-20 | [Spice Cloud v1.9.0: Introducing the Spice Cayenne Data Accelerator](https://spice.ai/blog/spice-cloud-v1-9-0-cayenne-data-accelerator) | 28; dated Cloud release and migration context |
| 2025-12-10 | [Spice Cloud v1.10: Caching Acceleration Mode, DynamoDB Streams Support, & More!](https://spice.ai/blog/spice-cloud-v1-10) | 28; dated Cloud release and migration context |
| 2025-12-17 | [Introducing Spice Cayenne: The Next-Generation Data Accelerator Built on Vortex for Performance and Scale](https://spice.ai/blog/introducing-spice-cayenne-data-accelerator) | 8, 12, 29; accelerator design |
| 2026-01-02 | [2025 Spice AI Year in Review](https://spice.ai/blog/2025-spice-ai-year-in-review) | 1, 28, 29; release evolution |
| 2026-01-15 | [How we use Apache DataFusion at Spice AI](https://spice.ai/blog/how-we-use-apache-datafusion-at-spice-ai) | 5, 24, 29; planning and extension points |
| 2026-01-22 | [Real-Time Control Plane Acceleration with DynamoDB Streams ](https://spice.ai/blog/real-time-acceleration-with-dynamodb-streams) | 11, 27; control-plane replication |
| 2026-01-30 | [Spice Cloud v1.11: Spice Cayenne Reaches Beta, Apache DataFusion v51, DynamoDB Streams Improvements, & More](https://spice.ai/blog/spice-cloud-v1-11) | 28; dated Cloud release and migration context |
| 2026-02-03 | [Operationalizing Amazon S3 for AI: From Data Lake to AI-Ready Platform in Minutes](https://spice.ai/blog/operationalizing-amazon-s3-for-ai) | 7, 14, 28; AWS data and AI roles |
| 2026-02-05 | [A Developer's Guide to Understanding Spice.ai](https://spice.ai/blog/a-developers-guide-to-understanding-spice-ai) | 1–9, 13–18; conceptual coverage |
| 2026-02-25 | [Apache Iceberg at Spice AI: How we Query, Accelerate, and Write to Open Table Formats](https://spice.ai/blog/apache-iceberg-at-spice-ai) | 7, 24, 29; metadata and lifecycle |
| 2026-03-26 | [Introducing Spice Skills for AI Agents](https://spice.ai/blog/introducing-spice-skills-for-ai-coding-agents) | 18, 29; agent construction workflows |
| 2026-04-07 | [Vortex at Spice AI: The Columnar Format for Data-Intensive Workloads](https://spice.ai/blog/vortex-at-spice-ai-the-columnar-format-for-data-intensive-workloads) | 12, 29; encoded data and maintenance |
| 2026-04-09 | [Apache Ballista at Spice AI: Distributed Query Execution Without the Operational Tax](https://spice.ai/blog/apache-ballista-at-spice-ai) | 22, 29; distributed execution |
| 2026-04-15 | [Multi-Tenancy for AI Agents without the Pipelines](https://spice.ai/blog/multi-tenancy-for-ai-agents-without-pipelines) | 20, 27; isolation alternatives |
| 2026-04-20 | [Spice Cloud v2.0-rc.2: Cayenne RC, ADBC BigQuery, and Catalog Connectors](https://spice.ai/blog/spice-cloud-v2-0-rc-2) | 28; dated Cloud release and migration context |
| 2026-04-21 | [Localhost Latency at Scale: The Spice Cluster-Sidecar Architecture](https://spice.ai/blog/cluster-sidecar-architecture) | 21, 22, 27; centralized ingestion and local serving |
| 2026-05-12 | [AWS Workshop: Federated Queries and Hybrid Search with Spice.ai](https://spice.ai/blog/aws-workshop-federated-queries-and-hybrid-search-with-spice) | 28; staged AWS integration |
| 2026-05-19 | [Building an Enterprise SRE Agent with OpenClaw and Spice](https://spice.ai/blog/openclaw-and-spice-governed-access-to-production-data-for-enterprise-agents) | 18, 23, 28; evidence-based SRE workflow |
| 2026-06-12 | [Barracuda Networks Gains 100x Faster Query Responses and 50% Reduction in Operational Costs with Spice.ai OSS](https://spice.ai/blog/barracuda-networks-100x-faster-query-responses-with-spice) | 27; customer workload study |
| 2026-07-09 | [Spice 2.0: Real-Time Analytical Query on Operational Data, Without ETL](https://spice.ai/blog/spice-2-0-is-now-available) | 10–12, 21, 22, 28; 2.x architecture |
| 2026-07-29 | [The Analytics Replica Pattern: The Shortest Path to Data-Grounded AI](https://spice.ai/blog/the-analytics-replica-pattern-shortening-the-path-to-data-based-ai) | 10, 27; incremental analytics adoption |

## F.4 Reading the engineering source

The configuration contracts come from `crates/spicepod/src`, including datasets, views, acceleration, models, embeddings, authentication, and search-column settings. HTTP request and route checks used `crates/runtime/src/http/routes.rs`, `http/v1/query.rs`, `http/v1/search.rs`, and the request types in `crates/runtime-search`.

Search score names and table-function behavior were cross-checked in `crates/search/src/lib.rs`, `crates/runtime-search/src/full_text_udtf.rs`, and `rrf.rs`, then exercised against the available runtime. Source-level behavior that was not run is identified as architectural or version-sensitive discussion rather than a reproduced guarantee.

Cayenne explanations were grounded in `docs/cayenne/cayenne.md` and `crates/cayenne`. Replication references included the source documents for PostgreSQL, MySQL, MongoDB, and Debezium ingestion. Distributed architecture used the repository's numbered design decisions as context, with no claim that a design document alone proves failover behavior.

## F.5 Related primary projects

[Apache Arrow](https://arrow.apache.org/), [ADBC](https://arrow.apache.org/adbc/), [Apache DataFusion](https://datafusion.apache.org/), [Apache Ballista](https://datafusion.apache.org/ballista/), [Apache Iceberg](https://iceberg.apache.org/), [Delta Lake](https://delta.io/), [DuckDB](https://duckdb.org/), [SQLite](https://www.sqlite.org/), [PostgreSQL](https://www.postgresql.org/docs/), and [Vortex](https://github.com/vortex-data/vortex) define the underlying projects and their own versioned behavior. Consult the versions used by the selected Spice build.

The [Model Context Protocol specification](https://modelcontextprotocol.io/specification) defines the protocol implemented by participating clients and servers. A matching transport name does not eliminate protocol-version or authentication compatibility checks.

## F.6 How this edition resolves source differences

When a blog explains a design and current source uses different field names, the book uses the inspected schema for configuration and retains the blog as architectural context. When a runtime run differs from an expected query result, the actual output is preserved and the acceptance limit is stated. When a feature requires an external service that was not provisioned, the book provides an integration procedure and does not print an invented success transcript.

This method lets the manuscript benefit from the full body of project writing without merging different historical releases into an imaginary single product version.

## F.7 Enterprise source register

The Enterprise chapters use three additional supplied repositories:

| Source | Pinned checkout commit | Role in this edition |
|---|---|---|
| `~/dev/ent` | `7fae16569c601ffd310375ce3cd8e5abd077548e` | Enterprise runtime schemas, authentication and policy, cluster coordination, snapshots, functions, and inference boundaries |
| `~/dev/spice-k8s-operator` | `4de706cabe416e20afe9ef343fc7fb2d375cd00f` | v2 CRDs, chart values, resource reconciliation, Services, storage and rollout behavior |
| `~/dev/docs` | `fd4a879ebc932996295fe9c5fda4178c94b7bb60` | Enterprise product documentation, deployment guides, and production operating context |

The document sources under `enterprise/` were inventoried across getting started, deployment, Kubernetes, features, and production. The following map gives the principal reading path. These are local source paths within the supplied Cloud documentation checkout; the public entry point is [Enterprise documentation](https://docs.spice.ai/docs/enterprise).

| Chapter | Principal documentation | Source cross-check |
|---|---|---|
| 30 | `README.md`, `getting-started/distributions.md`, deployment guides | Runtime distribution and build/configuration boundaries; operator chart identities |
| 31 | `features/authentication.md`, `features/policy.md` | `runtime-auth`, `runtime-policy`, runtime policy enforcer, Spicepod auth/authorization schema |
| 32 | `kubernetes/`, deployment and workload-identity guides | Operator v2 types, rendered CRDs, `resources/service.rs`, chart values and current user guide |
| 33 | `features/distributed-query.md`, `distributed-accelerations.md`, `mtls.md` | Scheduler schema, cluster partition registration, `runtime-cluster`, operator cluster types |
| 34 | `features/acceleration-snapshots.md`, production storage, HA and upgrades | Snapshot behavior and acceleration schema; operator update and standby interfaces |
| 35 | `features/functions.md`, `features/distributed-inference.md` | Function declarations and registration, distributed model configuration; actual local SQL-function run |
| 36 | Production readiness, observability and security guides | Operator OTLP guide, runtime observations, template and workbook acceptance boundaries |

When older public examples and the current operator use different field shapes, the book's Kubernetes templates follow the inspected v2 schema. Architectural promises such as availability or recovery are presented as integration acceptance requirements, not inferred from a prose example. The rendered chart, lint output, template validation and function results are retained under `evidence/enterprise/`.
