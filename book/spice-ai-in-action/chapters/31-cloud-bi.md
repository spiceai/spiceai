# 28. Cloud, enterprise operations, and BI clients

The same data runtime can sit behind a developer's terminal, a managed application endpoint, an enterprise deployment, or a business-intelligence client. Those environments share concepts but have different operational owners. The blog's Cloud release history, AWS integration guides, Power BI walkthrough, and SRE-agent example help identify the additional boundaries to review.

## 28.1 Separate the management plane from query traffic

The data plane serves SQL, search, inference, and related requests. The management plane creates applications, deploys configurations, manages credentials, and controls organization-level resources. A credential authorized to query a dataset should not automatically be able to redeploy the application that defines it.

The [Cloud v2.0-rc.2 post](https://spice.ai/blog/spice-cloud-v2-0-rc-2) describes a management API, infrastructure-as-code integration, update channels, and audit-log features at its publication date. Use it to understand the management responsibilities, then consult current Cloud documentation for endpoints, scopes, availability, and limits. This book does not freeze pricing, plan entitlements, or retention durations from a release announcement.

A deployment pipeline should use a management identity with only the resource scopes it needs. The application uses a separate data-plane identity. Record which configuration was deployed and link that revision to query and evaluation artifacts.

## 28.2 Managed and self-managed responsibilities

In a managed cluster arrangement, the provider can own scheduler infrastructure, upgrades, and some operational monitoring. The application team still owns its source data contract, permissions, query semantics, tenant routing, and product freshness requirements. Outsourcing a runtime's operations does not outsource the definition of net revenue.

In a self-managed enterprise deployment, the team also owns nodes, certificates, storage, rollout behavior, and capacity planning. An operator can automate parts of that lifecycle, but the custom resources and controller version become another compatibility boundary.

The [Spice 2.0 announcement](https://spice.ai/blog/spice-2-0-is-now-available) describes enterprise operator resources for clusters and single-node or sidecar deployments. Treat these as product-distribution features whose exact availability and schema must be checked in the selected enterprise version. Do not assume every Enterprise control is present in an arbitrary OSS binary.

A responsibility matrix should name the owner of source credentials, data residency, backup, recovery testing, certificate rotation, upgrade approval, and incident response. Shared responsibility becomes manageable when each row has an owner.

## 28.3 An AWS application data path

The [AWS workshop post](https://spice.ai/blog/aws-workshop-federated-queries-and-hybrid-search-with-spice) connects operational data, S3 Tables, S3 Vectors, and Bedrock in a single application-oriented workflow. The durable lesson is that table storage, vector indexing, model inference, and query coordination remain distinct services even when configured through one runtime.

For a Northstar integration, begin with one typed source and one SQL query. Add the table catalog and reconcile its snapshot. Add embeddings and vector storage with a versioned model. Add a generator only after retrieval passes its judgment set. This sequence gives each external dependency a clear acceptance artifact.

Use workload identity and the documented credential mechanism for the environment. Test permissions from the actual runtime role, including catalog operations, data-object access, vector operations, and model invocation. Avoid accepting a lab that works only because the developer's shell has broad credentials.

## 28.4 S3 Vectors is a vector service, not a Parquet prefix

The July 2025 [S3 Vectors introduction](https://spice.ai/blog/amazon-s3-vectors) and [walkthrough](https://spice.ai/blog/getting-started-with-amazon-s3-vectors-and-spice) describe a vector-storage and query integration. A vector bucket or index has a different API and schema from a conventional object prefix containing data files.

The integration must align vector dimension, metric, model space, key identity, and metadata. Source text and relational attributes may live elsewhere, so retrieving a vector candidate can require reconciling it with current source rows. Updates and deletions need to propagate across that boundary.

The historical launch posts contain preview-era availability and limits. Check the current AWS and Spice primary documentation before provisioning. Do not carry preview pricing, dimension limits, or regional availability into a current design by inference.

For evaluation, compare the same query judgments with the local model exercise. Measure candidate recall, update visibility, and authorized filtering as well as response time. An external vector service's cost or scale claims do not establish relevance on Northstar's policies.

## 28.5 Power BI adds its own data lifecycle

The September 15, 2025 [Power BI post](https://spice.ai/blog/spice-and-power-bi) describes a connector built on the Flight SQL ADBC path and discusses Import and DirectQuery modes. These modes create different freshness and capacity relationships.

In an import-oriented workflow, the BI system retains a copy and refreshes it according to its own policy. Spice's up-to-date dataset does not make the BI copy current until that refresh occurs. In a direct-query workflow, report interactions generate requests through the connector, so visual design and concurrent users contribute to runtime load.

Use the current connector installation instructions for the supported desktop or gateway environment. Verify authentication, TLS, driver versions, and data type mappings. Do not assume that a successful Python ADBC session proves a BI gateway is configured correctly.

Build the report against stable business views. Keep the paid-status and refund definitions in one reviewed layer instead of reproducing them differently in SQL, Power Query, and measures. Validate the first visual against the fixture's exact totals before introducing filters and calculations.

## 28.6 Diagnose the query the BI tool actually emits

A visual can produce SQL different from the query you tested in a terminal. Capture the actual runtime request and plan for a representative interaction. Check whether filters, aggregations, and limits reach the intended layer and whether the client imports a larger relation than expected.

Also check type serialization. A decimal amount, timestamp, or large integer identifier may be transformed by the BI client. Reconcile values at the source, Spice, and visual boundary. A formatted currency label does not prove that precision was preserved in the underlying calculation.

Test a dashboard refresh while data is changing. Decide whether all visuals need a consistent data version or whether independently refreshed cards are acceptable. A page can display individually valid values that came from different points in time.

## 28.7 An SRE assistant is a separate capstone variant

The May 19, 2026 [OpenClaw SRE-agent article](https://spice.ai/blog/openclaw-and-spice-governed-access-to-production-data-for-enterprise-agents) combines operational metrics, business records, and runbook retrieval. Its incident narrative illustrates why a useful diagnosis often needs both structured observations and unstructured procedures.

Adapt the Northstar assistant by adding read-only tools for a bounded metrics interval and relevant runbooks. Require a trace connecting a recommendation to the observed service state and retrieved procedure. Keep remediation as a distinct authenticated workflow, with a human or deterministic policy making the required deployment decision.

Do not turn the article's particular PgBouncer change into universal operational advice. Connection-pooling modes have application compatibility consequences. The transferable pattern is to retrieve the applicable runbook, inspect the actual configuration, and validate the proposed change in its own context.

## 28.8 Release history is a migration map

The Cloud posts from v1.7 through v2.0-rc.2 describe the evolution of search indexing, embeddings, Iceberg writes, acceleration snapshots, caching modes, Cayenne, catalogs, and management features. Their value is to identify when an assumption may have changed and which capability needs a release-specific check.

Use a release matrix for upgrades: old version, target version, changed configuration, changed persisted state, changed API behavior, required tests, and rollback path. A feature announced in preview should not be represented as generally supported in every subsequent image without checking the chosen distribution.

**Workshop.** Extend Northstar's release manifest with a managed data endpoint, a separate deployment identity, a BI client, and an optional external vector index. Mark every retained copy and every credential boundary. Then calculate the maximum possible age of a displayed dashboard value under the chosen refresh policies.

The dedicated Enterprise section in Chapters 30–36 develops the self-hosted deployment, identity, policy, operator, and recovery interfaces in detail.
