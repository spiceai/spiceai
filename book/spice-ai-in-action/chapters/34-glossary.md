# Appendix E. Glossary

**Acceleration.** Maintaining a selected representation of source data for serving queries. It introduces refresh, storage, visibility, and recovery responsibilities.

**ADBC.** Arrow Database Connectivity, an interface family for database access with Arrow-oriented results. A client driver and server protocol must be compatible.

**Agent.** A workflow that uses a model to select or sequence operations. Its tool access, resource budget, and termination conditions require explicit control.

**Analytics replica.** A serving copy of operational data arranged for analytical access, with its own compute and update path.

**Arrow Flight.** An Arrow-oriented RPC transport used to transfer data and associated operations. Flight SQL defines a SQL-oriented service on that foundation.

**Arrow.** A typed columnar data representation and ecosystem used for in-memory interchange and related interfaces.

**Authentication.** Establishing the identity or credential of a caller. It is distinct from deciding which rows or operations that caller may access.

**Authorization.** Deciding which resources and actions an established caller may use. It must be enforced at all exposed paths.

**Ballista.** A distributed query foundation used by Spice to coordinate eligible execution across schedulers and executors.

**Batch.** A bounded group of records processed together. An Arrow record batch has a schema and equal-length column arrays.

**BM25.** A lexical relevance scoring family based on term and corpus properties. Its scores are not directly comparable to vector similarity values.

**Bootstrap.** Establishing the initial usable state of a dataset, index, runtime, or consumer before ordinary incremental operation.

**Cache key.** The identity under which a reusable result is stored. It must reflect all relevant query and authorization context.

**Candidate set.** The preliminary retrieved items available for fusion, reranking, or evidence selection. Later stages cannot recover an absent candidate without another retrieval.

**Catalog.** A namespace and discovery mechanism for tables and related metadata. Broad catalog access can expand an application's exposed data surface.

**Cayenne.** Spice's native data accelerator, using Vortex-backed data and managed metadata with mutable ingestion and maintenance behavior.

**CDC.** Change data capture: a mechanism representing source mutations so another system can update its own state.

**Cedar.** A policy language used by the Enterprise authorization engine to evaluate principals, actions, and resources.

**Checkpoint.** A recorded progress or recovery boundary. Its meaning depends on the source and consumer protocol.

**Chunk.** A passage-level retrieval unit derived from a document. It needs identity and provenance back to the source version.

**Citation.** A reference from an answer to supporting evidence. A valid identifier does not by itself prove that the passage supports the claim.

**Compaction.** Reorganizing stored data and mutation state through the engine's supported maintenance process.

**Connector.** A component that exposes a source's data and capabilities to the runtime, including schema, access, and supported pushdowns.

**Coverage.** The subset of source history or entities represented by a dataset. It is distinct from how recently that subset was refreshed.

**CRD.** Kubernetes Custom Resource Definition: the API schema and lifecycle registration for a resource such as SpicepodSet.

**Data contract.** The intended schema, keys, grain, semantics, scope, and lifecycle of data exposed to an application.

**Data plane.** The path that serves queries, retrieval, inference, and application data operations.

**DataFusion.** The Rust and Arrow-based SQL planning and execution foundation extended by Spice.

**Dataset.** A named relation configured through a source connector and optionally an accelerator or search representation.

**Decimal.** A fixed-precision numeric representation suited to domains requiring controlled scale and rounding.

**Delegation.** Sending a supported query or operation to another runtime or execution service instead of resolving it entirely locally.

**Differential test.** Comparing observable results across two execution paths while holding the intended semantics constant.

**Embedding.** A vector produced by a specified model and preprocessing pipeline to represent an input in a learned space.

**Enterprise.** The self-hosted Spice distribution and associated enterprise capabilities and support arrangements.

**Event time.** The business timestamp associated with an event. It can differ from commit, arrival, application, and response times.

**Exact statistic.** A statistic guaranteed to describe the data relevant to the operation. Marking an estimate exact can affect correctness.

**Executor.** A node or process assigned tasks in a distributed query deployment.

**Federation.** Querying data across separately managed sources through a common interface, with work divided according to capabilities and planning.

**Flight SQL.** A SQL-oriented protocol on Arrow Flight used by compatible database clients and drivers.

**Freshness.** How far the data visible to a request trails the source state the application expects it to represent.

**Full refresh.** Rebuilding or replacing the configured accelerated representation from a complete extraction under the selected engine's semantics.

**Grain.** What one row represents, such as one order, one line item, or one refund event. Correct joins and aggregates depend on it.

**Hybrid search.** Combining retrieval signals such as lexical and vector search, often with relational filters and rank fusion.

**Idempotency.** A property under which repeating a defined operation does not create an additional unintended logical effect.

**Index.** A derived access structure maintained to support selected lookup or search operations. Its update and deletion lifecycle matters.

**Inference.** Running a model on input to produce an output, locally or through a provider service.

**IRSA.** IAM Roles for Service Accounts, an EKS workload-identity mechanism for obtaining temporary AWS credentials.

**JWKS.** JSON Web Key Set, used by token validators to obtain public keys for supported signed tokens.

**Lakehouse.** An architecture using object-stored data with table management and analytical access, often through open table formats.

**Logical plan.** A representation of query meaning before all physical execution choices are fixed.

**Management plane.** The interfaces and workflows that create, configure, deploy, and govern runtime resources and identities.

**Materialization.** Storing the result or representation of a data transformation so later operations can reuse it.

**MCP.** Model Context Protocol, used by compatible clients and servers to discover and invoke tools and related capabilities.

**Metadata.** Information used to interpret, discover, or manage data, such as schema, file membership, versions, and statistics.

**Model alias.** An application-facing model name that refers to a configured provider or local model selection.

**mTLS.** Mutual TLS, in which peers authenticate through certificates as part of an encrypted connection.

**NSQL.** Spice's natural-language SQL capability. Its generated queries still require semantic, authorization, and resource validation appropriate to the application.

**NULL.** SQL's representation of a missing or unknown value, with three-valued logic that differs from zero or an empty string.

**OIDC.** OpenID Connect, the identity protocol underlying the documented Enterprise bearer-token integration.

**Partition.** A subdivision of data or execution work. Storage partitioning, stream partitioning, and query partitions are different concepts.

**Physical plan.** The executable operators and data-access choices selected for a query.

**Policy annotation.** Metadata attached to a Cedar policy, used by Spice for documented row-filter and column-mask expressions.

**Primary key.** The column or columns identifying a logical row within a declared uniqueness scope.

**Projection.** Selecting columns or expressions from a relation. Projection pushdown can reduce data read or transferred.

**Provenance.** Information connecting a value or passage to its source, identity, version, and transformation history.

**Pushdown.** Executing a supported operation closer to the source or storage representation while preserving query semantics.

**PVC.** Kubernetes PersistentVolumeClaim: a request for storage with an access mode, capacity, and storage-class contract.

**RAG.** Retrieval-augmented generation: supplying retrieved evidence to a generator as part of an answer workflow.

**Readiness.** The deployment's condition for accepting intended work. It is more specific than a process responding to a health check.

**Reconciliation.** A controller loop that compares desired and observed resources and acts to bring them into agreement.

**Reconciliation.** Comparing representations using keys, values, counts, or other invariants to establish whether they agree.

**Replica identity.** Source-specific information identifying rows in a change stream, particularly for updates and deletes.

**Reranker.** A stage that reorders a candidate set using an additional relevance model or scoring method.

**Residual filter.** A predicate still evaluated after a source or storage operation because earlier filtering was absent or insufficiently exact.

**Result cache.** Stored results of eligible requests, distinct from a generally queryable accelerated dataset.

**RRF.** Reciprocal rank fusion, which combines ranked lists using rank-based contributions rather than equating raw scoring scales.

**RSS.** Resident set size, a process memory observation. It is not identical to a configured query-memory budget.

**Scheduler.** The distributed-query component coordinating jobs, stages, and task assignment.

**Schema evolution.** A change to data structure or types and the policy for propagating it through consumers and persisted representations.

**Sentinel.** A uniquely identifiable probe change used to observe a path's visibility and freshness.

**Shuffle.** Redistribution or transfer of intermediate data between distributed execution stages.

**Sidecar.** A runtime deployed beside an application instance, often sharing its host or pod lifecycle while providing a separate service boundary.

**Snapshot.** A coherent version of state or a supported persisted artifact representing it. Different systems use the term for different objects.

**Spicepod.** The declarative application configuration for Spice datasets, views, models, tools, and runtime behavior.

**SpicepodCluster.** The operator resource describing scheduler and executor pools for distributed Spice execution.

**SpicepodSet.** The operator resource describing managed replicas of a Spice application.

**Staleness.** The age or divergence of visible state relative to the application's expected source state.

**Standby generation.** Retained workloads from a previous specification, available for a supported traffic rollback while retained.

**System of record.** The authoritative owner of a business fact or transaction.

**Tenant.** A security and ownership domain within an application. Tenant identity must come from a trusted context.

**Tensor parallelism.** Dividing one model's computation across participating devices or nodes; distinct from distributing SQL tasks.

**Tombstone.** A deletion marker used by a storage or event protocol. Its semantics depend on that protocol.

**UDF.** User-defined function, typically a scalar operation with a declared argument and return signature.

**UDTF.** A user-defined table function, which exposes a relation through function syntax, such as search results.

**UDTF.** User-defined table function, returning a relation to a SQL FROM clause.

**Upsert.** An operation that inserts a new logical row or updates an existing row according to a key and conflict policy.

**Visibility.** Which version of data a query is allowed to observe under the engine and application contract.

**Volatility.** A function's semantic declaration of whether its result is immutable, stable within a query, or variable per call.

**Vortex.** A columnar format and array foundation used by Cayenne and selected transport paths, with encoding-aware execution opportunities.

**WAL.** Write-ahead log; in PostgreSQL CDC, logical replication derives committed changes from the source's logging mechanisms.

**Watermark.** A progress boundary used to select or reason about incremental data. Its units and inclusion rules must be explicit.

**Working set.** The bounded subset of data an application or runtime retains for its expected serving workload.


**Workload identity.** The identity used by a running workload to access infrastructure services, distinct from its end-user identity.
