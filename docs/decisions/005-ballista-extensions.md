# DR: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security

## Context

Following the decision to adopt Apache Ballista as Spice's distributed query framework (DR-004), Spice needs to integrate Ballista's scheduler-executor model with its existing features including metrics, telemetry, custom UDFs, search capabilities, and security requirements.

Stock Apache Ballista has several gaps that prevent seamless integration with Spice:

1. Clients cannot discover or query tables registered only on the scheduler without explicit registration on the client side.
2. Custom UDFs defined on the scheduler are not available for client-side query planning.
3. No built-in support for TLS communication between cluster components or API key authentication.
4. No programmatic way to detect when executors are ready to process work.

These limitations would require users to manually replicate schema definitions and function registrations across all clients, undermining the developer experience. Additionally, Spice's multi-tenant architecture requires secure cluster communication that stock Ballista does not provide.

Related decisions:
* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)

## Assumptions

1. Spice will maintain a fork of Apache Ballista to implement these extensions until they can be upstreamed.
2. The scheduler will be the source of truth for catalog metadata and UDF definitions in a Ballista cluster.
3. Cluster components (scheduler, executors, clients) need secure communication for production deployments.
4. Extensions should be backward compatible and not require changes to the core Ballista execution model.
5. Client-side planning with remote metadata should feel transparent to users.

## Options

These options describe solutions for each high level problem.

### Remote Catalog and UDF Synchronization

1. Require users to manually register tables and UDFs on both scheduler and clients.
2. Implement catalog and UDF metadata synchronization from scheduler to clients.
3. Use a shared external catalog service (e.g., Hive Metastore, AWS Glue).
4. External catalog service: Hive Metastore or AWS Glue would provide catalog synchronization without custom code. However, this would require a custom client build.

### Cluster Security

1. Rely on external infrastructure (e.g., service mesh, VPN) for cluster security.
2. Implement TLS and authentication as optional Ballista configuration.
3. Use middleware/interceptors for custom security requirements.
4. Documentation / recommendation to use a service mesh (e.g. Istio) and defer control plane security to the deployer.

### Executor Readiness

1. Poll executor endpoints until they respond successfully.
2. Add explicit readiness signaling through a channel or callback.
3. Use existing gRPC health check protocols.

## First-Principles

- **Developer experience first**
- **Simplicity**
- **Security by default**

## Decision

Spice will extend Apache Ballista with the following capabilities:

1. **Remote Catalog Synchronization** - Clients automatically fetch and populate catalog metadata from the scheduler, creating stub table providers for remote tables.
2. **Remote UDF Synchronization** - Clients automatically fetch UDF metadata (signatures, return types, documentation) from the scheduler for query planning.
3. **Cluster Security Extensions** - Support for TLS communication between cluster components and gRPC metadata interceptors for API key authentication.
4. **Executor Readiness Signaling** - Executors signal readiness through an optional oneshot channel after completing their first poll loop iteration.

These extensions will be maintained in a Spice fork of Apache Ballista at `github.com/spiceai/datafusion-ballista`.

### Why

##### Remote Catalog and UDF Synchronization

- Eliminates manual schema and UDF registration on clients, dramatically improving developer experience.
- Enables `SHOW TABLES` and catalog discovery queries to work seamlessly from any client.
- Clients can plan queries against the full catalog without pre-registration.
- `RemoteTableProvider` and `RemoteScalarUDF` stubs provide clean separation: they participate in query planning but defer execution to the scheduler, ensuring actual data access happens on the cluster.
- Serialization via protobuf ensures efficient metadata transfer and compatibility with Ballista's existing RPC mechanisms.
- Users can write queries in notebooks or applications without needing intimate knowledge of the cluster's registered tables.
- A single build of a Ballista client (after these changes) would remain compatible with subsequent releases unless there is a significant change in RPC schema/workflow.

##### Cluster Security Extensions

- Production deployments require secure communication between cluster components.
- Spice's multi-tenant architecture requires API key authentication for cluster access.
- The endpoint customization pattern (`BallistaConfigGrpcEndpoint`) provides flexibility for TLS configuration without hardcoding specific certificate paths.
- gRPC metadata interceptors (`BallistaGrpcMetadataInterceptor`) enable arbitrary header injection for authentication and tracing.
- Making security optional (via builder methods) maintains backward compatibility and allows incremental adoption.
- The extensions integrate cleanly with tonic's `Endpoint` and `Interceptor` traits, following Rust ecosystem patterns.

##### Executor Readiness Signaling

- Allows Spice runtime to coordinate executor lifecycle without brittle polling.
- One-time signal via oneshot channel is non-intrusive and doesn't affect hot path performance.
- Useful for testing and deployment orchestration where waiting for "ready" state is necessary.

### Why not

##### Fork Maintenance Burden

- Maintaining a fork increases complexity during DataFusion/Ballista upgrades.
  - **Mitigation**: Keep extensions minimal and isolated. Track upstream changes. Contribute extensions back to Apache Ballista when mature.

##### Catalog Sync Performance

- Automatic catalog population on every client connection could be expensive for large catalogs.
  - **Mitigation**: Accept this tradeoff for developer experience. Future optimization could add caching or lazy population if needed.


## Consequences

- Spice will maintain a fork of Apache Ballista at `github.com/spiceai/datafusion-ballista` with the extensions.
- The Ballista fork will need to be kept in sync with upstream Apache Ballista releases, particularly during DataFusion upgrades.
- New RPC endpoints will be added to Ballista's protobuf definitions:
  - `GetCatalog` - Returns catalog, schema, and table metadata with Arrow schemas.
  - `GetFunctions` - Returns UDF signatures, return types, and documentation.
- `SessionContextExt` in the Ballista client will expose new builder methods:
  - `remote_with_catalog()` - Connects to scheduler and populates catalog.
  - `populate_functions_from_scheduler()` - Fetches and registers remote UDFs.
  - `with_ballista_grpc_metadata()` - Configures metadata headers for authentication.
  - `with_ballista_override_create_grpc_client_endpoint()` - Customizes gRPC endpoints for TLS.
- `SchedulerConfig` and `ExecutorConfig` will expose endpoint customization hooks for TLS configuration.
- `RemoteTableProvider` and `RemoteScalarUDF` will be introduced as stub implementations that error if execution is attempted locally (they must be executed on the cluster).
- `BallistaLogicalExtensionCodec` will handle serialization/deserialization of remote stubs, resolving them to concrete implementations when queries reach the scheduler.
- The executor's `poll_loop` function will accept an optional `oneshot::Sender` to signal readiness after the first successful poll.
- Integration testing will need to cover TLS-enabled cluster scenarios and remote metadata synchronization.
- Documentation will be needed for configuring TLS and API key authentication in distributed Spice deployments.
- The development team will need processes for managing the fork, tracking upstream changes, and potentially upstreaming features.

## Links

- DR-004: Use Apache Ballista as Spice's distributed query framework
- [Spice DataFusion Ballista Fork](https://github.com/spiceai/datafusion-ballista)
- [PR #1: Catalog metadata sync for Ballista clients with stub tables](https://github.com/spiceai/datafusion-ballista/pull/1)
- [PR #2: Executor poll_loop readiness signaling](https://github.com/spiceai/datafusion-ballista/pull/2)
- [PR #3: Cluster RPC customizations to support TLS and API key auth](https://github.com/spiceai/datafusion-ballista/pull/3)
- [PR #4: Scheduler UDF sync to client planning with stubs](https://github.com/spiceai/datafusion-ballista/pull/4)
- [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)
