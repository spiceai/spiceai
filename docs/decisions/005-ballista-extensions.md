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
5. Spice's API will ultimately support multiple query interfaces: SQL (scheduler plans), DataFrame API (client plans), and Substrait (client provides pre-planned queries). The initial implementation will be SQL-only, but the architecture must support client-side planning for future DataFrame and Substrait support.
6. "Client" in this context means an external SDK or application that connects to the Ballista cluster. Initially clients submit SQL; in the future, clients may perform local query planning (DataFrame API) or submit pre-planned queries (Substrait).
7. Initial implementation targets single-scheduler deployments; HA scheduler design is out of scope (see [HA Scheduler Design](https://github.com/spiceai/spiceai/issues/8559)).
8. Extensions should not preclude future HA scheduler implementations.

## Options

These options describe solutions for each high-level problem.

> **Architecture Note**: Initial implementation will be SQL-only (scheduler does planning). However, the architecture must support future client-side planning for DataFrame API and Substrait plan submission. Remote Catalog/UDF Sync will be implemented to enable this future capability.

### Catalog and UDF Synchronization Options

1. **Manual registration**: Require users to manually register tables and UDFs on both scheduler and clients.
2. **Scheduler-to-client sync**: Implement catalog and UDF metadata synchronization from scheduler to clients, with stub providers for remote tables.
3. **External catalog service**: Use a shared catalog (e.g., Hive Metastore, AWS Glue) for synchronization. Requires additional infrastructure and custom client build.
4. **SQL-only interface**: Treat scheduler as the only query planner; clients submit SQL strings only. *(Initial implementation; architecture must support future client-side planning)*

### Cluster Security

1. **External infrastructure**: Rely on service mesh (Istio), VPN, or network policies for cluster security. Defers security to deployer.
2. **TLS with API key auth**: Implement TLS and API key authentication as optional Ballista configuration via gRPC interceptors.
3. **mTLS (mutual TLS)**: Implement mutual TLS for cluster components. Stronger security model but more complex certificate management.
4. **Middleware/interceptors**: Use gRPC interceptors for custom security requirements without modifying Ballista core.

### Executor Readiness

1. **Scheduler-side polling**: Scheduler polls executor health endpoints until they respond. Compatible with HA scheduler designs.
2. **Executor-to-scheduler signal**: Executors signal readiness through a channel or callback after first poll loop. Simpler but limits HA scheduler designs.
3. **gRPC health check protocol**: Use existing gRPC health check protocols (standard, well-supported).
4. **Kubernetes readiness probes**: Rely on K8s orchestration for readiness; Spice runtime doesn't manage executor lifecycle directly.

## First-Principles

* **Developer experience first**: Extensions should be transparent to users; no manual schema replication or complex TLS setup required.
* **Secure by default**: Cluster communication must support TLS/mTLS encryption without requiring users to opt-in.
* **Simplicity**: Prefer minimal fork changes that can potentially be upstreamed; avoid reimplementing Ballista internals.
* **First-class extensibility**: Extensions should integrate via well-defined Ballista extension points (codecs, interceptors, config hooks).
* **Align to industry standards**: Use standard gRPC patterns (interceptors, health checks) and TLS/mTLS rather than custom protocols.

## Decision

Spice will extend Apache Ballista with the following capabilities:

1. **Remote Catalog Synchronization** - Clients can fetch and populate catalog metadata from the scheduler, creating stub table providers for remote tables. Initially used for catalog discovery (`SHOW TABLES`); required for future DataFrame API and Substrait support.
2. **Remote UDF Synchronization** - Clients can fetch UDF metadata (signatures, return types, documentation) from the scheduler. Initially used for function discovery; required for future DataFrame API and Substrait support.
3. **Cluster Security Extensions** - Support for mTLS communication between cluster components. gRPC metadata interceptors enable additional authentication mechanisms if needed.
4. **Executor Readiness Detection** - Scheduler-side health checking of executors, compatible with future HA scheduler designs.

> **Implementation Phases**: Initial release will be SQL-only with scheduler-side planning. Remote Catalog/UDF Sync provides catalog discovery in Phase 1 and enables client-side planning (DataFrame, Substrait) in Phase 2.

These extensions will be maintained in a Spice fork of Apache Ballista at `github.com/spiceai/datafusion-ballista`.

### Why

#### Remote Catalog and UDF Synchronization

* **Phase 1 (SQL-only)**: Enables catalog discovery via `SHOW TABLES`, `DESCRIBE TABLE`, and function introspection from any client.
* **Phase 2 (Future)**: Required for DataFrame API - clients using DataFrame operations (e.g., `df.filter().select()`) will perform local query planning and need catalog metadata to resolve table schemas.
* **Phase 2 (Future)**: Required for Substrait support - clients submitting pre-planned Substrait queries will need UDF signatures to build valid plans.
* Eliminates manual schema and UDF registration on clients, dramatically improving developer experience.
* Clients can discover and explore the full catalog without pre-registration.
* `RemoteTableProvider` and `RemoteScalarUDF` stubs provide clean separation: they participate in query planning but defer execution to the scheduler, ensuring actual data access happens on the cluster.
* Serialization via protobuf ensures efficient metadata transfer and compatibility with Ballista's existing RPC mechanisms.
* Users can write queries in notebooks or applications without needing intimate knowledge of the cluster's registered tables.

#### Cluster Security Extensions

* Production deployments require secure communication between cluster components.
* Spice's multi-tenant architecture requires mTLS for cluster access (stronger than API key auth).
* The endpoint customization pattern (`BallistaConfigGrpcEndpoint`) provides flexibility for TLS configuration without hardcoding specific certificate paths.
* gRPC metadata interceptors (`BallistaGrpcMetadataInterceptor`) enable arbitrary header injection for tracing and auxiliary authentication if needed.
* Making security optional (via builder methods) maintains backward compatibility and allows incremental adoption.
* The extensions integrate cleanly with tonic's `Endpoint` and `Interceptor` traits, following Rust ecosystem patterns.
* mTLS provides mutual authentication, ensuring both scheduler→executor and executor→scheduler connections are verified.

#### Executor Readiness Detection

* Scheduler-side health checking of executors is compatible with HA scheduler designs (each scheduler can independently check executor health).
* Uses standard gRPC health check protocol where possible.
* Avoids executor-to-scheduler signaling that would require reliable delivery to multiple schedulers in HA configurations.
* Useful for testing and deployment orchestration where waiting for "ready" state is necessary.

### Why not

#### Fork Maintenance Burden

* Maintaining a fork increases complexity during DataFusion/Ballista upgrades.
  * **Mitigation**: Keep extensions minimal and isolated. Track upstream changes. Contribute extensions back to Apache Ballista when mature. TLS/security extensions are likely upstreamable.

#### Catalog Sync Performance

* Automatic catalog population on every client connection could be expensive for large catalogs.
  * **Mitigation**: Accept this tradeoff for developer experience. Future optimization could add caching, lazy population, or incremental sync if needed.

#### mTLS Complexity

* mTLS requires certificate management infrastructure (CA, cert rotation, distribution).
  * **Mitigation**: Provide documentation and tooling for certificate generation. Consider integration with cert-manager in Kubernetes environments.

#### Executor-to-Scheduler Signaling (rejected)

* Original design had executors signal readiness to scheduler via oneshot channel.
  * **Problem**: Limits HA scheduler designs—each scheduler would need to reliably receive the signal.
  * **Resolution**: Use scheduler-initiated health checks instead.


## Consequences

### Fork Management

* Spice will maintain a fork of Apache Ballista at `github.com/spiceai/datafusion-ballista` with the extensions.
* The Ballista fork will need to be kept in sync with upstream Apache Ballista releases, particularly during DataFusion upgrades.
* The development team will need processes for managing the fork, tracking upstream changes, and potentially upstreaming features.
* TLS/mTLS extensions are likely candidates for upstreaming to Apache Ballista.

### Security Configuration

* `SchedulerConfig` and `ExecutorConfig` will expose endpoint customization hooks for mTLS configuration.
* gRPC metadata interceptors (`BallistaGrpcMetadataInterceptor`) will be available for additional header injection (tracing, auxiliary auth).
* Documentation will be needed for configuring mTLS in distributed Spice deployments, including certificate management guidance.
* Integration testing will need to cover mTLS-enabled cluster scenarios.

### Readiness Detection

* Scheduler will implement health checking of registered executors using gRPC health check protocol.
* Design is compatible with future HA scheduler implementations (each scheduler independently checks health).

### Remote Catalog/UDF Sync

* New RPC endpoints will be added to Ballista's protobuf definitions:
  * `GetCatalog` - Returns catalog, schema, and table metadata with Arrow schemas.
  * `GetFunctions` - Returns UDF signatures, return types, and documentation.
* `SessionContextExt` in the Ballista client will expose new builder methods:
  * `remote_with_catalog()` - Connects to scheduler and populates catalog.
  * `populate_functions_from_scheduler()` - Fetches and registers remote UDFs.
* `RemoteTableProvider` and `RemoteScalarUDF` will be introduced as stub implementations that error if execution is attempted locally.
* `BallistaLogicalExtensionCodec` will handle serialization/deserialization of remote stubs.

### Dependencies

* HA scheduler design ([#8559](https://github.com/spiceai/spiceai/issues/8559)) should be reviewed in conjunction with this DR to ensure compatibility.

## Links

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [HA Scheduler Design Issue](https://github.com/spiceai/spiceai/issues/8559)
* [Spice DataFusion Ballista Fork](https://github.com/spiceai/datafusion-ballista)
* [PR #1: Catalog metadata sync for Ballista clients with stub tables](https://github.com/spiceai/datafusion-ballista/pull/1)
* [PR #2: Executor poll_loop readiness signaling](https://github.com/spiceai/datafusion-ballista/pull/2)
* [PR #3: Cluster RPC customizations to support TLS and API key auth](https://github.com/spiceai/datafusion-ballista/pull/3)
* [PR #4: Scheduler UDF sync to client planning with stubs](https://github.com/spiceai/datafusion-ballista/pull/4)
* [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)
