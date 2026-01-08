# DR-007: mTLS for Distributed Query Cluster Communication

## Status

Accepted

## Context

Following the decision to adopt Apache Ballista as Spice's distributed query framework (DR-004), internal cluster communication between scheduler and executor nodes includes highly privileged RPC calls:

* **GetAppDefinition**: Executors fetch the full Spicepod configuration from the scheduler, including dataset definitions and runtime settings.
* **ExpandSecret**: Executors request secret values from the scheduler's secret store. This is the most sensitive RPC—secrets should only be available to authenticated cluster members.
* **SchedulerGrpcServer**: Ballista's task dispatch protocol for scheduling query fragments to executors.

mTLS provides mutual authentication (both parties verify identity) and transport encryption, ensuring:

1. Only authenticated nodes can join the cluster and receive secrets.
2. All traffic is encrypted, preventing interception of secrets or query data.
3. No rogue processes can impersonate executors to exfiltrate data.

Related decisions:

* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)
* [DR-006: High Availability Distributed Query with Stateless Schedulers](./006-ha-distributed-query.md)

## Design Principles

* **Secure by default**: mTLS is required for cluster mode unless explicitly opting out with `--allow-insecure-connections`.
* **Developer experience first**: CLI tooling (`spice cluster tls init`, `spice cluster tls add`) simplifies certificate generation; `--allow-insecure-connections` enables quick dev/test setup without certificates.
* **Simplicity**: Standard TLS/mTLS patterns (three-file configuration) work with existing certificate management tools.
* **Industry standards**: gRPC with `tonic` TLS support; standard X.509 certificates; SANs for hostname verification.
* **Extensibility**: Certificate paths are CLI arguments, allowing integration with any certificate management solution.

## Design

### Port Separation

| Port  | Visibility | Services                                            | mTLS Required              |
| ----- | ---------- | --------------------------------------------------- | -------------------------- |
| 50051 | Public     | `FlightServiceServer` (user queries), `OtelService` | Optional (user TLS config) |
| 8090  | Public     | HTTP API (REST queries, health, status)             | Optional (user TLS config) |
| 9090  | Public     | Prometheus metrics                                  | No                         |
| 50052 | Internal   | `SchedulerGrpcServer`, `ClusterService`             | **Required**               |

### CLI Arguments

```bash
# Role selection
--role {scheduler|executor}               # Explicit role (executor implied if --scheduler-address set)

# mTLS configuration (required unless --allow-insecure-connections)
--node-mtls-ca-certificate-file <path>    # CA cert to validate peer certificates
--node-mtls-certificate-file <path>       # This node's certificate
--node-mtls-key-file <path>               # This node's private key

# Network configuration
--node-advertise-address <host>           # How other nodes reach this node
--node-bind-address <bind_address>        # Internal service bind (default: 0.0.0.0:50052)

# Executor-specific
--scheduler-address <url>                 # Scheduler's internal load-balanced/virtual IP cluster URL
                                          # If set, --role executor is implied

# Development/testing only
--allow-insecure-connections              # Disable mTLS (WARNING: never use in production)
```

### Internal gRPC Service

A new `ClusterService` gRPC service replaces the previous Flight Actions for internal communication:

```protobuf
service ClusterService {
    rpc GetAppDefinition(GetAppDefinitionRequest) returns (GetAppDefinitionResponse);
    rpc ExpandSecret(ExpandSecretRequest) returns (ExpandSecretResponse);
}
```

This service is only served on the internal cluster port (50052) with mTLS enforced.

### Certificate Requirements

When mTLS is enabled (the default), all three certificate files are required:

* **CA Certificate**: Used by all nodes to validate peer certificates.
* **Node Certificate**: Signed by the CA; includes SAN matching the `--node-advertise-address`.
* **Private Key**: Corresponding to the node certificate.

For development/testing, use `--allow-insecure-connections` to skip certificate requirements.

### CLI Tooling

```bash
# Initialize a PKI for development/testing
spice cluster tls init

# Generate certificates for a node
spice cluster tls add scheduler1
spice cluster tls add executor1 --host executor1.cluster.local
```

## Consequences

### Breaking Change

* Cluster mode now requires mTLS configuration (or explicit `--allow-insecure-connections` for dev/test). Existing deployments using `--cluster-mode` with API key auth must migrate.

### Renamed CLI Arguments

| Old Name                        | New Name                          |
| ------------------------------- | --------------------------------- |
| `--cluster-mode`                | `--role`                          |
| `--cluster-ca-certificate-file` | `--node-mtls-ca-certificate-file` |
| `--cluster-certificate-file`    | `--node-mtls-certificate-file`    |
| `--cluster-key-file`            | `--node-mtls-key-file`            |
| `--cluster-address`             | `--node-bind-address`             |
| `--cluster-advertise-address`   | `--node-advertise-address`        |
| `--cluster-scheduler-url`       | `--scheduler-address`             |

### New CLI Behavior

* `--scheduler-address`: If set, `--role executor` is implied and can be omitted.
* `--allow-insecure-connections`: Disables mTLS requirement for dev/test (WARNING: never use in production).

### Removed CLI Arguments

* `--cluster-api-key`: Replaced by mTLS.

### New gRPC Service

* `ClusterService` with `GetAppDefinition` and `ExpandSecret` RPCs.
* Served on port 50052 with mTLS.
* `SchedulerGrpcServer` (Ballista) also moved to port 50052.

### Flight Actions Removed

* `GetAppDefinition` Flight Action removed from public port.
* `ExpandSecret` Flight Action removed from public port.
* These are security-sensitive and should never be on the public port.

### TLS Configuration Structure

```rust
pub struct ClusterTlsConfig {
    /// CA certificate used to validate other cluster nodes
    pub ca_certificate: Certificate,
    /// Client TLS config with CA and client identity for mTLS
    pub client_tls_config: ClientTlsConfig,
    /// Server identity (cert + key) for serving TLS
    pub server_identity: Identity,
}
```

### Server Configuration

* Scheduler starts internal cluster server on `--node-bind-address` (default 0.0.0.0:50052).
* Executor starts flight/cluster server on `--node-bind-address` (default 0.0.0.0:50052) with mTLS.
* Both use `ServerTlsConfig` with `client_ca_root` to require client certificates.

### Client Configuration

* Executors connect to scheduler using `ClientTlsConfig` with `identity()` for mTLS.
* Scheduler connects to executors using same pattern for task dispatch.

### Documentation Requirements

* Guide for generating certificates with `spice cluster tls`.
* Guide for using cert-manager in Kubernetes.
* Guide for using corporate/enterprise CAs.
* Migration guide from API key auth to mTLS.

### Testing Requirements

* Integration tests with mTLS-enabled cluster.
* Certificate validation tests (expired, wrong CA, wrong SAN).
* Negative tests (reject connections without valid client cert).

## Links

* [Issue #8558: Enhancement: Distributed Query: mTLS](https://github.com/spiceai/spiceai/issues/8558)
* [Issue #8562: Require mTLS certs for scheduler/executor nodes on internal port](https://github.com/spiceai/spiceai/issues/8562)
* [PR #8580: Require mTLS for distributed query cluster mode](https://github.com/spiceai/spiceai/pull/8580)
* [DR-004: Use Apache Ballista as Spice's distributed query framework](./004-distributed-query-framework.md)
* [DR-005: Extend Apache Ballista with Remote Catalog, UDF Sync, and Cluster Security](./005-ballista-extensions.md)
* [DR-006: High Availability Distributed Query with Stateless Schedulers](./006-ha-distributed-query.md)
* [gRPC TLS Authentication](https://grpc.io/docs/guides/auth/)
* [tonic TLS Configuration](https://docs.rs/tonic/latest/tonic/transport/struct.ServerTlsConfig.html)
