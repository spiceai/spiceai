# DR: Default Ports

## Context

As of release v0.15.3-alpha, Spice.ai OSS exposes up to four endpoints that bind ports when starting the runtime:

- An HTTP API endpoint for SQL query, defaulted to port 3000.
- An Apache Arrow Flight SQL endpoint for SQL query, defaulted to port 50051.
- An OpenTelemetry endpoint to collect metrics and ingest data, defaulted to port 50052.
- A Metrics endpoint to emit metrics used in operational dashboards, defaulted to port 9000.

When developing applications with Spice, developers will have different databases, web servers, and front-end applications that will be running concurrently to connect to Spice.  Developers should be able to set up Spice without overriding default ports to avoid conflicts.

## Assumptions

1. The ports selected for each API should be in a common range for similar types of applications.
2. The ports selected should not conflict with Spice Data Connectors or Accelerator defaults that would be running concurrently.
3. Developers can override the ports for production or local application development if needed.

## Options

1. All ports will be in the same 500XX range. For instance: 50051, 50052.
2. Keep the default ports selected when Spice originally launched.
3. Use the most common ports used in data applications: 8080 and 9000 for HTTP endpoints, 50051 and 50052 for GRPC endpoints.
4. Only change the HTTP ports to 8090 and 9090 to avoid conflicts with HTTP applications, but keep the GRPC endpoints using 50051 and 50052.

## First-Principles

- **Developer experience first**

## Decision

Spice will use these port numbers as defaults:

- Metrics: 9090
- HTTP SQL API: 8090
- Apache Arrow Flight SQL: 50051
- OpenTelemetry: 50052

**Why**:

- Keeping the GRPC ports as-is will not require a breaking change in all Spice client SDK's.
- Back-end and data applications more commonly use ports in the 8000-9000 range for local development.  Examples:
  - Clickhouse, an analytics database, uses port 9000 by default.  
  - Trino, an analytics query engine, uses port 8080 by default.
- The HTTP API endpoint default of 3000 is used by front-end and web applications that could be used to query spice during development. Examples:
  - Next.js and Express, both web application frameworks that could be used to query Spice, default to port 3000.
- No Spice Data Connectors or Accelerators use the selected ports by default.

**Why not**:

- Changing the HTTP endpoints introduces a breaking change in the Spice runtime. The cost of the breaking change needs to be weighed against the benefit for local development.
- Changing the default HTTP ports can reduce the probability of port conflict in local development, but there is no guarantee that a developers environment could use ports that would conflict with the Spice defaults.
- Port 50051 is commonly used as the default port in GRPC tutorials. Not changing the default GRPC ports has a higher probability of port conflicts with GRPC applications.

## Consequences

- A breaking change will be introduced in the v0.16.0-alpha release.  Developers using prior releases of Spice will need to override the default HTTP ports to continue using the old ports, or change their application to use the new defaults.
- All documents, quickstarts, and samples will be updated to the latest defaults.  Developers that pulled from these repos before the breaking change will need to upgrade their port defaults.
- Fewer databases, front-end frameworks, and services run alongside spice for development will have HTTP port conflicts during initial setup.
