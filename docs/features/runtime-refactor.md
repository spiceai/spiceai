# Runtime Data Connector/Accelerator Decoupling

## Objective
- Remove compile-time coupling between the runtime crate and specific DataConnector/DataAccelerator implementations.
- Introduce a lightweight `runtime-interfaces` crate that exposes only the shared traits, structs, and distributed slice macros used to register implementations.
- Relocate concrete connector/accelerator implementations into other crates (starting with `data_components`) while keeping distributed slice registration working.

## Scope and Constraints
- Keep existing runtime behavior and distributed slice discovery working during the transition.
- Prefer incremental moves; connectors/accelerators that the runtime downcasts directly may stay temporarily until call sites are abstracted.
- Avoid adding heavyweight dependencies to `runtime-interfaces`; if a type forces a heavy dependency, consider extracting a slimmer wrapper or deferring that move.
- No feature gating changes for the runtime binary should alter user-facing defaults without a deliberate follow-up.

## Current State Snapshot
- Traits, registration macros, and distributed slices live inside `crates/runtime/src/dataconnector` and `crates/runtime/src/dataaccelerator`.
- Implementations for many connectors/accelerators also live inside the runtime crate; `data_components` already hosts connector-related code but not the runtime traits.
- `linkme` distributed slices collect registrations at link time; connectors call `register_data_connector!` and accelerators call `register_data_accelerator!`.

## Risks / Open Questions
- Which connectors/accelerators are downcasted in the runtime (e.g., to access engine-specific behaviors) and therefore harder to move?
- How to minimize `datafusion`/`arrow`/`runtime`-internal type leakage into `runtime-interfaces` while keeping signatures usable?
- Do any macros or type aliases rely on private runtime modules that would need public exposure or redesign?
- Are there registration ordering constraints or feature-flag interactions that would break `linkme`-based discovery when implementations move into other crates?

## Plan and Phases

- [ ] **Phase 1: Inventory and design the interface crate**
  - [ ] Enumerate traits, structs, type aliases, and macros required by connectors/accelerators (e.g., `DataConnectorFactory`, `DataConnector`, `DataAccelerator`, `ParameterSpec`, registration macros, distributed slice statics).
  - [ ] Decide minimal dependencies for `runtime-interfaces` and whether any helper types need to be duplicated or simplified to keep the crate small.
  - [ ] Identify blocking references from runtime to concrete connector/accelerator types (downcasts, concrete config structs, helper modules like secrets/paths).
- [ ] **Phase 2: Introduce `runtime-interfaces` crate**
  - [ ] Create `crates/runtime-interfaces` with Cargo metadata, license headers, and feature flags mirroring the current optional connector/accelerator gates where needed.
  - [ ] Move shared traits/macros/types into the new crate and re-export them from the runtime crate temporarily to reduce churn.
  - [ ] Update `runtime` to depend on `runtime-interfaces` instead of its local modules for these definitions.
  - [ ] Add `linkme` (and any macro exports) to `runtime-interfaces`, and confirm distributed slice registration works via a small connector test.
- [ ] **Phase 3: Rehome connector/accelerator implementations to `data_components`**
  - [ ] Move DataConnector implementations from `crates/runtime/src/dataconnector/*` into `crates/data_components` (or subcrates later), preserving feature flags and registration macros from `runtime-interfaces`.
  - [ ] Move DataAccelerator implementations similarly, ensuring any runtime-only utilities (e.g., path resolution, secrets) are provided through interfaces or new small helper crates.
  - [ ] Adjust Cargo features/Makefile targets so consumers enable connector/accelerator features through the new crate boundaries.
  - [ ] Create a migration order (low-risk first: connectors without downcasts or runtime-only helpers; defer special cases).
  - [ ] For each moved connector/accelerator, add/adjust tests and ensure `register_*` macro usage points to the `runtime-interfaces` definitions.
- [ ] **Phase 4: Handle special cases and cleanup**
  - [ ] Identify connectors/accelerators that still require runtime downcasts; either add trait hooks to eliminate downcasts or leave them in runtime with TODOs to revisit.
  - [ ] Remove deprecated code paths in runtime once all implementations are externalized; ensure distributed slice registration still pulls in all enabled crates.
  - [ ] Run lint/tests/benchmarks to validate no regressions and update docs (README, extensibility) to describe the new crate layout.
  - [ ] Update developer docs (extensibility, style guide sections) to point contributors to the new crate layout.

### Connector/Accelerator migration ordering (initial draft)
- Low-risk connectors (no runtime-only helpers; mostly TableProvider construction): `file`, `memory`, `localpod`, `git/github/graphql`, `https`, `sink`, `deferred`, `glue`, `iceberg` (if no downcasts), `s3` flavors, `flightsql/flight`.
- Medium-risk (uses secrets/parameters helpers, or runtime metrics hooks): `postgres`, `mysql`, `mssql`, `oracle`, `odbc`, `kafka`, `mongodb`.
- High-risk / likely to defer (runtime downcasts or tight coupling): accelerators using path helpers (`duckdb`, `sqlite`, `turso`), snapshot logic, or any connector/accelerator with runtime-specific types in signatures.

## Phase 1 Inventory (working list)
- **Traits/macros to extract**: `DataConnectorFactory`, `DataConnector`, `DataAccelerator`, registration macros (`register_data_connector!`, `register_data_accelerator!`), distributed slice statics, `ParameterSpec`/`Parameters` structs used in connector/accelerator signatures, `AccelerationSource` interfaces, metrics hooks used by connectors.
- **Dependencies to minimize**: `datafusion`/`arrow` types exposed in trait signatures; `secrecy` wrappers; `linkme`; `async_trait`; `snafu` for error types; parameter parsing utilities; `runtime`-specific helpers (secrets, base paths, metrics).
- **Known runtime downcasts (likely blockers)**:
  - Connectors: `dynamodb` (downcasts to `DynamoDBTableProvider`), `debezium` (downcasts to `DebeziumKafka`), `kafka` (downcasts to `data_components::kafka::Kafka`), `listing` (downcasts to `dataconnector::s3::S3`), `spiceai` (downcasts to `FederatedTableProviderAdaptor`/`FlightTable` and error types), `github` (downcasts to `StructArray`), `postgres`/`mysql` (error downcasts to `dbconnection::Error`).
  - Accelerators: `duckdb` (downcasts to `DuckDBTableWriter`), `sqlite` (downcasts to `SqliteTableWriter`), `postgres` (downcasts to `PostgresTableWriter`), `partitioned_duckdb` (downcasts to `PolyTableProvider`/`DuckDBTableWriter`), `turso` (array downcasts), `spice_sys` helpers (downcasts to duckdb/sqlite/turso accelerators), dataset checkpoint paths (`sqlite`), and time/array downcasts in `dataaccelerator::mod.rs`.
- **Actions**:
  - Decide which downcasts can be swapped for trait hooks (e.g., table writer capability traits) and which to defer.
  - Draft dependency list for `runtime-interfaces` `Cargo.toml` with optional features mirroring current gates.
  - Identify any shared config/parameter structs that should move with traits vs. stay in `runtime` to avoid churn.
  - Map connector parameter plumbing: `ConnectorParams`/builder rely on `runtime`-local registries, secrets, app/runtime handles, and `Parameters`; likely keep builder/validation in `runtime` and expose only the consumable `Parameters`/`ParameterSpec` types from `runtime-interfaces`.
  - Catalog which helper modules (e.g., `runtime_secrets`, base path helpers, metrics providers) are referenced directly by connectors/accelerators and may need small façade traits to avoid pulling full crates into `runtime-interfaces`.

## Progress Log
- [x] 2025-12-04: Plan authored; added phased milestones and initial migration ordering.
- [ ] 2025-12-04: Phase 1 inventory started (downcasts cataloged; dependency minimization goals captured).
