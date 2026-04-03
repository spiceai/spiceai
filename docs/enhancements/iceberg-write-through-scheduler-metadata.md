# Iceberg Write-Through Scheduler Metadata Follow-Up

Current MVP behavior for DDL-created Iceberg write-through tables creates a local Cayenne-backed accelerated table on the scheduler so the registered provider can expose write-through and partition metadata.

Follow-up design to track:

- Stop creating a local Cayenne accelerator on the scheduler for DDL-created Iceberg write-through tables.
- Register a scheduler-only write-through provider that carries:
  - the federated Iceberg provider
  - the table schema
  - the `PARTITION BY` SQL expression(s)
  - enough identity for distributed accelerated read routing
- Keep real Cayenne accelerator creation on executors only, driven by forwarded DDL.
- Use scheduler-stored DDL partition metadata for distributed insert routing instead of relying on a local Cayenne provider shape.

Why:

- The scheduler does not need local Cayenne storage to forward inserts.
- Scheduler-local Cayenne instantiation complicates provider detection and wrapper unwrapping.
- A metadata-only scheduler provider is closer to the intended distributed architecture.