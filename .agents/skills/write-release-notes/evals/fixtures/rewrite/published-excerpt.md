# Spice v2.1.0 (June 29, 2026)

Spice v2.1.0 is the next minor release of Spice, headlined by **high-throughput Cayenne CDC**, scaling and resilience improvements to **PostgreSQL logical replication**, expanded **distributed query** with Iceberg catalog scans and broadcast joins, and the upgrade to **DataFusion v54** (including v53), Arrow v58.3, and Vortex v0.74. The release also adds experimental adaptive self-tuning for the Cayenne accelerator, distributed GLM inference, and a range of security, search, and connector improvements.

### Change Data Capture & HTAP

PostgreSQL logical replication (CDC, `refresh_mode: changes`, [introduced in v2.0](https://spiceai.org/docs/components/data-connectors/postgres)) gets significant scaling and resilience work in v2.1:

- **Shared Replication Slot**: Multiple `refresh_mode: changes` [PostgreSQL](https://spiceai.org/docs/components/data-connectors/postgres) datasets on the same connection can name the same `pg_replication_slot` to share a single replication slot, walsender decoder, and publication, with decoded changes multiplexed by `(schema, table)` to each dataset. This collapses the slot count from one-per-dataset to one — staying well under Postgres's default `max_replication_slots = 10`.

```yaml
datasets:
  - from: postgres:public.orders
    name: orders
    params:
      pg_db: mydb
      pg_replication_slot: spice_cdc # shared slot name
    acceleration:
      refresh_mode: changes
  - from: postgres:public.customers
    name: customers
    params:
      pg_db: mydb
      pg_replication_slot: spice_cdc # same name -> one slot, walsender & publication
    acceleration:
      refresh_mode: changes
```

- **Unchanged-TOAST Recovery**: Under `REPLICA IDENTITY FULL`, when an `UPDATE` leaves a large TOASTed column unchanged, pgoutput sends an "unchanged" marker; Spice now fills that value from the old tuple — its old value is its current value — so updates no longer error or drop columns. Without an old tuple, the error persists with a hint to enable `REPLICA IDENTITY FULL`.
- **Transient Walsender Contention**: Slot-contention errors during rolling deploys — `SQLSTATE 55006` ("replication slot is active for PID") and `53300` ("requested standby connections exceeds max_wal_senders") — are now classified as transient and retried with backoff instead of fatally terminating the stream. Replication connections are also released at shutdown start (not process exit), freeing walsender seats for replacement instances.
- **Strict CDC Param Validation**: PostgreSQL CDC parameters are strictly validated rather than silently defaulted.
- **Debezium Schema Evolution**: Fixes for Debezium schema-evolution support, including tombstone-message handling and sign-extension of minimal-width base64 decimals.

