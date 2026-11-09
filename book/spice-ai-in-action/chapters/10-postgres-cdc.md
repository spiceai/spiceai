# 10. PostgreSQL CDC from snapshot to recovery

Northstar wants order analytics that follow operational changes without repeatedly running full-table analytical scans. PostgreSQL logical replication can provide a stream of committed changes. Spice's PostgreSQL integration can use that stream for change-driven acceleration. The useful unit of reasoning is the entire pipeline: snapshot, stream, apply, visible state, durable state, and source acknowledgement.

This is an **integration lab**. It requires a disposable PostgreSQL server configured for logical replication and a Spice build supporting the connector and chosen accelerator. The book did not provision an external PostgreSQL service during its local verification. The steps below define the acceptance procedure and expected business results.

## 10.1 Prepare a disposable source

Create the typed `orders` table from Chapter 6 and load the eight fixture rows. For the CSV with a header, PostgreSQL's client-side copy command can load the data from the companion directory:

```sql
\copy public.orders FROM 'data/orders.csv' WITH (FORMAT csv, HEADER true)
```

This is a `psql` command, not SQL submitted to Spice. Run it from the directory containing `data/`. Check the eight-row count and paid totals at the source before introducing replication.

Logical replication requires appropriate server settings, including `wal_level = logical`, sufficient replication slots, and sufficient WAL senders. Changing server settings may require a restart. Managed services expose these settings differently; use the provider's current procedure for the deployed PostgreSQL version.

The replication role needs the source-specific privileges described by the connector documentation, including replication capability and access to the table. Automatic publication creation also has ownership and privilege requirements. A `GRANT CREATE` alone does not grant arbitrary rights to publish someone else's table. For a controlled deployment, have the database owner create the publication and grant the reader the minimum required access.

## 10.2 Keep the row identity unambiguous

Updates and deletes must identify the row to change. A primary key is the straightforward choice for the fixture. PostgreSQL's replica identity controls which old-row information appears in the logical stream. `REPLICA IDENTITY FULL` changes that representation and its cost; it is not a casual replacement for a missing data model.

Test an update to a non-key value, a delete, and—if the application permits one—a key change. Validate how the configured connector and accelerator handle each. A stream that copies inserts successfully has not yet demonstrated mutable-table correctness.

## 10.3 Configure the change-driven dataset

```yaml
# Dataset fragment; database and credentials must exist
- from: postgres:public.orders
  name: orders
  params:
    pg_host: ${ env:PG_HOST }
    pg_port: '5432'
    pg_db: northstar
    pg_user: spice_replication
    pg_pass: ${ env:PG_PASS }
    pg_sslmode: verify-full
    pg_replication_slot: northstar_orders_lab
    pg_publication: northstar_orders_lab_pub
    pg_replication_initial_snapshot: auto
  acceleration:
    enabled: true
    engine: duckdb
    mode: file
    refresh_mode: changes
    primary_key: order_id
    on_conflict:
      order_id: upsert
    params:
      duckdb_file: .spice/duckdb/orders-cdc.db
```

Use slot and publication names owned by this disposable integration. A slot represents consumer progress and retained log history. Do not point unrelated consumers at the same slot unless you are deliberately using a supported coordinated sharing mechanism. For normal independent replicas, plan separate consumer state and source retention.

The inspected connector documentation describes `auto`, `disabled`, and `always` initial-snapshot policies. Older recipes may use legacy values. Verify the accepted modes against the installed release. An existing slot plus an empty accelerator is not automatically a safe starting state: resuming later changes cannot recreate rows that existed only in the missing snapshot.

## 10.4 Understand the bootstrap boundary

A correct bootstrap must connect a consistent initial snapshot with a stream position that covers later changes. Otherwise, a row can be missed between “copy complete” and “stream begins,” or counted twice without correct reconciliation. The connector owns this protocol; an application should not independently invent a timestamp boundary to stitch the two together.

During bootstrap, inspect readiness and load progress. Do not expose a partially loaded analytical dataset merely because the HTTP process responds. Define whether the application waits for the dataset's ready condition or supports a documented loading state.

After the initial load, run the paid-order query. The acceptance result remains 22,200 cents for `north` and 24,900 for `south`. Compare individual keys as well as totals: two offsetting mistakes can leave a sum unchanged.

## 10.5 Exercise insert, update, and delete

At the source, run a transaction on the disposable fixture:

```sql
BEGIN;
INSERT INTO public.orders VALUES
  (1010, 'north', 1, '2026-08-07T09:00:00Z', 'paid', 1200);
UPDATE public.orders
SET total_cents = 3000
WHERE order_id = 1005;
DELETE FROM public.orders WHERE order_id = 1007;
COMMIT;
```

Poll Spice until it contains order 1010 with 1,200 cents, order 1005 with 3,000 cents, and no order 1007. These are the acceptance conditions. The northern paid-order count remains four: one insert and one delete offset. The northern total becomes 23,900 cents: 22,200 + 1,200 + 500. That arithmetic illustrates why both keys and aggregates belong in a CDC test.

Use a bounded polling loop. On timeout, retain the last rows, runtime log, and source slot state. Do not repeat the source transaction blindly; the insert's primary key is intentionally fixed so a repeated mutation is detectable.

## 10.6 Visibility and durability are distinct

A change can be query-visible before every part of the replication pipeline has reached a durable recovery point. Different accelerators and versions use different checkpoint strategies. Reason about which state survives a process crash, which stream position the source has acknowledged, and which events remain available for replay.

A source acknowledgement that outruns recoverable accelerator state would require special recovery guarantees. Conversely, retaining more source history than necessary consumes WAL storage. The production objective is a correct recoverable boundary with bounded backlog, not merely the smallest observed lag metric.

Inspect the documented replication and accelerator behavior together. Claims such as “exactly once” need a stated failure model: process crash, disk loss, source failover, and network interruption are different failures. Repeated application of a keyed event may be idempotent, but that does not make every surrounding transaction or side effect exactly once.

## 10.7 Restart and outage drills

First, restart Spice gracefully with its persistent state intact. Verify the existing rows and then apply a new source update. Second, stop the consumer while writing a known sequence of changes, then restart it and reconcile the full key set and final values. Third, rehearse loss of the accelerator in an isolated copy and use the documented resnapshot procedure.

At the source, inspect `pg_replication_slots` and its retained or confirmed positions using PostgreSQL's version-appropriate queries. Monitor WAL retention and storage pressure. A stopped consumer can retain history, and source policies may invalidate an excessively lagging slot. The recovery procedure must handle that condition explicitly.

## 10.8 Decommission deliberately

Stopping Spice does not necessarily remove a durable replication slot. Once the consumer is permanently retired and no recovery depends on it, the database owner should remove only the slot and publication owned by that integration. Inventory consumers before deletion. A generic cleanup command against every inactive slot can destroy another consumer's recovery path.

**Exercise.** Design a restart test where the consumer stops after the source commit but before the application sees the update. Specify the rows, positions, and logs you will capture. Explain why a successful `SELECT COUNT(*)` after restart is insufficient.

**Further reading.** Use cookbook `postgres/cdc/`, the [PostgreSQL connector reference](https://spiceai.org/docs/components/data-connectors/postgres), and the pinned source document `docs/features/postgres-replication.md`.
