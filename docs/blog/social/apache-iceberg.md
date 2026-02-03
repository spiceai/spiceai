# Apache Iceberg

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

Apache Iceberg: How Modern Data Lakes Get ACID Transactions

Traditional data lake formats like Hive treat files as the table. Write a file, it's in the table. Overwrite a file, the old data is gone. This simplicity creates serious problems at scale.

Apache Iceberg adds a metadata layer that gives data lakes the transactional guarantees we expect from databases—without giving up the benefits of open file formats.

The core problem with Hive-style data lakes: concurrent writes and reads can corrupt data, there's no rollback (overwritten files are gone), schema changes require file rewrites, and partition structure leaks into queries.

Iceberg solves this with a layered architecture. At the top is a catalog (REST API, AWS Glue, or Hive Metastore) that holds a pointer to the current table metadata. The metadata file contains the schema, partition spec, and snapshot history. Each snapshot points to a manifest list, which references the actual data files stored as immutable Parquet.

Key Iceberg concepts:

→ Snapshot Isolation: Readers always see a consistent snapshot. Reader starts on snapshot v5, reads v5's files—even if v6 commits mid-read. No corruption possible.

→ Atomic Commits: Write new files, then atomically update metadata to point to them. Either the full transaction commits or nothing changes.

→ Time Travel: Every snapshot is retained until explicitly cleaned up. Query data as of any historical snapshot or timestamp with syntax like SELECT FROM table FOR VERSION AS OF 5.

→ Schema Evolution: Add, rename, or drop columns as metadata changes. Parquet files aren't modified. Old files read with old schema, new files with new schema. Iceberg reconciles at query time.

→ Hidden Partitioning: Define partition transforms (year, month, day, bucket, truncate), but query with natural predicates. Write WHERE date greater than 2024-03-01 and Iceberg applies partition pruning automatically. Query logic decoupled from physical layout.

How writes work: Writer creates new Parquet files, creates new manifests referencing those files, creates new snapshot pointing to manifest list, then atomic commit updates metadata pointer to new snapshot. Old snapshot retained for time travel and rollback.

Benefits over Hive: Concurrent read/write is safe with snapshot isolation instead of corruption risk. Rollback is ALTER TABLE ROLLBACK instead of restoring from backup. Schema and partition changes are metadata-only instead of file rewrites. Query syntax uses natural predicates instead of requiring partition knowledge.

From experience advising companies: I've seen data lake corruption cost millions in lost data and recovery time. The root cause is always the same—Hive's lack of transaction isolation. Iceberg eliminates this entire class of problem.

---

## X (5 posts, 280 characters each)

Post 1:
Apache Iceberg: table format for data lakehouses. Hive treats files as the table—concurrent writes corrupt data, no rollback, schema changes rewrite everything. Iceberg adds a metadata layer for ACID transactions on open file formats.

Post 2:
Iceberg architecture: Catalog points to metadata JSON, which tracks schema, partitions, and snapshot history. Snapshots point to manifest lists, which reference immutable Parquet files. Data is never modified, only added or marked deleted.

Post 3:
Snapshot isolation means readers always see consistent data. Start reading snapshot v5, keep reading v5's files even if v6 commits mid-query. Time travel lets you query any historical snapshot. Rollback is one command.

Post 4:
Hidden partitioning is the game-changer. Define partition transforms (year, month, bucket), but query with natural predicates. WHERE date greater than 2024-03-01 triggers automatic pruning. Query logic decoupled from physical layout.

Post 5:
Schema evolution without file rewrites. Add, rename, drop columns as metadata changes. Iceberg reconciles old and new schemas at query time. Open format plus transactional semantics equals modern data lakehouse.
