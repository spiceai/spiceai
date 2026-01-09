# Apache Iceberg

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

**Apache Iceberg: How Modern Data Lakes Get ACID Transactions**

Traditional data lake formats (like Hive) treat files as the table. Write a file, it's in the table. Overwrite a file, the old data is gone. This simplicity creates serious problems at scale.

Apache Iceberg adds a metadata layer that gives data lakes the transactional guarantees we expect from databases—without giving up the benefits of open file formats.

```
┌─────────────────────────────────────────────────────────────────┐
│              ICEBERG: HOW TABLE STATE WORKS                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   HIVE-STYLE DATA LAKES (files = table):                        │
│                                                                  │
│   /table/year=2024/month=01/                                    │
│       file1.parquet                                             │
│       file2.parquet                                             │
│                                                                  │
│   Problems:                                                      │
│   • Concurrent write + read = corruption                         │
│   • No rollback (overwritten files are gone)                    │
│   • Schema changes require file rewrites                         │
│   • Partition structure leaks into queries                       │
│                                                                  │
│   ICEBERG (metadata + immutable files):                          │
│                                                                  │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │                    Catalog                                │  │
│   │         (REST API, AWS Glue, Hive Metastore)             │  │
│   │                        │                                  │  │
│   │                        ▼                                  │  │
│   │   Current metadata pointer: metadata-v3.json             │  │
│   └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │              metadata-v3.json                             │  │
│   │   • Schema (columns, types)                               │  │
│   │   • Partition spec                                        │  │
│   │   • Snapshot history: [snap1, snap2, snap3 (current)]     │  │
│   └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │              Snapshot (snap3)                             │  │
│   │   • Manifest list pointer                                 │  │
│   │   • Parent snapshot: snap2                                │  │
│   │   • Timestamp                                             │  │
│   └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │              Manifest List                                │  │
│   │   Points to manifests that list data files               │  │
│   └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │              Data Files (Parquet)                         │  │
│   │   • Immutable once written                                │  │
│   │   • Never modified, only added or marked for deletion    │  │
│   └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key Iceberg concepts:**

**Snapshot Isolation**: Readers always see a consistent snapshot. Reader starts on snapshot v5, reads v5's files—even if v6 commits mid-read. No corruption possible.

**Atomic Commits**: Write new files, then atomically update metadata to point to them. Either the full transaction commits or nothing changes.

**Time Travel**: Every snapshot is retained (until explicitly cleaned up). Query data as of any historical snapshot or timestamp: `SELECT * FROM table FOR VERSION AS OF 5`.

**Schema Evolution**: Add, rename, or drop columns as metadata changes. Parquet files aren't modified. Old files read with old schema, new files with new schema. Iceberg reconciles at query time.

**Hidden Partitioning**: Define partition transforms (year, month, day, bucket, truncate), but query with natural predicates. `WHERE date > '2024-03-01'`—Iceberg applies partition pruning automatically. Query logic decoupled from physical layout.

**How writes work:**

1. Writer creates new Parquet files
2. Writer creates new manifest(s) referencing those files
3. Writer creates new snapshot pointing to manifest list
4. Atomic commit: metadata pointer updated to new snapshot
5. Old snapshot retained for time travel / rollback

**Benefits over Hive:**

| Capability            | Hive                 | Iceberg                   |
| --------------------- | -------------------- | ------------------------- |
| Concurrent read/write | Corruption risk      | Safe (snapshot isolation) |
| Rollback              | Restore from backup  | ALTER TABLE ROLLBACK      |
| Schema changes        | Rewrite files        | Metadata only             |
| Partition changes     | Rewrite files        | Metadata only             |
| Query syntax          | Must know partitions | Natural predicates        |

From experience advising companies: I've seen data lake corruption cost millions in lost data and recovery time. The root cause is always the same—Hive's lack of transaction isolation. Iceberg eliminates this entire class of problem.

---

## X

Apache Iceberg: table format for data lakehouses

Problems with Hive-style lakes:
❌ Concurrent writes corrupt data
❌ Schema changes = rewrite everything
❌ Partition structure leaks into queries
❌ No rollback from bad writes

Iceberg solutions:
✓ ACID transactions (atomic commits)
✓ Schema evolution (metadata only)
✓ Hidden partitioning (automatic pruning)
✓ Time travel (snapshot history)

Architecture:
```
Catalog (REST/Glue/Hadoop)
    ↓
Table Metadata (JSON/Avro)
    ↓
Data Files (Parquet on S3/GCS/HDFS)
```

Key insight: data in standard Parquet, intelligence in metadata

```sql
-- Hive (partition leaks)
WHERE year=2024 AND month=03

-- Iceberg (hidden partitioning)
WHERE date > '2024-03-01'
```

Iceberg handles pruning automatically.

Open format + transactional semantics = modern data lakehouse.
