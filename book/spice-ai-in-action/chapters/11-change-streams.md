# 11. MySQL, MongoDB, and event-driven ingestion

The idea of CDC travels across databases; the recovery protocol does not. MySQL binlogs, MongoDB change streams, and Kafka-delivered events have different identities, retention rules, and bootstrap behavior. A good integration keeps a common business contract while testing each source's actual failure model.

## 11.1 What remains common

Every mutable serving replica needs a stable row identity, an initial state, an ordered or reconciled change history, and a recoverable progress marker. It also needs a deletion representation. If any of those are implicit, write them down before configuring the connector.

For Northstar, the acceptance cases remain the same: eight initial orders; a paid-order sum of 47,100 cents; one insert; one amount update; one delete; a consumer outage; and a restart. Reusing this business contract is valuable because it reveals source-specific differences without changing the question.

What must not be reused blindly is the PostgreSQL configuration. A slot is not a binlog position. A binlog position is not a MongoDB resume token. A broker offset is not necessarily the source transaction position embedded inside an event.

## 11.2 MySQL binlog replication

**Integration lab.** The cookbook's `mysql/cdc/` directory provides a disposable source pattern and matching Spicepod. Before loading Northstar, verify the source's binary logging settings, row-based event representation, retention, and replication permissions using the MySQL version's documentation.

Use the exact `mysql_*` parameters supported by the connector. Configure a persistent accelerator with `refresh_mode: changes`, a stable primary key, and the documented conflict behavior. Make server identity and consumer progress state unique where required. The initial snapshot and resume settings belong to the MySQL connector; a PostgreSQL parameter copied into the map is not a substitute.

Run the same mutations from Chapter 10 using MySQL's SQL syntax and data-loading tools. Verify final keys and values, not only counts. Test values that often cross type boundaries: unsigned integers, high-precision decimals, zero or invalid date values if the source allows them, and timestamps around timezone conversions. Choose a structured error over a silent lossy transformation when the contract cannot be represented.

A consumer outage is limited by binlog retention. If the required history has been removed, the consumer needs a documented rebuild or resnapshot. Increasing a retry count cannot restore a deleted log segment. Monitor both backlog and the remaining recoverable retention window.

## 11.3 MongoDB change streams

MongoDB documents do not guarantee a uniform relational schema. Before asking SQL questions, decide how missing fields, explicit nulls, nested objects, arrays, and polymorphic values map into the dataset. A field that is a number in one document and a string in another is a modeling decision, not just a parser inconvenience.

The cookbook's `mongodb/change-streams/` recipe is a source-specific starting point. Verify that the deployment supports change streams and that the connector's bootstrap and resume behavior match your chosen accelerator lifecycle. Change-stream resume tokens are opaque progress artifacts; store and interpret them through the supported connector path rather than parsing them in application code.

An update event may carry a description of changed fields or require retrieving a full document, depending on the configured source behavior. Verify what the connector consumes and how deletes identify their document. A document lookup performed after an event can have a different timing relationship from the original operation; the integration's supported semantics matter.

Use a fixture with one field absent, one explicitly null, one nested value, an array, an update, and a delete. Query the resulting schema and rows. A rectangular demo with five identical documents does not validate the source's actual variability.

## 11.4 DynamoDB and partitioned change histories

A partitioned change stream can have independent shards and ordering boundaries. Do not infer a single global order from per-key or per-shard order. A pipeline may be correct for individual item updates while a cross-item analytical join observes different progress points.

For a DynamoDB integration, verify stream configuration, record retention, key mapping, and source permissions against the documented connector. If bootstrap includes an initial scan, establish how it is reconciled with the stream. Test a hot key updated repeatedly and a low-traffic key deleted while the consumer is stopped.

Source read capacity and stream consumption are operational costs. A separate analytical serving path still has bootstrap and replication demands. Measure them with the source's own telemetry when evaluating the integration.

## 11.5 Broker-delivered CDC

Debezium and Kafka can provide a shared transport when an organization already operates that infrastructure or needs multiple consumers. They add an event envelope, topic and partition policy, schema management, and broker retention to the recovery path.

A CDC envelope is not the same as an application event. A database update says which stored row changed. A business event such as “order approved” may carry a different identity and interpretation. Decide whether the dataset represents current row state, an immutable event log, or a derived aggregate.

For current-state materialization, the consumer must distinguish inserts, updates, deletes, and tombstones according to the supported format. For event analytics, duplicate delivery needs an event ID and de-duplication policy. A topic partition offset identifies a position in the transport, not necessarily a unique business action.

## 11.6 Direct change ingestion

Some deployments accept CDC payloads through a runtime API or a connector-specific ingress mechanism. Treat this as a write interface with an explicit contract. Validate the payload schema, target dataset, operation types, authorization, size limits, and retry semantics using the release's OpenAPI and connector documentation.

Do not send an arbitrary JSON object to a CDC endpoint and assume it behaves like a row insert. An operation envelope can carry schema, keys, and before/after values whose absence changes interpretation. Integration tests should replay the same event and deliberately interrupt a request to establish how the caller resolves an uncertain outcome.

## 11.7 A recovery matrix beats a generic promise

| Condition | Question the integration must answer |
|---|---|
| Consumer restarts; storage intact | Which progress marker is resumed? |
| Accelerator is empty | Where does preexisting state come from? |
| Source history expired | What is the resnapshot procedure? |
| Same event arrives twice | What prevents duplicate logical state? |
| Older update arrives after newer update | Which ordering or reconciliation wins? |
| Source schema changes | Is the dataset blocked, failed, or migrated? |
| Two tables lag differently | What consistency can a join claim? |

Fill this table for each production source. A claim tested on PostgreSQL does not automatically transfer to MySQL or MongoDB, even when the acceleration block is identical.

**Exercise.** Design an immutable event dataset and a current-state order dataset from the same broker feed. State the key, retention policy, duplicate policy, and deletion meaning of each. Explain why the two tables should not share a generic “upsert everything” rule.

**Further reading.** See cookbook `mysql/cdc/`, `mongodb/change-streams/`, `dynamodb/streams/`, and `cdc-debezium/`. Source documents include `docs/features/mysql-binlog-replication.md`, `docs/features/mongodb-change-streams.md`, and `docs/features/cdc-debezium-ingest.md`.
