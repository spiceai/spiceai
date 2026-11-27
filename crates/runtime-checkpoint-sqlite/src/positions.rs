/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! A dataset's source positions: every `spice_sys_*` table keyed by `dataset_name`
//! except the dataset checkpoint. A snapshot carries all of them. A clear and an
//! import replace all of them, and an import writes those in [`RESTORED_OVER_LOCAL`]
//! only where the dataset already holds one.

use std::collections::HashMap;

use runtime_acceleration::sidecar::{PositionRows, PositionValue, SourcePositions};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, types::Value};

use crate::{blob, dataset_checkpoint, debezium, kafka, mongodb, mysql_binlog};

const PREFIX: &str = "spice_sys_";

/// Positions that their source drops after a retention: the MySQL binlog, a
/// `DynamoDB` stream and the `MongoDB` oplog. A restored one can be older than the
/// source keeps, and its connector then fails the dataset by default. An import
/// writes one only over a position the dataset holds in the same table, as on a
/// `mode: file_create` restart; a dataset that holds none, as on a new node, starts
/// without it, as on a first load.
const RESTORED_OVER_LOCAL: &[&str] = &[
    mysql_binlog::MYSQL_BINLOG_TABLE_NAME,
    "spice_sys_dynamodb_streams",
    mongodb::MONGODB_TABLE_NAME,
];

/// The columns of a blob checkpoint table, which every connector-named sidecar
/// table shares.
const BLOB_COLUMNS: &[&str] = &[
    "dataset_name",
    "checkpoint_data",
    "created_at",
    "updated_at",
];

/// Sidecar table names are written by this crate's stores, so a name outside this
/// alphabet is not one of them.
fn is_table_name(name: &str) -> bool {
    name.starts_with(PREFIX)
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn columns(conn: &Connection, table: &str) -> rusqlite::Result<Vec<String>> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
    statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect()
}

/// The tables in this database that hold the dataset's source positions.
fn position_tables(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND substr(name, 1, 10) = 'spice_sys_' ORDER BY name",
    )?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        if name == dataset_checkpoint::CHECKPOINT_TABLE_NAME || !is_table_name(&name) {
            continue;
        }
        if columns(conn, &name)?
            .iter()
            .any(|column| column == "dataset_name")
        {
            tables.push(name);
        }
    }
    Ok(tables)
}

fn delete_positions(tx: &Transaction<'_>, dataset: &str) -> rusqlite::Result<()> {
    for table in position_tables(tx)? {
        tx.execute(
            &format!("DELETE FROM \"{table}\" WHERE dataset_name = ?1"),
            [dataset],
        )?;
    }
    Ok(())
}

pub(crate) fn clear(conn: &mut Connection, dataset: &str) -> rusqlite::Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    delete_positions(&tx, dataset)?;
    tx.commit()
}

pub(crate) fn export(conn: &mut Connection, dataset: &str) -> rusqlite::Result<SourcePositions> {
    // One transaction, so every table is read at the same point.
    let tx = conn.transaction()?;
    let mut positions = SourcePositions::default();
    for table in position_tables(&tx)? {
        let columns = columns(&tx, &table)?;
        let select = format!(
            "SELECT {} FROM \"{table}\" WHERE dataset_name = ?1",
            columns
                .iter()
                .map(|column| format!("\"{column}\""))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut statement = tx.prepare(&select)?;
        let rows = statement
            .query_map([dataset], |row| {
                (0..columns.len())
                    .map(|index| row.get::<_, Value>(index).map(position_value))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        if !rows.is_empty() {
            positions
                .tables
                .insert(table, PositionRows { columns, rows });
        }
    }
    tx.commit()?;
    Ok(positions)
}

/// Replaces the dataset's source positions with `positions`. The outer error is
/// the database's; the inner one names a position this sidecar cannot hold, and
/// leaves the existing positions in place.
pub(crate) fn import(
    conn: &mut Connection,
    dataset: &str,
    positions: Option<&SourcePositions>,
) -> rusqlite::Result<Result<(), String>> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut local_groups = HashMap::new();
    for table in [kafka::KAFKA_TABLE_NAME, debezium::DEBEZIUM_KAFKA_TABLE_NAME] {
        if let Some(group) = local_consumer_group(&tx, table, dataset)? {
            local_groups.insert(table, group);
        }
    }
    let mut held = Vec::new();
    for table in RESTORED_OVER_LOCAL {
        if holds_position(&tx, table, dataset)? {
            held.push(*table);
        }
    }
    delete_positions(&tx, dataset)?;
    for (table, rows) in positions.iter().flat_map(|positions| &positions.tables) {
        if !is_table_name(table) || table == dataset_checkpoint::CHECKPOINT_TABLE_NAME {
            return Ok(Err(format!("'{table}' is not a source position table")));
        }
        if RESTORED_OVER_LOCAL.contains(&table.as_str()) && !held.contains(&table.as_str()) {
            continue;
        }
        if !ensure_table(&tx, table, &rows.columns)? {
            // A table that does not exist here and that no store here creates: its
            // connector starts without the position.
            continue;
        }
        let local = columns(&tx, table)?;
        if let Some(column) = rows.columns.iter().find(|column| !local.contains(column)) {
            return Ok(Err(format!("'{table}' has no column '{column}'")));
        }
        let Some(dataset_index) = rows
            .columns
            .iter()
            .position(|column| column == "dataset_name")
        else {
            return Ok(Err(format!("the rows of '{table}' have no dataset_name")));
        };
        let group_index =
            if table == kafka::KAFKA_TABLE_NAME || table == debezium::DEBEZIUM_KAFKA_TABLE_NAME {
                rows.columns
                    .iter()
                    .position(|column| column == "consumer_group_id")
            } else {
                None
            };
        let insert = format!(
            "INSERT INTO \"{table}\" ({}) VALUES ({})",
            rows.columns
                .iter()
                .map(|column| format!("\"{column}\""))
                .collect::<Vec<_>>()
                .join(", "),
            vec!["?"; rows.columns.len()].join(", ")
        );
        for row in &rows.rows {
            if row.len() != rows.columns.len() {
                return Ok(Err(format!(
                    "a row of '{table}' has {} values for {} columns",
                    row.len(),
                    rows.columns.len()
                )));
            }
            if row[dataset_index] != PositionValue::Text(dataset.to_string()) {
                return Ok(Err(format!(
                    "a row of '{table}' belongs to another dataset than '{dataset}'"
                )));
            }
            let mut row = row.clone();
            if let Some(index) = group_index {
                row[index] = own_consumer_group(
                    dataset,
                    &row[index],
                    local_groups.get(table.as_str()).map(String::as_str),
                );
            }
            tx.execute(
                &insert,
                rusqlite::params_from_iter(row.iter().map(sql_value)),
            )?;
        }
    }
    tx.commit()?;
    Ok(Ok(()))
}

/// The consumer group a restoring instance reads with. A group Spice generated
/// (`spice.ai-<dataset>-<uuid>`) is replaced with the generated group this instance
/// already holds for the dataset (`local`), or else with a new one: members of one
/// group share the topic's partitions, so the instance that wrote the snapshot and
/// this one would each miss the messages of the partitions the other holds. The
/// restored offsets still set where the group starts. A configured group is kept,
/// as the connector requires it to match.
fn own_consumer_group(dataset: &str, group: &PositionValue, local: Option<&str>) -> PositionValue {
    let generated = format!("spice.ai-{dataset}-");
    match group {
        PositionValue::Text(group) if group.starts_with(&generated) => PositionValue::Text(
            local
                .filter(|local| local.starts_with(&generated))
                .map_or_else(
                    || format!("{generated}{}", uuid::Uuid::new_v4()),
                    ToString::to_string,
                ),
        ),
        _ => group.clone(),
    }
}

/// The consumer group `table` holds for `dataset`, if the table exists and has a row.
fn local_consumer_group(
    tx: &Transaction<'_>,
    table: &str,
    dataset: &str,
) -> rusqlite::Result<Option<String>> {
    if !table_exists(tx, table)? {
        return Ok(None);
    }
    tx.query_row(
        &format!("SELECT consumer_group_id FROM \"{table}\" WHERE dataset_name = ?1"),
        [dataset],
        |row| row.get(0),
    )
    .optional()
}

/// Whether `table` exists and holds a row for `dataset`.
fn holds_position(tx: &Transaction<'_>, table: &str, dataset: &str) -> rusqlite::Result<bool> {
    if !table_exists(tx, table)? {
        return Ok(false);
    }
    tx.query_row(
        &format!("SELECT EXISTS (SELECT 1 FROM \"{table}\" WHERE dataset_name = ?1)"),
        [dataset],
        |row| row.get(0),
    )
}

fn table_exists(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get(0),
    )
}

/// Creates `table` as its store creates it, unless it exists. `false` for a table
/// that does not exist and that no store here creates.
fn ensure_table(conn: &Connection, table: &str, columns: &[String]) -> rusqlite::Result<bool> {
    if table_exists(conn, table)? {
        return Ok(true);
    }
    match table {
        kafka::KAFKA_TABLE_NAME | kafka::KAFKA_OFFSETS_TABLE_NAME => {
            kafka::ensure_kafka_tables(conn)?;
        }
        debezium::DEBEZIUM_KAFKA_TABLE_NAME | debezium::DEBEZIUM_KAFKA_OFFSETS_TABLE_NAME => {
            debezium::ensure_debezium_kafka_tables(conn)?;
        }
        _ if columns
            .iter()
            .all(|column| BLOB_COLUMNS.contains(&column.as_str())) =>
        {
            conn.execute(
                &blob::SqliteBlobCheckpointStore::create_table_sql(table),
                [],
            )?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn position_value(value: Value) -> PositionValue {
    match value {
        Value::Null => PositionValue::Null,
        Value::Integer(value) => PositionValue::Integer(value),
        Value::Real(value) => PositionValue::Real(value),
        Value::Text(value) => PositionValue::Text(value),
        Value::Blob(value) => PositionValue::Blob(value),
    }
}

fn sql_value(value: &PositionValue) -> Value {
    match value {
        PositionValue::Null => Value::Null,
        PositionValue::Integer(value) => Value::Integer(*value),
        PositionValue::Real(value) => Value::Real(*value),
        PositionValue::Text(value) => Value::Text(value.clone()),
        PositionValue::Blob(value) => Value::Blob(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use runtime_acceleration::{sidecar::AcceleratorSidecar, snapshot::SnapshotBehavior};
    use runtime_checkpoint_api::{
        debezium::DebeziumCheckpoint,
        kafka::{KafkaCheckpoint, KafkaOffset},
        mongodb::MongoCheckpointMetadata,
        mysql_binlog::MySqlBinlogCheckpoint,
    };

    use crate::{SqliteSidecar, test_support::temp_pool};

    const WATERMARK: &str = "spice_sys_postgres_replication";
    const DYNAMODB: &str = "spice_sys_dynamodb_streams";

    fn kafka_checkpoint() -> KafkaCheckpoint {
        KafkaCheckpoint {
            consumer_group_id: "group-1".to_string(),
            topic: "events".to_string(),
            schema_json: "{}".to_string(),
            offsets: vec![KafkaOffset {
                topic: "events".to_string(),
                partition: 0,
                offset: 999,
            }],
        }
    }

    /// Positions in three stores and a dataset checkpoint for `sidecar`'s dataset.
    async fn seed(sidecar: &SqliteSidecar) {
        sidecar
            .kafka_checkpoint_store()
            .expect("kafka store")
            .upsert(&kafka_checkpoint())
            .await
            .expect("kafka checkpoint");
        sidecar
            .mysql_binlog_store()
            .expect("mysql store")
            .upsert(&MySqlBinlogCheckpoint {
                binlog_file: "binlog.000002".to_string(),
                binlog_pos: 10_605,
                schema_json: None,
                gtid_executed: None,
                cursor_type: None,
                updated_at: None,
            })
            .await
            .expect("binlog position");
        sidecar
            .blob_checkpoint_store(WATERMARK)
            .expect("blob store")
            .upsert(r#"{"lsn":42}"#)
            .await
            .expect("watermark");
        sidecar
            .dataset_checkpointer(SnapshotBehavior::Disabled)
            .await
            .expect("checkpointer")
            .checkpoint(
                &Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                None,
            )
            .await
            .expect("dataset checkpoint");
    }

    /// A MySQL binlog position, a `DynamoDB` stream checkpoint and a `MongoDB` resume
    /// token for `sidecar`'s dataset.
    async fn seed_dropped(sidecar: &SqliteSidecar, binlog_file: &str, stream: &str, token: &str) {
        sidecar
            .mysql_binlog_store()
            .expect("mysql store")
            .upsert(&MySqlBinlogCheckpoint {
                binlog_file: binlog_file.to_string(),
                binlog_pos: 4,
                schema_json: None,
                gtid_executed: None,
                cursor_type: None,
                updated_at: None,
            })
            .await
            .expect("binlog position");
        sidecar
            .blob_checkpoint_store(DYNAMODB)
            .expect("blob store")
            .upsert(stream)
            .await
            .expect("stream checkpoint");
        sidecar
            .mongo_checkpoint_store()
            .expect("mongodb store")
            .upsert(&MongoCheckpointMetadata {
                resume_token_json: token.to_string(),
                ..MongoCheckpointMetadata::default()
            })
            .await
            .expect("resume token");
    }

    /// The positions [`seed_dropped`] writes, as `sidecar` holds them.
    async fn dropped(sidecar: &SqliteSidecar) -> (Option<String>, Option<String>, Option<String>) {
        (
            sidecar
                .mysql_binlog_store()
                .expect("mysql store")
                .get()
                .await
                .map(|position| position.binlog_file),
            sidecar
                .blob_checkpoint_store(DYNAMODB)
                .expect("blob store")
                .get()
                .await
                .expect("blob get")
                .map(|checkpoint| checkpoint.data),
            sidecar
                .mongo_checkpoint_store()
                .expect("mongodb store")
                .get()
                .await
                .map(|token| token.resume_token_json),
        )
    }

    async fn has_positions(sidecar: &SqliteSidecar) -> [bool; 3] {
        [
            sidecar
                .kafka_checkpoint_store()
                .expect("kafka store")
                .get()
                .await
                .expect("kafka get")
                .is_some(),
            sidecar
                .mysql_binlog_store()
                .expect("mysql store")
                .get()
                .await
                .is_some(),
            sidecar
                .blob_checkpoint_store(WATERMARK)
                .expect("blob store")
                .get()
                .await
                .expect("blob get")
                .is_some(),
        ]
    }

    #[tokio::test]
    async fn clearing_source_positions_keeps_the_dataset_checkpoint_and_other_datasets() {
        let (pool, _dir) = temp_pool("clear").await;
        let orders = SqliteSidecar::new(Arc::clone(&pool), "orders".to_string());
        let other = SqliteSidecar::new(Arc::clone(&pool), "other".to_string());
        seed(&orders).await;
        seed(&other).await;

        orders.clear_source_positions().await.expect("clear");

        assert_eq!(has_positions(&orders).await, [false; 3]);
        assert!(
            orders
                .dataset_checkpointer(SnapshotBehavior::Disabled)
                .await
                .expect("checkpointer")
                .exists()
                .await,
            "the dataset checkpoint is not a source position"
        );
        assert_eq!(has_positions(&other).await, [true; 3]);
    }

    #[tokio::test]
    async fn a_restored_sidecar_gets_its_own_generated_consumer_groups() {
        let kafka_group = "spice.ai-orders-5b0e1f7a-0c1d-4e2f-8a3b-4c5d6e7f8091";
        let debezium_group = "spice.ai-orders-0f1e2d3c-4b5a-4968-8776-655443322110";
        let (writer_pool, _writer_dir) = temp_pool("writer").await;
        let writer = SqliteSidecar::new(writer_pool, "orders".to_string());
        writer
            .kafka_checkpoint_store()
            .expect("kafka store")
            .upsert(&KafkaCheckpoint {
                consumer_group_id: kafka_group.to_string(),
                ..kafka_checkpoint()
            })
            .await
            .expect("kafka checkpoint");
        writer
            .debezium_checkpoint_store()
            .expect("debezium store")
            .upsert(&DebeziumCheckpoint {
                consumer_group_id: debezium_group.to_string(),
                topic: "orders".to_string(),
                primary_keys: vec!["id".to_string()],
                schema_fields_json: "[]".to_string(),
                offsets: kafka_checkpoint().offsets,
            })
            .await
            .expect("debezium checkpoint");
        let positions = writer
            .export_source_positions()
            .await
            .expect("export")
            .expect("the SQLite sidecar carries positions");

        let (reader_pool, _reader_dir) = temp_pool("reader").await;
        let reader = SqliteSidecar::new(reader_pool, "orders".to_string());
        reader
            .import_source_positions(Some(&positions))
            .await
            .expect("import");

        let kafka = reader
            .kafka_checkpoint_store()
            .expect("kafka store")
            .get()
            .await
            .expect("kafka get")
            .expect("the Kafka checkpoint is restored");
        let debezium = reader
            .debezium_checkpoint_store()
            .expect("debezium store")
            .get()
            .await
            .expect("debezium get")
            .expect("the Debezium checkpoint is restored");
        for (restored, written) in [
            (&kafka.consumer_group_id, kafka_group),
            (&debezium.consumer_group_id, debezium_group),
        ] {
            assert_ne!(restored, written);
            assert!(restored.starts_with("spice.ai-orders-"), "{restored}");
        }
        assert_eq!(kafka.offsets, kafka_checkpoint().offsets);
        assert_eq!(debezium.offsets, kafka_checkpoint().offsets);
    }

    #[tokio::test]
    async fn an_instance_restoring_its_own_snapshot_keeps_its_consumer_groups() {
        let kafka_group = "spice.ai-orders-5b0e1f7a-0c1d-4e2f-8a3b-4c5d6e7f8091";
        let debezium_group = "spice.ai-orders-0f1e2d3c-4b5a-4968-8776-655443322110";
        let (pool, _dir) = temp_pool("instance").await;
        let sidecar = SqliteSidecar::new(pool, "orders".to_string());
        sidecar
            .kafka_checkpoint_store()
            .expect("kafka store")
            .upsert(&KafkaCheckpoint {
                consumer_group_id: kafka_group.to_string(),
                ..kafka_checkpoint()
            })
            .await
            .expect("kafka checkpoint");
        sidecar
            .debezium_checkpoint_store()
            .expect("debezium store")
            .upsert(&DebeziumCheckpoint {
                consumer_group_id: debezium_group.to_string(),
                topic: "orders".to_string(),
                primary_keys: vec!["id".to_string()],
                schema_fields_json: "[]".to_string(),
                offsets: kafka_checkpoint().offsets,
            })
            .await
            .expect("debezium checkpoint");
        let positions = sidecar
            .export_source_positions()
            .await
            .expect("export")
            .expect("the SQLite sidecar carries positions");

        sidecar
            .import_source_positions(Some(&positions))
            .await
            .expect("import");

        let kafka = sidecar
            .kafka_checkpoint_store()
            .expect("kafka store")
            .get()
            .await
            .expect("kafka get")
            .expect("the Kafka checkpoint is restored");
        let debezium = sidecar
            .debezium_checkpoint_store()
            .expect("debezium store")
            .get()
            .await
            .expect("debezium get")
            .expect("the Debezium checkpoint is restored");
        assert_eq!(kafka.consumer_group_id, kafka_group);
        assert_eq!(debezium.consumer_group_id, debezium_group);
    }

    #[tokio::test]
    async fn source_positions_move_to_a_new_sidecar_with_a_snapshot() {
        let (writer_pool, _writer_dir) = temp_pool("writer").await;
        let writer = SqliteSidecar::new(writer_pool, "orders".to_string());
        seed(&writer).await;
        seed_dropped(
            &writer,
            "binlog.000009",
            r#"{"shards":[]}"#,
            r#"{"_data":"82"}"#,
        )
        .await;
        let positions = writer
            .export_source_positions()
            .await
            .expect("export")
            .expect("the SQLite sidecar carries positions");
        assert!(
            !positions
                .tables
                .contains_key(super::dataset_checkpoint::CHECKPOINT_TABLE_NAME)
        );

        let (reader_pool, _reader_dir) = temp_pool("reader").await;
        let reader = SqliteSidecar::new(reader_pool, "orders".to_string());
        reader
            .import_source_positions(Some(&positions))
            .await
            .expect("import into a sidecar without the tables");

        let restored = reader
            .kafka_checkpoint_store()
            .expect("kafka store")
            .get()
            .await
            .expect("kafka get")
            .expect("the Kafka checkpoint is restored");
        assert_eq!(restored.consumer_group_id, "group-1");
        assert_eq!(restored.offsets, kafka_checkpoint().offsets);
        assert_eq!(
            dropped(&reader).await,
            (None, None, None),
            "the source can drop these positions before the snapshot is restored"
        );
        assert_eq!(
            reader
                .blob_checkpoint_store(WATERMARK)
                .expect("blob store")
                .get()
                .await
                .expect("blob get")
                .map(|checkpoint| checkpoint.data),
            Some(r#"{"lsn":42}"#.to_string())
        );
    }

    #[tokio::test]
    async fn a_restore_writes_the_positions_a_source_drops_over_those_it_holds() {
        let (writer_pool, _writer_dir) = temp_pool("writer").await;
        let writer = SqliteSidecar::new(writer_pool, "orders".to_string());
        seed_dropped(
            &writer,
            "binlog.000009",
            r#"{"shards":["w"]}"#,
            r#"{"_data":"w"}"#,
        )
        .await;
        let positions = writer
            .export_source_positions()
            .await
            .expect("export")
            .expect("the SQLite sidecar carries positions");

        let (reader_pool, _reader_dir) = temp_pool("reader").await;
        let reader = SqliteSidecar::new(reader_pool, "orders".to_string());
        seed_dropped(
            &reader,
            "binlog.000001",
            r#"{"shards":["r"]}"#,
            r#"{"_data":"r"}"#,
        )
        .await;
        reader
            .import_source_positions(Some(&positions))
            .await
            .expect("import");

        assert_eq!(
            dropped(&reader).await,
            (
                Some("binlog.000009".to_string()),
                Some(r#"{"shards":["w"]}"#.to_string()),
                Some(r#"{"_data":"w"}"#.to_string())
            ),
            "the snapshot's positions replace the ones the dataset held"
        );
    }

    #[tokio::test]
    async fn a_snapshot_without_source_positions_clears_them() {
        let (pool, _dir) = temp_pool("none").await;
        let orders = SqliteSidecar::new(pool, "orders".to_string());
        seed(&orders).await;

        orders.import_source_positions(None).await.expect("import");

        assert_eq!(has_positions(&orders).await, [false; 3]);
    }

    #[tokio::test]
    async fn positions_of_another_dataset_are_refused() {
        let (writer_pool, _writer_dir) = temp_pool("other-writer").await;
        let other = SqliteSidecar::new(writer_pool, "other".to_string());
        seed(&other).await;
        let positions = other
            .export_source_positions()
            .await
            .expect("export")
            .expect("positions");

        let (pool, _dir) = temp_pool("orders-reader").await;
        let orders = SqliteSidecar::new(pool, "orders".to_string());
        seed(&orders).await;
        orders
            .import_source_positions(Some(&positions))
            .await
            .expect_err("rows of another dataset");

        assert_eq!(
            has_positions(&orders).await,
            [true; 3],
            "a refused import keeps the positions it found"
        );
    }

    #[test]
    fn positions_in_a_local_store_the_import_does_not_name_are_restored() {
        let create = "CREATE TABLE spice_sys_stream (dataset_name TEXT PRIMARY KEY, position INTEGER NOT NULL)";
        let mut writer = rusqlite::Connection::open_in_memory().expect("writer");
        writer.execute(create, []).expect("writer store");
        writer
            .execute("INSERT INTO spice_sys_stream VALUES ('orders', 7)", [])
            .expect("writer position");
        let positions = super::export(&mut writer, "orders").expect("export");

        let mut reader = rusqlite::Connection::open_in_memory().expect("reader");
        reader.execute(create, []).expect("reader store");
        super::import(&mut reader, "orders", Some(&positions))
            .expect("import")
            .expect("positions this sidecar can hold");

        let position: i64 = reader
            .query_row(
                "SELECT position FROM spice_sys_stream WHERE dataset_name = 'orders'",
                [],
                |row| row.get(0),
            )
            .expect("the position is restored");
        assert_eq!(position, 7);
    }
}
