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

//! A real Iceberg REST catalog over object storage, and a writer that commits every
//! kind of snapshot the append refresh has to tell apart.
//!
//! Appends go through the Iceberg writer (`fast_append`). Compactions, copy-on-write
//! overwrites, whole-file deletes, and rollbacks are written with the public manifest
//! writers and committed straight to the catalog's REST commit endpoint, because the
//! Rust Iceberg stack has no `rewrite_data_files` of its own and its transaction
//! actions are sealed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use iceberg::arrow::FieldMatchMode;
use iceberg::io::StorageFactory;
use iceberg::spec::{
    DataFile, FormatVersion, MAIN_BRANCH, ManifestEntry, ManifestListWriter, ManifestStatus,
    ManifestWriterBuilder, NestedField, Operation, PrimitiveType, Snapshot, SnapshotReference,
    SnapshotRetention, Summary, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent, TableRequirement,
    TableUpdate,
};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::{Dataset, TimeFormat},
    param::Params,
};

use crate::docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port};
use crate::iceberg::metadata_file::{
    S3_ACCESS_KEY, S3_BUCKET, S3_SECRET_KEY, create_bucket, start_object_store,
};
pub const NAMESPACE: &str = "ns";
pub const TABLE: &str = "events";

/// 2026-10-14T10:00:00Z in microseconds: the instant every row's time is an offset from.
pub const BASE_MICROS: i64 = 1_792_058_400_000_000;

/// How the table's `event_ingestion` column is typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeKind {
    /// An Iceberg `timestamp` (microseconds, no zone).
    Timestamp,
    /// An Iceberg `string` holding ISO 8601 text, with whatever UTC offset the row
    /// was written with.
    Text,
}

/// A row's `event_ingestion` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Time {
    /// Seconds after [`BASE_MICROS`].
    Secs(i64),
    /// ISO 8601 text, for a [`TimeKind::Text`] table.
    Text(String),
    Null,
}

/// One source row. `sort_key` is always `"s"`, so `(sort_key, hash_key)` is the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub key: String,
    pub time: Time,
    pub val: String,
}

impl Row {
    pub fn at(key: &str, secs: i64, val: &str) -> Self {
        Self {
            key: key.to_string(),
            time: Time::Secs(secs),
            val: val.to_string(),
        }
    }

    pub fn text(key: &str, time: &str, val: &str) -> Self {
        Self {
            key: key.to_string(),
            time: Time::Text(time.to_string()),
            val: val.to_string(),
        }
    }

    pub fn null_time(key: &str, val: &str) -> Self {
        Self {
            key: key.to_string(),
            time: Time::Null,
            val: val.to_string(),
        }
    }
}

/// `rows` as `(key, secs)` pairs over an inclusive range of key numbers.
pub fn keys(prefix: &str, range: std::ops::RangeInclusive<u32>, secs: i64, val: &str) -> Vec<Row> {
    range
        .map(|n| Row::at(&format!("{prefix}{n}"), secs, val))
        .collect()
}

/// `secs` after the base, as the `CAST(ts AS VARCHAR)` text Spice returns for it.
pub fn at_text(secs: i64) -> String {
    let micros = BASE_MICROS + secs * 1_000_000;
    chrono::DateTime::from_timestamp_micros(micros)
        .expect("in range")
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string()
}

/// The acceleration a dataset is given.
#[derive(Debug, Clone)]
pub struct Accel {
    pub mode: Mode,
    pub primary_key: bool,
    pub overlap: &'static str,
}

impl Default for Accel {
    fn default() -> Self {
        Self {
            mode: Mode::File,
            primary_key: true,
            overlap: "20m",
        }
    }
}

pub struct Lab {
    _store: RunningContainer,
    _rest: RunningContainer,
    pub s3_endpoint: String,
    pub rest_uri: String,
    pub catalog: Arc<dyn Catalog>,
    pub ident: TableIdent,
    pub time_kind: TimeKind,
    pub temp: tempfile::TempDir,
}

impl Lab {
    /// Starts the object store and the REST catalog, and creates the table.
    pub async fn start(time_kind: TimeKind) -> anyhow::Result<Self> {
        let store = start_object_store("spice_test_rustfs_iceberg_append").await?;
        let s3_endpoint = format!("http://127.0.0.1:{}", store.host_port(9000)?);
        create_bucket(&s3_endpoint).await?;

        // The catalog reaches the store on the Docker bridge, by the store's own
        // address: the port published on the host loopback is not visible from
        // inside a container on a Linux daemon.
        let store_bridge = store.bridge_ip().await?;
        let rest = ContainerRunnerBuilder::new("spice_test_iceberg_rest_append")
            .image("apache/iceberg-rest-fixture:1.10.1".to_string())
            .publish_port(8181)
            .add_env_var("CATALOG_WAREHOUSE", &format!("s3://{S3_BUCKET}/warehouse/"))
            .add_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .add_env_var("CATALOG_S3_ENDPOINT", &format!("http://{store_bridge}:9000"))
            .add_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .add_env_var("AWS_ACCESS_KEY_ID", S3_ACCESS_KEY)
            .add_env_var("AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY)
            .add_env_var("AWS_REGION", "us-east-1")
            .build()?
            .run(Some(Duration::from_mins(2)))
            .await?;
        let rest_port = rest.host_port(8181)?;
        wait_for_tcp_port("127.0.0.1", rest_port, Duration::from_mins(1)).await?;
        let rest_uri = format!("http://127.0.0.1:{rest_port}");

        let catalog = connect_catalog(&rest_uri, &s3_endpoint).await?;
        let namespace = NamespaceIdent::new(NAMESPACE.to_string());
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let ident = TableIdent::new(namespace, TABLE.to_string());
        let lab = Self {
            _store: store,
            _rest: rest,
            s3_endpoint,
            rest_uri,
            catalog: Arc::new(catalog),
            ident,
            time_kind,
            temp: tempfile::tempdir()?,
        };
        lab.create_table().await?;
        Ok(lab)
    }

    async fn create_table(&self) -> anyhow::Result<()> {
        let time_type = match self.time_kind {
            TimeKind::Timestamp => PrimitiveType::Timestamp,
            TimeKind::Text => PrimitiveType::String,
        };
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::optional(1, "sort_key", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, "hash_key", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "event_ingestion", Type::Primitive(time_type)).into(),
                NestedField::optional(4, "val", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;
        self.catalog
            .create_table(
                self.ident.namespace(),
                TableCreation::builder()
                    .name(TABLE.to_string())
                    .schema(schema)
                    .build(),
            )
            .await?;
        Ok(())
    }

    /// Drops the table and creates it again, with a new UUID, holding `rows`.
    pub async fn recreate_with(&self, rows: &[Row]) -> anyhow::Result<i64> {
        self.catalog.drop_table(&self.ident).await?;
        self.create_table().await?;
        self.append(rows).await
    }

    pub async fn load_table(&self) -> anyhow::Result<Table> {
        Ok(self.catalog.load_table(&self.ident).await?)
    }

    pub async fn current_snapshot(&self) -> anyhow::Result<i64> {
        self.load_table()
            .await?
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| anyhow::anyhow!("the table has no snapshot"))
    }

    pub async fn table_uuid(&self) -> anyhow::Result<String> {
        Ok(self.load_table().await?.metadata().uuid().to_string())
    }

    /// The Spice dataset reading the table, accelerated as `accel` says, or federated.
    pub fn dataset(&self, name: &str, accel: Option<&Accel>) -> Dataset {
        let mut dataset = Dataset::new(
            format!(
                "iceberg:{}/v1/namespaces/{NAMESPACE}/tables/{TABLE}",
                self.rest_uri
            ),
            name,
        );
        dataset.params = Some(Params::from_string_map(HashMap::from([
            ("iceberg_s3_endpoint".to_string(), self.s3_endpoint.clone()),
            (
                "iceberg_s3_access_key_id".to_string(),
                S3_ACCESS_KEY.to_string(),
            ),
            (
                "iceberg_s3_secret_access_key".to_string(),
                S3_SECRET_KEY.to_string(),
            ),
            ("iceberg_s3_region".to_string(), "us-east-1".to_string()),
        ])));
        let Some(accel) = accel else {
            return dataset;
        };
        dataset.time_column = Some("event_ingestion".to_string());
        dataset.time_format = Some(match self.time_kind {
            TimeKind::Timestamp => TimeFormat::Timestamp,
            TimeKind::Text => TimeFormat::ISO8601,
        });
        let data_path = self.temp.path().join(format!("accelerator-{name}"));
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            mode: accel.mode.clone(),
            refresh_mode: Some(RefreshMode::Append),
            refresh_append_overlap: Some(accel.overlap.to_string()),
            primary_key: accel
                .primary_key
                .then(|| "(sort_key, hash_key)".to_string()),
            params: (accel.mode == Mode::File).then(|| {
                Params::from_string_map(HashMap::from([(
                    "cayenne_file_path".to_string(),
                    data_path.to_string_lossy().to_string(),
                )]))
            }),
            ..Acceleration::default()
        });
        dataset
    }

    fn arrow_schema(&self) -> Arc<ArrowSchema> {
        let time_type = match self.time_kind {
            TimeKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            TimeKind::Text => DataType::Utf8,
        };
        Arc::new(ArrowSchema::new(vec![
            Field::new("sort_key", DataType::Utf8, true),
            Field::new("hash_key", DataType::Utf8, true),
            Field::new("event_ingestion", time_type, true),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    fn batch(&self, rows: &[Row]) -> anyhow::Result<RecordBatch> {
        let sort_keys: ArrayRef = Arc::new(StringArray::from(vec!["s"; rows.len()]));
        let hash_keys: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
        ));
        let vals: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.val.as_str()).collect::<Vec<_>>(),
        ));
        let times: ArrayRef = match self.time_kind {
            TimeKind::Timestamp => Arc::new(TimestampMicrosecondArray::from(
                rows.iter()
                    .map(|r| match &r.time {
                        Time::Secs(secs) => Some(BASE_MICROS + secs * 1_000_000),
                        Time::Null => None,
                        Time::Text(text) => {
                            panic!("row {} has text time {text} in a timestamp table", r.key)
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
            TimeKind::Text => Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| match &r.time {
                        Time::Text(text) => Some(text.as_str()),
                        Time::Null => None,
                        Time::Secs(_) => panic!("row {} has a numeric time in a text table", r.key),
                    })
                    .collect::<Vec<_>>(),
            )),
        };
        Ok(RecordBatch::try_new(
            self.arrow_schema(),
            vec![sort_keys, hash_keys, times, vals],
        )?)
    }

    /// Writes `rows` as one Parquet data file of `table`, returning its manifest entry.
    async fn write_data_file(&self, table: &Table, rows: &[Row]) -> anyhow::Result<DataFile> {
        let metadata = table.metadata();
        let parquet = ParquetWriterBuilder::from_table_properties(
            &metadata.table_properties(),
            metadata.current_schema().clone(),
        )?
        .with_match_mode(FieldMatchMode::Name);
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            DefaultLocationGenerator::new(metadata)?,
            DefaultFileNameGenerator::new(
                uuid::Uuid::now_v7().to_string(),
                None,
                iceberg::spec::DataFileFormat::Parquet,
            ),
        );
        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        writer.write(self.batch(rows)?).await?;
        let mut files = writer.close().await?;
        anyhow::ensure!(files.len() == 1, "one data file per write, got {}", files.len());
        Ok(files.remove(0))
    }

    /// Commits `rows` as one `append` snapshot and returns its id.
    pub async fn append(&self, rows: &[Row]) -> anyhow::Result<i64> {
        let table = self.load_table().await?;
        let file = self.write_data_file(&table, rows).await?;
        let tx = Transaction::new(&table);
        let tx = tx.fast_append().add_data_files(vec![file]).apply(tx)?;
        let table = tx.commit(self.catalog.as_ref()).await?;
        table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| anyhow::anyhow!("the append produced no snapshot"))
    }

    /// The paths of the data files `snapshot_id` added.
    pub async fn added_files(&self, snapshot_id: i64) -> anyhow::Result<Vec<String>> {
        let table = self.load_table().await?;
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| anyhow::anyhow!("snapshot {snapshot_id} is not in the table"))?;
        let list = table.manifest_list_reader(snapshot).load().await?;
        let mut paths = Vec::new();
        for manifest_file in list
            .entries()
            .iter()
            .filter(|m| m.added_snapshot_id == snapshot_id)
        {
            let manifest = table.manifest_reader().read(manifest_file).await?;
            paths.extend(
                manifest
                    .entries()
                    .iter()
                    .filter(|e| e.status() == ManifestStatus::Added)
                    .map(|e| e.file_path().to_string()),
            );
        }
        Ok(paths)
    }

    /// Commits a snapshot with summary `operation` that removes the data files at
    /// `removed` and adds one file holding `rows` (none when `rows` is empty): a
    /// compaction (`replace`), a copy-on-write rewrite (`overwrite`), or a whole-file
    /// delete (`delete`). Returns the new snapshot's id.
    pub async fn rewrite(
        &self,
        operation: Operation,
        removed: &[String],
        rows: &[Row],
    ) -> anyhow::Result<i64> {
        let table = self.load_table().await?;
        let added = if rows.is_empty() {
            Vec::new()
        } else {
            vec![self.write_data_file(&table, rows).await?]
        };
        let rewrite = RewriteFiles {
            operation,
            removed: removed.iter().cloned().collect(),
            added,
        };
        let (updates, requirements) = rewrite.commit(&table).await?;
        self.commit_rest(requirements, updates).await?;
        self.current_snapshot().await
    }

    /// Points `main` back at `snapshot_id`: a rollback.
    pub async fn set_main(&self, snapshot_id: i64) -> anyhow::Result<()> {
        let table = self.load_table().await?;
        let metadata = table.metadata();
        anyhow::ensure!(
            metadata.snapshot_by_id(snapshot_id).is_some(),
            "snapshot {snapshot_id} is not in the table"
        );
        self.commit_rest(
            vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            }],
            vec![TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            }],
        )
        .await
    }

    /// Commits `updates` under `requirements` through the catalog's REST commit
    /// endpoint, the request the Iceberg client itself sends.
    async fn commit_rest(
        &self,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> anyhow::Result<()> {
        let body = serde_json::json!({
            "requirements": requirements,
            "updates": updates,
        });
        let response = reqwest::Client::new()
            .post(format!(
                "{}/v1/namespaces/{NAMESPACE}/tables/{TABLE}",
                self.rest_uri
            ))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        anyhow::ensure!(
            status.is_success(),
            "the REST catalog refused the commit ({status}): {text}"
        );
        Ok(())
    }

    /// Expires `snapshot_ids`, removing them from the table's history.
    pub async fn expire(&self, snapshot_ids: &[i64]) -> anyhow::Result<()> {
        let table = self.load_table().await?;
        let tx = Transaction::new(&table);
        let action = tx
            .expire_snapshots()
            .expire_snapshot_ids(snapshot_ids.iter().copied());
        let tx = action.apply(tx)?;
        tx.commit(self.catalog.as_ref()).await?;
        Ok(())
    }
}

/// A snapshot built from explicit manifest entries.
struct RewriteFiles {
    operation: Operation,
    removed: HashSet<String>,
    added: Vec<DataFile>,
}

impl RewriteFiles {
    /// Writes the manifests and manifest list, and returns the commit that publishes
    /// them as the new `main` snapshot.
    async fn commit(
        &self,
        table: &Table,
    ) -> anyhow::Result<(Vec<TableUpdate>, Vec<TableRequirement>)> {
        let metadata = table.metadata();
        anyhow::ensure!(
            metadata.format_version() == FormatVersion::V2,
            "the lab writes format version 2 manifests"
        );
        let parent = metadata.current_snapshot().cloned();
        let parent_id = parent.as_ref().map(|s| s.snapshot_id());
        let snapshot_id = fresh_snapshot_id(metadata);
        let sequence_number = metadata.next_sequence_number();
        let commit_uuid = uuid::Uuid::new_v4();
        let metadata_dir = metadata.metadata_location()?;

        // Every manifest of the parent is carried over unless it names a removed
        // file; those are rewritten into the new manifest, their other entries as
        // `EXISTING` and the removed ones as `DELETED`, beside the `ADDED` files.
        let mut carried = Vec::new();
        let mut rewritten: Vec<ManifestEntry> = Vec::new();
        if let Some(parent) = &parent {
            let list = table.manifest_list_reader(parent).load().await?;
            for manifest_file in list.entries() {
                let manifest = table.manifest_reader().read(manifest_file).await?;
                let live: Vec<&ManifestEntry> = manifest
                    .entries()
                    .iter()
                    .map(AsRef::as_ref)
                    .filter(|e| e.status() != ManifestStatus::Deleted)
                    .collect();
                if live.iter().any(|e| self.removed.contains(e.file_path())) {
                    rewritten.extend(live.into_iter().cloned());
                } else {
                    carried.push(manifest_file.clone());
                }
            }
        }
        let missing: Vec<&String> = self
            .removed
            .iter()
            .filter(|path| !rewritten.iter().any(|e| e.file_path() == path.as_str()))
            .collect();
        anyhow::ensure!(
            missing.is_empty(),
            "removed files are not live in the table: {missing:?}"
        );

        let output = table
            .file_io()
            .new_output(format!("{metadata_dir}/{commit_uuid}-m0.avro"))?;
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for file in &self.added {
            writer.add_file(file.clone(), sequence_number)?;
        }
        for entry in &rewritten {
            let entry_sequence = entry.sequence_number().unwrap_or(sequence_number);
            if self.removed.contains(entry.file_path()) {
                writer.add_delete_file(entry.data_file().clone(), entry_sequence, None)?;
            } else {
                writer.add_existing_file(
                    entry.data_file().clone(),
                    entry.snapshot_id().unwrap_or(snapshot_id),
                    entry_sequence,
                    None,
                )?;
            }
        }
        let new_manifest = writer.write_manifest_file().await?;

        let list_path = format!("{metadata_dir}/snap-{snapshot_id}-0-{commit_uuid}.avro");
        let list_output = table.file_io().new_output(list_path.clone())?;
        let mut list_writer = ManifestListWriter::v2(
            list_output.writer().await?,
            snapshot_id,
            parent_id,
            sequence_number,
        );
        list_writer.add_manifests(std::iter::once(new_manifest).chain(carried))?;
        list_writer.close().await?;

        let snapshot = Snapshot::builder()
            .with_manifest_list(list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_id)
            .with_sequence_number(sequence_number)
            .with_summary(Summary {
                operation: self.operation.clone(),
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
            .build();

        Ok((
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
                    reference: SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
            vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: parent_id,
                },
            ],
        ))
    }
}

fn fresh_snapshot_id(metadata: &iceberg::spec::TableMetadata) -> i64 {
    loop {
        let (lhs, rhs) = uuid::Uuid::new_v4().as_u64_pair();
        let id = i64::try_from((lhs ^ rhs) >> 1).unwrap_or(1).max(1);
        if metadata.snapshot_by_id(id).is_none() {
            return id;
        }
    }
}

async fn connect_catalog(
    rest_uri: &str,
    s3_endpoint: &str,
) -> anyhow::Result<iceberg_catalog_rest::RestCatalog> {
    let props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), rest_uri.to_string()),
        ("s3.endpoint".to_string(), s3_endpoint.to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
        ("s3.access-key-id".to_string(), S3_ACCESS_KEY.to_string()),
        (
            "s3.secret-access-key".to_string(),
            S3_SECRET_KEY.to_string(),
        ),
        ("s3.region".to_string(), "us-east-1".to_string()),
    ]);
    let factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    });
    // The catalog can accept connections a moment before it answers requests.
    let mut last_error = None;
    for _ in 0..100 {
        match RestCatalogBuilder::default()
            .with_storage_factory(Arc::clone(&factory))
            .load("rest", props.clone())
            .await
        {
            Ok(catalog) => match catalog.list_namespaces(None).await {
                Ok(_) => return Ok(catalog),
                Err(e) => last_error = Some(e.to_string()),
            },
            Err(e) => last_error = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    anyhow::bail!("the REST catalog at {rest_uri} did not answer: {last_error:?}")
}
