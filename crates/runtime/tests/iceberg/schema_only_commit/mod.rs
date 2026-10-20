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

//! Iceberg tables whose last commit is schema-only: a column renamed, added or
//! dropped with no write since.
//!
//! A schema update creates no snapshot, so the table's current snapshot still
//! names the schema it was written with. A reader must project the table's
//! *current* schema regardless — that is what Spice registers the table with,
//! and what every other engine (Java, `PyIceberg`) reads. Projecting the
//! snapshot's schema instead made a renamed or added column unresolvable, so
//! every query against the table failed, and an accelerated copy never loaded.
//!
//! Each table is read every way a Spice app can reach it: catalog-less from its
//! root, pinned to its latest metadata file with `iceberg_metadata_path`,
//! through the Iceberg catalog connector, and accelerated into Arrow. An
//! unchanged table is the control.
//!
//! Guards the `spiceai/iceberg-rust` patch listed in `docs/dev/fork_patches.md`
//! (cherry-pick of apache/iceberg-rust#2904).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Array as _, AsArray as _, RecordBatch};
use arrow::datatypes::Int64Type;
use datafusion::prelude::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    NestedField, PrimitiveType, Schema, TableMetadata, TableMetadataBuilder, Type,
};
use iceberg::{Catalog as _, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_datafusion::IcebergTableProvider;
use runtime::Runtime;
use spicepod::{
    acceleration::Acceleration,
    component::{caching::SQLResultsCacheConfig, catalog::Catalog, dataset::Dataset},
    param::Params as DatasetParams,
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{run_query, runtime_ready_check, test_request_context},
};

const NAMESPACE: &str = "s";
const ROWS: i64 = 100;
/// `sum(v)` over the rows [`write_table`] writes: `3 * (0 + 1 + … + 99)`.
const SUM_V: i64 = 14_850;

/// The schema-only change a table's last commit makes.
#[derive(Clone, Copy, Debug)]
enum Change {
    /// `v` renamed to `value`.
    Rename,
    /// `note STRING` added.
    Add,
    /// `extra` dropped.
    Drop,
    /// No schema change: the control.
    None,
}

impl Change {
    const ALL: [Change; 4] = [Change::Rename, Change::Add, Change::Drop, Change::None];

    fn table(self) -> &'static str {
        match self {
            Change::Rename => "rename_only",
            Change::Add => "add_only",
            Change::Drop => "drop_only",
            Change::None => "control",
        }
    }

    /// The current schema's fields after the change, by field id: data files
    /// are matched to it by id, not by name.
    fn fields(self) -> Vec<NestedField> {
        let id = NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long));
        let v = |name: &str| NestedField::optional(2, name, Type::Primitive(PrimitiveType::Long));
        let extra = NestedField::optional(3, "extra", Type::Primitive(PrimitiveType::String));
        match self {
            Change::Rename => vec![id, v("value"), extra],
            Change::Add => vec![
                id,
                v("v"),
                extra,
                NestedField::optional(4, "note", Type::Primitive(PrimitiveType::String)),
            ],
            Change::Drop => vec![id, v("v")],
            Change::None => vec![id, v("v"), extra],
        }
    }

    /// The columns `SELECT *` must return: the current schema's.
    fn columns(self) -> Vec<String> {
        self.fields().into_iter().map(|f| f.name).collect()
    }

    /// The name the current schema gives field 2.
    fn value_column(self) -> &'static str {
        match self {
            Change::Rename => "value",
            _ => "v",
        }
    }
}

/// Writes `<warehouse>/s/<table>` with columns `(id BIGINT, v BIGINT, extra
/// STRING)` and [`ROWS`] rows `(i, 3i, 'x<i>')` in one commit, then commits
/// `change` as a new metadata file with no new snapshot — what an engine's
/// `ALTER TABLE` does. Returns the table root.
async fn write_table(warehouse: &Path, change: Change) -> anyhow::Result<PathBuf> {
    let props = HashMap::from([(
        MEMORY_CATALOG_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.display()),
    )]);
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("memory", props)
            .await?,
    );
    let namespace = NamespaceIdent::new(NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(Change::None.fields().into_iter().map(Arc::new))
        .build()?;
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(change.table().to_string())
                .schema(schema)
                .build(),
        )
        .await?;

    let ctx = SessionContext::new();
    let provider = IcebergTableProvider::try_new(
        Arc::clone(&catalog) as Arc<dyn iceberg::Catalog>,
        namespace.clone(),
        change.table(),
    )
    .await?;
    ctx.register_table("t", Arc::new(provider))?;
    let values = (0..ROWS)
        .map(|i| format!("({i}, {}, 'x{i}')", i * 3))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!("INSERT INTO t VALUES {values}"))
        .await?
        .collect()
        .await?;

    let table_root = warehouse.join(NAMESPACE).join(change.table());
    if !matches!(change, Change::None) {
        commit_schema(&table_root, change)?;
    }
    Ok(table_root)
}

/// Commits `change`'s schema as the table's current one, in a new metadata
/// file that carries the same current snapshot.
fn commit_schema(table_root: &Path, change: Change) -> anyhow::Result<()> {
    let (version, latest) = latest_metadata(table_root)?;
    let latest_path = table_root.join("metadata").join(&latest);
    let metadata: TableMetadata = serde_json::from_slice(&std::fs::read(&latest_path)?)?;
    let snapshot_before = metadata.current_snapshot_id();

    let schema = Schema::builder()
        .with_fields(change.fields().into_iter().map(Arc::new))
        .build()?;
    let updated = TableMetadataBuilder::new_from_metadata(
        metadata,
        Some(format!("file://{}", latest_path.display())),
    )
    .add_schema(schema)?
    .set_current_schema(-1)?
    .build()?
    .metadata;
    anyhow::ensure!(
        updated.current_snapshot_id() == snapshot_before && snapshot_before.is_some(),
        "a schema-only commit must keep the current snapshot"
    );
    let snapshot_schema = updated
        .current_snapshot()
        .and_then(|s| s.schema_id())
        .ok_or_else(|| anyhow::anyhow!("the snapshot must name its schema"))?;
    anyhow::ensure!(
        snapshot_schema != updated.current_schema_id(),
        "the fixture must leave the snapshot on an older schema than the table's"
    );

    std::fs::write(
        table_root.join("metadata").join(format!(
            "{:05}-{}.metadata.json",
            version + 1,
            uuid::Uuid::new_v4()
        )),
        serde_json::to_vec(&updated)?,
    )?;
    Ok(())
}

/// The highest-versioned `<N>-<uuid>.metadata.json` under `table_root`.
fn latest_metadata(table_root: &Path) -> anyhow::Result<(u64, String)> {
    std::fs::read_dir(table_root.join("metadata"))?
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().to_string_lossy().to_string();
            let version = name
                .strip_suffix(".metadata.json")?
                .split('-')
                .next()?
                .parse()
                .ok()?;
            Some((version, name))
        })
        .max()
        .ok_or_else(|| anyhow::anyhow!("no metadata files under {}", table_root.display()))
}

fn dataset(table_root: &Path, name: &str, params: &[(&str, String)]) -> Dataset {
    let mut dataset = Dataset::new(format!("iceberg:file://{}", table_root.display()), name);
    if !params.is_empty() {
        dataset.params = Some(DatasetParams::from_string_map(
            params
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        ));
    }
    dataset
}

/// Every way a Spice app reads `change`'s table, by the name it is queried as.
fn readers(table_root: &Path, change: Change) -> anyhow::Result<(Vec<Dataset>, Vec<String>)> {
    let table = change.table();
    let (_, latest) = latest_metadata(table_root)?;
    let mut arrow = dataset(table_root, &format!("{table}_arrow"), &[]);
    arrow.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("arrow".to_string()),
        ..Acceleration::default()
    });
    let datasets = vec![
        dataset(table_root, &format!("{table}_root"), &[]),
        dataset(
            table_root,
            &format!("{table}_pinned"),
            &[("iceberg_metadata_path", format!("/metadata/{latest}"))],
        ),
        arrow,
    ];
    let mut names: Vec<String> = datasets.iter().map(|d| d.name.clone()).collect();
    names.push(format!("ice.{NAMESPACE}.{table}"));
    Ok((datasets, names))
}

fn column_names(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default()
}

async fn scalar(rt: &Arc<Runtime>, sql: &str) -> anyhow::Result<i64> {
    let batches = run_query(rt, sql).await?;
    let column = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| anyhow::anyhow!("`{sql}` returned no rows"))?
        .column(0);
    anyhow::ensure!(!column.is_null(0), "`{sql}` returned NULL");
    Ok(column.as_primitive::<Int64Type>().value(0))
}

/// A table whose last commit renamed, added or dropped a column reads with the
/// table's current schema through every reader: `SELECT *` returns the current
/// columns, a renamed column reads under its new name, an added column is NULL
/// on rows written before it, and a dropped column is gone.
#[tokio::test]
async fn a_schema_only_last_commit_reads_with_the_current_schema() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let mut builder = AppBuilder::new("iceberg_schema_only_commit")
                .with_sql_cache(SQLResultsCacheConfig {
                    enabled: false,
                    ..SQLResultsCacheConfig::default()
                })
                .with_catalog(Catalog::new(
                    format!("iceberg:file://{}", warehouse.path().display()),
                    "ice".to_string(),
                ));
            let mut readers_by_change = Vec::new();
            for change in Change::ALL {
                let table_root = write_table(warehouse.path(), change).await?;
                let (datasets, names) = readers(&table_root, change)?;
                for dataset in datasets {
                    builder = builder.with_dataset(dataset);
                }
                readers_by_change.push((change, names));
            }

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(builder.build()).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            // An accelerated table over a schema-only commit used to fail its
            // refresh, so the runtime never became ready.
            runtime_ready_check(&rt).await;

            for (change, names) in readers_by_change {
                for name in names {
                    let all = run_query(&rt, &format!("SELECT * FROM {name} ORDER BY id"))
                        .await
                        .map_err(|e| anyhow::anyhow!("SELECT * FROM {name} ({change:?}): {e}"))?;
                    assert_eq!(
                        column_names(&all),
                        change.columns(),
                        "SELECT * FROM {name} must return the current schema's columns"
                    );
                    assert_eq!(
                        all.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        usize::try_from(ROWS)?,
                        "SELECT * FROM {name} must return every row"
                    );

                    let value = change.value_column();
                    assert_eq!(
                        scalar(&rt, &format!("SELECT sum({value}) FROM {name}")).await?,
                        SUM_V,
                        "field 2 of {name} must read the written values as `{value}`"
                    );

                    match change {
                        Change::Add => assert_eq!(
                            scalar(
                                &rt,
                                &format!("SELECT count(*) FROM {name} WHERE note IS NULL")
                            )
                            .await?,
                            ROWS,
                            "the added column of {name} must be NULL on every older row"
                        ),
                        Change::Drop => {
                            let err = run_query(&rt, &format!("SELECT extra FROM {name}")).await;
                            assert!(
                                err.is_err(),
                                "the dropped column of {name} must not be selectable"
                            );
                        }
                        Change::Rename => {
                            let err = run_query(&rt, &format!("SELECT v FROM {name}")).await;
                            assert!(
                                err.is_err(),
                                "the old name of {name}'s renamed column must not resolve"
                            );
                        }
                        Change::None => {}
                    }
                }
            }
            Ok(())
        })
        .await
}
