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

//! A file-mode Cayenne acceleration refuses to open data stored under a
//! different partitioning than its `partition_by`. Cayenne does not
//! re-partition stored data, so opening it would serve none of its rows.

use std::{path::Path, sync::Arc};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use data_accelerator_api::{AccelerationSource, AcceleratorEngineRegistry};
use datafusion::{
    common::TableReference, datasource::TableProvider, execution::context::SessionContext,
};
use runtime_acceleration::{
    Engine,
    acceleration::{Acceleration, Mode, RefreshMode, SettingsRebuild},
    testing::TestAccelerationSource,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// A file-mode Cayenne acceleration of `t (id, w, v)` stored under `dir` and
/// partitioned by the columns `partition_by`, as the runtime creates it.
/// A partitioning change is refused under `refresh_mode: append`, whose acceleration
/// can hold rows the source no longer has, and rebuilds the acceleration under
/// `refresh_mode: full`.
async fn table(
    ctx: &SessionContext,
    dir: &Path,
    partition_by: &[&str],
    refresh_mode: RefreshMode,
) -> Result<Arc<dyn TableProvider>, String> {
    table_with(
        ctx,
        dir,
        partition_by,
        refresh_mode,
        SettingsRebuild::Allowed,
    )
    .await
}

/// [`table`], registered with the runtime's decision on rebuilding it for changed
/// settings.
async fn table_with(
    ctx: &SessionContext,
    dir: &Path,
    partition_by: &[&str],
    refresh_mode: RefreshMode,
    settings_rebuild: SettingsRebuild,
) -> Result<Arc<dyn TableProvider>, String> {
    table_configured(
        ctx,
        dir,
        partition_by,
        refresh_mode,
        settings_rebuild,
        Setup::default(),
    )
    .await
}

/// How [`table_configured`] sets `t` up, beyond its partitioning and refresh mode.
struct Setup<'a> {
    primary_key: Option<&'a str>,
    /// Where `t`'s data is configured, under the test's directory. Its metastore stays
    /// under `metadata` whatever this is.
    data: &'a str,
    schema: SchemaRef,
    /// Acceleration params besides the data and metastore locations.
    params: &'a [(&'a str, &'a str)],
    maintained_aggregates: Vec<spicepod::acceleration::MaintainedAggregate>,
}

impl Default for Setup<'_> {
    fn default() -> Self {
        Self {
            primary_key: None,
            data: "data",
            schema: schema(),
            params: &[],
            maintained_aggregates: Vec::new(),
        }
    }
}

/// [`table_with`], set up as `setup` says.
async fn table_configured(
    ctx: &SessionContext,
    dir: &Path,
    partition_by: &[&str],
    refresh_mode: RefreshMode,
    settings_rebuild: SettingsRebuild,
    setup: Setup<'_>,
) -> Result<Arc<dyn TableProvider>, String> {
    let source = TestAccelerationSource::new("t").with_acceleration(Acceleration {
        enabled: true,
        engine: Engine::Cayenne,
        mode: Mode::File,
        refresh_mode: Some(refresh_mode),
        settings_rebuild,
        maintained_aggregates: setup.maintained_aggregates.into(),
        primary_key: setup.primary_key.map(|key| {
            datafusion_table_providers::util::column_reference::ColumnReference::try_from(key)
                .expect("valid column reference")
        }),
        partition_by: partition_by
            .iter()
            .map(|column| spicepod::partitioning::PartitionedBy {
                name: (*column).to_string(),
                expression: (*column).to_string(),
            })
            .collect(),
        params: [
            (
                "cayenne_file_path".to_string(),
                dir.join(setup.data).to_string_lossy().into_owned(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                dir.join("metadata").to_string_lossy().into_owned(),
            ),
        ]
        .into_iter()
        .chain(
            setup
                .params
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        )
        .collect(),
        ..Default::default()
    });
    let registry = AcceleratorEngineRegistry::new();
    registry
        .register_accelerator_engine(Engine::Cayenne, Arc::new(CayenneAccelerator::new()))
        .await;
    let acceleration = source.acceleration().expect("acceleration").clone();
    registry
        .create_accelerator_table(
            TableReference::bare("t"),
            setup.schema,
            None,
            &acceleration,
            source.secrets(),
            Some(&source),
            Arc::new(ctx.clone()),
        )
        .await
        .map_err(|error| error.to_string())
}

/// Stores two rows in `t` partitioned by `partition_by`, then closes it.
async fn store(dir: &Path, partition_by: &[&str]) {
    let ctx = SessionContext::new();
    let table = table(&ctx, dir, partition_by, RefreshMode::Append)
        .await
        .expect("table");
    ctx.register_table("t", table).expect("register");
    ctx.sql("INSERT INTO t VALUES (1, 0, 0), (2, 1, 0)")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("insert");
}

/// The `id` of every row of `t`, ordered.
async fn ids(ctx: &SessionContext) -> Vec<i64> {
    let batches = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("rows");
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_data_reopens_under_its_own_partitioning() {
    for partition_by in [&[][..], &["w"][..]] {
        let dir = tempfile::tempdir().expect("dir");
        store(dir.path(), partition_by).await;

        let ctx = SessionContext::new();
        let table = table(&ctx, dir.path(), partition_by, RefreshMode::Append)
            .await
            .expect("reopen");
        ctx.register_table("t", table).expect("register");
        assert_eq!(ids(&ctx).await, [1, 2], "partition_by: {partition_by:?}");
    }
}

/// A stored partitioned table that keeps its settings through a change (here a new
/// primary key and data location under `refresh_mode: append`, which can hold rows a
/// reload would not bring back) keeps them for every partition, including one created
/// after the change. A partition created with the requested settings instead would
/// leave one table keyed and placed two ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_created_after_a_kept_change_takes_the_stored_settings() {
    use cayenne::{CayenneCatalog, MetadataCatalog};

    let dir = tempfile::tempdir().expect("dir");
    // Unkeyed, under `dir/data`, with partitions `w = 0` and `w = 1`.
    store(dir.path(), &["w"]).await;

    let ctx = SessionContext::new();
    let table = table_configured(
        &ctx,
        dir.path(),
        &["w"],
        RefreshMode::Append,
        SettingsRebuild::Allowed,
        Setup {
            primary_key: Some("id"),
            data: "moved",
            ..Setup::default()
        },
    )
    .await
    .expect("a kept change opens the stored table");
    ctx.register_table("t", table).expect("register");
    // A row for a partition the table has not seen, `w = 2`.
    ctx.sql("INSERT INTO t VALUES (3, 2, 0)")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("insert");
    assert_eq!(ids(&ctx).await, [1, 2, 3]);

    let catalog = CayenneCatalog::new(format!(
        "sqlite://{}",
        dir.path().join("metadata").join("cayenne.db").display()
    ))
    .expect("catalog");
    catalog.init().await.expect("init catalog");
    let parent = catalog.get_table("t").await.expect("the stored table");
    assert!(parent.primary_key.is_empty());
    let stored_root = parent.path.trim_end_matches('/').to_string();
    let partitions = catalog
        .get_partitions(&parent.table_id)
        .await
        .expect("partitions");
    assert_eq!(partitions.len(), 3, "partitions: {partitions:?}");
    for partition in partitions {
        let key = partition.composite_key();
        let child = catalog
            .get_table(&cayenne::partition_naming::partition_child_table_name(
                "t", &key,
            ))
            .await
            .expect("the partition's table");
        assert_eq!(
            (child.primary_key.clone(), child.on_conflict.clone()),
            (parent.primary_key.clone(), parent.on_conflict.clone()),
            "partition {key} must have the stored table's key"
        );
        assert!(
            child.path.starts_with(&stored_root),
            "partition {key} must be stored under '{stored_root}', got '{}'",
            child.path
        );
    }
}

/// A table whose maintained MIN/MAX need its primary key keeps serving them through a
/// change it keeps, even one that drops the requested key: under `append`, a table keyed
/// on `id` keeps `id`, so MIN/MAX still have the key they retract prior extrema by.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintained_min_max_hold_for_the_stored_key_after_a_kept_change() {
    use cayenne::{CayenneCatalog, MetadataCatalog};
    use spicepod::acceleration::{
        MaintainedAggregate, MaintainedAggregateExpr, MaintainedAggregateFunction,
    };

    let min_max = || {
        vec![MaintainedAggregate {
            group_by: vec!["w".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Min,
                    column: Some("v".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Max,
                    column: Some("v".to_string()),
                },
            ],
            filter_sql: None,
        }]
    };
    let dir = tempfile::tempdir().expect("dir");
    // Keyed on `id`, then reopened with no key requested, which keeps `id`.
    for primary_key in [Some("id"), None] {
        let ctx = SessionContext::new();
        if let Err(error) = table_configured(
            &ctx,
            dir.path(),
            &[],
            RefreshMode::Append,
            SettingsRebuild::Allowed,
            Setup {
                primary_key,
                maintained_aggregates: min_max(),
                ..Setup::default()
            },
        )
        .await
        {
            panic!("requested key {primary_key:?}: the table must open, got: {error}");
        }
    }
    let catalog = CayenneCatalog::new(format!(
        "sqlite://{}",
        dir.path().join("metadata").join("cayenne.db").display()
    ))
    .expect("catalog");
    catalog.init().await.expect("init catalog");
    assert_eq!(
        catalog
            .get_table("t")
            .await
            .expect("the stored table")
            .primary_key,
        vec!["id".to_string()]
    );
}

/// `t (id, w, k, e)`, where `e` is a two-dimensional embedding, so the table is served
/// with a native vector index.
fn vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new(
            "e",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]))
}

/// The key each native vector index of `table` identifies its search results by.
fn vector_index_keys(table: &Arc<dyn TableProvider>) -> Vec<Vec<String>> {
    let table = table
        .downcast_ref::<spice_table::SpiceTable>()
        .expect("a table with a vector column is served with its vector index");
    table
        .indexes()
        .iter()
        .map(|index| {
            let index = index
                .as_any()
                .downcast_ref::<search::index::NativeVectorIndex>()
                .expect("a native vector index");
            search::index::SearchIndex::primary_fields(index)
                .iter()
                .map(|field| field.name().clone())
                .collect()
        })
        .collect()
}

/// A table that keeps its settings through a change keeps the conflict detection they
/// include. Here it is reopened under `append` with a new primary key and
/// `cayenne_pk_conflict_detection: none`, which keeps the stored key `id` and detection
/// `auto`, so writing a stored `id` again still replaces its row. Admitting a second row
/// for `id` would break the table the metastore still describes as keyed on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kept_change_keeps_the_stored_conflict_detection() {
    async fn run(ctx: &SessionContext, sql: &str) {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("insert");
    }

    for partition_by in [&[][..], &["w"][..]] {
        let dir = tempfile::tempdir().expect("dir");
        let ctx = SessionContext::new();
        let table = table_configured(
            &ctx,
            dir.path(),
            partition_by,
            RefreshMode::Append,
            SettingsRebuild::Allowed,
            Setup {
                primary_key: Some("id"),
                ..Setup::default()
            },
        )
        .await
        .expect("table");
        ctx.register_table("t", table).expect("register");
        run(&ctx, "INSERT INTO t VALUES (1, 0, 0)").await;

        let ctx = SessionContext::new();
        let table = table_configured(
            &ctx,
            dir.path(),
            partition_by,
            RefreshMode::Append,
            SettingsRebuild::Allowed,
            Setup {
                primary_key: Some("v"),
                params: &[("cayenne_pk_conflict_detection", "none")],
                ..Setup::default()
            },
        )
        .await
        .expect("a kept change opens the stored table");
        ctx.register_table("t", table).expect("register");
        run(&ctx, "INSERT INTO t VALUES (1, 0, 5)").await;
        // Twice into a partition the table has not seen, `w = 1`, which the change creates.
        run(&ctx, "INSERT INTO t VALUES (2, 1, 0)").await;
        run(&ctx, "INSERT INTO t VALUES (2, 1, 7)").await;
        assert_eq!(
            ids(&ctx).await,
            [1, 2],
            "partition_by: {partition_by:?}: the stored key `id` must still be enforced"
        );
    }
}

/// Turning `cayenne_pk_conflict_detection` off is applied in place: the reopened table
/// persists `none` to its metastore row, and its writes follow that row, so writing a
/// stored `id` again adds a second row instead of replacing the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_conflict_detection_off_in_place_admits_a_repeated_key() {
    async fn run(ctx: &SessionContext, sql: &str) {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("insert");
    }

    for partition_by in [&[][..], &["w"][..]] {
        let dir = tempfile::tempdir().expect("dir");
        let ctx = SessionContext::new();
        let table = table_configured(
            &ctx,
            dir.path(),
            partition_by,
            RefreshMode::Append,
            SettingsRebuild::Allowed,
            Setup {
                primary_key: Some("id"),
                ..Setup::default()
            },
        )
        .await
        .expect("table");
        ctx.register_table("t", table).expect("register");
        run(&ctx, "INSERT INTO t VALUES (1, 0, 0)").await;
        run(&ctx, "INSERT INTO t VALUES (1, 0, 3)").await;
        assert_eq!(
            ids(&ctx).await,
            [1],
            "partition_by: {partition_by:?}: conflict detection starts on"
        );

        let ctx = SessionContext::new();
        let table = table_configured(
            &ctx,
            dir.path(),
            partition_by,
            RefreshMode::Append,
            SettingsRebuild::Allowed,
            Setup {
                primary_key: Some("id"),
                params: &[("cayenne_pk_conflict_detection", "none")],
                ..Setup::default()
            },
        )
        .await
        .expect("turning conflict detection off opens the stored table");
        ctx.register_table("t", table).expect("register");
        run(&ctx, "INSERT INTO t VALUES (1, 0, 5)").await;
        assert_eq!(
            ids(&ctx).await,
            [1, 1],
            "partition_by: {partition_by:?}: writes must follow the check turned off in place"
        );
    }
}

/// Vector search identifies rows by the key the stored table holds them by, the key its
/// writes and partitions use, including after a key change the table keeps. Identifying
/// them by the requested key would give search results a key the table never adopted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_identifies_rows_by_the_stored_key_after_a_kept_change() {
    for partition_by in [&[][..], &["w"][..]] {
        let dir = tempfile::tempdir().expect("dir");
        // Keyed on `id`, then reopened keyed on `k` under `append`, which keeps `id`.
        for requested in ["id", "k"] {
            let ctx = SessionContext::new();
            let table = table_configured(
                &ctx,
                dir.path(),
                partition_by,
                RefreshMode::Append,
                SettingsRebuild::Allowed,
                Setup {
                    primary_key: Some(requested),
                    schema: vector_schema(),
                    ..Setup::default()
                },
            )
            .await
            .expect("table");
            assert_eq!(
                vector_index_keys(&table),
                [vec!["id".to_string()]],
                "partition_by: {partition_by:?}, requested key: {requested}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioning_unpartitioned_data_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &[]).await;

    let error = table(
        &SessionContext::new(),
        dir.path(),
        &["w"],
        RefreshMode::Append,
    )
    .await
    .expect_err("unpartitioned data must not open as a partitioned table");
    assert!(
        error.contains(
            "dataset t (cayenne): its acceleration data was written without 'partition_by', but the dataset now sets 'partition_by: [w]'"
        ),
        "{error}"
    );
    assert!(
        error.contains("To keep using the existing data, remove 'partition_by'"),
        "{error}"
    );
    assert!(
        error.contains(&format!(
            "the Cayenne metastore directory '{}', then start Spice to reload the dataset from the source",
            dir.path().join("metadata").display()
        )),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpartitioning_partitioned_data_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &["w"]).await;

    let error = table(&SessionContext::new(), dir.path(), &[], RefreshMode::Append)
        .await
        .expect_err("partitioned data must not open as an unpartitioned table");
    assert!(
        error.contains(
            "dataset t (cayenne): its acceleration data was written partitioned by [w], but the dataset now sets no 'partition_by'"
        ),
        "{error}"
    );
    assert!(
        error.contains("restore the 'partition_by' it was written with"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repartitioning_partitioned_data_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &["w"]).await;

    let error = table(
        &SessionContext::new(),
        dir.path(),
        &["v"],
        RefreshMode::Append,
    )
    .await
    .expect_err("data partitioned by w must not open partitioned by v");
    assert!(
        error.contains(
            "dataset t (cayenne): its acceleration data was written partitioned by [w], but the dataset now sets 'partition_by: [v]'"
        ),
        "{error}"
    );
}

/// Under `refresh_mode: full` every refresh rewrites every row, so a partitioning change
/// is reported for the runtime to rebuild the acceleration rather than refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repartitioning_a_full_refresh_acceleration_rebuilds_it() {
    for (stored, configured, change) in [
        (&[][..], &["w"][..], "none -> ['w']"),
        (&["w"][..], &[][..], "['w'] -> none"),
        (&["w"][..], &["v"][..], "['w'] -> ['v']"),
    ] {
        let dir = tempfile::tempdir().expect("dir");
        store(dir.path(), stored).await;

        let error = table(
            &SessionContext::new(),
            dir.path(),
            configured,
            RefreshMode::Full,
        )
        .await
        .expect_err("a changed partitioning must not open the stored table");
        assert_eq!(
            error,
            format!(
                "the existing acceleration of dataset 't' was built with settings it cannot adopt in place (`partition_by`: {change})"
            )
        );
    }
}

/// A full-refresh acceleration whose rebuild the runtime holds until its source can be
/// read cannot serve its rows under the new partitioning meanwhile, so the change is
/// refused, with the fixes that apply then, and the stored data is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_repartitioning_is_refused_until_the_source_can_be_read() {
    let dir = tempfile::tempdir().expect("dir");
    store(dir.path(), &[]).await;

    let error = table_with(
        &SessionContext::new(),
        dir.path(),
        &["w"],
        RefreshMode::Full,
        SettingsRebuild::HeldUntilSourceReadable,
    )
    .await
    .expect_err("a held rebuild must not open unpartitioned data as partitioned");
    assert_eq!(
        error,
        "Acceleration creation failed: Failed to load dataset t (cayenne): its acceleration data was written without 'partition_by', but the dataset now sets 'partition_by: [w]'. Applying it rebuilds the acceleration from the source, which cannot be read on the acceleration's schema yet. To keep using the existing data, remove 'partition_by'. To apply the new 'partition_by', restart once the source is reachable and any change to its schema is resolved (revert it, or set 'on_schema_change' to adopt it). See: https://spiceai.org/docs/features/data-acceleration/partitioning"
    );

    let ctx = SessionContext::new();
    let table = table(&ctx, dir.path(), &[], RefreshMode::Full)
        .await
        .expect("the stored data reopens under its own partitioning");
    ctx.register_table("t", table).expect("register");
    assert_eq!(ids(&ctx).await, [1, 2]);
}
