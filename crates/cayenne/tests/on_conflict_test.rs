/*
Copyright 2025 The Spice.ai OSS Authors

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

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

//! Tests for primary key on-conflict handling in Cayenne.

mod common;

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use cayenne::metadata::{CreateTableOptions, PkConflictDetection, VortexConfig};

use cayenne::{CayenneTableProvider, MetadataCatalog};

use datafusion::prelude::SessionContext;

use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

// Run against all supported backends.
test_with_backends!(test_on_conflict_upsert_impl);
test_with_backends!(test_pk_conflict_detection_none_blind_appends_impl);
test_with_backends!(test_pk_conflict_detection_none_rejects_upsert_impl);

async fn test_on_conflict_upsert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "conflict_upsert".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table =
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?;
    let table = Arc::new(table);

    ctx.register_table(
        "conflict_upsert",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // Initial insert.
    ctx.sql("INSERT INTO conflict_upsert VALUES (1, 'Alice'), (2, 'Bob')")
        .await?
        .collect()
        .await?;

    // Second insert with conflicting primary key (id=1) should upsert and drop old row.
    ctx.sql("INSERT INTO conflict_upsert VALUES (1, 'Updated')")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, name FROM conflict_upsert ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("id column");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("name column");

    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(names.value(0), "Updated");
    assert_eq!(names.value(1), "Bob");

    Ok(())
}

async fn test_pk_conflict_detection_none_blind_appends_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let vortex_config = VortexConfig {
        pk_conflict_detection: PkConflictDetection::None,
        ..VortexConfig::default()
    };
    let table_options = CreateTableOptions {
        table_name: "conflict_blind_append".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table =
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?;
    let table = Arc::new(table);

    ctx.register_table(
        "conflict_blind_append",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    ctx.sql("INSERT INTO conflict_blind_append VALUES (1, 'Alice')")
        .await?
        .collect()
        .await?;
    ctx.sql("INSERT INTO conflict_blind_append VALUES (1, 'Duplicate')")
        .await?
        .collect()
        .await?;

    let results = ctx
        .sql("SELECT id, name FROM conflict_blind_append ORDER BY name")
        .await?
        .collect()
        .await?;

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2);

    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("name column");

    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Duplicate");

    Ok(())
}

async fn test_pk_conflict_detection_none_rejects_upsert_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let vortex_config = VortexConfig {
        pk_conflict_detection: PkConflictDetection::None,
        ..VortexConfig::default()
    };
    let table_options = CreateTableOptions {
        table_name: "conflict_none_upsert".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };

    let err = fixture
        .catalog
        .create_table(table_options)
        .await
        .expect_err("pk_conflict_detection=none with upsert should be rejected");

    assert!(
        err.to_string().contains(
            "cayenne_pk_conflict_detection=none cannot be combined with on_conflict=upsert"
        ),
        "unexpected error: {err}"
    );

    Ok(())
}

// --- P1 regression: in-batch duplicate primary keys ---
// A single `INSERT ... VALUES (1,'a'),(1,'b')` is ONE RecordBatch with two rows
// sharing a PK. Before the fix the Exact path kept both (silent double-insert);
// dedup is now derived from the OnConflict variant (Upsert keep-last, DoNothing
// keep-first) as an in-batch pre-pass.

test_with_backends!(test_upsert_in_batch_duplicate_keeps_last_impl);
test_with_backends!(test_upsert_in_batch_duplicate_over_existing_keeps_last_impl);
test_with_backends!(test_do_nothing_in_batch_duplicate_keeps_first_impl);
test_with_backends!(test_upsert_in_batch_duplicate_composite_pk_impl);

/// Helper: collect `(id, name)` rows sorted by id.
async fn collect_id_name(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<(i64, String)>, Box<dyn std::error::Error>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id column");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("name column");
        for i in 0..batch.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

async fn make_id_name_table(
    fixture: &common::TestFixture,
    name: &str,
    on_conflict: OnConflict,
) -> Result<(Arc<CayenneTableProvider>, SessionContext), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: name.to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: Some(on_conflict),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        name,
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;
    Ok((table, ctx))
}

/// Upsert: a single batch with a duplicate PK keeps the LAST occurrence.
async fn test_upsert_in_batch_duplicate_keeps_last_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, ctx) = make_id_name_table(
        &fixture,
        "upsert_in_batch_dup",
        OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()])),
    )
    .await?;

    ctx.sql("INSERT INTO upsert_in_batch_dup VALUES (1, 'a'), (1, 'b')")
        .await?
        .collect()
        .await?;

    let rows =
        collect_id_name(&ctx, "SELECT id, name FROM upsert_in_batch_dup ORDER BY id").await?;
    assert_eq!(
        rows,
        vec![(1, "b".to_string())],
        "Upsert in-batch duplicate must keep exactly one row, the LAST value"
    );
    Ok(())
}

/// Upsert: an in-batch duplicate that ALSO conflicts with a pre-existing row must
/// still yield one row (the last in-batch value) — exercises the delete path so a
/// double-counted supersede would corrupt the live-row bookkeeping.
async fn test_upsert_in_batch_duplicate_over_existing_keeps_last_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, ctx) = make_id_name_table(
        &fixture,
        "upsert_in_batch_dup_existing",
        OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()])),
    )
    .await?;

    ctx.sql("INSERT INTO upsert_in_batch_dup_existing VALUES (1, 'orig')")
        .await?
        .collect()
        .await?;
    ctx.sql("INSERT INTO upsert_in_batch_dup_existing VALUES (1, 'a'), (1, 'b')")
        .await?
        .collect()
        .await?;

    let rows = collect_id_name(
        &ctx,
        "SELECT id, name FROM upsert_in_batch_dup_existing ORDER BY id",
    )
    .await?;
    assert_eq!(
        rows,
        vec![(1, "b".to_string())],
        "Upsert in-batch duplicate over an existing row must leave one row, the LAST value"
    );
    Ok(())
}

/// `DoNothing`: a single batch with a duplicate PK keeps the FIRST occurrence.
async fn test_do_nothing_in_batch_duplicate_keeps_first_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_table, ctx) = make_id_name_table(
        &fixture,
        "do_nothing_in_batch_dup",
        OnConflict::DoNothing(ColumnReference::new(vec!["id".to_string()])),
    )
    .await?;

    ctx.sql("INSERT INTO do_nothing_in_batch_dup VALUES (1, 'a'), (1, 'b')")
        .await?
        .collect()
        .await?;

    let rows = collect_id_name(
        &ctx,
        "SELECT id, name FROM do_nothing_in_batch_dup ORDER BY id",
    )
    .await?;
    assert_eq!(
        rows,
        vec![(1, "a".to_string())],
        "DoNothing in-batch duplicate must keep exactly one row, the FIRST value"
    );
    Ok(())
}

/// Composite PK: in-batch dedup keys on the FULL composite, so only rows sharing
/// every PK column collapse (Upsert keep-last); a partial-key match does not.
async fn test_upsert_in_batch_duplicate_composite_pk_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: "upsert_in_batch_dup_composite".to_string(),
        schema,
        primary_key: vec!["region".to_string(), "id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "region".to_string(),
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };
    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        "upsert_in_batch_dup_composite",
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;

    // ('US',1) duplicated (keep last → 200); ('EU',1) distinct composite → kept.
    ctx.sql(
        "INSERT INTO upsert_in_batch_dup_composite VALUES \
         ('US', 1, 100), ('US', 1, 200), ('EU', 1, 300)",
    )
    .await?
    .collect()
    .await?;

    let batches = ctx
        .sql("SELECT region, id, value FROM upsert_in_batch_dup_composite ORDER BY region, id")
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in &batches {
        let regions = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("region column");
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id column");
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("value column");
        for i in 0..batch.num_rows() {
            rows.push((regions.value(i).to_string(), ids.value(i), values.value(i)));
        }
    }
    assert_eq!(
        rows,
        vec![("EU".to_string(), 1, 300), ("US".to_string(), 1, 200),],
        "composite-PK in-batch dedup collapses only the full-key duplicate, keep-last"
    );
    Ok(())
}

// --- Null primary key values ---
// A composite key is only actionable if the rejection says which of its columns
// carried the null.

test_with_backends!(test_null_composite_pk_names_the_null_column_impl);
test_with_backends!(test_null_composite_pk_names_the_null_column_blind_append_impl);

/// A table keyed on `(region, id)` whose `region` is nullable, so a null reaches the
/// write path's validation instead of `DataFusion`'s non-nullable column check.
async fn nullable_composite_pk_table(
    fixture: &common::TestFixture,
    table_name: &str,
    vortex_config: VortexConfig,
    on_conflict: Option<OnConflict>,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema,
        primary_key: vec!["region".to_string(), "id".to_string()],
        on_conflict,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog_arc: Arc<dyn MetadataCatalog> = fixture.catalog.clone();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog_arc, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(
        table_name,
        Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
    )?;
    Ok(ctx)
}

fn assert_names_the_null_pk_column(err: &dyn std::error::Error, table_name: &str) {
    let message = err.to_string();
    assert!(
        message.contains("Primary key column 'region' has null values"),
        "the rejection must name the null column of the composite key: {message}"
    );
    assert!(
        message.contains(table_name),
        "the rejection must name the table: {message}"
    );
    assert!(
        message.contains("https://spiceai.org/docs/features/data-acceleration/constraints"),
        "the rejection must link the constraints docs: {message}"
    );
}

async fn test_null_composite_pk_names_the_null_column_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = nullable_composite_pk_table(
        &fixture,
        "null_composite_pk",
        VortexConfig::default(),
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "region".to_string(),
            "id".to_string(),
        ]))),
    )
    .await?;

    let err = ctx
        .sql("INSERT INTO null_composite_pk VALUES ('US', 1, 100), (NULL, 2, 200)")
        .await?
        .collect()
        .await
        .expect_err("a null in a primary key column must be rejected");

    assert_names_the_null_pk_column(&err, "null_composite_pk");
    Ok(())
}

/// `pk_conflict_detection: none` skips the existence lookup but not the validation,
/// and it reports the null column the same way.
async fn test_null_composite_pk_names_the_null_column_blind_append_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let vortex_config = VortexConfig {
        pk_conflict_detection: PkConflictDetection::None,
        ..VortexConfig::default()
    };
    let ctx = nullable_composite_pk_table(&fixture, "null_composite_pk_blind", vortex_config, None)
        .await?;

    let err = ctx
        .sql("INSERT INTO null_composite_pk_blind VALUES ('US', 1, 100), (NULL, 2, 200)")
        .await?
        .collect()
        .await
        .expect_err("a null in a primary key column must be rejected");

    assert_names_the_null_pk_column(&err, "null_composite_pk_blind");
    Ok(())
}
