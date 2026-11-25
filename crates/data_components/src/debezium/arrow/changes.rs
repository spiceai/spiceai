/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{
    arrow::struct_builder::StructBuilder,
    cdc::{ChangeBatch, changes_schema},
    debezium::{
        arrow::{ColumnDefaults, downcast_builder},
        change_event::{ChangeEvent, Op},
    },
    schema_projection::SchemaProjection,
};
use arrow::{
    array::{ArrayBuilder, ListBuilder, RecordBatch, StringBuilder},
    datatypes::{Schema, SchemaRef},
};
use snafu::prelude::*;

/// Converts a `ChangeEvent` into a `ChangeBatch`
pub fn to_change_batch(
    table_schema: &SchemaRef,
    primary_key: &[String],
    change: &ChangeEvent,
    projection: Option<&SchemaProjection>,
) -> super::Result<ChangeBatch> {
    let schema = changes_schema(table_schema);

    let mut struct_builder = StructBuilder::from_fields(schema.fields().clone(), 1);

    append_change_event(
        &mut struct_builder,
        &schema,
        primary_key,
        change,
        projection,
        &ColumnDefaults::new(),
    )?;

    let struct_array = struct_builder.finish();
    let record_batch: RecordBatch = struct_array.into();

    let Ok(change_batch) = ChangeBatch::try_new(record_batch) else {
        unreachable!(
            "We constructed the record batch with the correct schema, so this shouldn't fail"
        );
    };

    Ok(change_batch)
}

pub fn vector_to_change_batch(
    table_schema: &SchemaRef,
    primary_key: &[String],
    changes: &[&ChangeEvent],
    projection: Option<&SchemaProjection>,
) -> super::Result<ChangeBatch> {
    vector_to_change_batch_with_defaults(
        table_schema,
        primary_key,
        changes,
        projection,
        &ColumnDefaults::new(),
    )
}

/// [`vector_to_change_batch`], where a required column an event does not carry
/// takes its value from `defaults` (see [`super::column_defaults`]).
pub fn vector_to_change_batch_with_defaults(
    table_schema: &SchemaRef,
    primary_key: &[String],
    changes: &[&ChangeEvent],
    projection: Option<&SchemaProjection>,
    defaults: &ColumnDefaults,
) -> super::Result<ChangeBatch> {
    let schema = changes_schema(table_schema);

    let mut struct_builder = StructBuilder::from_fields(schema.fields().clone(), changes.len());

    for change in changes {
        append_change_event(
            &mut struct_builder,
            &schema,
            primary_key,
            change,
            projection,
            defaults,
        )?;
    }

    let struct_array = struct_builder.finish();
    let record_batch: RecordBatch = struct_array.into();

    let Ok(change_batch) = ChangeBatch::try_new(record_batch) else {
        unreachable!(
            "Record batch was constructed with the correct schema, so this shouldn't fail"
        );
    };

    Ok(change_batch)
}

fn append_change_event(
    struct_builder: &mut StructBuilder,
    schema: &Schema,
    primary_key: &[String],
    change: &ChangeEvent,
    projection: Option<&SchemaProjection>,
    defaults: &ColumnDefaults,
) -> super::Result<()> {
    if primary_key.is_empty() && matches!(change.payload.op, Op::Update) {
        let before = change
            .payload
            .before
            .clone()
            .context(super::UpdateOpWithoutBeforeFieldSnafu)?;
        append_change_row(
            struct_builder,
            schema,
            primary_key,
            "d",
            before,
            projection,
            defaults,
        )?;
        append_change_row(
            struct_builder,
            schema,
            primary_key,
            "c",
            change.payload.after.clone(),
            projection,
            defaults,
        )?;
        return Ok(());
    }

    let op = change.payload.op.as_str();
    let change_data = match change.payload.op {
        Op::Delete => change
            .payload
            .before
            .clone()
            .context(super::DeleteOpWithoutBeforeFieldSnafu)?,
        _ => change.payload.after.clone(),
    };

    append_change_row(
        struct_builder,
        schema,
        primary_key,
        op,
        change_data,
        projection,
        defaults,
    )
}

fn append_change_row(
    struct_builder: &mut StructBuilder,
    schema: &Schema,
    primary_key: &[String],
    op: &str,
    change_data: serde_json::Value,
    projection: Option<&SchemaProjection>,
    defaults: &ColumnDefaults,
) -> super::Result<()> {
    struct_builder.append(true);
    // Apply JSON nesting: fold non-declared fields of the `before`/`after`
    // row into the catch-all column before Arrow conversion. Type-only
    // projections (no catch-all) leave the row unchanged.
    let change_data = match projection.filter(|p| p.has_catch_all()) {
        Some(projection) => projection.project_row(change_data),
        None => change_data,
    };
    let mut change_data = Some(change_data);

    for (idx, field) in schema.fields().iter().enumerate() {
        let field_builder = struct_builder.field_builder_array(idx);
        match field.name().as_str() {
            "op" => {
                let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                str_builder.append_value(op);
            }
            "primary_keys" => {
                let list_builder =
                    downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(field_builder)?;
                if primary_key.is_empty() {
                    list_builder.append(false);
                } else {
                    let str_builder = downcast_builder::<StringBuilder>(list_builder.values())?;
                    for key in primary_key {
                        str_builder.append_value(key);
                    }
                    list_builder.append(true);
                }
            }
            "data" => {
                let data_struct_builder = downcast_builder::<StructBuilder>(field_builder)?;
                let change_data =
                    change_data
                        .take()
                        .context(super::InvalidChangeEventSchemaSnafu {
                            reason: "data field appears more than once",
                        })?;
                super::append_value_to_struct_builder_with_defaults(
                    change_data,
                    data_struct_builder,
                    defaults,
                )?;
            }
            _ => unreachable!("Unexpected field in changes schema {}", field.name()),
        }
    }

    ensure!(
        change_data.is_none(),
        super::InvalidChangeEventSchemaSnafu {
            reason: "data field is missing"
        }
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::debezium::arrow::Error;
    use crate::schema_projection::{ColumnSource, ProjectedColumn, SchemaProjection};
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::{DataType, Field as ArrowField};
    use std::sync::Arc;

    fn change_event(after: &serde_json::Value) -> ChangeEvent {
        // Minimal Debezium create event; only `payload.after`/`op` matter here.
        let value = serde_json::json!({
            "schema": {"type": "struct", "fields": [], "optional": false, "name": "s"},
            "payload": {
                "before": null,
                "after": after.clone(),
                "source": {
                    "version": "x", "connector": "x", "name": "x", "ts_ms": 0,
                    "snapshot": "false", "db": "x", "table": "x"
                },
                "op": "c",
                "ts_ms": 0,
                "transaction": null
            }
        });
        serde_json::from_value(value).expect("valid change event")
    }

    /// A `json_object` catch-all column folds every non-declared `after` field
    /// into one sorted-JSON string column on the Debezium change path.
    #[test]
    fn json_nesting_folds_into_catch_all() {
        // Projected (exposed) data schema: declared `id` + catch-all `data`.
        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Utf8, true),
            ArrowField::new("data", DataType::Utf8, true),
        ]));
        let projection = SchemaProjection::new(
            vec![
                ProjectedColumn {
                    output_name: "id".to_string(),
                    source: ColumnSource::Field,
                    declared_type: Some(DataType::Utf8),
                    nullable: true,
                },
                ProjectedColumn {
                    output_name: "data".to_string(),
                    source: ColumnSource::JsonObject,
                    declared_type: None,
                    nullable: true,
                },
            ],
            &["id".to_string()],
        )
        .expect("projection");

        let change = change_event(&serde_json::json!({
            "id": "row1",
            "zeta": 2,
            "alpha": 1
        }));

        let batch = to_change_batch(
            &table_schema,
            &["id".to_string()],
            &change,
            Some(&projection),
        )
        .expect("change batch");
        let data = batch.data(0);

        let id = data
            .column(data.schema().index_of("id").expect("id col"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id utf8");
        assert_eq!(id.value(0), "row1");

        let catch_all = data
            .column(data.schema().index_of("data").expect("data col"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("data utf8");
        // non-declared fields, alphabetically sorted
        assert_eq!(catch_all.value(0), r#"{"alpha":1,"zeta":2}"#);
    }

    /// Table schema used by the CDC-semantics tests below. Both columns are
    /// nullable so a partial before-image still builds.
    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ArrowField::new("id", DataType::Int32, true),
            ArrowField::new("name", DataType::Utf8, true),
        ]))
    }

    /// A Debezium event with an explicit op and before/after images.
    fn event(op: &str, before: serde_json::Value, after: serde_json::Value) -> ChangeEvent {
        let mut value = serde_json::json!({"op": op, "ts_ms": 0});
        value["before"] = before;
        value["after"] = after;
        ChangeEvent::from_json_value(value).expect("valid change event")
    }

    fn id_of(batch: &arrow::array::RecordBatch) -> Option<i32> {
        let col = batch
            .column(batch.schema().index_of("id").expect("id col"))
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("id is Int32");
        (!col.is_null(0)).then(|| col.value(0))
    }

    fn name_of(batch: &arrow::array::RecordBatch) -> Option<String> {
        let col = batch
            .column(batch.schema().index_of("name").expect("name col"))
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is Utf8");
        (!col.is_null(0)).then(|| col.value(0).to_string())
    }

    /// A delete's row image lives in `before`; `after` is null. Reading the
    /// delete off `after` would emit an all-null key and delete the wrong row
    /// (or nothing at all).
    #[test]
    fn a_delete_row_carries_the_before_image() {
        let change = event(
            "d",
            serde_json::json!({"id": 1, "name": "old"}),
            serde_json::Value::Null,
        );

        let batch = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect("change batch");

        assert_eq!(batch.record.num_rows(), 1);
        assert_eq!(batch.op(0).to_string(), "d");
        let data = batch.data(0);
        assert_eq!(id_of(&data), Some(1));
        assert_eq!(name_of(&data), Some("old".to_string()));
    }

    /// Without a before-image a delete has no key to delete by. Emitting an
    /// all-null row instead would silently drop the deletion.
    #[test]
    fn a_delete_without_a_before_image_is_an_error() {
        let change = event("d", serde_json::Value::Null, serde_json::Value::Null);

        let err = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect_err("a delete with no before image must fail");
        assert!(
            matches!(err, Error::DeleteOpWithoutBeforeField),
            "unexpected error: {err}"
        );
    }

    /// With a primary key the accelerator can locate the row itself, so an
    /// update stays a single `u` row built from the after-image.
    #[test]
    fn a_keyed_update_emits_one_row_from_the_after_image() {
        let change = event(
            "u",
            serde_json::json!({"id": 1, "name": "old"}),
            serde_json::json!({"id": 1, "name": "new"}),
        );

        let batch = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect("change batch");

        assert_eq!(batch.record.num_rows(), 1);
        assert_eq!(batch.op(0).to_string(), "u");
        let data = batch.data(0);
        assert_eq!(id_of(&data), Some(1));
        assert_eq!(name_of(&data), Some("new".to_string()));
    }

    /// Without a primary key there is no way to address the existing row, so
    /// an update becomes a delete of the exact old row followed by a create of
    /// the new one. Emitting only the create would duplicate the row.
    #[test]
    fn a_keyless_update_is_split_into_a_delete_of_before_and_a_create_of_after() {
        let change = event(
            "u",
            serde_json::json!({"id": 1, "name": "old"}),
            serde_json::json!({"id": 1, "name": "new"}),
        );

        let batch = to_change_batch(&orders_schema(), &[], &change, None).expect("change batch");

        assert_eq!(batch.record.num_rows(), 2, "one delete plus one create");
        assert_eq!(batch.op(0).to_string(), "d");
        assert_eq!(name_of(&batch.data(0)), Some("old".to_string()));
        assert_eq!(batch.op(1).to_string(), "c");
        assert_eq!(name_of(&batch.data(1)), Some("new".to_string()));
    }

    /// A keyless update needs the full before-image to build its delete leg.
    /// Falling back to the after-image would delete a row that never existed
    /// and leave the stale row behind.
    #[test]
    fn a_keyless_update_without_a_before_image_is_an_error() {
        let change = event(
            "u",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "new"}),
        );

        let err = to_change_batch(&orders_schema(), &[], &change, None)
            .expect_err("a keyless update with no before image must fail");
        assert!(
            matches!(err, Error::UpdateOpWithoutBeforeField),
            "unexpected error: {err}"
        );
    }

    /// A keyed update keeps its before-image requirement scoped to the keyless
    /// path — the accelerator addresses the row by key, so no before-image is
    /// needed.
    #[test]
    fn a_keyed_update_does_not_require_a_before_image() {
        let change = event(
            "u",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "new"}),
        );

        let batch = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect("change batch");
        assert_eq!(batch.record.num_rows(), 1);
        assert_eq!(batch.op(0).to_string(), "u");
    }

    #[test]
    fn a_snapshot_read_row_uses_the_after_image() {
        let change = event(
            "r",
            serde_json::Value::Null,
            serde_json::json!({"id": 5, "name": "snapshot"}),
        );

        let batch = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect("change batch");

        assert_eq!(batch.op(0).to_string(), "r");
        assert_eq!(id_of(&batch.data(0)), Some(5));
    }

    #[test]
    fn every_row_carries_the_declared_primary_key_names() {
        let change = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "a"}),
        );
        let keys = vec!["id".to_string(), "name".to_string()];

        let batch = to_change_batch(&orders_schema(), &keys, &change, None).expect("change batch");

        assert!(batch.has_primary_keys(0));
        assert_eq!(batch.primary_keys(0), keys);
    }

    /// A keyless row must report *no* primary keys, so the write path routes it
    /// down the keyless (full-row match) branch instead of keying on nothing.
    #[test]
    fn a_keyless_row_reports_no_primary_keys() {
        let change = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "a"}),
        );

        let batch = to_change_batch(&orders_schema(), &[], &change, None).expect("change batch");

        assert!(!batch.has_primary_keys(0));
        assert!(batch.primary_keys(0).is_empty());
    }

    /// CDC is order-sensitive: a create followed by a delete of the same key
    /// must not be reordered, or the row survives a deletion it should not.
    #[test]
    fn a_vector_of_events_keeps_its_order() {
        let schema = orders_schema();
        let keys = vec!["id".to_string()];
        let create = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "a"}),
        );
        let delete = event(
            "d",
            serde_json::json!({"id": 1, "name": "a"}),
            serde_json::Value::Null,
        );
        let recreate = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "b"}),
        );

        let batch = vector_to_change_batch(&schema, &keys, &[&create, &delete, &recreate], None)
            .expect("change batch");

        assert_eq!(batch.record.num_rows(), 3);
        let ops: Vec<String> = (0..3).map(|row| batch.op(row).to_string()).collect();
        assert_eq!(ops, vec!["c", "d", "c"]);
        assert_eq!(name_of(&batch.data(0)), Some("a".to_string()));
        assert_eq!(name_of(&batch.data(2)), Some("b".to_string()));
    }

    /// The keyless-update expansion also applies mid-vector, so a batch of N
    /// events can emit more than N rows.
    #[test]
    fn a_keyless_update_expands_to_two_rows_inside_a_vector() {
        let schema = orders_schema();
        let create = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": "a"}),
        );
        let update = event(
            "u",
            serde_json::json!({"id": 1, "name": "a"}),
            serde_json::json!({"id": 1, "name": "b"}),
        );

        let batch =
            vector_to_change_batch(&schema, &[], &[&create, &update], None).expect("change batch");

        assert_eq!(batch.record.num_rows(), 3, "create + (delete, create)");
        let ops: Vec<String> = (0..3).map(|row| batch.op(row).to_string()).collect();
        assert_eq!(ops, vec!["c", "d", "c"]);
    }

    /// A column absent from the change event is filled with null rather than
    /// failing the batch — the source may add columns before the dataset schema
    /// is updated.
    #[test]
    fn a_column_missing_from_the_event_becomes_null() {
        let change = event("c", serde_json::Value::Null, serde_json::json!({"id": 1}));

        let batch = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect("change batch");

        let data = batch.data(0);
        assert_eq!(id_of(&data), Some(1));
        assert_eq!(name_of(&data), None, "absent column is null, not an error");
    }

    /// An explicit JSON null in the event becomes an Arrow null, preserving SQL
    /// NULL semantics through the CDC path.
    #[test]
    fn an_explicit_json_null_becomes_an_arrow_null() {
        let change = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"id": 1, "name": null}),
        );

        let batch = to_change_batch(&orders_schema(), &["id".to_string()], &change, None)
            .expect("change batch");

        assert_eq!(name_of(&batch.data(0)), None);
    }

    /// A Debezium envelope for one row whose embedded row schema is
    /// `row_fields`, as a JSON-converter event carries it.
    fn apps_event(row_fields: &serde_json::Value, after: &serde_json::Value) -> ChangeEvent {
        let value = serde_json::json!({
            "schema": {"type": "struct", "optional": false, "fields": [
                {"field": "before", "type": "struct", "optional": true, "fields": row_fields},
                {"field": "after", "type": "struct", "optional": true, "fields": row_fields},
            ]},
            "payload": {"before": null, "after": after, "op": "u", "ts_ms": 0},
        });
        serde_json::from_value(value).expect("valid change event")
    }

    /// The table schema and declared defaults the connector adopts from the
    /// newest event.
    fn adopted(event: &ChangeEvent) -> (SchemaRef, crate::debezium::arrow::ColumnDefaults) {
        let row_fields: Vec<crate::debezium::change_event::Field> = event
            .get_schema_fields()
            .expect("row fields")
            .into_iter()
            .cloned()
            .collect();
        let schema = Arc::new(
            crate::debezium::arrow::convert_fields_to_arrow_schema(row_fields.iter().collect())
                .expect("schema"),
        );
        (schema, crate::debezium::arrow::column_defaults(&row_fields))
    }

    /// `ALTER TABLE apps ADD COLUMN flag boolean NOT NULL DEFAULT false`
    /// rewrites no row, so a row unchanged since keeps its older change event,
    /// which has no `flag`. The table schema comes from the newest event, where
    /// `flag` is required and Debezium declares its default. Replaying the
    /// topic must give the old row the default, as Postgres does, instead of
    /// failing the batch that holds it.
    #[test]
    fn an_event_from_before_a_not_null_column_was_added_takes_its_default() {
        let old_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
            {"type": "string", "optional": true, "field": "name"},
        ]);
        let new_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
            {"type": "string", "optional": true, "field": "name"},
            {"type": "boolean", "optional": false, "default": false, "field": "flag"},
        ]);
        let old = apps_event(&old_fields, &serde_json::json!({"id": 1, "name": "old"}));
        let new = apps_event(
            &new_fields,
            &serde_json::json!({"id": 2, "name": "new", "flag": true}),
        );
        let (table_schema, defaults) = adopted(&new);
        assert!(
            !table_schema
                .field_with_name("flag")
                .expect("flag")
                .is_nullable(),
            "a NOT NULL column is a required Arrow field"
        );

        let batch = vector_to_change_batch_with_defaults(
            &table_schema,
            &["id".to_string()],
            &[&old, &new],
            None,
            &defaults,
        )
        .expect("the older event decodes with the column's default");

        let flags: Vec<bool> = (0..2)
            .map(|row| {
                batch
                    .data(row)
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .expect("flag is Boolean")
                    .value(0)
            })
            .collect();
        assert_eq!(flags, vec![false, true]);
    }

    /// The chunk-at-a-time helper the `release/2.4` changes stream used: with
    /// the default fix alone, one undecodable event still fails the whole poll
    /// chunk, so the old event (which now has a default) and the current event
    /// in the same chunk are dropped with it. The Kafka changes stream decodes
    /// per event instead (see `kafka::tests::replay_applies_old_event_with_default_and_skips_only_the_bad_event`).
    #[test]
    fn one_bad_event_fails_the_whole_chunk_even_with_defaults() {
        let old_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
        ]);
        let new_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
            {"type": "boolean", "optional": false, "default": false, "field": "flag"},
        ]);
        let old = apps_event(&old_fields, &serde_json::json!({"id": 1}));
        let bad = apps_event(&new_fields, &serde_json::json!({"flag": true}));
        let new = apps_event(&new_fields, &serde_json::json!({"id": 3, "flag": true}));
        let (table_schema, defaults) = adopted(&new);
        let pk = ["id".to_string()];

        let err = vector_to_change_batch_with_defaults(
            &table_schema,
            &pk,
            &[&old, &bad, &new],
            None,
            &defaults,
        )
        .expect_err("one bad event fails the whole vector");
        assert_eq!(err.kind(), "missing_required_field");

        let mut ids = Vec::new();
        for change in [&old, &bad, &new] {
            if let Ok(batch) =
                vector_to_change_batch_with_defaults(&table_schema, &pk, &[change], None, &defaults)
            {
                ids.push(
                    batch
                        .data(0)
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("id is Int64")
                        .value(0),
                );
            }
        }
        assert_eq!(
            ids,
            vec![1, 3],
            "decoded one at a time, only the bad event is lost"
        );
    }

    /// Without a declared default, a required column an event lacks is still
    /// an error: there is no value to give it.
    #[test]
    fn a_required_column_without_a_default_is_still_an_error() {
        let old_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
        ]);
        let new_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
            {"type": "boolean", "optional": false, "field": "flag"},
        ]);
        let old = apps_event(&old_fields, &serde_json::json!({"id": 1}));
        let new = apps_event(&new_fields, &serde_json::json!({"id": 2, "flag": true}));
        let (table_schema, defaults) = adopted(&new);
        assert!(defaults.is_empty());

        let err = vector_to_change_batch_with_defaults(
            &table_schema,
            &["id".to_string()],
            &[&old, &new],
            None,
            &defaults,
        )
        .expect_err("no default to fill a required column");
        assert!(
            matches!(err, Error::MissingFieldInValue { ref field_name, .. } if field_name == "flag"),
            "got {err:?}"
        );
    }

    /// A nullable column an older event lacks stays null even when Debezium
    /// declares a default, as before.
    #[test]
    fn a_nullable_column_missing_from_an_older_event_stays_null() {
        let old_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
        ]);
        let new_fields = serde_json::json!([
            {"type": "int64", "optional": false, "field": "id"},
            {"type": "int32", "optional": true, "default": 5, "field": "n"},
        ]);
        let old = apps_event(&old_fields, &serde_json::json!({"id": 1}));
        let new = apps_event(&new_fields, &serde_json::json!({"id": 2, "n": 7}));
        let (table_schema, defaults) = adopted(&new);

        let batch = vector_to_change_batch_with_defaults(
            &table_schema,
            &["id".to_string()],
            &[&old, &new],
            None,
            &defaults,
        )
        .expect("change batch");
        assert!(batch.data(0).column(1).is_null(0));
        assert!(!batch.data(1).column(1).is_null(0));
    }

    const SECRET: &str = "s3cr3t-token-value";

    fn required_id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]))
    }

    /// HTTP ingest still uses [`vector_to_change_batch`]: one bad event fails the
    /// whole vector. The Kafka changes stream decodes per event instead.
    #[test]
    fn vector_to_change_batch_fails_the_whole_chunk_when_one_event_is_bad() {
        let schema = required_id_schema();
        let pk = ["id".to_string()];
        let valid = event("c", serde_json::Value::Null, serde_json::json!({"id": 1}));
        let bad = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"secret": SECRET}),
        );
        let later = event("c", serde_json::Value::Null, serde_json::json!({"id": 3}));
        let err = vector_to_change_batch(&schema, &pk, &[&valid, &bad, &later], None)
            .expect_err("one bad event must fail the whole vector");
        assert_eq!(err.kind(), "missing_required_field");
    }

    #[test]
    fn decoding_each_event_keeps_valid_neighbors_of_a_bad_event() {
        let schema = required_id_schema();
        let pk = ["id".to_string()];
        let valid = event("c", serde_json::Value::Null, serde_json::json!({"id": 1}));
        let bad = event(
            "c",
            serde_json::Value::Null,
            serde_json::json!({"secret": SECRET}),
        );
        let later = event("c", serde_json::Value::Null, serde_json::json!({"id": 3}));

        let mut ids = Vec::new();
        let mut skipped_kind = None;
        for change in [&valid, &bad, &later] {
            match to_change_batch(&schema, &pk, change, None) {
                Ok(batch) => ids.push(id_of(&batch.data(0))),
                Err(err) => {
                    assert_eq!(err.kind(), "missing_required_field");
                    assert!(
                        !err.kind().contains(SECRET),
                        "kind must not carry the field value, got {}",
                        err.kind()
                    );
                    skipped_kind = Some(err.kind());
                }
            }
        }
        assert_eq!(ids, vec![Some(1), Some(3)]);
        assert_eq!(skipped_kind, Some("missing_required_field"));
    }
}
