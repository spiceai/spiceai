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

//! The primary key an acceleration was built with, recorded in its checkpoint.
//!
//! A source that reports primary-key constraints (`DynamoDB`'s key schema, for
//! one) creates the acceleration's table with that key. When the dataset later
//! registers while its source is unreachable, those constraints are not
//! available, and an accelerator built without them rejects every refresh write
//! against the existing keyed table ("Primary keys do not match"). Recording the
//! key in the checkpoint schema's metadata lets that registration rebuild the
//! same constraints without the source.
//!
//! Every checkpoint records the key, as an empty list when the acceleration has
//! none, so a checkpoint without the entry is one written before the key was
//! recorded: its key is unknown, and the dataset waits for its source once.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::TableProvider;

use arrow_tools::metadata_keys::ACCELERATION_PRIMARY_KEY_METADATA_KEY;
use search::generation::util::get_primary_keys;

/// `schema` with the primary key `accelerator` was built with recorded in its
/// metadata, as an empty list when it has none. `schema` is returned unchanged
/// when the key cannot be read, so the checkpoint does not claim a key it may lack.
#[must_use]
pub fn with_acceleration_primary_key(
    schema: SchemaRef,
    accelerator: &Arc<dyn TableProvider>,
) -> SchemaRef {
    let Ok(columns) = get_primary_keys(accelerator) else {
        return schema;
    };
    let Ok(encoded) = serde_json::to_string(&columns) else {
        return schema;
    };
    let mut metadata = schema.metadata().clone();
    metadata.insert(ACCELERATION_PRIMARY_KEY_METADATA_KEY.to_string(), encoded);
    Arc::new(schema.as_ref().clone().with_metadata(metadata))
}

/// The primary key a checkpoint schema records, as constraints over that
/// schema's columns. `None` when none is recorded, or when a recorded column is
/// no longer in the schema.
#[must_use]
pub fn acceleration_primary_key(checkpoint_schema: &Schema) -> Option<Constraints> {
    let encoded = checkpoint_schema
        .metadata()
        .get(ACCELERATION_PRIMARY_KEY_METADATA_KEY)?;
    let columns: Vec<String> = serde_json::from_str(encoded).ok()?;
    let indices = columns
        .iter()
        .map(|column| checkpoint_schema.index_of(column).ok())
        .collect::<Option<Vec<_>>>()?;
    if indices.is_empty() {
        return None;
    }
    Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
        indices,
    )]))
}

/// Whether a checkpoint schema records the acceleration's primary key (possibly
/// as having none). A checkpoint written before the key was recorded does not.
#[must_use]
pub fn records_acceleration_primary_key(checkpoint_schema: &Schema) -> bool {
    checkpoint_schema
        .metadata()
        .contains_key(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
}

/// `schema` without the recorded primary key.
///
/// The key describes the acceleration, not the data: it must not reach a
/// registered table's schema, where schema-level metadata is merged into query
/// output schemas and compared against the accelerator's stored schema.
#[must_use]
pub fn without_acceleration_primary_key(schema: SchemaRef) -> SchemaRef {
    if !schema
        .metadata()
        .contains_key(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
    {
        return schema;
    }
    let mut metadata = schema.metadata().clone();
    metadata.remove(ACCELERATION_PRIMARY_KEY_METADATA_KEY);
    Arc::new(schema.as_ref().clone().with_metadata(metadata))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::{MemTable, TableProvider};

    use super::{
        acceleration_primary_key, records_acceleration_primary_key, with_acceleration_primary_key,
        without_acceleration_primary_key,
    };
    use arrow_tools::metadata_keys::ACCELERATION_PRIMARY_KEY_METADATA_KEY;

    fn accelerator_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn accelerator(constraints: Option<Constraints>) -> Arc<dyn TableProvider> {
        let table = MemTable::try_new(accelerator_schema(), vec![vec![]]).expect("empty MemTable");
        Arc::new(match constraints {
            Some(constraints) => table.with_constraints(constraints),
            None => table,
        })
    }

    #[test]
    fn a_recorded_primary_key_round_trips_by_column_name() {
        let keyed = accelerator(Some(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![2, 0]),
        ])));
        // The checkpoint orders columns differently from the accelerator, so the key
        // must be resolved by name, not position.
        let checkpoint = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));

        let recorded = with_acceleration_primary_key(checkpoint, &keyed);

        assert_eq!(
            acceleration_primary_key(&recorded),
            Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0, 1]
            )])),
        );
    }

    #[test]
    fn an_acceleration_without_a_primary_key_records_that_it_has_none() {
        let recorded = with_acceleration_primary_key(accelerator_schema(), &accelerator(None));

        assert_eq!(
            recorded
                .metadata()
                .get(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
                .map(String::as_str),
            Some("[]")
        );
        assert!(records_acceleration_primary_key(&recorded));
        assert_eq!(acceleration_primary_key(&recorded), None);
    }

    #[test]
    fn a_checkpoint_from_before_the_key_was_recorded_is_told_apart() {
        assert!(!records_acceleration_primary_key(&accelerator_schema()));
    }

    #[test]
    fn a_recorded_column_missing_from_the_schema_yields_no_key() {
        let keyed = accelerator(Some(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![2]),
        ])));
        let recorded = with_acceleration_primary_key(accelerator_schema(), &keyed);
        let without_id = Schema::new_with_metadata(
            vec![Field::new("v", DataType::Int64, true)],
            recorded.metadata().clone(),
        );

        assert_eq!(acceleration_primary_key(&without_id), None);
    }

    #[test]
    fn the_recorded_key_is_removed_from_a_schema_a_table_registers_with() {
        let keyed = accelerator(Some(Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![2]),
        ])));
        let mut source_metadata = std::collections::HashMap::new();
        source_metadata.insert("source.owned".to_string(), "kept".to_string());
        let recorded = with_acceleration_primary_key(
            Arc::new(
                accelerator_schema()
                    .as_ref()
                    .clone()
                    .with_metadata(source_metadata),
            ),
            &keyed,
        );

        let registered = without_acceleration_primary_key(recorded);

        assert!(
            !registered
                .metadata()
                .contains_key(ACCELERATION_PRIMARY_KEY_METADATA_KEY)
        );
        assert_eq!(
            registered
                .metadata()
                .get("source.owned")
                .map(String::as_str),
            Some("kept")
        );
    }
}
