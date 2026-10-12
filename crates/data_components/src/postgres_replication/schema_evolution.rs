/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Stream-time schema reconciliation for pgoutput `Relation` messages.
//!
//! Postgres sends a table's `Relation` message before the first change to it on
//! a connection, and again before the first change after its schema changes.
//! The baseline the first one is diffed against is the column types the dataset
//! was registered from. With `on_schema_change` set to a policy
//! other than `block`, [`RelationSchemaTracker`] diffs each `Relation` against
//! the dataset's working schema:
//!
//! - new source columns are adopted — appended at the end of the working schema
//!   as nullable fields so subsequent [`super::changes::build_change_batch`]
//!   calls emit the wider data struct (the runtime apply loop enforces the
//!   per-policy evolution set);
//! - a column whose pg type OID changed to one whose Arrow mapping is a
//!   lossless widening (per [`is_widening_cast`]) is adopted in place;
//! - dropped non-nullable columns, renames, and non-widening type changes
//!   produce a clear actionable error naming the column, the change, and the
//!   recovery — replacing the internals-leaking errors from issue #10969.
//!
//! Dropped *nullable* columns are deliberately NOT an error: after restart-time
//! evolution the slot may replay pre-ALTER WAL whose `Relation` lacks the added
//! (nullable) column, which is indistinguishable from a genuine `DROP COLUMN`.
//! Those columns are null-filled by `build_change_batch` instead.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef, TimeUnit};
use arrow_tools::schema_evolution::is_widening_cast;

use super::config::SchemaEvolutionPolicy;
use super::pgoutput::Relation;
use super::{Result, SchemaMismatchSnafu};

/// Outcome of observing one `Relation` message against the working schema.
#[derive(Debug)]
pub(crate) struct RelationObservation {
    /// `true` when the working schema was widened by this observation.
    pub schema_changed: bool,
    /// Human summary of the adopted changes for logs; empty when unchanged.
    pub summary: String,
}

/// Tracks the dataset's working schema and the last-seen pg type OID per
/// column across `Relation` messages within a single replication stream.
///
/// Type-change assessment only runs when a column's OID actually changes, so
/// benign differences between this module's OID mapping and the
/// provider-inferred dataset types (e.g. timestamp units) can never
/// false-positive on an unchanged column.
pub(crate) struct RelationSchemaTracker {
    working_schema: SchemaRef,
    policy: SchemaEvolutionPolicy,
    dataset_name: String,
    /// Dataset-declared primary-key columns. Widening a key column in place
    /// would corrupt typed key encodings downstream (the same constraint guard
    /// the runtime classifier applies), so it is rejected here too.
    constraint_columns: Vec<String>,
    column_oids: HashMap<String, u32>,
    /// The baseline was seeded and no `Relation` has been diffed against it
    /// yet. That first `Relation` can describe WAL written before the source
    /// catalog the seed was read from (a resumed slot replaying changes from
    /// before an `ALTER`), so a type it reports that the working type already
    /// holds every value of is accepted rather than read as a narrowing.
    seed_pending: bool,
}

impl RelationSchemaTracker {
    pub(crate) fn new(
        working_schema: SchemaRef,
        policy: SchemaEvolutionPolicy,
        dataset_name: String,
        constraint_columns: Vec<String>,
    ) -> Self {
        Self {
            working_schema,
            policy,
            dataset_name,
            constraint_columns,
            column_oids: HashMap::new(),
            seed_pending: false,
        }
    }

    /// Seed the per-column type OID baseline with the types the dataset's
    /// schema was registered from (the source catalog at subscribe time).
    ///
    /// Without a seed, the first `Relation` is taken as the baseline. Postgres
    /// sends that message only before the first change on the table, which can
    /// come after the column's type has already changed, so the change would
    /// never be diffed and its values would be decoded as the registered type.
    #[must_use]
    ///
    /// The catalog is read after the schema was inferred, so a type change can
    /// land in between. A seeded numeric type wider than the registered one is
    /// that change, not the baseline: it is seeded as unknown (OID 0), so the
    /// first `Relation` reports it and it is adopted per the policy.
    pub(crate) fn with_column_type_oids(mut self, mut column_oids: HashMap<String, u32>) -> Self {
        for field in self.working_schema.fields() {
            if let Some(oid) = column_oids.get_mut(field.name())
                && map_pg_oid_to_arrow(*oid, -1).is_some_and(|seeded| {
                    seeded.is_numeric()
                        && field.data_type().is_numeric()
                        && is_widening_cast(field.data_type(), &seeded)
                })
            {
                *oid = 0;
            }
        }
        self.seed_pending = !column_oids.is_empty();
        self.column_oids = column_oids;
        self
    }

    /// The current working schema (dataset schema plus any adopted widening).
    pub(crate) fn working_schema(&self) -> &SchemaRef {
        &self.working_schema
    }

    /// Reconcile one `Relation` message against the working schema, adopting
    /// widening changes per the policy or returning an actionable error.
    ///
    /// Each column's OID is diffed against the baseline: the OIDs seeded by
    /// [`Self::with_column_type_oids`], then those of the previous `Relation`.
    /// With no seed, the first observation only records the baseline, since
    /// there is no prior OID to diff against.
    pub(crate) fn observe_relation(&mut self, rel: &Relation) -> Result<RelationObservation> {
        let first_observation = self.column_oids.is_empty();
        let mut incompatibilities: Vec<String> = Vec::new();
        let mut adopted: Vec<String> = Vec::new();
        let mut evolved_fields: Vec<FieldRef> = Vec::with_capacity(rel.columns.len());

        for field in self.working_schema.fields() {
            let Some(col) = rel.columns.iter().find(|c| c.name == *field.name()) else {
                match self.policy {
                    SchemaEvolutionPolicy::Fail => incompatibilities.push(format!(
                        "the column `{}` was dropped or renamed on source relation {}.{} and `on_schema_change: fail` is set",
                        field.name(),
                        rel.namespace,
                        rel.name
                    )),
                    _ if field.is_nullable() => {
                        // Pre-ALTER WAL replay after restart-time evolution, or a
                        // genuine DROP COLUMN of a nullable column — both null-fill.
                        tracing::warn!(
                            dataset = %self.dataset_name,
                            column = %field.name(),
                            "column is absent from source relation {}.{} (pre-evolution WAL replay or a source DROP COLUMN); its values will be NULL until the source sends it again",
                            rel.namespace,
                            rel.name
                        );
                    }
                    _ => incompatibilities.push(format!(
                        "the non-nullable column `{}` is missing from source relation {}.{} (dropped or renamed). To recover: re-add the column on the source, or remove the dataset's acceleration data and restart so the dataset re-registers with the new schema",
                        field.name(),
                        rel.namespace,
                        rel.name
                    )),
                }
                evolved_fields.push(Arc::clone(field));
                continue;
            };

            let previous_oid = self.column_oids.get(field.name()).copied();
            if first_observation || previous_oid == Some(col.type_oid) {
                evolved_fields.push(Arc::clone(field));
                continue;
            }

            // WAL older than the seeded catalog: its values are widened into the
            // working type on decode.
            if self.seed_pending
                && map_pg_oid_to_arrow(col.type_oid, col.type_modifier)
                    .is_some_and(|mapped| is_widening_cast(&mapped, field.data_type()))
            {
                evolved_fields.push(Arc::clone(field));
                continue;
            }

            if self.policy == SchemaEvolutionPolicy::Fail {
                incompatibilities.push(format!(
                    "the type of column `{}` changed on source relation {}.{} (pg type oid {} -> {}) and `on_schema_change: fail` is set",
                    field.name(),
                    rel.namespace,
                    rel.name,
                    previous_oid.unwrap_or_default(),
                    col.type_oid
                ));
                evolved_fields.push(Arc::clone(field));
                continue;
            }

            let Some(mapped) = map_pg_oid_to_arrow(col.type_oid, col.type_modifier) else {
                tracing::warn!(
                    dataset = %self.dataset_name,
                    column = %field.name(),
                    type_oid = col.type_oid,
                    "column changed to a pg type with no known Arrow mapping; keeping the current type `{}` — values that no longer parse will error",
                    field.data_type()
                );
                evolved_fields.push(Arc::clone(field));
                continue;
            };

            if mapped == *field.data_type() {
                // OID alias change (e.g. varchar -> text) — same Arrow type.
                evolved_fields.push(Arc::clone(field));
            } else if is_widening_cast(field.data_type(), &mapped) {
                if self.constraint_columns.iter().any(|c| c == field.name()) {
                    incompatibilities.push(format!(
                        "the column `{}` is part of the dataset's primary key and cannot be widened from `{}` to `{mapped}` in place. To recover: revert the source column type, or remove the dataset's acceleration data and restart so the dataset re-registers with the new schema",
                        field.name(),
                        field.data_type()
                    ));
                    evolved_fields.push(Arc::clone(field));
                    continue;
                }
                adopted.push(format!(
                    "widened `{}`: {} -> {mapped}",
                    field.name(),
                    field.data_type()
                ));
                evolved_fields.push(Arc::new(field.as_ref().clone().with_data_type(mapped)));
            } else {
                incompatibilities.push(format!(
                    "the type of column `{}` changed from `{}` to `{mapped}` on source relation {}.{}, which is not a lossless widening. To recover: revert the source column type, or remove the dataset's acceleration data and restart so the dataset re-registers with the new schema",
                    field.name(),
                    field.data_type(),
                    rel.namespace,
                    rel.name
                ));
                evolved_fields.push(Arc::clone(field));
            }
        }

        // Added-column adoption only applies to columns absent from the baseline
        // (the seeded OIDs, or the previous Relation message). Relation columns
        // absent from the dataset schema but present in the baseline may be a
        // deliberate user subset, and are ignored.
        for col in &rel.columns {
            if first_observation {
                break;
            }
            if self.column_oids.contains_key(&col.name)
                || self
                    .working_schema
                    .fields()
                    .iter()
                    .any(|f| f.name() == &col.name)
            {
                continue;
            }
            if self.policy == SchemaEvolutionPolicy::Fail {
                incompatibilities.push(format!(
                    "the column `{}` was added on source relation {}.{} and `on_schema_change: fail` is set",
                    col.name, rel.namespace, rel.name
                ));
                continue;
            }
            let Some(mapped) = map_pg_oid_to_arrow(col.type_oid, col.type_modifier) else {
                tracing::warn!(
                    dataset = %self.dataset_name,
                    column = %col.name,
                    type_oid = col.type_oid,
                    "new source column has a pg type with no known Arrow mapping; the column is NOT adopted and its values are dropped"
                );
                continue;
            };
            adopted.push(format!("added `{}` ({mapped})", col.name));
            // pgoutput Relation messages carry no nullability — adopt as
            // nullable, which is also what the evolution classifier requires
            // so existing rows can be NULL-backfilled.
            evolved_fields.push(Arc::new(arrow::datatypes::Field::new(
                col.name.clone(),
                mapped,
                true,
            )));
        }

        if !incompatibilities.is_empty() {
            return SchemaMismatchSnafu {
                message: incompatibilities.join(". "),
            }
            .fail();
        }

        self.seed_pending = false;
        self.column_oids = rel
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.type_oid))
            .collect();

        if adopted.is_empty() {
            return Ok(RelationObservation {
                schema_changed: false,
                summary: String::new(),
            });
        }

        self.working_schema = Arc::new(Schema::new_with_metadata(
            evolved_fields,
            self.working_schema.metadata().clone(),
        ));
        Ok(RelationObservation {
            schema_changed: true,
            summary: adopted.join(", "),
        })
    }
}

/// Map a pg type OID (+ type modifier) to the Arrow type the dataset would
/// have inferred for it, or `None` when no safe mapping is known.
///
/// Aligned with `datafusion-table-providers`' `pg_data_type_to_arrow_type` so
/// stream-time adoption and restart-time (provider-inferred) classification
/// agree on the evolved type — a mismatch would re-classify on restart.
pub(crate) fn map_pg_oid_to_arrow(type_oid: u32, type_modifier: i32) -> Option<DataType> {
    Some(match type_oid {
        16 => DataType::Boolean,
        // "char" (single byte) maps to Int8, matching the provider.
        18 => DataType::Int8,
        20 => DataType::Int64,
        21 => DataType::Int16,
        23 => DataType::Int32,
        26 => DataType::UInt32,
        700 => DataType::Float32,
        701 => DataType::Float64,
        // text, name, bpchar, varchar, uuid, json, jsonb, xml, cidr, inet,
        // macaddr. The provider maps `json` and `jsonb` alike, and the WAL
        // decoder reads a `jsonb` value's text past its version byte, so the
        // two agree on `Utf8` here as well.
        25 | 19 | 1042 | 1043 | 2950 | 114 | 3802 | 142 | 650 | 869 | 829 => DataType::Utf8,
        17 => DataType::Binary,
        1082 => DataType::Date32,
        1083 => DataType::Time64(TimeUnit::Nanosecond),
        1114 => DataType::Timestamp(TimeUnit::Nanosecond, None),
        1184 => DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        1700 => {
            // numeric typmod packs ((precision << 16) | scale) + VARHDRSZ (4).
            // typmod -1 means unconstrained numeric — precision/scale unknown,
            // so no safe Decimal mapping exists.
            if type_modifier < 4 {
                return None;
            }
            let packed = type_modifier - 4;
            let precision = u8::try_from((packed >> 16) & 0xFFFF).ok()?;
            let scale = i8::try_from(packed & 0xFFFF).ok()?;
            DataType::Decimal128(precision, scale)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::super::pgoutput::Column as PgColumn;
    use super::*;
    use arrow::datatypes::Field;

    fn relation(columns: Vec<(&str, u32)>) -> Relation {
        Relation {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: b'd',
            columns: columns
                .into_iter()
                .map(|(name, oid)| PgColumn {
                    is_key: name == "id",
                    name: name.to_string(),
                    type_oid: oid,
                    type_modifier: -1,
                })
                .collect(),
        }
    }

    fn base_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn tracker(policy: SchemaEvolutionPolicy) -> RelationSchemaTracker {
        RelationSchemaTracker::new(
            base_schema(),
            policy,
            "users".to_string(),
            vec!["id".to_string()],
        )
    }

    fn tracker_with_schema(
        schema: SchemaRef,
        policy: SchemaEvolutionPolicy,
    ) -> RelationSchemaTracker {
        RelationSchemaTracker::new(schema, policy, "users".to_string(), vec!["id".to_string()])
    }

    #[test]
    fn first_observation_records_baseline_without_change() {
        let mut t = tracker(SchemaEvolutionPolicy::SyncAllColumns);
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("observe");
        assert!(!obs.schema_changed);
        assert_eq!(t.working_schema().fields().len(), 2);
    }

    /// Regression test for #14718: seeded with the registered column types, the
    /// first `Relation` is diffed rather than taken as the baseline, so a type
    /// change made before the stream's first change is still adopted.
    #[test]
    fn seeded_baseline_adopts_a_widening_in_the_first_relation() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Int32, true),
            ])),
            SchemaEvolutionPolicy::SyncAllColumns,
        )
        .with_column_type_oids(HashMap::from([
            ("id".to_string(), 23),
            ("k".to_string(), 23),
        ]));
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("k", 701)]))
            .expect("int4 -> float8 widens");
        assert!(obs.schema_changed);
        assert_eq!(obs.summary, "widened `k`: Int32 -> Float64");
        assert_eq!(t.working_schema().field(1).data_type(), &DataType::Float64);
    }

    /// A resumed slot can replay WAL from before the `ALTER` the seed already
    /// reflects: the first `Relation` then reports the older, narrower type,
    /// which the working type holds, so it is not a narrowing. Only that first
    /// `Relation` is read this way; a later narrowing is still an error.
    #[test]
    fn seeded_baseline_accepts_older_narrower_wal_only_first() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Float64, true),
            ])),
            SchemaEvolutionPolicy::SyncAllColumns,
        )
        .with_column_type_oids(HashMap::from([
            ("id".to_string(), 23),
            ("k".to_string(), 701),
        ]));
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("k", 23)]))
            .expect("replayed int4 relation fits the Float64 working type");
        assert!(!obs.schema_changed);
        assert_eq!(t.working_schema().field(1).data_type(), &DataType::Float64);
        t.observe_relation(&relation(vec![("id", 23), ("k", 701)]))
            .expect("the post-ALTER relation matches the working type");
        let msg = t
            .observe_relation(&relation(vec![("id", 23), ("k", 23)]))
            .expect_err("a later narrowing is an error")
            .to_string();
        assert!(msg.contains("not a lossless widening"), "{msg}");
    }

    /// The catalog can already reflect a widening the registered schema predates
    /// (an `ALTER` between schema inference and the catalog read). The first
    /// `Relation`, which reports the same type, is then still adopted.
    #[test]
    fn seeded_type_wider_than_registered_is_adopted_from_the_first_relation() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Int32, true),
            ])),
            SchemaEvolutionPolicy::SyncAllColumns,
        )
        .with_column_type_oids(HashMap::from([
            ("id".to_string(), 23),
            ("k".to_string(), 701),
        ]));
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("k", 701)]))
            .expect("float8 widens Int32");
        assert!(obs.schema_changed);
        assert_eq!(obs.summary, "widened `k`: Int32 -> Float64");
    }

    /// A seeded column the dataset does not project is not "added" when the
    /// first `Relation` carries it: the dataset deliberately left it out.
    #[test]
    fn seeded_baseline_does_not_adopt_an_unprojected_column() {
        let mut t = tracker(SchemaEvolutionPolicy::AppendNewColumns).with_column_type_oids(
            HashMap::from([
                ("id".to_string(), 23),
                ("name".to_string(), 25),
                ("secret".to_string(), 25),
            ]),
        );
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("name", 25), ("secret", 25)]))
            .expect("observe");
        assert!(!obs.schema_changed, "{}", obs.summary);
        assert_eq!(t.working_schema().fields().len(), 2);
    }

    /// Under `fail`, a type change already present in the first `Relation` fails.
    #[test]
    fn seeded_baseline_fails_a_type_change_under_fail() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Int32, true),
            ])),
            SchemaEvolutionPolicy::Fail,
        )
        .with_column_type_oids(HashMap::from([
            ("id".to_string(), 23),
            ("k".to_string(), 23),
        ]));
        let msg = t
            .observe_relation(&relation(vec![("id", 23), ("k", 701)]))
            .expect_err("fail rejects the type change")
            .to_string();
        assert!(
            msg.contains("the type of column `k` changed on source relation public.users (pg type oid 23 -> 701) and `on_schema_change: fail` is set"),
            "{msg}"
        );
    }

    #[test]
    fn new_column_is_adopted_appended_nullable() {
        let mut t = tracker(SchemaEvolutionPolicy::AppendNewColumns);
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("name", 25), ("age", 23)]))
            .expect("observe with added column");
        assert!(obs.schema_changed);
        assert!(obs.summary.contains("added `age`"), "{}", obs.summary);

        let schema = t.working_schema();
        assert_eq!(schema.fields().len(), 3);
        let added = schema.field(2);
        assert_eq!(added.name(), "age", "new column must append at the END");
        assert_eq!(added.data_type(), &DataType::Int32);
        assert!(added.is_nullable(), "adopted columns must be nullable");
    }

    #[test]
    fn oid_widening_int4_to_int8_is_adopted() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("qty", DataType::Int32, true),
            ])),
            SchemaEvolutionPolicy::SyncAllColumns,
        );
        t.observe_relation(&relation(vec![("id", 23), ("qty", 23)]))
            .expect("baseline");
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("qty", 20)]))
            .expect("widen int4 -> int8");
        assert!(obs.schema_changed);
        assert!(obs.summary.contains("widened `qty`"), "{}", obs.summary);
        assert_eq!(
            t.working_schema().field(1).data_type(),
            &DataType::Int64,
            "qty must widen in place to Int64"
        );
    }

    #[test]
    fn oid_narrowing_is_actionable_error() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("qty", DataType::Int64, true),
            ])),
            SchemaEvolutionPolicy::SyncAllColumns,
        );
        t.observe_relation(&relation(vec![("id", 23), ("qty", 20)]))
            .expect("baseline");
        let err = t
            .observe_relation(&relation(vec![("id", 23), ("qty", 23)]))
            .expect_err("int8 -> int4 must error");
        let msg = err.to_string();
        assert!(msg.contains("`qty`"), "must name the column: {msg}");
        assert!(
            msg.contains("not a lossless widening"),
            "must name the change: {msg}"
        );
        assert!(msg.contains("To recover"), "must name the recovery: {msg}");
    }

    #[test]
    fn unrelated_type_change_is_actionable_error() {
        let mut t = tracker(SchemaEvolutionPolicy::SyncAllColumns);
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        // name: text (25) -> int4 (23)
        let err = t
            .observe_relation(&relation(vec![("id", 23), ("name", 23)]))
            .expect_err("text -> int must error");
        let msg = err.to_string();
        assert!(msg.contains("`name`"), "must name the column: {msg}");
        assert!(msg.contains("Utf8") && msg.contains("Int32"), "{msg}");
    }

    #[test]
    fn dropped_nullable_column_null_fills_without_error() {
        let mut t = tracker(SchemaEvolutionPolicy::AppendNewColumns);
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        // Pre-ALTER replay: relation without the nullable `name` column.
        let obs = t
            .observe_relation(&relation(vec![("id", 23)]))
            .expect("nullable absent column must not error");
        assert!(!obs.schema_changed);
        assert_eq!(
            t.working_schema().fields().len(),
            2,
            "the working schema keeps the column for null-fill"
        );
    }

    #[test]
    fn primary_key_widening_is_rejected() {
        let mut t = tracker(SchemaEvolutionPolicy::SyncAllColumns);
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        // id (the declared PK): int4 -> int8 is a widening, but key columns
        // must not widen in place.
        let err = t
            .observe_relation(&relation(vec![("id", 20), ("name", 25)]))
            .expect_err("PK widening must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("`id`"), "must name the column: {msg}");
        assert!(
            msg.contains("primary key"),
            "must name the constraint: {msg}"
        );
    }

    #[test]
    fn dropped_non_nullable_column_is_actionable_error() {
        let mut t = tracker_with_schema(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            SchemaEvolutionPolicy::SyncAllColumns,
        );
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        let err = t
            .observe_relation(&relation(vec![("id", 23)]))
            .expect_err("non-nullable absent column must error");
        let msg = err.to_string();
        assert!(msg.contains("`name`"), "must name the column: {msg}");
        assert!(
            msg.contains("dropped or renamed"),
            "must name the change: {msg}"
        );
        assert!(msg.contains("To recover"), "must name the recovery: {msg}");
    }

    #[test]
    fn fail_policy_errors_on_any_change() {
        let mut t = tracker(SchemaEvolutionPolicy::Fail);
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        let err = t
            .observe_relation(&relation(vec![("id", 23), ("name", 25), ("age", 23)]))
            .expect_err("fail policy must error on an added column");
        assert!(
            err.to_string().contains("on_schema_change: fail"),
            "must name the policy: {err}"
        );
    }

    #[test]
    fn unmappable_added_column_is_skipped_not_adopted() {
        let mut t = tracker(SchemaEvolutionPolicy::SyncAllColumns);
        t.observe_relation(&relation(vec![("id", 23), ("name", 25)]))
            .expect("baseline");
        // OID 600 = point — no mapping in map_pg_oid_to_arrow.
        let obs = t
            .observe_relation(&relation(vec![("id", 23), ("name", 25), ("loc", 600)]))
            .expect("unmappable added column must not error");
        assert!(!obs.schema_changed);
        assert_eq!(t.working_schema().fields().len(), 2);
    }

    #[test]
    fn numeric_typmod_maps_to_decimal128() {
        // numeric(10, 2): typmod = ((10 << 16) | 2) + 4
        let typmod = ((10 << 16) | 2) + 4;
        assert_eq!(
            map_pg_oid_to_arrow(1700, typmod),
            Some(DataType::Decimal128(10, 2))
        );
        // Unconstrained numeric has no safe mapping.
        assert_eq!(map_pg_oid_to_arrow(1700, -1), None);
    }

    #[test]
    fn oid_mapping_matches_provider_for_core_types() {
        assert_eq!(map_pg_oid_to_arrow(16, -1), Some(DataType::Boolean));
        assert_eq!(map_pg_oid_to_arrow(21, -1), Some(DataType::Int16));
        assert_eq!(map_pg_oid_to_arrow(23, -1), Some(DataType::Int32));
        assert_eq!(map_pg_oid_to_arrow(20, -1), Some(DataType::Int64));
        assert_eq!(map_pg_oid_to_arrow(700, -1), Some(DataType::Float32));
        assert_eq!(map_pg_oid_to_arrow(701, -1), Some(DataType::Float64));
        assert_eq!(map_pg_oid_to_arrow(25, -1), Some(DataType::Utf8));
        assert_eq!(map_pg_oid_to_arrow(114, -1), Some(DataType::Utf8));
        assert_eq!(map_pg_oid_to_arrow(3802, -1), Some(DataType::Utf8));
        assert_eq!(map_pg_oid_to_arrow(17, -1), Some(DataType::Binary));
        assert_eq!(map_pg_oid_to_arrow(1082, -1), Some(DataType::Date32));
        assert_eq!(
            map_pg_oid_to_arrow(1114, -1),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
        assert_eq!(
            map_pg_oid_to_arrow(1184, -1),
            Some(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from("UTC"))
            ))
        );
        assert_eq!(map_pg_oid_to_arrow(600, -1), None);
    }
}
