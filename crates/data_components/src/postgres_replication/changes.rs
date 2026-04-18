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

//! Turn a stream of per-transaction `DecodedMessage`s into Arrow
//! [`crate::cdc::ChangeBatch`]es that the existing refresh loop knows how to
//! apply.

use std::sync::{Arc, atomic::AtomicU64};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, LargeStringBuilder, ListArray, RecordBatch, StringArray,
        StringBuilder, StructArray, TimestampMicrosecondBuilder,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, SchemaRef, TimeUnit},
};
use async_trait::async_trait;

use super::pgoutput::{Relation, TupleData, Value};
use super::{PgOutputDecodeSnafu, Result};
use crate::cdc::{ChangeBatch, ChangeEnvelope, CommitChange, CommitError, changes_schema};

/// One logical change derived from a pgoutput message.
#[derive(Debug, Clone)]
pub struct DecodedChange {
    pub op: ChangeOp,
    pub primary_keys: Vec<String>,
    pub row: TupleData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Create,
    Update,
    Delete,
    Truncate,
}

impl ChangeOp {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "c",
            Self::Update => "u",
            Self::Delete => "d",
            Self::Truncate => "t",
        }
    }
}

/// Buffer collecting `DecodedChange`s within a single transaction.
pub struct TransactionBuffer {
    pub begin_lsn: u64,
    pub changes: Vec<DecodedChange>,
}

impl TransactionBuffer {
    #[must_use]
    pub fn new(begin_lsn: u64) -> Self {
        Self {
            begin_lsn,
            changes: Vec::new(),
        }
    }

    pub fn push_insert(&mut self, relation: &Relation, tuple: TupleData) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Create,
            primary_keys: relation
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect(),
            row: tuple,
        });
    }

    pub fn push_update(&mut self, relation: &Relation, new: TupleData) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Update,
            primary_keys: relation
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect(),
            row: new,
        });
    }

    pub fn push_delete(&mut self, relation: &Relation, old: TupleData) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Delete,
            primary_keys: relation
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect(),
            row: old,
        });
    }

    /// Record a TRUNCATE for the relation. Row payload is empty — the
    /// accelerator path applies it as an unconditional delete-all.
    pub fn push_truncate(&mut self, relation: &Relation) {
        self.changes.push(DecodedChange {
            op: ChangeOp::Truncate,
            primary_keys: relation
                .columns
                .iter()
                .filter(|c| c.is_key)
                .map(|c| c.name.clone())
                .collect(),
            row: TupleData { columns: vec![] },
        });
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// Build a `ChangeBatch` from a list of decoded changes, typing the `data`
/// struct to the accelerator's Arrow schema.
pub fn build_change_batch(
    dataset_schema: &SchemaRef,
    relation: &Relation,
    changes: &[DecodedChange],
) -> Result<ChangeBatch> {
    let num_rows = changes.len();
    let wrapper_schema = changes_schema(dataset_schema);

    let mut op_builder = StringBuilder::with_capacity(num_rows, num_rows * 2);
    let mut pk_offsets = Vec::<i32>::with_capacity(num_rows + 1);
    pk_offsets.push(0);
    let mut pk_values: Vec<String> = Vec::new();

    // One builder per output field, typed from dataset schema.
    let mut data_builders: Vec<FieldBuilder> = dataset_schema
        .fields()
        .iter()
        .map(|f| FieldBuilder::new(f.data_type()))
        .collect::<Result<Vec<_>>>()?;

    // Precompute dataset_field_idx → relation_column_idx once per batch so the
    // hot path is O(rows × fields) rather than O(rows × fields²).
    let column_map: Vec<usize> = dataset_schema
        .fields()
        .iter()
        .map(|field| {
            relation
                .columns
                .iter()
                .position(|c| c.name == *field.name())
                .ok_or_else(|| super::Error::SchemaMismatch {
                    message: format!(
                        "dataset column {} not present in source relation {}.{}",
                        field.name(),
                        relation.namespace,
                        relation.name
                    ),
                })
        })
        .collect::<Result<Vec<_>>>()?;

    for change in changes {
        op_builder.append_value(change.op.as_str());
        for pk in &change.primary_keys {
            pk_values.push(pk.clone());
        }
        pk_offsets.push(i32::try_from(pk_values.len()).map_err(|e| {
            super::Error::PgOutputDecode {
                message: format!("too many primary keys: {e}"),
            }
        })?);

        for (col_idx, &source_idx) in column_map.iter().enumerate() {
            let value = change.row.columns.get(source_idx).unwrap_or(&None);
            data_builders[col_idx].append(value, change.op)?;
        }
    }

    let op_array: ArrayRef = Arc::new(op_builder.finish());
    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let pk_list = ListArray::new(
        Arc::clone(&pk_field),
        OffsetBuffer::new(pk_offsets.into()),
        Arc::new(StringArray::from(pk_values)),
        None,
    );

    let data_columns: Vec<ArrayRef> = data_builders
        .into_iter()
        .map(FieldBuilder::finish)
        .collect();
    let data_struct = StructArray::new(dataset_schema.fields().clone(), data_columns, None);

    let record = RecordBatch::try_new(
        Arc::new(wrapper_schema),
        vec![op_array, Arc::new(pk_list), Arc::new(data_struct)],
    )
    .map_err(|e| super::Error::SchemaMismatch {
        message: format!("failed to build change record batch: {e}"),
    })?;

    ChangeBatch::try_new(record).map_err(|e| super::Error::SchemaMismatch {
        message: format!("change batch validation failed: {e}"),
    })
}

/// Wrap a batch into a `ChangeEnvelope` whose `commit()` advances the
/// shared confirmed-flush LSN atomic.
#[must_use]
pub fn envelope_with_lsn(
    batch: ChangeBatch,
    confirmed_flush: Arc<AtomicU64>,
    flush_to: u64,
    is_dataset_ready: bool,
) -> ChangeEnvelope {
    ChangeEnvelope::new(
        Box::new(LsnCommitter {
            confirmed_flush,
            flush_to,
        }),
        batch,
        is_dataset_ready,
    )
}

/// `CommitChange` impl that monotonically advances a shared LSN atomic.
/// The replication client's keepalive task periodically reads this atomic and
/// forwards it to Postgres as a `StandbyStatusUpdate`.
struct LsnCommitter {
    confirmed_flush: Arc<AtomicU64>,
    flush_to: u64,
}

#[async_trait]
impl CommitChange for LsnCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        use std::sync::atomic::Ordering;
        // Monotonic CAS loop: only advance; never regress.
        let mut current = self.confirmed_flush.load(Ordering::Relaxed);
        loop {
            if self.flush_to <= current {
                return Ok(());
            }
            match self.confirmed_flush.compare_exchange(
                current,
                self.flush_to,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }
}

/// Per-field Arrow builder that accepts `Option<&Value>` (text/null/unchanged)
/// and parses strings into the appropriate typed column.
enum FieldBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    TimestampMicros(TimestampMicrosecondBuilder, Option<Arc<str>>),
}

impl FieldBuilder {
    fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::new(), tz.clone())
            }
            other => {
                return PgOutputDecodeSnafu {
                    message: format!(
                        "postgres_replication: unsupported Arrow data type in dataset schema: {other}"
                    ),
                }
                .fail();
            }
        })
    }

    fn append(&mut self, value: &Option<Value>, op: ChangeOp) -> Result<()> {
        let Some(v) = value else {
            self.append_null();
            return Ok(());
        };
        let s = match v {
            Value::Text(s) => s,
            Value::Unchanged => {
                // For UPDATE with a TOASTed column that wasn't changed, pgoutput
                // omits the value. Silently coercing to NULL would overwrite the
                // existing accelerator value — real data corruption. Fail loudly
                // so the operator sets REPLICA IDENTITY FULL or excludes the
                // column. For non-UPDATE ops this shouldn't appear.
                return PgOutputDecodeSnafu {
                    message: format!(
                        "postgres_replication: received Value::Unchanged (TOASTed column \
                         omitted) during {:?} — this would silently overwrite the \
                         accelerator value with NULL. Set `ALTER TABLE ... REPLICA IDENTITY \
                         FULL;` on the source so the old tuple is sent with every update.",
                        op
                    ),
                }
                .fail();
            }
            Value::Binary(_) => {
                // pgoutput binary-format values aren't supported yet; coercing to
                // NULL silently would be wrong.
                return PgOutputDecodeSnafu {
                    message: "postgres_replication: binary-format pgoutput values are not yet \
                              supported. Ensure the publication uses text format or upgrade \
                              the decoder."
                        .to_string(),
                }
                .fail();
            }
        };
        match self {
            Self::Utf8(b) => b.append_value(s),
            Self::LargeUtf8(b) => b.append_value(s),
            Self::Bool(b) => b.append_value(matches!(s.as_str(), "t" | "true" | "TRUE")),
            Self::Int16(b) => {
                b.append_value(s.parse::<i16>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int16 parse '{s}': {e}"),
                })?)
            }
            Self::Int32(b) => {
                b.append_value(s.parse::<i32>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int32 parse '{s}': {e}"),
                })?)
            }
            Self::Int64(b) => {
                b.append_value(s.parse::<i64>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("int64 parse '{s}': {e}"),
                })?)
            }
            Self::Float32(b) => {
                b.append_value(s.parse::<f32>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("float32 parse '{s}': {e}"),
                })?)
            }
            Self::Float64(b) => {
                b.append_value(s.parse::<f64>().map_err(|e| super::Error::PgOutputDecode {
                    message: format!("float64 parse '{s}': {e}"),
                })?)
            }
            Self::Date32(b) => {
                // Postgres text format for date: 'YYYY-MM-DD'
                let parsed = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| {
                    super::Error::PgOutputDecode {
                        message: format!("date parse '{s}': {e}"),
                    }
                })?;
                let epoch = match chrono::NaiveDate::from_ymd_opt(1970, 1, 1) {
                    Some(epoch) => epoch,
                    None => unreachable!("1970-01-01 is a valid NaiveDate"),
                };
                let days_since_epoch = (parsed - epoch).num_days();
                b.append_value(i32::try_from(days_since_epoch).map_err(|e| {
                    super::Error::PgOutputDecode {
                        message: format!("date overflow: {e}"),
                    }
                })?);
            }
            Self::TimestampMicros(b, _tz) => {
                // Postgres text format for timestamp: 'YYYY-MM-DD HH:MM:SS[.ffffff][+TZ]'
                let micros = parse_pg_timestamp_micros(s)?;
                b.append_value(micros);
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            Self::Utf8(b) => b.append_null(),
            Self::LargeUtf8(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::TimestampMicros(b, _) => b.append_null(),
        }
    }

    fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::TimestampMicros(b, tz) => {
                let arr = b.finish();
                Arc::new(match tz {
                    Some(tz) => arr.with_timezone(tz.clone()),
                    None => arr,
                })
            }
        }
    }
}

fn parse_pg_timestamp_micros(s: &str) -> Result<i64> {
    // Try with timezone, then without.
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Ok(dt.timestamp_micros());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    PgOutputDecodeSnafu {
        message: format!("timestamp parse '{s}' failed"),
    }
    .fail()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_replication::pgoutput::{Column as PgColumn, Value as PgValue};
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_relation() -> Relation {
        Relation {
            relation_id: 1,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: b'd',
            columns: vec![
                PgColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                PgColumn {
                    is_key: false,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }

    fn tuple_for(id: &str, name: Option<&str>) -> TupleData {
        TupleData {
            columns: vec![
                Some(PgValue::Text(id.to_string())),
                name.map(|n| PgValue::Text(n.to_string())),
            ],
        }
    }

    #[test]
    fn build_batch_with_insert_and_delete() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let relation = make_relation();
        let changes = vec![
            DecodedChange {
                op: ChangeOp::Create,
                primary_keys: vec!["id".into()],
                row: tuple_for("1", Some("Alice")),
            },
            DecodedChange {
                op: ChangeOp::Delete,
                primary_keys: vec!["id".into()],
                row: tuple_for("2", None),
            },
        ];
        let batch = build_change_batch(&schema, &relation, &changes).expect("build batch");
        assert_eq!(batch.record.num_rows(), 2);

        // op column
        let ops = batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>();
        assert_eq!(ops.value(0), "c");
        assert_eq!(ops.value(1), "d");

        // primary_keys list
        let pks = batch
            .record
            .column_by_name("primary_keys")
            .expect("pks")
            .as_list::<i32>();
        let first = pks.value(0);
        assert_eq!(first.as_string::<i32>().value(0), "id");

        // data struct — id column
        let data = batch.record.column_by_name("data").expect("data");
        let data = data.as_struct();
        let id_col = data
            .column_by_name("id")
            .expect("id")
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        let name_col = data
            .column_by_name("name")
            .expect("name")
            .as_string::<i32>();
        assert_eq!(name_col.value(0), "Alice");
        assert!(name_col.is_null(1));
    }

    #[tokio::test]
    async fn lsn_committer_advances_monotonically() {
        let lsn = Arc::new(AtomicU64::new(0));
        let c1 = LsnCommitter {
            confirmed_flush: Arc::clone(&lsn),
            flush_to: 100,
        };
        c1.commit().await.expect("commit");
        assert_eq!(lsn.load(std::sync::atomic::Ordering::Relaxed), 100);

        // older commit should not regress.
        let c2 = LsnCommitter {
            confirmed_flush: Arc::clone(&lsn),
            flush_to: 50,
        };
        c2.commit().await.expect("commit");
        assert_eq!(lsn.load(std::sync::atomic::Ordering::Relaxed), 100);
    }
}
