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

//! Mode A: `DataFusion` consumer baseline.
//!
//! Registers the IBM TPC-H CSVs as in-memory-backed listing tables and lowers
//! each suite plan with `datafusion-substrait::from_substrait_plan` — the same
//! consumer `spiced` uses on the `FlightSQL` path. This is a DF-fork signal, not
//! product CI.

use std::collections::HashSet;
use std::path::Path;
use std::time::Instant;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use snafu::ResultExt;

use crate::compare::{ColumnSpec, TableData, compare};
use crate::error::{self, Result};
use crate::report::{CaseResult, TestStatus};
use crate::schema::{TPCH_TABLES, schema_for};
use crate::suite::{InputTable, LoadedCase, LoadedSuite};

pub const ENGINE_NAME: &str = "DataFusion";
pub const ENGINE_VERSION: &str = "54.1";

pub struct ModeAEngine {
    ctx: SessionContext,
}

impl ModeAEngine {
    /// Register every TPC-H CSV under the Isthmus plan name (`LINEITEM`).
    ///
    /// `register_csv(&str, …)` goes through `TableReference::parse_str`, which
    /// lowercases even when `enable_ident_normalization` is off. The Substrait
    /// consumer then looks up the exact Isthmus name on a case-sensitive
    /// catalog, so we register via `TableReference::bare`.
    pub async fn with_tpch_data(data_dir: &Path) -> Result<Self> {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.enable_ident_normalization = false;
        let ctx = SessionContext::new_with_config(config);
        for table in TPCH_TABLES {
            let csv_path = data_dir.join(format!("{}.csv", table.file_stem));
            register_csv(&ctx, table.plan_name, &csv_path, table.file_stem).await?;
        }
        Ok(Self { ctx })
    }

    pub async fn run_suite(
        &self,
        suite: &LoadedSuite,
        only: Option<&str>,
    ) -> Result<Vec<CaseResult>> {
        let mut results = Vec::with_capacity(suite.cases.len());
        for case in &suite.cases {
            if let Some(filter) = only
                && !case.id.eq_ignore_ascii_case(filter)
            {
                continue;
            }
            results.push(self.run_case(case).await);
        }
        Ok(results)
    }

    async fn run_case(&self, case: &LoadedCase) -> CaseResult {
        let start = Instant::now();
        let Some(expected) = case.expected.as_ref() else {
            return CaseResult {
                test_id: case.id.clone(),
                description: case.description.clone(),
                status: TestStatus::Skipped,
                execution_time_ms: elapsed_ms(start),
                error_message: Some("No expected output — cannot verify correctness".to_string()),
            };
        };

        match self.execute(case).await {
            Ok(actual) => match compare(&actual, expected) {
                None => CaseResult {
                    test_id: case.id.clone(),
                    description: case.description.clone(),
                    status: TestStatus::Passed,
                    execution_time_ms: elapsed_ms(start),
                    error_message: None,
                },
                Some(mismatch) => CaseResult {
                    test_id: case.id.clone(),
                    description: case.description.clone(),
                    status: TestStatus::Failed,
                    execution_time_ms: elapsed_ms(start),
                    error_message: Some(mismatch.to_string()),
                },
            },
            Err(err) => CaseResult {
                test_id: case.id.clone(),
                description: case.description.clone(),
                status: TestStatus::Error,
                execution_time_ms: elapsed_ms(start),
                error_message: Some(err),
            },
        }
    }

    async fn execute(&self, case: &LoadedCase) -> std::result::Result<TableData, String> {
        ensure_inputs_registered(case)?;

        let proto = Plan::decode(case.plan_bytes.as_slice()).map_err(|e| {
            format!(
                "Failed to decode Substrait plan {}: {e}",
                case.plan_path.display()
            )
        })?;

        let state = self.ctx.state();
        let logical_plan = from_substrait_plan(&state, &proto)
            .await
            .map_err(|e| format!("from_substrait_plan: {e}"))?;

        let df = self
            .ctx
            .execute_logical_plan(logical_plan)
            .await
            .map_err(|e| format!("execute_logical_plan: {e}"))?;
        // Collect can return no batches for an empty result; keep the plan
        // schema so a header-only golden can still type-check.
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;

        Ok(batches_to_table(&batches, &schema))
    }
}

async fn register_csv(
    ctx: &SessionContext,
    table_name: &str,
    csv_path: &Path,
    file_stem: &str,
) -> Result<()> {
    let schema = schema_for(file_stem).ok_or_else(|| error::Error::UnknownTable {
        name: file_stem.to_string(),
        test_id: String::new(),
    })?;
    let options = CsvReadOptions::new()
        .delimiter(b'|')
        .has_header(false)
        .schema(schema.as_ref())
        .file_extension("csv");
    ctx.register_csv(
        TableReference::bare(table_name),
        csv_path.to_string_lossy().as_ref(),
        options,
    )
    .await
    .context(error::RegisterTableSnafu {
        table: table_name.to_string(),
        path: csv_path.to_path_buf(),
    })
}

fn ensure_inputs_registered(case: &LoadedCase) -> std::result::Result<(), String> {
    let known: HashSet<&str> = TPCH_TABLES
        .iter()
        .flat_map(|t| [t.file_stem, t.plan_name])
        .collect();
    for InputTable { name, csv_path } in &case.input_tables {
        if !csv_path.exists() {
            return Err(format!(
                "test '{}' input CSV '{}' does not exist",
                case.id,
                csv_path.display()
            ));
        }
        if !known.contains(name.as_str()) && !known.contains(name.to_ascii_uppercase().as_str()) {
            return Err(format!(
                "test '{}' references unknown TPC-H table '{name}'",
                case.id
            ));
        }
    }
    Ok(())
}

fn batches_to_table(batches: &[RecordBatch], schema: &arrow::datatypes::Schema) -> TableData {
    let columns = schema
        .fields()
        .iter()
        .map(|f| ColumnSpec {
            name: f.name().clone(),
            type_token: arrow_type_token(f.data_type()),
        })
        .collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                row.push(cell_to_string(batch.column(col_idx).as_ref(), row_idx));
            }
            rows.push(row);
        }
    }
    TableData { columns, rows }
}

/// The typed-header token for an engine column. A decimal keeps its declared
/// precision and scale (`decimal(19,6)`): `compare` bounds a `double` golden
/// by that scale, so the engine's own schema, not the printed value, decides.
fn arrow_type_token(dt: &DataType) -> String {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            "integer".to_string()
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "bigint".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        DataType::Boolean => "boolean".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        _ => "string".to_string(),
    }
}

fn cell_to_string(array: &dyn Array, idx: usize) -> String {
    if array.is_null(idx) {
        return String::new();
    }
    match array.data_type() {
        DataType::Boolean => array.as_boolean().value(idx).to_string(),
        DataType::Int8 => array
            .as_primitive::<arrow::datatypes::Int8Type>()
            .value(idx)
            .to_string(),
        DataType::Int16 => array
            .as_primitive::<arrow::datatypes::Int16Type>()
            .value(idx)
            .to_string(),
        DataType::Int32 => array
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(idx)
            .to_string(),
        DataType::Int64 => array
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(idx)
            .to_string(),
        DataType::UInt8 => array
            .as_primitive::<arrow::datatypes::UInt8Type>()
            .value(idx)
            .to_string(),
        DataType::UInt16 => array
            .as_primitive::<arrow::datatypes::UInt16Type>()
            .value(idx)
            .to_string(),
        DataType::UInt32 => array
            .as_primitive::<arrow::datatypes::UInt32Type>()
            .value(idx)
            .to_string(),
        DataType::UInt64 => array
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(idx)
            .to_string(),
        DataType::Float32 => array
            .as_primitive::<arrow::datatypes::Float32Type>()
            .value(idx)
            .to_string(),
        DataType::Float64 => array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(idx)
            .to_string(),
        DataType::Decimal128(_, scale) => {
            let raw = array
                .as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(idx);
            format_decimal(raw, i32::from(*scale))
        }
        DataType::Date32 => {
            let days = array
                .as_primitive::<arrow::datatypes::Date32Type>()
                .value(idx);
            format_date32(days)
        }
        DataType::Utf8 => array.as_string::<i32>().value(idx).to_string(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(idx).to_string(),
        DataType::Utf8View => array.as_string_view().value(idx).to_string(),
        other => format!("<{other:?}>"),
    }
}

fn format_decimal(raw: i128, scale: i32) -> String {
    if scale <= 0 {
        return raw.to_string();
    }
    let scale_u = u32::try_from(scale).unwrap_or(0);
    let factor = 10_u128.saturating_pow(scale_u);
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    let whole = abs / factor;
    let frac = abs % factor;
    let frac_width = usize::try_from(scale).unwrap_or(0);
    format!("{sign}{whole}.{frac:0frac_width$}")
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn format_date32(days: i32) -> String {
    // Date32 is days since UNIX epoch. Keep ISO-8601 so it can match a golden
    // `date` column stored as a string.
    match chrono::DateTime::from_timestamp(i64::from(days) * 86_400, 0) {
        Some(ts) => ts.format("%Y-%m-%d").to_string(),
        None => days.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_substrait::substrait::proto::{
        NamedStruct, Plan, PlanRel, ReadRel, Rel, RelRoot, Type,
        expression::{
            Literal,
            literal::{LiteralType, VarChar},
        },
        plan_rel,
        read_rel::{ReadType, VirtualTable},
        rel,
        r#type::{self, Nullability},
    };

    #[test]
    fn empty_batches_preserve_execution_schema() {
        let schema = Schema::new(vec![
            Field::new("flag", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ]);
        let table = batches_to_table(&[], &schema);
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "flag");
        assert_eq!(table.columns[0].type_token, "string");
        assert_eq!(table.columns[1].name, "n");
        assert_eq!(table.columns[1].type_token, "integer");
        assert!(table.rows.is_empty());
    }

    /// Isthmus TPC-H emits `LiteralType::VarChar`. Without spiceai/datafusion#215
    /// `from_substrait_plan` errors and Mode A reports ERROR (13/22 queries).
    #[tokio::test]
    async fn varchar_literal_lowers_to_utf8() {
        let varchar = Type {
            kind: Some(r#type::Kind::Varchar(r#type::VarChar {
                length: 25,
                type_variation_reference: 0,
                nullability: i32::from(Nullability::Nullable),
            })),
        };
        let proto = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(Rel {
                        rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                            base_schema: Some(NamedStruct {
                                names: vec!["r_name".to_string()],
                                r#struct: Some(r#type::Struct {
                                    types: vec![varchar],
                                    type_variation_reference: 0,
                                    nullability: i32::from(Nullability::Nullable),
                                }),
                            }),
                            read_type: Some(ReadType::VirtualTable(VirtualTable {
                                expressions: vec![
                                    datafusion_substrait::substrait::proto::expression::nested::Struct {
                                        fields: vec![datafusion_substrait::substrait::proto::Expression {
                                            rex_type: Some(
                                                datafusion_substrait::substrait::proto::expression::RexType::Literal(
                                                    Literal {
                                                        nullable: false,
                                                        type_variation_reference: 0,
                                                        literal_type: Some(LiteralType::VarChar(VarChar {
                                                            value: "EUROPE".to_string(),
                                                            length: 25,
                                                        })),
                                                    },
                                                ),
                                            ),
                                        }],
                                    },
                                ],
                                ..Default::default()
                            })),
                            ..Default::default()
                        }))),
                    }),
                    names: vec!["r_name".to_string()],
                })),
            }],
            ..Default::default()
        };

        let ctx = SessionContext::new();
        let plan = from_substrait_plan(&ctx.state(), &proto)
            .await
            .expect("VarChar literal must lower (spiceai/datafusion#215)");
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute varchar literal plan");
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await.expect("collect varchar literal plan");
        let table = batches_to_table(&batches, &schema);
        assert_eq!(table.rows, vec![vec!["EUROPE".to_string()]]);
    }

    /// Isthmus TPC-H q07/q08/q09 call `extract:req_date` with an enum `YEAR`
    /// argument. Without spiceai/datafusion#220 `from_substrait_plan` errors
    /// (`Function argument non-Value type not supported`) and Mode A reports
    /// ERROR for all three.
    #[tokio::test]
    async fn enum_function_argument_lowers_to_date_part() {
        use std::sync::Arc;

        use arrow::array::Date32Array;
        use datafusion_substrait::substrait::proto::{
            Expression, FunctionArgument, ProjectRel,
            expression::{
                FieldReference, ReferenceSegment, RexType, ScalarFunction,
                field_reference::{ReferenceType, RootReference, RootType},
                reference_segment::{self, StructField},
            },
            extensions::{
                SimpleExtensionDeclaration,
                simple_extension_declaration::{ExtensionFunction, MappingType},
            },
            function_argument::ArgType,
            read_rel::NamedTable,
        };

        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Date32Array::from(vec![10470]))], // 1998-09-01
        )
        .expect("one-row Date32 batch");
        ctx.register_batch("t", batch).expect("register table t");

        let date_type = Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: 0,
                nullability: i32::from(Nullability::Required),
            })),
        };
        let i64_type = Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: 0,
                nullability: i32::from(Nullability::Required),
            })),
        };
        let date_column = Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                        StructField {
                            field: 0,
                            child: None,
                        },
                    ))),
                })),
                root_type: Some(RootType::RootReference(RootReference {})),
            }))),
        };
        let extract_year = Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: 1,
                arguments: vec![
                    FunctionArgument {
                        arg_type: Some(ArgType::Enum("YEAR".to_string())),
                    },
                    FunctionArgument {
                        arg_type: Some(ArgType::Value(date_column)),
                    },
                ],
                output_type: Some(i64_type),
                ..Default::default()
            })),
        };
        let proto = Plan {
            extensions: vec![SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                    extension_urn_reference: 0,
                    function_anchor: 1,
                    name: "extract:req_date".to_string(),
                })),
            }],
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(Rel {
                        rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
                            input: Some(Box::new(Rel {
                                rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                                    base_schema: Some(NamedStruct {
                                        names: vec!["d".to_string()],
                                        r#struct: Some(r#type::Struct {
                                            types: vec![date_type],
                                            type_variation_reference: 0,
                                            nullability: i32::from(Nullability::Required),
                                        }),
                                    }),
                                    read_type: Some(ReadType::NamedTable(NamedTable {
                                        names: vec!["t".to_string()],
                                        advanced_extension: None,
                                    })),
                                    ..Default::default()
                                }))),
                            })),
                            expressions: vec![extract_year],
                            ..Default::default()
                        }))),
                    }),
                    names: vec!["d".to_string(), "year".to_string()],
                })),
            }],
            ..Default::default()
        };

        let plan = from_substrait_plan(&ctx.state(), &proto)
            .await
            .expect("enum function argument must lower (spiceai/datafusion#220)");
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute extract plan");
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await.expect("collect extract plan");
        let table = batches_to_table(&batches, &schema);
        assert_eq!(table.columns[1].type_token, "bigint");
        assert_eq!(
            table.rows,
            vec![vec!["1998-09-01".to_string(), "1998".to_string()]]
        );
    }

    // --- Guards for spiceai/datafusion#226: correlated subqueries that read a
    // --- table the enclosing scope also reads. Each builds an Isthmus-shaped
    // --- plan over `t(a, b)` = {1|10, 1|20, 2|30} and executes it.

    fn i64_type() -> Type {
        Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: 0,
                nullability: i32::from(Nullability::Required),
            })),
        }
    }

    fn bool_type() -> Type {
        Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: 0,
                nullability: i32::from(Nullability::Required),
            })),
        }
    }

    /// `READ t` with base schema `(a, b)`, optionally carrying its own
    /// `ReadRel.filter`.
    fn read_t(filter: Option<datafusion_substrait::substrait::proto::Expression>) -> Rel {
        read_named("t", filter)
    }

    fn read_named(
        name: &str,
        filter: Option<datafusion_substrait::substrait::proto::Expression>,
    ) -> Rel {
        use datafusion_substrait::substrait::proto::read_rel::NamedTable;
        Rel {
            rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                base_schema: Some(NamedStruct {
                    names: vec!["a".to_string(), "b".to_string()],
                    r#struct: Some(r#type::Struct {
                        types: vec![i64_type(), i64_type()],
                        type_variation_reference: 0,
                        nullability: i32::from(Nullability::Required),
                    }),
                }),
                read_type: Some(ReadType::NamedTable(NamedTable {
                    names: vec![name.to_string()],
                    advanced_extension: None,
                })),
                filter: filter.map(Box::new),
                ..Default::default()
            }))),
        }
    }

    fn cross(left: Rel, right: Rel) -> Rel {
        use datafusion_substrait::substrait::proto::CrossRel;
        Rel {
            rel_type: Some(rel::RelType::Cross(Box::new(CrossRel {
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
                ..Default::default()
            }))),
        }
    }

    fn filter(input: Rel, condition: datafusion_substrait::substrait::proto::Expression) -> Rel {
        use datafusion_substrait::substrait::proto::FilterRel;
        Rel {
            rel_type: Some(rel::RelType::Filter(Box::new(FilterRel {
                input: Some(Box::new(input)),
                condition: Some(Box::new(condition)),
                ..Default::default()
            }))),
        }
    }

    /// Field `index` of the current input, or of the scope `steps_out` levels up.
    fn field(
        index: i32,
        steps_out: Option<u32>,
    ) -> datafusion_substrait::substrait::proto::Expression {
        use datafusion_substrait::substrait::proto::{
            Expression,
            expression::{
                FieldReference, ReferenceSegment, RexType,
                field_reference::{OuterReference, ReferenceType, RootReference, RootType},
                reference_segment::{self, StructField},
            },
        };
        Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(Box::new(
                        StructField {
                            field: index,
                            child: None,
                        },
                    ))),
                })),
                root_type: Some(match steps_out {
                    Some(steps_out) => RootType::OuterReference(OuterReference { steps_out }),
                    None => RootType::RootReference(RootReference {}),
                }),
            }))),
        }
    }

    /// Function anchor 1 = `and:bool`, 2 = `equal:any_any`, 3 = `not_equal:any_any`.
    fn call(
        reference: u32,
        args: Vec<datafusion_substrait::substrait::proto::Expression>,
    ) -> datafusion_substrait::substrait::proto::Expression {
        use datafusion_substrait::substrait::proto::{
            Expression, FunctionArgument,
            expression::{RexType, ScalarFunction},
            function_argument::ArgType,
        };
        Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: reference,
                arguments: args
                    .into_iter()
                    .map(|value| FunctionArgument {
                        arg_type: Some(ArgType::Value(value)),
                    })
                    .collect(),
                output_type: Some(bool_type()),
                ..Default::default()
            })),
        }
    }

    /// `f0 = outer.f<a> AND f1 <> outer.f<b>`: the row has the same `a` as the
    /// enclosing row and a different `b`.
    fn correlated_on(
        outer_a: i32,
        outer_b: i32,
    ) -> datafusion_substrait::substrait::proto::Expression {
        call(
            1,
            vec![
                call(2, vec![field(0, None), field(outer_a, Some(1))]),
                call(3, vec![field(1, None), field(outer_b, Some(1))]),
            ],
        )
    }

    fn exists(inner: Rel) -> datafusion_substrait::substrait::proto::Expression {
        use datafusion_substrait::substrait::proto::{
            Expression,
            expression::{
                RexType, Subquery,
                subquery::{SetPredicate, SubqueryType, set_predicate::PredicateOp},
            },
        };
        Expression {
            rex_type: Some(RexType::Subquery(Box::new(Subquery {
                subquery_type: Some(SubqueryType::SetPredicate(Box::new(SetPredicate {
                    predicate_op: i32::from(PredicateOp::Exists),
                    tuples: Some(Box::new(inner)),
                }))),
            }))),
        }
    }

    fn plan(root: Rel, names: &[&str]) -> Plan {
        plan_with(
            root,
            names,
            &[
                (1, "and:bool"),
                (2, "equal:any_any"),
                (3, "not_equal:any_any"),
            ],
        )
    }

    fn plan_with(root: Rel, names: &[&str], extensions: &[(u32, &str)]) -> Plan {
        use datafusion_substrait::substrait::proto::extensions::{
            SimpleExtensionDeclaration,
            simple_extension_declaration::{ExtensionFunction, MappingType},
        };
        let extension = |anchor: u32, name: &str| SimpleExtensionDeclaration {
            mapping_type: Some(MappingType::ExtensionFunction(ExtensionFunction {
                extension_urn_reference: 0,
                function_anchor: anchor,
                name: name.to_string(),
            })),
        };
        Plan {
            extensions: extensions
                .iter()
                .map(|(anchor, name)| extension(*anchor, name))
                .collect(),
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(RelRoot {
                    input: Some(root),
                    names: names.iter().map(|n| (*n).to_string()).collect(),
                })),
            }],
            ..Default::default()
        }
    }

    /// `t(a, b)` = {1|10, 1|20, 2|30}, optionally behind a leading `extra`
    /// column the plan's base schema does not mention.
    fn register_t(ctx: &SessionContext, with_extra_column: bool) {
        register_named(ctx, "t", with_extra_column);
    }

    fn register_named(ctx: &SessionContext, name: &str, with_extra_column: bool) {
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        let mut fields = vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ];
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ];
        if with_extra_column {
            fields.insert(0, Field::new("extra", DataType::Utf8, false));
            columns.insert(0, Arc::new(StringArray::from(vec!["x", "y", "z"])));
        }
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("three-row batch");
        ctx.register_batch(name, batch).expect("register table");
    }

    async fn execute(ctx: &SessionContext, proto: &Plan) -> Vec<Vec<String>> {
        let plan = from_substrait_plan(&ctx.state(), proto)
            .await
            .expect("plan must lower (spiceai/datafusion#226)");
        let df = ctx.execute_logical_plan(plan).await.expect("execute plan");
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await.expect("collect plan");
        let mut rows = batches_to_table(&batches, &schema).rows;
        rows.sort();
        rows
    }

    fn rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| row.iter().map(|cell| (*cell).to_string()).collect())
            .collect()
    }

    /// TPC-H q21's EXISTS / NOT EXISTS subqueries read `LINEITEM` while the
    /// enclosing scope also reads `LINEITEM`. Without spiceai/datafusion#226
    /// both scans share the qualifier, decorrelation resolves the correlated
    /// predicate to the inner scan alone, and the query returns no rows
    /// (Mode A q21: `row count 0 != 1`).
    #[tokio::test]
    async fn correlated_subquery_over_the_same_table_keeps_its_rows() {
        let ctx = SessionContext::new();
        register_t(&ctx, false);
        let proto = plan(
            filter(
                read_t(None),
                exists(filter(read_t(None), correlated_on(0, 1))),
            ),
            &["a", "b"],
        );
        // 1|10 and 1|20 each have a partner with the same `a` and a different
        // `b`; 2|30 has none. Without the fork fix the result is empty.
        assert_eq!(
            execute(&ctx, &proto).await,
            rows(&[&["1", "10"], &["1", "20"]])
        );
    }

    /// The same correlated predicate carried as the inner scan's own
    /// `ReadRel.filter`, against a provider whose schema carries a leading
    /// column the plan's base schema does not mention: the filter's field
    /// indices must bind to the Substrait base schema (`a`, `b`), not to the
    /// provider's first column, and the aliased scan must be projected to the
    /// base schema by name. Without the fork's follow-up the filter bound
    /// field 0 to `extra` and failed with a cast error.
    #[tokio::test]
    async fn correlated_read_filter_binds_to_the_substrait_schema() {
        let ctx = SessionContext::new();
        register_t(&ctx, true);
        let proto = plan(
            filter(read_t(None), exists(read_t(Some(correlated_on(0, 1))))),
            &["a", "b"],
        );
        assert_eq!(
            execute(&ctx, &proto).await,
            rows(&[&["1", "10"], &["1", "20"]])
        );
    }

    /// `t INTERSECT t`, which `intersect` builds as a semi join whose sides
    /// are requalified like a join's.
    fn intersect_t_with_t() -> Rel {
        use datafusion_substrait::substrait::proto::{SetRel, set_rel::SetOp};
        Rel {
            rel_type: Some(rel::RelType::Set(SetRel {
                inputs: vec![read_t(None), read_t(None)],
                op: i32::from(SetOp::IntersectionPrimary),
                ..Default::default()
            })),
        }
    }

    /// A correlated `ReadRel.filter` on a table no enclosing scope reads: no
    /// alias is involved, but the filter must still sit above the scan, since a
    /// `TableScan`'s filters cannot evaluate an outer reference and the
    /// decorrelation rules cannot lift one from there.
    #[tokio::test]
    async fn correlated_read_filter_on_another_table_keeps_its_rows() {
        let ctx = SessionContext::new();
        register_t(&ctx, false);
        register_named(&ctx, "u", false);
        let proto = plan(
            filter(
                read_t(None),
                exists(read_named("u", Some(correlated_on(0, 1)))),
            ),
            &["a", "b"],
        );
        assert_eq!(
            execute(&ctx, &proto).await,
            rows(&[&["1", "10"], &["1", "20"]])
        );
    }

    /// Both scopes self-join `t`, so both joins requalify their sides. The
    /// inner join must not take the enclosing join's `left`/`right` names or
    /// the predicate correlating to the outer `left` collapses to nothing;
    /// its sides become `left_1`/`right_1`.
    #[tokio::test]
    async fn requalified_join_inside_a_subquery_keeps_its_correlation() {
        let ctx = SessionContext::new();
        register_t(&ctx, false);
        let proto = plan(
            filter(
                cross(read_t(None), read_t(None)),
                exists(filter(
                    cross(read_t(None), read_t(None)),
                    correlated_on(0, 1),
                )),
            ),
            &["a1", "b1", "a2", "b2"],
        );
        // The outer `left` rows 1|10 and 1|20 have a partner in `t` with the
        // same `a` and a different `b`; 2|30 has none. Each keeps its three
        // outer `right` partners. Without the fork fix the result is empty.
        assert_eq!(execute(&ctx, &proto).await, six_rows_for_outer_left_1());
    }

    /// The enclosing scope self-joins `t` and the subquery self-intersects
    /// it: `intersect`/`except` requalify their sides too, and must keep
    /// clear of the enclosing `left`/`right` as a join does.
    #[tokio::test]
    async fn intersect_inside_a_subquery_keeps_its_correlation() {
        let ctx = SessionContext::new();
        register_t(&ctx, false);
        let proto = plan(
            filter(
                cross(read_t(None), read_t(None)),
                exists(filter(intersect_t_with_t(), correlated_on(0, 1))),
            ),
            &["a1", "b1", "a2", "b2"],
        );
        // `t INTERSECT t` is `t`; the same six rows as for the self-join.
        // Without the fork fix the result is empty.
        assert_eq!(execute(&ctx, &proto).await, six_rows_for_outer_left_1());
    }

    fn six_rows_for_outer_left_1() -> Vec<Vec<String>> {
        rows(&[
            &["1", "10", "1", "10"],
            &["1", "10", "1", "20"],
            &["1", "10", "2", "30"],
            &["1", "20", "1", "10"],
            &["1", "20", "1", "20"],
            &["1", "20", "2", "30"],
        ])
    }

    // --- Guards for the fork-only sub-behaviors of spiceai/datafusion#220 (the
    // --- `extract` translation) and #226 (alias uniqueness).

    /// `dates(d)` = {1998-09-01}, a Tuesday.
    fn register_dates(ctx: &SessionContext) {
        use std::sync::Arc;

        use arrow::array::Date32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Date32Array::from(vec![10470]))])
            .expect("one-row Date32 batch");
        ctx.register_batch("dates", batch)
            .expect("register table dates");
    }

    fn read_dates() -> Rel {
        use datafusion_substrait::substrait::proto::read_rel::NamedTable;
        let date_type = Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: 0,
                nullability: i32::from(Nullability::Required),
            })),
        };
        Rel {
            rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                base_schema: Some(NamedStruct {
                    names: vec!["d".to_string()],
                    r#struct: Some(r#type::Struct {
                        types: vec![date_type],
                        type_variation_reference: 0,
                        nullability: i32::from(Nullability::Required),
                    }),
                }),
                read_type: Some(ReadType::NamedTable(NamedTable {
                    names: vec!["dates".to_string()],
                    advanced_extension: None,
                })),
                ..Default::default()
            }))),
        }
    }

    fn project(
        input: Rel,
        expressions: Vec<datafusion_substrait::substrait::proto::Expression>,
    ) -> Rel {
        use datafusion_substrait::substrait::proto::ProjectRel;
        Rel {
            rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
                input: Some(Box::new(input)),
                expressions,
                ..Default::default()
            }))),
        }
    }

    /// `extract(<options…>, value)` through function anchor 1, declared `i64`
    /// as Isthmus declares it.
    fn extract_call(
        options: &[&str],
        value: datafusion_substrait::substrait::proto::Expression,
    ) -> datafusion_substrait::substrait::proto::Expression {
        use datafusion_substrait::substrait::proto::{
            Expression, FunctionArgument,
            expression::{RexType, ScalarFunction},
            function_argument::ArgType,
        };
        let mut arguments: Vec<FunctionArgument> = options
            .iter()
            .map(|option| FunctionArgument {
                arg_type: Some(ArgType::Enum((*option).to_string())),
            })
            .collect();
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(value)),
        });
        Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: 1,
                arguments,
                output_type: Some(i64_type()),
                ..Default::default()
            })),
        }
    }

    /// A UDF registered under the exact name `extract` keeps precedence over
    /// the `date_part` mapping, as for every other function name.
    #[tokio::test]
    async fn registered_extract_udf_takes_precedence_over_date_part() {
        use std::sync::Arc;

        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
        let ctx = SessionContext::new();
        register_dates(&ctx);
        ctx.register_udf(create_udf(
            "extract",
            vec![DataType::Utf8, DataType::Date32],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(7))))
            }),
        ));
        let proto = plan_with(
            project(read_dates(), vec![extract_call(&["YEAR"], field(0, None))]),
            &["d", "year"],
            &[(1, "extract:req_date")],
        );
        assert_eq!(execute(&ctx, &proto).await, rows(&[&["1998-09-01", "7"]]));
    }

    /// The `ONE`/`ZERO` indexing option is an offset from `date_part`'s own
    /// base: 1998-09-01 is month 9 and a Tuesday (Sunday-based day 2), so
    /// `MONTH ZERO` is 8 and `SUNDAY_DAY_OF_WEEK ONE` is 3.
    #[tokio::test]
    async fn extract_indexing_option_is_an_offset_from_date_part() {
        let ctx = SessionContext::new();
        register_dates(&ctx);
        let proto = plan_with(
            project(
                read_dates(),
                vec![
                    extract_call(&["MONTH", "ZERO"], field(0, None)),
                    extract_call(&["SUNDAY_DAY_OF_WEEK", "ONE"], field(0, None)),
                ],
            ),
            &["d", "month0", "dow1"],
            &[(1, "extract:req_req_date")],
        );
        assert_eq!(
            execute(&ctx, &proto).await,
            rows(&[&["1998-09-01", "8", "3"]])
        );
    }

    /// A component `date_part` defines differently is refused by name, not
    /// silently mapped: `MILLISECOND` counts from the previous whole second in
    /// Substrait and from the start of the minute in `date_part`. The message
    /// names the component; the pre-patch consumer failed on the argument kind
    /// instead (`Function argument non-Value type not supported`).
    #[tokio::test]
    async fn unmapped_extract_component_is_rejected_by_name() {
        let ctx = SessionContext::new();
        register_dates(&ctx);
        let proto = plan_with(
            project(
                read_dates(),
                vec![extract_call(&["MILLISECOND"], field(0, None))],
            ),
            &["d", "ms"],
            &[(1, "extract:req_date")],
        );
        let err = from_substrait_plan(&ctx.state(), &proto)
            .await
            .expect_err("MILLISECOND must not lower to date_part");
        assert!(
            err.to_string().contains("extract component MILLISECOND"),
            "{err}"
        );
    }

    /// The enclosing scope reads `t` and a table that is already named `t_1`;
    /// the inner scan of `t`, correlated to that `t_1`, must not take the name
    /// `t_1` or the collision comes straight back. It becomes `t_2`.
    #[tokio::test]
    async fn subquery_scan_alias_skips_a_taken_name() {
        let ctx = SessionContext::new();
        register_t(&ctx, false);
        register_named(&ctx, "t_1", false);
        let proto = plan(
            filter(
                cross(read_t(None), read_named("t_1", None)),
                exists(filter(read_t(None), correlated_on(2, 3))),
            ),
            &["a", "b", "a1", "b1"],
        );
        // Every `t` row pairs with the two `t_1` rows that have a partner in
        // `t` with the same `a` and a different `b`; `t_1`'s 2|30 has none.
        assert_eq!(
            execute(&ctx, &proto).await,
            rows(&[
                &["1", "10", "1", "10"],
                &["1", "10", "1", "20"],
                &["1", "20", "1", "10"],
                &["1", "20", "1", "20"],
                &["2", "30", "1", "10"],
                &["2", "30", "1", "20"],
            ])
        );
    }
}
