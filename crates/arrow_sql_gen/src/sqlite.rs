use std::sync::Arc;

use crate::arrow::map_data_type_to_array_builder;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::RecordBatchOptions;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use rusqlite::types::Type;
use rusqlite::Rows;
use rusqlite::Statement;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {sqlite_type}"))]
    FailedToDowncastBuilder { sqlite_type: String },

    #[snafu(display("Failed to extract row value: {source}"))]
    FailedToExtractRowValue { source: rusqlite::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts Postgres `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
#[allow(clippy::too_many_lines)]
#[allow(clippy::needless_pass_by_value)]
pub fn rows_to_arrow(mut rows: Rows, stmt: Statement) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Field> = Vec::new();
    let mut arrow_columns_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut sqlite_types: Vec<Type> = Vec::new();

    let sqlite_columns = stmt.columns();
    for column in sqlite_columns {
        let column_name = column.name();
        let Some(column_type_str) = column.decl_type() else {
            unimplemented!("Unsupported column type: expression");
        };

        let column_type = map_column_type_str_to_type(column_type_str);
        let data_type = map_column_type_to_data_type(&column_type);
        arrow_fields.push(Field::new(column_name, data_type.clone(), true));
        arrow_columns_builders.push(map_data_type_to_array_builder(&data_type));
        sqlite_types.push(column_type.clone());
    }

    let mut row_count = 0;
    while let Ok(Some(row)) = rows.next() {
        for (i, sqlite_type) in sqlite_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            match *sqlite_type {
                Type::Null => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::NullBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            sqlite_type: format!("{sqlite_type}"),
                        }
                        .fail();
                    };
                    builder.append_null();
                }
                Type::Integer => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int64Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            sqlite_type: format!("{sqlite_type}"),
                        }
                        .fail();
                    };
                    let v: i64 = row.get(i).context(FailedToExtractRowValueSnafu)?;
                    builder.append_value(v);
                }
                Type::Real => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Float64Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            sqlite_type: format!("{sqlite_type}"),
                        }
                        .fail();
                    };
                    let v: f64 = row.get(i).context(FailedToExtractRowValueSnafu)?;
                    builder.append_value(v);
                }
                Type::Text => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::StringBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            sqlite_type: format!("{sqlite_type}"),
                        }
                        .fail();
                    };
                    let v: String = row.get(i).context(FailedToExtractRowValueSnafu)?;
                    builder.append_value(v);
                }
                Type::Blob => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::BinaryBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            sqlite_type: format!("{sqlite_type}"),
                        }
                        .fail();
                    };
                    let v: Vec<u8> = row.get(i).context(FailedToExtractRowValueSnafu)?;
                    builder.append_value(v);
                }
            }
        }

        row_count += 1;
    }

    let columns = arrow_columns_builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect::<Vec<ArrayRef>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(row_count));
    match RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

fn map_column_type_str_to_type(column_type_str: &str) -> Type {
    match column_type_str {
        "Null" => Type::Null,
        "Integer" => Type::Integer,
        "Real" => Type::Real,
        "Text" => Type::Text,
        "Blob" => Type::Blob,
        _ => unimplemented!("Unsupported column type {:?}", column_type_str),
    }
}

fn map_column_type_to_data_type(column_type: &Type) -> DataType {
    match *column_type {
        Type::Null => DataType::Null,
        Type::Integer => DataType::Int64,
        Type::Real => DataType::Float64,
        Type::Text => DataType::Utf8,
        Type::Blob => DataType::Binary,
    }
}
