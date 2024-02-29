use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::RecordBatchOptions;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use postgres::{types::Type, Row};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unsupported type {:?} for column {:?}", r#type, col))]
    UnsupportedType { r#type: String, col: String },

    #[snafu(display("Unsupported type {:?} for column index {index}", r#type))]
    UnsupportedTypeIndex { r#type: String, index: usize },

    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {postgres_type}"))]
    FailedToDowncastBuilder { postgres_type: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::missing_errors_doc)]
#[allow(clippy::too_many_lines)]
pub fn rows_to_arrow(rows: &[Row]) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Field> = Vec::new();
    let mut arrow_columns_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut postgres_types: Vec<Type> = Vec::new();

    if !rows.is_empty() {
        let row = &rows[0];
        for column in row.columns() {
            let column_name = column.name();
            let column_type = column.type_();

            arrow_fields.push(Field::new(
                column_name,
                map_column_type_to_data_type(column_type, column_name)?,
                true, // TODO: Set nullable properly based on postgres schema
            ));
            postgres_types.push(column_type.clone());

            match *column_type {
                Type::INT2 => {
                    arrow_columns_builders.push(Box::new(arrow::array::Int16Builder::new()));
                }
                Type::INT4 => {
                    arrow_columns_builders.push(Box::new(arrow::array::Int32Builder::new()));
                }
                Type::INT8 => {
                    arrow_columns_builders.push(Box::new(arrow::array::Int64Builder::new()));
                }
                Type::FLOAT4 => {
                    arrow_columns_builders.push(Box::new(arrow::array::Float32Builder::new()));
                }
                Type::FLOAT8 => {
                    arrow_columns_builders.push(Box::new(arrow::array::Float64Builder::new()));
                }
                Type::TEXT => {
                    arrow_columns_builders.push(Box::new(arrow::array::StringBuilder::new()));
                }
                Type::BOOL => {
                    arrow_columns_builders.push(Box::new(arrow::array::BooleanBuilder::new()));
                }
                // TODO: Figure out how to handle decimal, isn't specified as a type in postgres types
                Type::NUMERIC => {
                    arrow_columns_builders.push(Box::new(arrow::array::Decimal128Builder::new()));
                }
                _ => UnsupportedTypeSnafu {
                    r#type: format!("{column_type}"),
                    col: column_name.to_string(),
                }
                .fail()?,
            }
        }
    }

    for row in rows {
        for (i, postgres_type) in postgres_types.iter().enumerate() {
            let Some(builder) = arrow_columns_builders.get_mut(i) else {
                return NoBuilderForIndexSnafu { index: i }.fail();
            };

            match *postgres_type {
                Type::INT2 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int16Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: i16 = row.get(i);
                    builder.append_value(v);
                }
                Type::INT4 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int32Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: i32 = row.get(i);
                    builder.append_value(v);
                }
                Type::INT8 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Int64Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: i64 = row.get(i);
                    builder.append_value(v);
                }
                Type::FLOAT4 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Float32Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: f32 = row.get(i);
                    builder.append_value(v);
                }
                Type::FLOAT8 => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Float64Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: f64 = row.get(i);
                    builder.append_value(v);
                }
                Type::TEXT => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::StringBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: &str = row.get(i);
                    builder.append_value(v);
                }
                Type::BOOL => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::BooleanBuilder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };
                    let v: bool = row.get(i);
                    builder.append_value(v);
                }
                Type::NUMERIC => {
                    let Some(builder) = builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::Decimal128Builder>()
                    else {
                        return FailedToDowncastBuilderSnafu {
                            postgres_type: format!("{postgres_type}"),
                        }
                        .fail();
                    };

                    // TODO: Figure out how to properly extract Decimal128 from postgres
                    let v: i64 = row.get(i);
                    #[allow(clippy::cast_lossless)]
                    builder.append_value(v as i128);
                }
                _ => UnsupportedTypeIndexSnafu {
                    r#type: format!("{postgres_type}"),
                    index: i,
                }
                .fail()?,
            }
        }
    }

    let columns = arrow_columns_builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect::<Vec<ArrayRef>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(rows.len()));
    match RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

fn map_column_type_to_data_type(column_type: &Type, column_name: &str) -> Result<DataType> {
    match *column_type {
        Type::INT2 => Ok(DataType::Int16),
        Type::INT4 => Ok(DataType::Int32),
        Type::INT8 => Ok(DataType::Int64),
        Type::FLOAT4 => Ok(DataType::Float32),
        Type::FLOAT8 => Ok(DataType::Float64),
        Type::TEXT => Ok(DataType::Utf8),
        Type::BOOL => Ok(DataType::Boolean),
        // TODO: Figure out how to handle decimal, isn't specified as a type in postgres types
        Type::NUMERIC => Ok(DataType::Decimal128(38, 9)),
        _ => UnsupportedTypeSnafu {
            r#type: format!("{column_type}"),
            col: column_name.to_string(),
        }
        .fail()?,
    }
}
