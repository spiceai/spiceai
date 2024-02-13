use super::{ModelRuntime, Runnable};
use arrow::array::ArrayRef;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use snafu::ResultExt;
use snafu::Snafu;
use std::sync::Arc;

use tract_core::tract_data::itertools::Itertools;
use tract_onnx::prelude::*;

pub struct Tract {
    pub path: String,
}

#[derive(Debug, Snafu)]
pub enum Error {
    TractError { source: tract_core::anyhow::Error },

    ArrowError { source: arrow::error::ArrowError },

    ShapeError { source: ndarray::ShapeError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

type Plan = SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>;
pub struct Model {
    model: Plan,
}

fn transpose_and_flatten(matrix: Vec<Vec<f64>>) -> Vec<f64> {
    if matrix.is_empty() || matrix[0].is_empty() {
        return Vec::new();
    }

    let nrows = matrix.len();
    let ncols = matrix[0].len();

    // Check for uniformity in inner vector lengths
    for row in &matrix {
        if row.len() != ncols {
            panic!("All rows must have the same number of columns");
        }
    }

    let mut result = Vec::with_capacity(nrows * ncols);

    for col in 0..ncols {
        for row in &matrix {
            result.push(row[col]);
        }
    }

    result
}

impl ModelRuntime for Tract {
    fn load(&self) -> std::result::Result<Box<dyn Runnable>, super::Error> {
        let model = load_tract_model(self.path.as_str()).context(TractSnafu)?;
        Ok(Box::new(Model { model }))
    }
}

fn load_tract_model(path: &str) -> TractResult<Plan> {
    tract_onnx::onnx()
        .model_for_path(path)?
        .into_optimized()?
        .with_input_fact(0, f32::fact([1, 20]))? // TODO: need to do this from model metadata
        .into_runnable()
}

impl Runnable for Model {
    fn run(
        &self,
        input: Vec<RecordBatch>,
        lookback_size: usize,
    ) -> std::result::Result<RecordBatch, super::Error> {
        let reader: &[RecordBatch] = &input;
        let return_schema = Schema::new(vec![Field::new("y", DataType::Float32, false)]);

        let Some(first_record) = reader.first() else {
            return Ok(RecordBatch::new_empty(Arc::new(return_schema)));
        };

        let schema = first_record.schema();
        let fields = schema.fields();
        let mut data: Vec<Vec<f64>> = fields.iter().map(|_| Vec::new()).collect_vec();
        let n_cols = data.len() - 1;
        for batch in reader {
            batch
                .columns()
                .iter()
                .enumerate()
                .for_each(|(i, column): (usize, &ArrayRef)| {
                    if schema.field(i).name() == "ts" || data[i].len() >= lookback_size {
                        // Don't process `ts`, or columns that have enough data.
                        // TODO: Stop early if all columns have `lookback_size` data.
                        return;
                    }

                    // TODO: read type from schema
                    if let Some(float_col) = column.as_any().downcast_ref::<Float64Array>() {
                        let col = float_col.iter().flatten().collect::<Vec<_>>();
                        let end_idx = std::cmp::min(lookback_size - data[i].len(), col.len());
                        data[i].extend_from_slice(&col[..end_idx]);
                    }
                });
        }

        let inp = data
            .iter()
            .enumerate()
            .filter(|(i, _)| schema.field(*i).name() != "ts")
            .map(|(_, x)| {
                let mut a = x.clone();
                a.reverse();
                a
            })
            .collect_vec();

        let small_vec: Tensor = tract_ndarray::Array2::from_shape_vec(
            (1, lookback_size * n_cols),
            transpose_and_flatten(inp),
        )
        .context(ShapeSnafu)?
        .into();

        let output = self
            .model
            .run(tvec!(small_vec
                .cast_to_dt(DatumType::F32)
                .context(TractSnafu)?
                .deep_clone()
                .into()))
            .context(TractSnafu)?;

        let result: Vec<f32> = output[0]
            .to_array_view::<f32>()
            .context(TractSnafu)?
            .iter()
            .copied()
            .collect_vec();

        let record_batch = RecordBatch::try_new(
            Arc::new(return_schema),
            vec![Arc::new(Float32Array::from(result))],
        )
        .context(ArrowSnafu)?;

        Ok(record_batch)
    }
}
