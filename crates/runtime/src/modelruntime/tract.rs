use super::ModelRuntime;
use super::Runnable;
use arrow::array::ArrayRef;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use std::sync::Arc;

use tract_core::tract_data::itertools::Itertools;
use tract_onnx::prelude::*;

pub struct Tract {
    pub path: String,
}

type Plan = SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>;
pub struct Model {
    model: Plan,
}

impl ModelRuntime for Tract {
    fn load(&self) -> super::Result<Box<dyn Runnable>> {
        let model = load_tract_model(self.path.as_str()).context(super::TractSnafu)?;
        Ok(Box::new(Model { model }))
    }
}

fn load_tract_model(path: &str) -> TractResult<Plan> {
    tract_onnx::onnx()
        .model_for_path(path)?
        .into_optimized()?
        .with_input_fact(0, f32::fact([1, 20]))? // TODO: remove
        .into_runnable()
}

impl Runnable for Model {
    fn run(&self, input: Vec<RecordBatch>, lookback_size: usize) -> super::Result<RecordBatch> {
        {
            let this = &self;
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
                    .for_each(|(i, a): (usize, &ArrayRef)| {
                        if schema.field(i).name() == "ts" {
                            // Don't process `ts`.
                            return;
                        }
                        if data[i].len() >= lookback_size {
                            // This particular column has all data.
                            // TODO: Stop early if all columns have `lookback_size` data.
                            return;
                        }

                        let Some(float_col) = a.as_any().downcast_ref::<Float64Array>() else {
                            return;
                        };
                        let col = float_col.into_iter().flatten().collect_vec();
                        let end_idx: usize =
                            std::cmp::min(lookback_size - data[i].len(), col.len());
                        data[i].append(&mut col[..end_idx].to_vec().clone());
                    });
            }
            let inp = data
                .iter()
                .enumerate()
                .filter(|(i, _)| schema.field(*i).name() != "ts") //: (usize, ArrayRef)
                .map(|(_, x)| x)
                .fold(Vec::new(), |mut acc: Vec<f64>, b: &Vec<f64>| {
                    let mut c = b.clone();
                    c.reverse();
                    acc.extend(c);
                    acc
                });

            let small_vec: Tensor =
                tract_ndarray::Array2::from_shape_vec((1, lookback_size * n_cols), inp)
                    .context(super::ShapeSnafu)?
                    .into();

            let output = this
                .model
                .run(tvec!(small_vec
                    .cast_to_dt(DatumType::F32)
                    .context(super::TractSnafu)?
                    .deep_clone()
                    .into()))
                .context(super::TractSnafu)?;

            let result: Vec<f32> = output[0]
                .to_array_view::<f32>()
                .context(super::TractSnafu)?
                .iter()
                .copied()
                .collect_vec();

            let record_batch = RecordBatch::try_new(
                Arc::new(return_schema),
                vec![Arc::new(Float32Array::from(result))],
            )
            .context(super::ArrowSnafu)?;

            Ok(record_batch)
        }
    }
}
