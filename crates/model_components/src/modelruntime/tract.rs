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

use super::{ModelRuntime, Runnable};
use crate::modelruntime::ModelFormat;
use arrow::array::ArrayRef;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
use snafu::ResultExt;
use std::sync::Arc;

use crate::modelformat::onnx;
use tract_core::tract_data::itertools::Itertools;
use tract_onnx::prelude::*;

pub struct Tract {
    pub path: String,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    TractError { source: anyhow::Error },

    #[snafu(display("{source}"))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("{source}"))]
    ShapeError { source: ndarray::ShapeError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

type Plan = SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>;
pub struct Model {
    model: Plan,
}
impl Model {
    // Attempts to get the shape of the input tensor expected by the Tract model. Parses the first
    // `input_fact`. Input shape of the form: [1, lookback_size, num_variates].
    fn try_get_input_shape(&self) -> Result<[usize; 3]> {
        let fact = self.model.model().input_fact(0).context(TractSnafu)?;
        let shape = fact
            .shape
            .iter()
            .filter_map(|dim| dim.as_i64().and_then(|dim| usize::try_from(dim).ok()))
            .collect::<Vec<usize>>();

        if shape.len() != 3 {
            return Err(Error::ShapeError {
                source: ndarray::ShapeError::from_kind(ndarray::ErrorKind::IncompatibleShape),
            });
        }

        Ok([shape[0], shape[1], shape[2]])
    }
}

impl ModelRuntime for Tract {
    fn load(&self) -> std::result::Result<Box<dyn Runnable>, super::Error> {
        let model = load_tract_model(self.path.as_str()).context(TractSnafu)?;
        Ok(Box::new(Model { model }))
    }

    fn supports_format(format: ModelFormat) -> bool {
        format == ModelFormat::Onnx(onnx::Onnx {})
    }
}

fn load_tract_model(path: &str) -> TractResult<Plan> {
    tract_onnx::onnx()
        .model_for_path(path)?
        .into_optimized()?
        .into_runnable()
}

impl Runnable for Model {
    fn run(&self, input: Vec<RecordBatch>) -> std::result::Result<RecordBatch, super::Error> {
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

            let [_, lookback_size, num_variates] = this.try_get_input_shape()?;

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
            let inp: Vec<Vec<f64>> = data
                .iter()
                .enumerate()
                .filter(|(i, _)| schema.field(*i).name() != "ts") //: (usize, ArrayRef)
                .map(|(_, x)| x.clone())
                .collect_vec();

            if inp.len() != num_variates {
                return Err(Box::new(Error::TractError {
                    source: anyhow::Error::msg(format!(
                        "Number of variates {} does not match expected ({}) from model",
                        inp.len(),
                        num_variates
                    )),
                }));
            }

            let small_vec: Tensor = tract_ndarray::Array3::from_shape_vec(
                (1, lookback_size, num_variates),
                inp.into_iter().concat(),
            )
            .map_err(|_| Error::ShapeError {
                source: ndarray::ShapeError::from_kind(ndarray::ErrorKind::IncompatibleShape),
            })?
            .into_tensor();

            let output = this.model.run(tvec!(small_vec
                .cast_to_dt(DatumType::F32)
                .context(TractSnafu)?
                .deep_clone()
                .into()));

            let result: Vec<f32> = output?[0]
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
}
