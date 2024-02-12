use super::ModelRuntime;
use super::Runnable;
use arrow::array::ArrayRef;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use snafu::ResultExt;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use tract_core::tract_data::itertools::Itertools;
use tract_onnx::prelude::*;

pub struct Tract {
    pub path: String,
}

pub struct TractModel {
    model: SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>,
}

impl TractModel {
    fn run_inference(
        &self,
        reader: Vec<RecordBatch>,
        lookback_size: usize,
    ) -> super::Result<RecordBatch> {
        let schema = reader.first().unwrap().schema();
        let fields = schema.fields();
        let mut data: Vec<Vec<f64>> = fields.iter().map(|_| Vec::new()).collect_vec();
        let n_cols = data.len() - 1;

        reader.iter().for_each(|batch| {
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
                    let col: Vec<f64> = a
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .into_iter()
                        .flatten()
                        .collect_vec();
                    let end_idx: usize = std::cmp::min(lookback_size - data[i].len(), col.len());
                    data[i].append(&mut col[..end_idx].to_vec().clone());
                })
        });
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
                .unwrap()
                .into();

        let output = self
            .model
            .run(tvec!(small_vec
                .cast_to_dt(DatumType::F32)
                .unwrap()
                .deep_clone()
                .into()))
            .unwrap();

        let result: Vec<f32> = output[0]
            .to_array_view::<f32>()
            .unwrap()
            .iter()
            .cloned()
            .collect_vec();

        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("y", DataType::Float32, false)])),
            vec![Arc::new(Float32Array::from(result))],
        )
        .unwrap())
    }
}

impl ModelRuntime for Tract {
    fn load(&self) -> super::Result<Box<dyn Runnable>> {
        let model = load_tract_model(self.path.as_str()).context(super::TractSnafu)?;
        return Ok(Box::new(TractModel { model }));
    }
}

fn load_tract_model(
    path: &str,
) -> TractResult<SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>> {
    return tract_onnx::onnx()
        .model_for_path(path)?
        .into_optimized()?
        .with_input_fact(0, f32::fact([1, 20]).into())? // TODO: remove
        .into_runnable();
}

impl Runnable for TractModel {
    fn run(&self, _input: Vec<RecordBatch>) -> super::Result<RecordBatch> {
        let lookback_size = 10;
        self.run_inference(_input, lookback_size)
    }
}
