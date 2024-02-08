use super::ModelRuntime;
use super::Runnable;
use arrow::array::Float32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use std::sync::Arc;
use tract_onnx::prelude::*;

pub struct Tract {
    pub path: String,
}

pub struct TractModel {
    model: SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>,
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
        .into_runnable();
}

impl Runnable for TractModel {
    fn run(&self, _input: Vec<RecordBatch>) -> super::Result<RecordBatch> {
        let id_array = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let schema = Schema::new(vec![Field::new("result", DataType::Float32, false)]);
        return RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])
            .context(super::ArrowSnafu);
    }
}
