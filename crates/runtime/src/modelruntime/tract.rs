use super::ModelRuntime;
use super::Runnable;
use snafu::ResultExt;
use tract_core::ndarray::{IxDyn, OwnedRepr};
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
    fn run(&self) -> Vec<Vec<f32>> {
        let io = std::fs::File::open("/Users/yong/Downloads/io.npz").unwrap();
        let npz = ndarray_npy::NpzReader::new(io);

        if npz.is_ok() {
            let input = npz.unwrap()
                .by_name::<OwnedRepr<f32>, IxDyn>("input.npy")
                .unwrap()
                .into_tensor();
            let result = self.model.run(tvec![input.into()]).unwrap();

            tracing::info!("result {:?}", result);
        }
        todo!()
    }
}
