pub mod onnx;

pub enum ModelFormat {
    Onnx(onnx::Onnx),
}
