pub mod onnx;

/// A `ModelFormat` specifies the supported format of a model artifacts.
///
/// Currently, only `onnx` is supported.
pub enum ModelFormat {
    Onnx(onnx::Onnx),
}
