pub mod onnx;

pub trait ModelFormat {
    fn from_bytes(bytes: &[u8]) -> Self;
}
