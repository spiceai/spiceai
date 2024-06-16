use arrow::datatypes::DataType;
use datafusion::{
    common::{plan_err, DataFusionError, Result as DataFusionResult},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

#[derive(Debug)]
pub struct Greatest {
    signature: Signature,
}

impl Default for Greatest {
    fn default() -> Self {
        Self::new()
    }
}

impl Greatest {
    #[must_use]
    pub fn new() -> Self {
        Greatest {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

pub static SUPPORTED_COMPARISON_TYPES: &[DataType] = &[
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
];

impl ScalarUDFImpl for Greatest {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.len() < 2 {
            return plan_err!("greatest takes at least 2 argument");
        }

        todo!()
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        todo!()
    }
}

pub struct Least {
    signature: Signature,
}
