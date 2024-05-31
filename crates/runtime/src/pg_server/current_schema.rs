use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CurrentSchema {
        signature: Signature::any(0, Volatility::Immutable),
    })
}

#[derive(Debug)]
struct CurrentSchema {
    signature: Signature,
}

impl ScalarUDFImpl for CurrentSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "public".to_owned(),
        ))))
    }
}