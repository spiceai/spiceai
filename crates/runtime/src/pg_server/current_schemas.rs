use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{plan_err, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CurrentSchemas {
        signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
    })
}

#[derive(Debug)]
struct CurrentSchemas {
    signature: Signature,
}

impl ScalarUDFImpl for CurrentSchemas {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_schemas"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        match &args[0] {
            ColumnarValue::Array(_) => {
                plan_err!("`current_schemas` expects a scalar boolean argument")
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(including_implicit))) => {
                let mut builder = ListBuilder::new(StringBuilder::with_capacity(2, 32));
                if *including_implicit {
                    builder.values().append_value("pg_catalog");
                }
                builder.values().append_value("public");
                builder.append(true);

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(None) | ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Null))
            }
            ColumnarValue::Scalar(scalar) => {
                plan_err!(
                    "`current_schemas` expects a boolean argument, actual: {}",
                    scalar.data_type()
                )
            }
        }
    }
}