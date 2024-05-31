use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Builder, NullArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_list_array;
use datafusion::common::{
    not_impl_err, plan_datafusion_err, plan_err, Result as DFResult, ScalarValue,
};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

pub fn create_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ArrayUpper {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug)]
struct ArrayUpper {
    signature: Signature,
}

impl ScalarUDFImpl for ArrayUpper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_upper"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        match &args[0] {
            ColumnarValue::Array(array) => {
                let anyarrays = as_list_array(array).map_err(|_| {
                    plan_datafusion_err!(
                        "first argument of `array_upper` must be an array, actual: {}",
                        array.data_type()
                    )
                })?;

                match &args[1] {
                    ColumnarValue::Array(_) => {
                        plan_err!("second argument of `array_upper` must be a scalar")
                    }
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(dimension))) => {
                        match dimension.cmp(&1) {
                            Ordering::Less => {
                                Ok(ColumnarValue::Array(Arc::new(NullArray::new(
                                    anyarrays.len(),
                                ))))
                            }
                            Ordering::Equal => {
                                let mut builder = Int64Builder::with_capacity(anyarrays.len());
                                for anyarray in anyarrays.iter() {
                                    match anyarray {
                                        None => builder.append_null(),
                                        Some(anyarray) => {
                                            builder.append_value(anyarray.len() as i64)
                                        }
                                    }
                                }
                                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                            }
                            Ordering::Greater => {
                                not_impl_err!(
                                    "argument dimension > 1 is not supported right now, actual: {}",
                                    dimension
                                )
                            }
                        }
                    }
                    ColumnarValue::Scalar(scalar) => {
                        plan_err!(
                            "second argument of `array_upper` must be an integer, actual: {}",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Scalar(ScalarValue::List(anyarray)) => {
                match &args[1] {
                    ColumnarValue::Array(_) => {
                        plan_err!("second argument of `array_upper` must be a scalar")
                    }
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(dimension))) => {
                        match dimension.cmp(&1) {
                            Ordering::Less => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
                            Ordering::Equal => {
                                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                                    anyarray.len() as i64,
                                ))))
                            }
                            Ordering::Greater => {
                                not_impl_err!(
                                    "argument dimension > 1 is not supported right now, actual: {}",
                                    dimension
                                )
                            }
                        }
                    }
                    ColumnarValue::Scalar(scalar) => {
                        plan_err!(
                            "second argument of `array_upper` must be an integer, actual: {}",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Scalar(ScalarValue::Null) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Null))
            }
            ColumnarValue::Scalar(scalar) => {
                plan_err!(
                    "first argument of `array_upper` must be an array, actual: {}",
                    scalar.data_type()
                )
            }
        }
    }
}