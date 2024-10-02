use arrow::array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, Float64, LargeList, List};
use datafusion::common::cast::{
    as_float32_array, as_float64_array, as_generic_list_array, as_int32_array,
    as_int64_array,
};
use datafusion::common::utils::coerced_fixed_size_list_to_list;
use datafusion::scalar::ScalarValue;
use datafusion::{
     common::{exec_err, DataFusionError, Result as DataFusionResult},
     logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
};
use core::any::type_name;
    
use std::any::Any;
use std::sync::Arc;

/// returns the Euclidean distance between two numeric arrays."]
pub fn cosine_distance(array: datafusion::logical_expr::Expr) -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::ScalarFunction(
        datafusion::logical_expr::expr::ScalarFunction::new_udf(
            cosine_distance_udf(),
            vec![array],
        )
    )
}

macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast to {}",
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

/// Singleton instance of [` CosineDistance`], ensures the UDF is only created once
/// named STATIC_ CosineDistance. For example `STATIC_ArrayToString`
#[allow(non_upper_case_globals)]
static STATIC_CosineDistance: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
    std::sync::OnceLock::new();

/// ScalarFunction that returns a [`ScalarUDF`](datafusion::logical_expr::ScalarUDF) for  CosineDistance.
pub fn cosine_distance_udf() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
    STATIC_CosineDistance
        .get_or_init(|| {
            std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                < CosineDistance>::new(),
            ))
        })
        .clone()
}

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(inner: F) -> impl Fn(&[ColumnarValue]) -> DataFusionResult<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> DataFusionResult<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        // If all inputs are scalar, keeps output as scalar
        if len.is_none() {
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

#[derive(Debug)]
pub(super) struct  CosineDistance {
    signature: Signature,
}

impl  CosineDistance {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for  CosineDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cosine_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Ok(Float64),
            _ => exec_err!("The cosine_distance function can only accept List/LargeList/FixedSizeList."),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("cosine_distance expects exactly two arguments");
        }
        let mut result = Vec::new();
        for arg_type in arg_types {
            match arg_type {
                List(_) | LargeList(_) | FixedSizeList(_, _) => result.push(coerced_fixed_size_list_to_list(arg_type)),
                _ => return exec_err!("The cosine_distance function can only accept List/LargeList/FixedSizeList."),
            }
        }

        Ok(result)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(cosine_distance_inner)(args)
    }
}

pub fn cosine_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("cosine_distance expects exactly two arguments");
    }

    match (&args[0].data_type(), &args[1].data_type()) {
        (List(_), List(_)) => general_cosine_distance::<i32>(args),
        (LargeList(_), LargeList(_)) => general_cosine_distance::<i64>(args),
        (array_type1, array_type2) => {
            exec_err!("cosine_distance does not support types '{array_type1:?}' and '{array_type2:?}'")
        }
    }
}

fn general_cosine_distance<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let result = list_array1
        .iter()
        .zip(list_array2.iter())
        .map(|(arr1, arr2)| compute_cosine_distance(arr1, arr2))
        .collect::<DataFusionResult<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Computes the Euclidean distance between two arrays
fn compute_cosine_distance(
    arr1: Option<ArrayRef>,
    arr2: Option<ArrayRef>,
) -> DataFusionResult<Option<f64>> {
    let value1 = match arr1 {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let value2 = match arr2 {
        Some(arr) => arr,
        None => return Ok(None),
    };

    let mut value1 = value1;
    let mut value2 = value2;

    loop {
        match value1.data_type() {
            List(_) => {
                if downcast_arg!(value1, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value1 = downcast_arg!(value1, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value1, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value1 = downcast_arg!(value1, LargeListArray).value(0);
            }
            _ => break,
        }

        match value2.data_type() {
            List(_) => {
                if downcast_arg!(value2, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value2 = downcast_arg!(value2, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value2, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value2 = downcast_arg!(value2, LargeListArray).value(0);
            }
            _ => break,
        }
    }

    // Check for NULL values inside the arrays
    if value1.null_count() != 0 || value2.null_count() != 0 {
        return Ok(None);
    }

    let values1 = convert_to_f64_array(&value1)?;
    let values2 = convert_to_f64_array(&value2)?;

    if values1.len() != values2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    let mut a1_length: f64 = 0.0;
    let mut a2_length: f64 = 0.0;

    let sum_squares: f64 = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| {
            let a = v1.unwrap_or(0.0);
            let b = v2.unwrap_or(0.0);

            a1_length += a * a;
            a2_length += b * b;

            a * b 
            
        })
        .sum();

    let a1_norm = (a1_length * a1_length).sqrt();
    let a2_norm = (a2_length * a2_length).sqrt();

    Ok(Some(sum_squares/(a1_norm * a2_norm)))
}

/// Converts an array of any numeric type to a Float64Array.
fn convert_to_f64_array(array: &ArrayRef) -> DataFusionResult<Float64Array> {
    match array.data_type() {
        DataType::Float64 => Ok(as_float64_array(array)?.clone()),
        DataType::Float32 => {
            let array = as_float32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int64 => {
            let array = as_int64_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int32 => {
            let array = as_int32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        _ => exec_err!("Unsupported array type for conversion to Float64Array"),
    }
}