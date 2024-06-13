/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
use arrow::{
    array::{as_list_array, Array, Float32Array, Float64Array, PrimitiveArray},
    datatypes::{DataType, Float32Type, Float64Type},
};
use datafusion::{
    common::{
        cast::as_fixed_size_list_array, plan_err, DataFusionError, Result as DataFusionResult,
    },
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use itertools::Itertools;
use std::{any::Any, ops::Index, sync::Arc};

// See: https://github.com/apache/datafusion/blob/888504a8da6d20f9caf3ecb6cd1a6b7d1956e23e/datafusion/expr/src/signature.rs#L36
pub const FIXED_SIZE_LIST_WILDCARD: i32 = i32::MIN;

#[derive(Debug)]
pub struct ArrayDistance {
    signature: Signature,
}

impl Default for ArrayDistance {
    fn default() -> Self {
        Self::new()
    }
}

/// [`ArrayDistance`] is a scalar UDF that calculates the Euclidean distance between elements in
/// [`DataType::FixedSizeList`] arrays with a numeric inner type. Limited support for
/// [`DataType::List`] is also provided.
///
/// For two [`DataType::FixedSizeList`], the inputs must have the same length, and have compatible
/// inner types. Compatible inner types are
///   - Both inputs are [`DataType::Float16`], [`DataType::Float32`], or [`DataType::Float64`].
/// The output will be the same type as both inputs.
///   - Two inputs of unequal type which are within: [`DataType::Float16`], [`DataType::Float32`],
/// and [`DataType::Float64`]. The output will be the less precise of the two inputs.
///   
/// Either [`DataType::FixedSizeList`] input may contain null elements (the resulting output for
/// that index will be null), however, all elements in a [`DataType::FixedSizeList`] must be
/// non-null.
///
/// When using an [`DataType::List`] input, only one of the two inputs can be so (i.e. the other
///  must be a [`DataType::FixedSizeList`]). The [`DataType::List`] will be converted to a
/// [`DataType::FixedSizeList`] of identical length. It can, like two [`DataType::FixedSizeList`],
/// have compatible inner types.
impl ArrayDistance {
    #[must_use]
    pub fn new() -> Self {
        let valid_types = [true, false]
            .iter()
            .cartesian_product([DataType::Float16, DataType::Float32, DataType::Float64])
            .flat_map(|(nullable, type_)| {
                vec![
                    DataType::new_fixed_size_list(
                        type_.clone(),
                        FIXED_SIZE_LIST_WILDCARD,
                        *nullable,
                    ),
                    DataType::new_list(type_.clone(), *nullable),
                    DataType::new_large_list(type_.clone(), *nullable),
                ]
            })
            .collect_vec();

        Self {
            signature: Signature::new(
                TypeSignature::Uniform(2, valid_types),
                Volatility::Immutable,
            ),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn convert_f64_to_f32(array: &PrimitiveArray<Float64Type>) -> PrimitiveArray<Float32Type> {
        let values = array.values();
        let converted_values: Vec<f32> = values.iter().map(|&x| x as f32).collect();
        Float32Array::from_iter_values(converted_values)
    }

    fn to_float32_array(input: &Arc<dyn Array>) -> Result<Vec<Float32Array>, DataFusionError> {
        as_fixed_size_list_array(input)?
            .iter()
            .map(|v| {
                v.ok_or_else(|| DataFusionError::Internal("no null entries allowed".into()))
                    .and_then(|vv| {
                        let binding = Arc::clone(&vv);
                        match binding.as_any().downcast_ref::<Float32Array>() {
                            Some(a) => Ok(a.clone()),
                            None => Err(DataFusionError::Internal("downcast failed".into())),
                        }
                    })
            })
            .collect::<Result<Vec<Float32Array>, DataFusionError>>()
    }
}

impl ScalarUDFImpl for ArrayDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_distance"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> DataFusionResult<DataType> {
        if args.len() != 2 {
            return plan_err!("array_distance takes exactly two arguments");
        }

        // Check that the two inputs are compatible and return their type
        let (f1, f2) = match (args[0].clone(), args[1].clone()) {
            (DataType::FixedSizeList(f1, size1), DataType::FixedSizeList(f2, size2)) => {
                if size1 != size2 {
                    return plan_err!("FixedSizeList arrays must have the same length");
                }
                (f1.data_type(), f2.data_type())
            },
            (DataType::FixedSizeList(f1, _size1), DataType::List(f2)) | 
            (DataType::List(f1), DataType::FixedSizeList(f2, _size1)) | 
            (DataType::LargeList(f1), DataType::FixedSizeList(f2, _size1)) | 
            (DataType::FixedSizeList(f1, _size1), DataType::LargeList(f2)) 
            => {
                (f1.data_type(), f2.data_type())
            },
            _ => {
                return plan_err!("Invalid combination of input types for 'array_distance'")
            }
        };

        if !f1.is_floating() || !f2.is_floating() {
            return plan_err!("array_distance only supports floating point types");
        }

        // Return the less precise of the Three types
        let float_types = [DataType::Float16, DataType::Float32, DataType::Float64];
        match [f1, f2].iter().filter_map(|&f| {
            float_types.iter().position(|t| t == f)
        }).min() {
            Some(i) => {
                Ok(float_types[i].clone())
            }
            None => plan_err!("Unexpected types in 'array_distance'"),
        }

    }

    // Basic implementation of the Euclidean distance between two arrays
    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let v1: Vec<Float32Array> = Self::to_float32_array(&args[0])?;
        let v2: Vec<Float32Array> = match args[1].data_type() {
            DataType::FixedSizeList(_, _) => Self::to_float32_array(&args[1])?,
            DataType::List(_) => {
                let lis = as_list_array(&args[1]);
                lis.iter()
                    .map(|v| {
                        v.ok_or_else(|| DataFusionError::Internal("no null entries allowed".into()))
                            .and_then(|vv| {
                                let binding = Arc::clone(&vv);
                                match binding.as_any().downcast_ref::<Float64Array>() {
                                    Some(a) => Ok(Self::convert_f64_to_f32(a)),
                                    None => {
                                        Err(DataFusionError::Internal("downcast failed".into()))
                                    }
                                }
                            })
                    })
                    .collect::<Result<Vec<Float32Array>, DataFusionError>>()?
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "second argument must be of type FixedSizeList or List".into(),
                ));
            }
        };

        let z = v1
            .iter()
            .zip(v2.iter())
            .map(|(a, b)| {
                if a.len() != b.len() {
                    return Err(DataFusionError::Internal(format!(
                        "arrays must have the same length {} != {}",
                        a.len(),
                        b.len()
                    )));
                }
                let mut sum: f32 = 0.0;
                for i in 0..a.len() {
                    sum += (a.value(i) - b.value(i)).powi(2);
                }
                Ok(sum.sqrt())
            })
            .collect::<DataFusionResult<Vec<f32>>>()?;

        Ok(ColumnarValue::Array(Arc::new(Float32Array::from(z))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, FixedSizeListArray, Float32Array, Float64Array, ListArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field},
    };
    use datafusion::{
        execution::context::SessionContext,
        logical_expr::{ColumnarValue, ScalarUDF},
    };

    use super::ArrayDistance;

    #[allow(clippy::float_cmp)]
    #[tokio::test]
    async fn test_basic() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let array_distance = ScalarUDF::from(ArrayDistance::new());
        ctx.register_udf(array_distance.clone());

        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Float32Array::try_new(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0].into(),
            None,
        )?;

        let list_array =
            FixedSizeListArray::try_new(Arc::clone(&field), 3_i32, Arc::new(values), None)?;

        let arc_list = Arc::new(list_array) as Arc<dyn Array>;

        let col_array = array_distance.invoke(&[
            ColumnarValue::Array(Arc::clone(&arc_list)),
            ColumnarValue::Array(Arc::clone(&arc_list)),
        ])?;

        let array_vec = ColumnarValue::values_to_arrays(&[col_array])?;
        let array = array_vec[0]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or("failed downcast of result")?;
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), 0.0);
        assert_eq!(array.value(1), 0.0);
        assert_eq!(array.value(2), 0.0);

        Ok(())
    }

    #[allow(clippy::float_cmp)]
    #[tokio::test]
    async fn test_list() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let array_distance = ScalarUDF::from(ArrayDistance::new());
        ctx.register_udf(array_distance.clone());

        let field = Arc::new(Field::new("item", DataType::Float32, false));
        let values = Float32Array::try_new(
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0].into(),
            None,
        )?;

        let list_array =
            FixedSizeListArray::try_new(Arc::clone(&field), 3_i32, Arc::new(values), None)?;

        let arc_array = Arc::new(list_array) as Arc<dyn Array>;

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 9]));
        let field2 = Arc::new(Field::new("item", DataType::Float64, true));
        let list = Arc::new(ListArray::new(
            field2,
            offsets,
            Arc::new(Float64Array::try_new(
                vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 1.0, 11.1].into(),
                None,
            )?),
            None,
        )) as Arc<dyn Array>;

        let col_array = array_distance.invoke(&[
            ColumnarValue::Array(Arc::clone(&arc_array)),
            ColumnarValue::Array(Arc::clone(&list)),
        ])?;

        let array_vec = ColumnarValue::values_to_arrays(&[col_array])?;
        let array = array_vec[0]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or("failed downcast of result")?;
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), 0.0);
        assert_eq!(array.value(1), 0.0);
        assert_eq!(array.value(2), 0.0);

        Ok(())
    }
}
