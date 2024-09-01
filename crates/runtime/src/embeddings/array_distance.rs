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
    array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array},
    compute::{binary, cast, sum},
    datatypes::{ArrowPrimitiveType, DataType, Float64Type},
};

use datafusion::{
    common::{plan_err, DataFusionError, Result as DataFusionResult},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use itertools::Itertools;
use std::{any::Any, convert::From, sync::Arc};

// See: https://github.com/apache/datafusion/blob/888504a8da6d20f9caf3ecb6cd1a6b7d1956e23e/datafusion/expr/src/signature.rs#L36
pub const FIXED_SIZE_LIST_WILDCARD: i32 = i32::MIN;

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum VectorScalarType {
    Float64,
    Float32,
}

impl From<&VectorScalarType> for DataType {
    fn from(val: &VectorScalarType) -> Self {
        match val {
            VectorScalarType::Float64 => DataType::Float64,
            VectorScalarType::Float32 => DataType::Float32,
        }
    }
}

impl TryFrom<&DataType> for VectorScalarType {
    type Error = String;

    fn try_from(dtype: &DataType) -> Result<Self, Self::Error> {
        match dtype {
            DataType::Float64 => Ok(Self::Float64),
            DataType::Float32 => Ok(Self::Float32),
            other => Err(format!("{other:?} cannot be used as a VectorScalarType.")),
        }
    }
}

impl VectorScalarType {
    /// Returns the scalar type to return for  [`ArrayDistance`] calculations.
    #[must_use]
    pub fn array_distance_scalar_type(&self, other: &VectorScalarType) -> VectorScalarType {
        match (self, other) {
            (VectorScalarType::Float64, VectorScalarType::Float64) => VectorScalarType::Float64,
            (_, _) => VectorScalarType::Float32,
        }
    }
}

pub enum VectorBatchType {
    /// Represents a nested [`ColumnarValue`] of type [`DataType::List`] or [`DataType::LargeList`]. Inner values are of type [`DataType::FixedSizeList`].
    /// Values have non-constant number of entries, each entries is of `size` length.
    ListOfFixedVectors {
        size: i32,
        scalar_type: VectorScalarType,
    },

    /// Represents a nested [`ColumnarValue`] of type [`DataType::FixedSizeList`]. Inner values are also of type [`DataType::FixedSizeList`]
    /// Values have `count` entries, each with `size` length.
    FixedOfFixedVectors {
        count: i32,
        size: i32,
        scalar_type: VectorScalarType,
    },

    /// Represents a non-nested [`ColumnarValue`] of type [`DataType::List`] or [`DataType::LargeList`]. Inner values are also of type [`DataType::List`] or [`DataType::LargeList`].
    ListOfListVectors { scalar_type: VectorScalarType },

    /// Represents a non-nested [`ColumnarValue`] of type [`DataType::FixedSizeList`]. Inner values are of type [`DataType::List`] or [`DataType::LargeList`].
    FixedOfListVectors {
        count: i32,
        scalar_type: VectorScalarType,
    },

    /// Represents a non-nested [`ColumnarValue`] of type [`DataType::FixedSizeList`].
    FixedVectors {
        size: i32,
        scalar_type: VectorScalarType,
    },

    /// Represents a non-nested [`ColumnarValue`] of type [`DataType::List`] or [`DataType::LargeList`].
    ListVectors { scalar_type: VectorScalarType },
}

impl TryFrom<&DataType> for VectorBatchType {
    type Error = String;

    fn try_from(dtype: &DataType) -> Result<Self, Self::Error> {
        match dtype {
            DataType::FixedSizeList(f1, count) => match f1.data_type() {
                DataType::FixedSizeList(f2, size) => {
                    if Self::allowed_scalar_type(f2.data_type()) {
                        Ok(VectorBatchType::FixedOfFixedVectors {
                            count: *count,
                            size: *size,
                            scalar_type: VectorScalarType::try_from(f2.data_type())?,
                        })
                    } else {
                        Err(format!(
                            "{} is not a valid inner scalar type for VectorBatches",
                            f2.data_type()
                        ))
                    }
                }
                DataType::LargeList(f2) | DataType::List(f2) => {
                    if Self::allowed_scalar_type(f2.data_type()) {
                        Ok(VectorBatchType::FixedOfListVectors {
                            count: *count,
                            scalar_type: VectorScalarType::try_from(f2.data_type())?,
                        })
                    } else {
                        Err(format!(
                            "{} is not a valid inner scalar type for VectorBatches",
                            f2.data_type()
                        ))
                    }
                }
                DataType::Float32 | DataType::Float64 => Ok(VectorBatchType::FixedVectors {
                    size: *count, // for flat FixedSizeList, inner elements are numerics so size=count.
                    scalar_type: VectorScalarType::try_from(f1.data_type())?,
                }),
                _ => unimplemented!(),
            },
            DataType::LargeList(f1) | DataType::List(f1) => match f1.data_type() {
                DataType::FixedSizeList(f2, size) => {
                    if Self::allowed_scalar_type(f2.data_type()) {
                        Ok(VectorBatchType::ListOfFixedVectors {
                            size: *size,
                            scalar_type: VectorScalarType::try_from(f2.data_type())?,
                        })
                    } else {
                        Err(format!(
                            "{} is not a valid inner scalar type for VectorBatches",
                            f2.data_type()
                        ))
                    }
                }
                DataType::LargeList(f2) | DataType::List(f2) => {
                    if Self::allowed_scalar_type(f2.data_type()) {
                        Ok(VectorBatchType::ListOfListVectors {
                            scalar_type: VectorScalarType::try_from(f2.data_type())?,
                        })
                    } else {
                        Err(format!(
                            "{} is not a valid inner scalar type for VectorBatches",
                            f2.data_type()
                        ))
                    }
                }
                DataType::Float32 | DataType::Float64 => Ok(VectorBatchType::ListVectors {
                    scalar_type: VectorScalarType::try_from(f1.data_type())?,
                }),
                _ => unimplemented!(),
            },
            _ => Err(format!("{dtype} is not a VectorBatchType.")),
        }
    }
}

impl VectorBatchType {
    fn allowed_scalar_type(dtype: &DataType) -> bool {
        matches!(dtype, DataType::Float32) || matches!(dtype, DataType::Float64)
    }

    /// Returns the [`ScalarUDFImpl::return_type`] for the [`ArrayDistance`] UDF. If None, the two [`VectorBatchType`]s are incompatible.
    pub fn array_distance_return_type(&self, other: &VectorBatchType) -> Result<DataType, String> {
        match (self, other) {
            (
                Self::FixedVectors {
                    scalar_type: s1,
                    size: len1,
                },
                Self::FixedVectors {
                    scalar_type: s2,
                    size: len2,
                },
            ) => {
                if len1 != len2 {
                    return Err(format!(
                        "FixedVectors must have the same length, found {len1}!={len2}."
                    ));
                }
                Ok((&s1.array_distance_scalar_type(s2)).into())
            }
            (
                Self::FixedVectors {
                    scalar_type: s1, ..
                },
                Self::ListVectors {
                    scalar_type: s2, ..
                },
            )
            | (
                Self::ListVectors {
                    scalar_type: s1, ..
                },
                Self::FixedVectors {
                    scalar_type: s2, ..
                },
            ) => Ok((&s1.array_distance_scalar_type(s2)).into()),
            (_, _) => Err("Only Flat vectors currently supported.".into()),
        }
    }

    fn scalar_type(&self) -> VectorScalarType {
        match self {
            Self::ListOfFixedVectors { scalar_type, .. }
            | Self::FixedOfFixedVectors { scalar_type, .. }
            | Self::FixedOfListVectors { scalar_type, .. }
            | Self::FixedVectors { scalar_type, .. }
            | Self::ListOfListVectors { scalar_type, .. }
            | Self::ListVectors { scalar_type, .. } => *scalar_type,
        }
    }
}

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
///
/// The output will be the same type as both inputs.
///   - Two inputs of unequal type which are within: [`DataType::Float16`], [`DataType::Float32`],
///     and [`DataType::Float64`]. The output will be the less precise of the two inputs.
///
/// Either [`DataType::FixedSizeList`] input may contain null elements (the resulting output for
/// that index will be null), however, all elements in a [`DataType::FixedSizeList`] must be
/// non-null.
///
/// ```
/// [[1, 2], null, [3, 4]] // Valid
/// [[1, 2], [3, null], [5, 6]] // Invalid
/// ```
///
/// When using an [`DataType::List`] input, only one of the two inputs can be so (i.e. the other
///  must be a [`DataType::FixedSizeList`]). The [`DataType::List`] will be converted to a
/// [`DataType::FixedSizeList`] of identical length. It can, like two [`DataType::FixedSizeList`],
/// have compatible inner types.
///
/// ## Nested Array Support
///
/// ### Nested Rules
/// `array_distance`([A, B], E) = [`array_distance`(A, E), `array_distance`(B, E)]
/// `array_distance`(E, [A, B]) = [`array_distance`(E, A), `array_distance`(E, B)]
/// `array_distance`([A, B], [C, D]) = [`array_distance`(A, C), `array_distance`(B, D)]
/// `array_distance`([A, B], [C, D, E]); invalid
impl ArrayDistance {
    #[must_use]
    pub fn new() -> Self {
        let valid_types = [true, false]
            .iter()
            .cartesian_product([DataType::Float32, DataType::Float64])
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

        // Force one of the two inputs to be a fixed length vector.
        let valid_signatures = valid_types
            .clone()
            .iter()
            .cartesian_product(valid_types)
            .filter(|(a, b)| {
                matches!(a, DataType::FixedSizeList(_, _))
                    || matches!(b, DataType::FixedSizeList(_, _))
            })
            .map(|(a, b)| TypeSignature::Exact(vec![a.clone(), b.clone()]))
            .collect_vec();

        Self {
            signature: Signature::one_of(valid_signatures, Volatility::Immutable),
        }
    }

    /// Returns the less precise Float16/32/64 of the two input types.
    fn least_precise_float_type(t1: &DataType, t2: &DataType) -> DataFusionResult<DataType> {
        let float_types = [DataType::Float32, DataType::Float64];
        let i1 = float_types
            .iter()
            .position(|t| t == t1)
            .ok_or_else(|| DataFusionError::Internal(format!("{t1} is not a float type")))?;
        let i2 = float_types
            .iter()
            .position(|t| t == t2)
            .ok_or_else(|| DataFusionError::Internal(format!("{t2} is not a float type")))?;

        float_types
            .get(i1.min(i2))
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("Unexpected error".into()))
    }

    /// Casts the [`ArrayRef`] to a [`FixedSizeListArray`].
    fn downcast_to_fixed_size_list(value: &ArrayRef) -> DataFusionResult<FixedSizeListArray> {
        value
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("downcast unexpectedly failed".into()))
    }

    /// Casts the input to a [`Float64Array`].
    fn cast_to_float64_array(value: &ArrayRef) -> DataFusionResult<Float64Array> {
        cast(value, &Float64Type::DATA_TYPE)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("downcast to Float64Array failed".into()))
    }

    /// Computes the L2 squared distance of two non-nested (i.e. not a `FixedSizeList`[`FixedSizeList`[f32]]) list types.
    #[allow(clippy::cast_possible_truncation)]
    fn calculate_distance(
        v1: &ArrayRef,
        v2: &ArrayRef,
        output_scalar_type: VectorScalarType,
    ) -> DataFusionResult<ArrayRef> {
        let z1 = Self::downcast_to_fixed_size_list(v1)?;
        let z2 = Self::downcast_to_fixed_size_list(v2)?;

        let result: DataFusionResult<Vec<Option<f64>>> = z1
            .iter()
            .zip(z2.iter())
            .map(|(a, b)| match (a, b) {
                (Some(a), Some(b)) => {
                    let z: Float64Array = binary(
                        &Self::cast_to_float64_array(&a)?,
                        &Self::cast_to_float64_array(&b)?,
                        |x, y| (x - y).powi(2),
                    )
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    Ok(sum(&z))
                }
                _ => Ok(None),
            })
            .collect();

        match output_scalar_type {
            VectorScalarType::Float32 => Ok(Arc::new(Float32Array::from(
                result
                    .iter()
                    .flat_map(|opt| {
                        opt.iter()
                            // Explicitly Okay with precision loss. Was either f32 to begin with, or the other input is only f32.
                            .map(|v| v.and_then(|f| Some(f as f32)))
                            .collect_vec()
                    })
                    .collect::<Vec<Option<f32>>>(),
            ))),
            VectorScalarType::Float64 => Ok(Arc::new(Float64Array::from(result?))),
        }
    }

    fn cast_to_fixed_size_list(
        array: &Arc<dyn Array>,
        data_type: &DataType,
        length: i32,
    ) -> DataFusionResult<Arc<dyn Array>> {
        cast(
            array,
            &DataType::new_fixed_size_list(data_type.clone(), length, true),
        )
        .map_err(|e| DataFusionError::Internal(e.to_string()))
    }

    /// Ensures both inputs are [`DataType::FixedSizeList`] with the same length and compatible inner Float types.
    fn cast_input_args(
        v1: &Arc<dyn Array>,
        v2: &Arc<dyn Array>,
    ) -> DataFusionResult<(Arc<dyn Array>, Arc<dyn Array>)> {
        match (v1.data_type(), v2.data_type()) {
            (DataType::FixedSizeList(f1, l1), DataType::FixedSizeList(f2, l2)) => {
                if f1.data_type() == f2.data_type() {
                    Ok((Arc::clone(v1), Arc::clone(v2)))
                } else {
                    let output_type =
                        Self::least_precise_float_type(f1.data_type(), f2.data_type())?;
                    Ok(if *f1.data_type() == output_type {
                        (
                            Arc::clone(v1),
                            Self::cast_to_fixed_size_list(v2, &output_type, *l2)?,
                        )
                    } else {
                        (
                            Self::cast_to_fixed_size_list(v1, &output_type, *l1)?,
                            Arc::clone(v2),
                        )
                    })
                }
            }
            (DataType::FixedSizeList(f1, length), DataType::LargeList(f2) | DataType::List(f2))
            | (DataType::LargeList(f1) | DataType::List(f1), DataType::FixedSizeList(f2, length)) =>
            {
                let output_type = Self::least_precise_float_type(f1.data_type(), f2.data_type())?;
                Ok(if matches!(v1.data_type(), DataType::FixedSizeList(_, _)) {
                    (
                        Arc::clone(v1),
                        Self::cast_to_fixed_size_list(v2, &output_type, *length)?,
                    )
                } else {
                    (
                        Self::cast_to_fixed_size_list(v1, &output_type, *length)?,
                        Arc::clone(v2),
                    )
                })
            }
            _ => Err(DataFusionError::Internal(
                "At least one of the two inputs must be a [`DataType::FixedSizeList`]".into(),
            )),
        }
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

        let type1 = VectorBatchType::try_from(&args[0])
            .map_err(|e| DataFusionError::Plan(format!("Inappropriate first argument: {e}")))?;
        let type2 = VectorBatchType::try_from(&args[1])
            .map_err(|e| DataFusionError::Plan(format!("Inappropriate second argument: {e}")))?;

        type1.array_distance_return_type(&type2).map_err(|e| {
            DataFusionError::Plan(format!(
                "{} and {} are incompatible for {}. Error: {e}",
                &args[0],
                &args[1],
                self.name()
            ))
        })
    }

    #[allow(clippy::cast_possible_truncation)]
    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let (v1, v2) = Self::cast_input_args(&arrays[0], &arrays[1])?;

        // Raise [`DataFusionError::Internal`] as type issues should have been resolved in [`ArrayDistance::return_type`].
        let type1 = VectorBatchType::try_from(v1.data_type())
            .map_err(|e| DataFusionError::Internal(format!("Inappropriate first argument: {e}")))?;
        let type2 = VectorBatchType::try_from(v2.data_type()).map_err(|e| {
            DataFusionError::Internal(format!("Inappropriate second argument: {e}"))
        })?;
        let scalar_type = type1
            .scalar_type()
            .array_distance_scalar_type(&type2.scalar_type());

        // TODO: this only handles non-nested v1, v2.
        let arr = Self::calculate_distance(&v1, &v2, scalar_type)?;

        Ok(ColumnarValue::Array(arr))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, ListArray},
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
    async fn test_f32_f64() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let array_distance = ScalarUDF::from(ArrayDistance::new());
        ctx.register_udf(array_distance.clone());

        let f32 = FixedSizeListArray::try_new(
            Arc::clone(&Arc::new(Field::new("item", DataType::Float32, false))),
            3_i32,
            Arc::new(Float32Array::try_new(
                vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0].into(),
                None,
            )?),
            None,
        )?;

        let f64 = FixedSizeListArray::try_new(
            Arc::clone(&Arc::new(Field::new("item", DataType::Float64, false))),
            3_i32,
            Arc::new(Float64Array::try_new(
                vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0].into(),
                None,
            )?),
            None,
        )?;

        let col_array = array_distance.invoke(&[
            ColumnarValue::Array(Arc::clone(&Arc::new(f32)) as ArrayRef),
            ColumnarValue::Array(Arc::clone(&Arc::new(f64)) as ArrayRef),
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
    async fn test_f64_f64() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let array_distance = ScalarUDF::from(ArrayDistance::new());
        ctx.register_udf(array_distance.clone());

        let field = Arc::new(Field::new("item", DataType::Float64, false));
        let values = Float64Array::try_new(
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
            .downcast_ref::<Float64Array>()
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
