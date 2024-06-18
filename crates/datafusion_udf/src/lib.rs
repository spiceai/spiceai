use arrow::datatypes::DataType;
use datafusion::{
    common::{plan_err, Result as DataFusionResult},
    logical_expr::{
        type_coercion::functions::data_types, Accumulator, ColumnarValue, ScalarUDFImpl, Signature,
        Volatility,
    },
    physical_plan::{expressions::MaxAccumulator, expressions::MinAccumulator},
    scalar::ScalarValue,
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
            signature: Signature::variadic(
                SUPPORTED_COMPARISON_TYPES.to_vec(),
                Volatility::Immutable,
            ),
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

        for arg_type in arg_types {
            if !SUPPORTED_COMPARISON_TYPES.contains(arg_type) {
                return plan_err!("greatest does not support arg_type {arg_type}");
            }
        }

        let types = data_types(arg_types, self.signature())?;

        match types.first() {
            Some(t) => Ok(t.clone()),
            None => plan_err!("greatest cannot work out the return type"),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let try_new = |data_type: &DataType| {
            MaxAccumulator::try_new(data_type).map(|x| Box::new(x) as Box<dyn Accumulator>)
        };
        accumulator_min_max(self, args, try_new)
    }
}

#[derive(Debug)]
pub struct Least {
    signature: Signature,
}

impl Default for Least {
    fn default() -> Self {
        Self::new()
    }
}

impl Least {
    #[must_use]
    pub fn new() -> Self {
        Least {
            signature: Signature::variadic(
                SUPPORTED_COMPARISON_TYPES.to_vec(),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Least {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "least"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.len() < 2 {
            return plan_err!("least takes at least 2 argument");
        }

        for arg_type in arg_types {
            if !SUPPORTED_COMPARISON_TYPES.contains(arg_type) {
                return plan_err!("least does not support arg_type {arg_type}");
            }
        }

        let types = data_types(arg_types, self.signature())?;

        match types.first() {
            Some(t) => Ok(t.clone()),
            None => plan_err!("least cannot work out the return type"),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let try_new = |data_type: &DataType| {
            MinAccumulator::try_new(data_type).map(|x| Box::new(x) as Box<dyn Accumulator>)
        };
        accumulator_min_max(self, args, try_new)
    }
}

fn accumulator_min_max(
    udf: &dyn ScalarUDFImpl,
    args: &[ColumnarValue],
    create_accumulator: fn(
        &DataType,
    ) -> Result<Box<dyn Accumulator>, datafusion::error::DataFusionError>,
) -> Result<ColumnarValue, datafusion::error::DataFusionError> {
    let return_type = udf.return_type(
        &args
            .iter()
            .map(ColumnarValue::data_type)
            .collect::<Vec<_>>(),
    )?;

    let mut columns = vec![];

    for arg in args {
        let casted = arg.cast_to(&return_type, None)?;
        columns.push(casted);
    }

    let arrays = ColumnarValue::values_to_arrays(&columns)?;
    let size = arrays.first().map_or(0, |x| x.len());

    let mut values = vec![];
    for i in 0..(size) {
        let mut acc = create_accumulator(&return_type)?;
        let slices = arrays.iter().map(|arr| arr.slice(i, 1)).collect::<Vec<_>>();
        for slice in slices {
            acc.update_batch(&[slice])?;
        }

        values.push(acc.evaluate()?);
    }

    let array = ScalarValue::iter_to_array(values.into_iter())?;

    Ok(ColumnarValue::from(array))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, RecordBatch},
        datatypes::{Field, Schema},
    };
    use datafusion::{
        assert_batches_eq, datasource::MemTable, logical_expr::ScalarUDF, prelude::SessionContext,
    };

    use super::*;
    #[tokio::test]
    async fn test_basic() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();
        let greatest = ScalarUDF::from(Greatest::new());
        ctx.register_udf(greatest.clone());
        let least = ScalarUDF::from(Least::new());
        ctx.register_udf(least.clone());

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![0.9, 2.1, 3.0])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("t1", Arc::new(table))?;
        let sql = "SELECT greatest(a, 2), least(a, 2), greatest(a, b), least(a, b), greatest(a, b, 2), least(a, b, 2) from t1";
        let actual = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
            "+-------------------------+----------------------+---------------------+------------------+------------------------------+---------------------------+",
            "| greatest(t1.a,Int64(2)) | least(t1.a,Int64(2)) | greatest(t1.a,t1.b) | least(t1.a,t1.b) | greatest(t1.a,t1.b,Int64(2)) | least(t1.a,t1.b,Int64(2)) |",
            "+-------------------------+----------------------+---------------------+------------------+------------------------------+---------------------------+",
            "| 2                       | 1                    | 1.0                 | 0.9              | 2.0                          | 0.9                       |",
            "| 2                       | 2                    | 2.1                 | 2.0              | 2.1                          | 2.0                       |",
            "| 3                       | 2                    | 3.0                 | 3.0              | 3.0                          | 2.0                       |",
            "+-------------------------+----------------------+---------------------+------------------+------------------------------+---------------------------+",
            ],
            &actual
        );
        Ok(())
    }
}
