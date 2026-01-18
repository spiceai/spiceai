/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow::array::{Array, ArrayRef, StringBuilder, StringViewArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::config::ConfigOptions;
use datafusion::functions::crypto;
use datafusion::logical_expr::{
    DocSection, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, lit,
};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::{Result as DataFusionResult, exec_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use std::any::Any;
use std::fmt::{Debug, Write};
use std::sync::{Arc, LazyLock};

pub static DIGEST_UDF_NAME: &str = "digest_many";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation {
    doc_section: DocSection::default(),
    description: "Emits a digest with the chosen function atop multiple columns of varying types by hashing their string representations".to_string(),
    syntax_example: "digest_many(col_a, col_b, ..., digest_function_name)".to_string(),
    sql_example: Some("SELECT digest_many(col_a, col_b, 'md5')".to_string()),
    arguments: Some(vec![
        (
            "col".to_string(),
            "The columns to yield to the hasher".to_string(),
        ),
        (
            "digest_function_name".to_string(),
            "A Datafusion hashing function".to_string(),
        ),
    ]),
    alternative_syntax: None,
    related_udfs: None,
}
});

pub static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Stable));

pub static INSTANCE: LazyLock<ScalarUDF> = LazyLock::new(|| DigestMany::default().into());

#[derive(Debug, Default, Hash, PartialEq, Eq)]
pub struct DigestMany {}

impl DigestMany {
    fn concrete_hash_function(value: Option<ColumnarValue>) -> DataFusionResult<Arc<ScalarUDF>> {
        let Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(fn_name)))) = value else {
            return exec_err!(
                "{DIGEST_UDF_NAME}: digest function value must be a string, given: {value:?}"
            );
        };

        if let Some(udf) = crypto::functions().iter().find(|f| f.name() == fn_name) {
            Ok(Arc::clone(udf))
        } else {
            exec_err!("{DIGEST_UDF_NAME}: digest function {fn_name} not found")
        }
    }

    fn make_scalar_function_args(
        args: Vec<ColumnarValue>,
        return_field: FieldRef,
    ) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            number_rows: 1,
            arg_fields: vec![],
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn get_hash_fn_return_field(hash_fn: &ScalarUDF) -> DataFusionResult<FieldRef> {
        hash_fn.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[FieldRef::new(Field::new("dummy", DataType::Utf8, false))],
            scalar_arguments: &[Some(&ScalarValue::Utf8(Some(String::new())))],
        })
    }
}

impl ScalarUDFImpl for DigestMany {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        DIGEST_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        exec_err!(
            "{DIGEST_UDF_NAME}: return type is input-dependent. Use return_field_from_args instead."
        )
    }

    // Delegate this to the underlying hash function, as it may want to return {Binary, Utf8, Utf8View}
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        if let Some(Some(scalar_value)) = args.scalar_arguments.last() {
            let hash_fn =
                Self::concrete_hash_function(Some(ColumnarValue::Scalar((*scalar_value).clone())))?;

            Self::get_hash_fn_return_field(&hash_fn)
        } else {
            exec_err!("{DIGEST_UDF_NAME}: cannot determine return type")
        }
    }

    fn invoke_with_args(&self, scalar_args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let mut args = scalar_args.args;
        let hash_fn = Self::concrete_hash_function(args.pop())?;

        // All scalars - process as before
        if args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            let mut hash_me = String::with_capacity(32 * args.len());
            for arg in args {
                if let ColumnarValue::Scalar(scalar) = arg {
                    write!(&mut hash_me, "{scalar}")?;
                }
            }

            // Get the correct return field from the hash function (e.g., md5 returns Utf8View in DataFusion v51+)
            let return_field = Self::get_hash_fn_return_field(&hash_fn)?;

            return hash_fn.invoke_with_args(Self::make_scalar_function_args(
                vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash_me)))],
                return_field,
            ));
        }

        // We have arrays - need to process row by row
        let Some(num_rows) = args.iter().find_map(|arg| match arg {
            ColumnarValue::Array(arr) if !arr.is_empty() => Some(arr.len()),
            ColumnarValue::Array(_) | ColumnarValue::Scalar(_) => None,
        }) else {
            return Ok(ColumnarValue::Array(Arc::new(StringViewArray::new_null(0))));
        };

        // Pre-allocate concatenated strings buffer with estimated capacity
        let estimated_row_size = args.len() * 16;
        let mut concatenated_builder =
            StringBuilder::with_capacity(num_rows, num_rows * estimated_row_size);

        // Reusable buffer for row concatenation (avoids per-row allocation)
        let mut row_buffer = String::with_capacity(estimated_row_size);

        // Build concatenated strings for all rows
        // This batches the string building, then delegates to hash function for vectorized hashing
        for row_idx in 0..num_rows {
            row_buffer.clear(); // Keeps allocated capacity (allocation minimization)

            for arg in &args {
                match arg {
                    ColumnarValue::Array(array) => {
                        let scalar = ScalarValue::try_from_array(array, row_idx)?;
                        write!(&mut row_buffer, "{scalar}")?;
                    }
                    ColumnarValue::Scalar(scalar) => {
                        write!(&mut row_buffer, "{scalar}")?;
                    }
                }
            }

            concatenated_builder.append_value(&row_buffer);
        }

        let concatenated_array = Arc::new(concatenated_builder.finish()) as ArrayRef;

        // Query the hash function's return field (e.g., md5 returns Utf8View in DataFusion v51+)
        let return_field = Self::get_hash_fn_return_field(&hash_fn)?;

        // Hash entire array in one call - hash function can leverage SIMD internally
        // This is more efficient than N separate hash calls for N rows
        hash_fn.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(concatenated_array)],
            number_rows: num_rows,
            arg_fields: vec![],
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

pub fn digest_many(args: Vec<Expr>, digest: &str) -> Expr {
    let mut args = args;
    args.push(lit(digest));
    INSTANCE.call(args)
}

#[cfg(test)]
mod tests {
    use crate::digest_many::{DigestMany, digest_many};

    use arrow::array::record_batch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result as DataFusionResult;
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::{SessionContext, make_array, named_struct};
    use std::process::ExitCode;

    #[tokio::test]
    async fn test_digest_many_record_batch() -> DataFusionResult<ExitCode> {
        let ctx = SessionContext::new();
        ctx.register_udf(DigestMany::default().into());
        let _ = ctx.register_batch(
            "tbl",
            record_batch!(
                ("a", Int32, [1, 2, 3, 4, 5, 6]),
                (
                    "b",
                    Float64,
                    [Some(4.0), None, Some(5.0), Some(6.0), Some(7.0), Some(8.0)]
                ),
                (
                    "c",
                    Utf8,
                    ["alpha", "beta", "gamma", "alpha", "beta", "gamma"]
                )
            )
            .expect("couldn't make record batch"),
        );

        let data = ctx
            .sql("select a, b, c, digest_many(a, b, c, 'md5') as 'digest_many(a, b, c)', digest_many(c, 'md5') as 'digest_many(c)', digest_many(c, 'foo', 'md5') as 'digest_many(c, ''foo'')' from tbl")
            .await
            .expect("failed to prepare SQL")
            .collect()
            .await
            .expect("failed to prepare SQL");
        insta::assert_snapshot!(
            pretty_format_batches(data.as_slice()).expect("couldn't format batches"),
            @r"
        +---+-----+-------+----------------------------------+----------------------------------+----------------------------------+
        | a | b   | c     | digest_many(a, b, c)             | digest_many(c)                   | digest_many(c, 'foo')            |
        +---+-----+-------+----------------------------------+----------------------------------+----------------------------------+
        | 1 | 4.0 | alpha | 3409f2c75ffd509c8984bbb074f0e04d | 2c1743a391305fbf367df8e4f069f9f9 | 8784bced698bc929e46475089cb0f674 |
        | 2 |     | beta  | 546bdc9d2972f2665650d628b4da0bb6 | 987bcab01b929eb2c07877b224215c92 | 16d4e759f170a3cd0928427fe29e41a1 |
        | 3 | 5.0 | gamma | 82ff28e3c0dd00b848f98ea90fc99a39 | 05b048d7242cb7b8b57cfa3b1d65ecea | 09a0d53b795ebe42be507eb8e36bffc3 |
        | 4 | 6.0 | alpha | d96dc6ee83c54921044e6f823fb54359 | 2c1743a391305fbf367df8e4f069f9f9 | 8784bced698bc929e46475089cb0f674 |
        | 5 | 7.0 | beta  | 896b722fd78bf5fe3e33d5baa96cbd88 | 987bcab01b929eb2c07877b224215c92 | 16d4e759f170a3cd0928427fe29e41a1 |
        | 6 | 8.0 | gamma | 5e53bce9cc7a3eaaca70d58d04947043 | 05b048d7242cb7b8b57cfa3b1d65ecea | 09a0d53b795ebe42be507eb8e36bffc3 |
        +---+-----+-------+----------------------------------+----------------------------------+----------------------------------+
        "
        );

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test]
    async fn test_digest_many() -> DataFusionResult<ExitCode> {
        let ctx = SessionContext::new();
        ctx.register_udf(DigestMany::default().into());

        let exprs_to_hash = [
            lit("abc"),
            lit(123),
            lit(123.0),
            make_array(vec![lit("a"), lit("b"), lit("c")]),
            make_array(vec![lit(1), lit(2), lit(3)]),
            make_array(vec![lit(1.0), lit(2.0), lit(3.0)]),
            named_struct(vec![
                lit("k1"),
                lit("v1"),
                lit("k2"),
                lit("v2"),
                lit("k3"),
                lit(3.0),
            ]),
            make_array(vec![
                named_struct(vec![
                    lit("k1"),
                    lit("v1"),
                    lit("k2"),
                    lit("v2"),
                    lit("k3"),
                    lit(3.0),
                ]),
                named_struct(vec![
                    lit("k1"),
                    lit("v1"),
                    lit("k2"),
                    lit("v2"),
                    lit("k3"),
                    lit(3.0),
                ]),
            ]),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, e)| e.alias(format!("c{i}")))
        .collect::<Vec<_>>();

        // All supported core Datafusion hash functions
        let hash_functions = ["md5", "sha224", "sha256", "sha384", "sha512"];

        let hash_exprs = hash_functions
            .into_iter()
            .map(|fn_name| {
                exprs_to_hash
                    .iter()
                    .map(|c| c.name_for_alias().map(col))
                    .collect::<DataFusionResult<Vec<_>>>()
                    .map(|exprs| digest_many(exprs, fn_name).alias(fn_name))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        let df_a = ctx
            .read_empty()?
            .select(exprs_to_hash.clone())?
            .select(hash_exprs.clone())?;

        let df_b = ctx
            .read_empty()?
            .select(exprs_to_hash.clone())?
            .select(hash_exprs.clone())?;

        // Running with same inputs should produce same outputs
        assert_eq!(df_a.to_string().await?, df_b.to_string().await?);

        Ok(ExitCode::SUCCESS)
    }
}
