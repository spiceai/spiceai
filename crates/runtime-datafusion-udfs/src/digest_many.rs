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

use arrow::array::Array;
use arrow_schema::{DataType, Field, FieldRef};
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

#[derive(Debug, Default)]
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

    fn make_scalar_function_args(args: Vec<ColumnarValue>) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("ignored_name", DataType::Utf8, false)),
        }
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

            let dummy_rfa = ReturnFieldArgs {
                arg_fields: &[FieldRef::new(Field::new("dummy", DataType::Utf8, false))],
                scalar_arguments: &[Some(&ScalarValue::Utf8(Some(String::new())))],
            };

            hash_fn.return_field_from_args(dummy_rfa)
        } else {
            exec_err!("{DIGEST_UDF_NAME}: cannot determine return type")
        }
    }

    fn invoke_with_args(&self, scalar_args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let mut args = scalar_args.args;
        let hash_fn = Self::concrete_hash_function(args.pop())?;

        // Collect variadic args into one string for hashing
        let mut hash_me = String::with_capacity(32 * args.len());
        for arg in args {
            match arg {
                ColumnarValue::Array(array) => {
                    for buf in array.to_data().buffers() {
                        for b in buf.as_slice() {
                            write!(&mut hash_me, "{b:02X}")?;
                        }
                    }
                }

                ColumnarValue::Scalar(scalar) => write!(&mut hash_me, "{scalar}")?,
            }
        }

        hash_fn.invoke_with_args(Self::make_scalar_function_args(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(hash_me))),
        ]))
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
    use datafusion::common::Result as DataFusionResult;
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::{SessionContext, make_array, named_struct};
    use std::process::ExitCode;

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
