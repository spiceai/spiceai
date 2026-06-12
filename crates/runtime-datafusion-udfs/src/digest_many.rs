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

    /// Append one column value to a row's pre-hash buffer using a length-prefixed,
    /// NULL-distinct encoding so that the concatenation is injective: distinct tuples
    /// always map to distinct strings, regardless of where the column boundaries fall.
    ///
    /// The previous implementation wrote each value's `ScalarValue` Display into one
    /// buffer with no separator and no NULL marker. That made the per-row digest input
    /// ambiguous: `('ab', 'c')` and `('a', 'bc')` produced the same string, integer
    /// composite keys `(1, 23)` and `(12, 3)` collided, and a SQL NULL (Display `"NULL"`)
    /// was indistinguishable from the literal string `'NULL'`. Because `digest_many`
    /// backs the RRF fusion row identity (`crates/runtime/src/search/rrf.rs`), a single
    /// such collision silently fuses two distinct documents into one — merging their
    /// ranks and dropping/misattributing one of them.
    ///
    /// Encoding: a NULL value is written as a bare `N`; a non-NULL value `v` is written
    /// as `<byte_len>:<v>`. A length prefix always begins with a digit, so it can never
    /// be confused with the `N` NULL marker, and the explicit byte length makes the
    /// boundary between adjacent values unambiguous even when a value contains digits,
    /// colons, or the literal text `N`.
    fn append_value(
        buffer: &mut String,
        scratch: &mut String,
        scalar: &ScalarValue,
    ) -> DataFusionResult<()> {
        if scalar.is_null() {
            buffer.push('N');
            return Ok(());
        }
        scratch.clear();
        write!(scratch, "{scalar}")?;
        write!(buffer, "{}:{scratch}", scratch.len())?;
        Ok(())
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
            let mut scratch = String::new();
            for arg in args {
                if let ColumnarValue::Scalar(scalar) = arg {
                    Self::append_value(&mut hash_me, &mut scratch, &scalar)?;
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

        // Reusable buffers for row concatenation (avoids per-row allocation)
        let mut row_buffer = String::with_capacity(estimated_row_size);
        let mut scratch = String::with_capacity(16);

        // Build concatenated strings for all rows
        // This batches the string building, then delegates to hash function for vectorized hashing
        for row_idx in 0..num_rows {
            row_buffer.clear(); // Keeps allocated capacity (allocation minimization)

            for arg in &args {
                match arg {
                    ColumnarValue::Array(array) => {
                        let scalar = ScalarValue::try_from_array(array, row_idx)?;
                        Self::append_value(&mut row_buffer, &mut scratch, &scalar)?;
                    }
                    ColumnarValue::Scalar(scalar) => {
                        Self::append_value(&mut row_buffer, &mut scratch, scalar)?;
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

    use arrow::array::{Array, RecordBatch, StringArray, record_batch};
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::DataType;
    use datafusion::common::Result as DataFusionResult;
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::{SessionContext, make_array, named_struct};
    use std::process::ExitCode;

    /// Extract a digest column's values as `Option<String>`, casting from whatever
    /// string type the hash function returned (md5 returns `Utf8View` in `DataFusion`
    /// v51+) so the assertion does not depend on the concrete Arrow string variant.
    fn digest_values(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let array = batch
            .column_by_name(column)
            .expect("digest column is present");
        let utf8 = arrow::compute::cast(array, &DataType::Utf8).expect("cast digest to Utf8");
        let strings = utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("digest column casts to StringArray");
        (0..strings.len())
            .map(|i| {
                if strings.is_null(i) {
                    None
                } else {
                    Some(strings.value(i).to_string())
                }
            })
            .collect()
    }

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
        | 1 | 4.0 | alpha | e10f0f3f9cab7d5a48eb5dca9752b239 | f6c1e637db50c80c30606accb7877791 | 1bc36584dfdf327f00541ebcee7b10b8 |
        | 2 |     | beta  | 13e76fda74b91ce42b64e114ce57cea6 | cfcedac45362b91523dc768e6d975abe | e52b73630a4f01834dc30a85baf0ac29 |
        | 3 | 5.0 | gamma | f1bec90248c17cdf4b3fb996bed20178 | 9ded07502923355cf0ad19b62b6b1289 | 640dedde6c6eeb86794482fdb2d35398 |
        | 4 | 6.0 | alpha | 81bb81945f98638c2d65e4c64c6e6a23 | f6c1e637db50c80c30606accb7877791 | 1bc36584dfdf327f00541ebcee7b10b8 |
        | 5 | 7.0 | beta  | 4d29b6913cd5b11a36206c4ace98abaf | cfcedac45362b91523dc768e6d975abe | e52b73630a4f01834dc30a85baf0ac29 |
        | 6 | 8.0 | gamma | e1d301c0702a902057d53b55c211015a | 9ded07502923355cf0ad19b62b6b1289 | 640dedde6c6eeb86794482fdb2d35398 |
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

    /// Regression for #11272: two rows whose string values differ only in where the
    /// column boundary falls — `('ab', 'c')` vs `('a', 'bc')` — must not collide.
    /// Without a delimiter both rows concatenated to `"abc"` and hashed identically,
    /// silently fusing two distinct documents in RRF.
    #[tokio::test]
    async fn test_digest_many_string_boundary_no_collision() -> DataFusionResult<ExitCode> {
        let ctx = SessionContext::new();
        ctx.register_udf(DigestMany::default().into());
        ctx.register_batch(
            "tbl",
            record_batch!(("a", Utf8, ["ab", "a"]), ("b", Utf8, ["c", "bc"]))
                .expect("couldn't make record batch"),
        )?;

        let data = ctx
            .sql("select digest_many(a, b, 'md5') as d from tbl")
            .await?
            .collect()
            .await?;

        let digests = digest_values(&data[0], "d");
        assert_eq!(digests.len(), 2);
        assert_ne!(
            digests[0], digests[1],
            "('ab','c') and ('a','bc') must hash differently"
        );

        Ok(ExitCode::SUCCESS)
    }

    /// Regression for #11272: integer composite keys `(1, 23)` and `(12, 3)` must not
    /// collide on the column boundary.
    #[tokio::test]
    async fn test_digest_many_integer_boundary_no_collision() -> DataFusionResult<ExitCode> {
        let ctx = SessionContext::new();
        ctx.register_udf(DigestMany::default().into());
        ctx.register_batch(
            "tbl",
            record_batch!(("a", Int32, [1, 12]), ("b", Int32, [23, 3]))
                .expect("couldn't make record batch"),
        )?;

        let data = ctx
            .sql("select digest_many(a, b, 'md5') as d from tbl")
            .await?
            .collect()
            .await?;

        let digests = digest_values(&data[0], "d");
        assert_eq!(digests.len(), 2);
        assert_ne!(
            digests[0], digests[1],
            "(1,23) and (12,3) must hash differently"
        );

        Ok(ExitCode::SUCCESS)
    }

    /// Regression for #11272: a SQL NULL must not collide with the literal string
    /// `'NULL'`. Both previously rendered as the text `NULL` and hashed identically.
    #[tokio::test]
    async fn test_digest_many_null_distinct_from_literal_null() -> DataFusionResult<ExitCode> {
        let ctx = SessionContext::new();
        ctx.register_udf(DigestMany::default().into());
        ctx.register_batch(
            "tbl",
            record_batch!(("a", Utf8, [None, Some("NULL")])).expect("couldn't make record batch"),
        )?;

        let data = ctx
            .sql("select digest_many(a, 'md5') as d from tbl")
            .await?
            .collect()
            .await?;

        let digests = digest_values(&data[0], "d");
        assert_eq!(digests.len(), 2);
        // Both rows produce a non-NULL digest of distinct inputs.
        assert!(digests[0].is_some() && digests[1].is_some());
        assert_ne!(
            digests[0], digests[1],
            "SQL NULL and the literal string 'NULL' must hash differently"
        );

        Ok(ExitCode::SUCCESS)
    }
}
