/*
Copyright 2026 The Spice.ai OSS Authors

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

//! T0 SQL user-defined scalar functions.
//!
//! At build time, the declared `body:` is parsed into a `DataFusion`
//! [`Expr`] against a schema derived from the function's argument list
//! and then lowered to a [`PhysicalExpr`]. At invoke time, the incoming
//! [`ColumnarValue`] arguments are packed into a [`RecordBatch`] that
//! matches that schema and the physical expression is evaluated.
//!
//! Parsing uses a fresh [`SessionContext`], which registers standard
//! `DataFusion` scalar functions (math, string, datetime, etc.).
//!
//! The beta surface supports scalar and complex Arrow types, including lists,
//! structs, decimals, and timestamps with timezones.

use std::hash::Hash;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility as DfVolatility,
};
use datafusion::physical_plan::{PhysicalExpr, expressions::CastExpr};
use datafusion::prelude::SessionContext;
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{Function, Volatility};

/// Monotonic identifier for built SQL UDFs — used as the basis for
/// [`Hash`] / [`Eq`] since physical expressions cannot derive them.
/// Each built UDF gets a unique id; two builds of the same declaration
/// are intentionally not equal, which matches how `DataFusion` treats its
/// own non-trivial UDFs.
static NEXT_UDF_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Snafu)]
pub enum SqlBuildError {
    #[snafu(display(
        "return type is required for a scalar SQL function — add `signature.returns: <arrow-type>` (e.g. `float64`)"
    ))]
    MissingReturnType,

    #[snafu(display(
        "unsupported or invalid Arrow type '{arrow_type}' for SQL UDF signature. \
        Use Arrow display types like `Int64`, `List(Int64)`, `Struct(\"name\": Utf8)`, \
        or Spicepod aliases like `int64`, `list<int64>`, `struct<name:utf8>`, `decimal(38,10)`."
    ))]
    UnsupportedArrowType { arrow_type: String },

    #[snafu(display("failed to build schema for arguments: {source}"))]
    BuildSchema { source: DataFusionError },

    #[snafu(display(
        "failed to parse function body as a SQL expression: {source}. \
        The body must be a single SQL expression using the function's argument names."
    ))]
    ParseBody { source: DataFusionError },

    #[snafu(display("failed to lower body to a physical expression: {source}"))]
    PlanExpression { source: DataFusionError },

    #[snafu(display(
        "body expression evaluates to type {actual:?}, which is not coercible to declared return type {expected:?}"
    ))]
    ReturnTypeMismatch {
        expected: DataType,
        actual: DataType,
    },
}

pub type Result<T, E = SqlBuildError> = std::result::Result<T, E>;

/// Build a [`ScalarUDF`] from a [`Function`] declaration whose `from: sql`
/// body has been extracted by the caller.
///
/// # Errors
///
/// Returns [`SqlBuildError`] when any argument or return type is
/// unsupported, `signature.returns` is missing, the body cannot be
/// parsed or lowered to a physical expression, or the body's computed
/// return type is not coercible to the declared return type.
pub fn build_scalar_udf(decl: &Function, body: &str) -> Result<Arc<ScalarUDF>> {
    let arg_specs = decl
        .signature
        .args
        .iter()
        .map(|a| Ok((a.name.clone(), parse_arrow_type(&a.arrow_type)?)))
        .collect::<Result<Vec<_>>>()?;

    let declared_return = decl
        .signature
        .returns
        .as_deref()
        .ok_or(SqlBuildError::MissingReturnType)
        .and_then(parse_arrow_type)?;

    let fields: Vec<Field> = arg_specs
        .iter()
        .map(|(name, ty)| Field::new(name, ty.clone(), /* nullable */ true))
        .collect();
    let arrow_schema = Arc::new(Schema::new(fields));
    let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone()).context(BuildSchemaSnafu)?;

    let ctx = SessionContext::new();

    let logical_expr = ctx
        .parse_sql_expr(body, &df_schema)
        .context(ParseBodySnafu)?;

    // Use the session state's physical planner so the logical-expression
    // type-coercion analyzer runs — this inserts the implicit casts that
    // a SQL author expects (e.g. `6371 * acos(...)` where one side is an
    // integer literal and the other is Float64).
    let state = ctx.state();
    let mut physical_expr = state
        .create_physical_expr(logical_expr, &df_schema)
        .context(PlanExpressionSnafu)?;

    let actual_return = physical_expr
        .data_type(&arrow_schema)
        .context(PlanExpressionSnafu)?;
    if !types_compatible(&actual_return, &declared_return) {
        return Err(SqlBuildError::ReturnTypeMismatch {
            expected: declared_return,
            actual: actual_return,
        });
    }
    if actual_return != declared_return {
        physical_expr = Arc::new(CastExpr::new(physical_expr, declared_return.clone(), None));
    }

    let arg_types: Vec<DataType> = arg_specs.iter().map(|(_, t)| t.clone()).collect();
    let signature = Signature::exact(arg_types, map_volatility(decl.volatility));

    let udf_impl = SqlScalarUdf {
        id: NEXT_UDF_ID.fetch_add(1, Ordering::Relaxed),
        name: decl.name.clone(),
        signature,
        return_type: declared_return,
        arrow_schema,
        physical_expr,
    };
    Ok(Arc::new(ScalarUDF::from(udf_impl)))
}

#[derive(Debug)]
struct SqlScalarUdf {
    id: u64,
    name: String,
    signature: Signature,
    return_type: DataType,
    arrow_schema: Arc<Schema>,
    physical_expr: Arc<dyn PhysicalExpr>,
}

impl PartialEq for SqlScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for SqlScalarUdf {}

impl Hash for SqlScalarUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SqlScalarUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let n = args.number_rows;
        let arrays = args
            .args
            .iter()
            .map(|cv| cv.to_array(n))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)?;
        self.physical_expr.evaluate(&batch)
    }
}

fn map_volatility(v: Volatility) -> DfVolatility {
    match v {
        Volatility::Immutable => DfVolatility::Immutable,
        Volatility::Stable => DfVolatility::Stable,
        Volatility::Volatile => DfVolatility::Volatile,
    }
}

/// Two types are compatible for the return-type check when they are equal
/// or `DataFusion` can implicitly coerce one to the other. Today we accept
/// exact equality plus a few common widen cases (any integer → Int64,
/// any float → Float64) because SQL literals often come back wider than
/// the declared return.
fn types_compatible(actual: &DataType, declared: &DataType) -> bool {
    if actual == declared {
        return true;
    }
    matches!(
        (actual, declared),
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32,
            DataType::Int64
        ) | (
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
            DataType::UInt64
        ) | (
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32,
            DataType::Float64,
        ) | (DataType::Utf8, DataType::LargeUtf8)
    )
}

fn parse_arrow_type(s: &str) -> Result<DataType> {
    super::arrow_type::parse_arrow_type(s).map_err(|_| SqlBuildError::UnsupportedArrowType {
        arrow_type: s.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
    };
    use datafusion::arrow::datatypes::{Field as ArrowField, Int64Type, TimeUnit};
    use spicepod::component::function::{FunctionArg, FunctionKind, Signature as YamlSignature};
    use std::collections::HashMap;

    fn decl(body: &str, args: Vec<(&str, &str)>, ret: &str) -> Function {
        Function {
            name: "f".into(),
            from: "sql".into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Immutable,
            signature: YamlSignature {
                args: args
                    .into_iter()
                    .map(|(n, t)| FunctionArg {
                        name: n.into(),
                        arrow_type: t.into(),
                    })
                    .collect(),
                returns: Some(ret.into()),
            },
            body: Some(body.into()),
            body_ref: None,
            metadata: HashMap::new(),
            params: HashMap::new(),
            depends_on: vec![],
            metrics: None,
            as_tool: true,
        }
    }

    #[test]
    fn parse_arrow_type_primitives() {
        assert_eq!(
            parse_arrow_type("float64").expect("test"),
            DataType::Float64
        );
        assert_eq!(
            parse_arrow_type("FLOAT64").expect("test"),
            DataType::Float64
        );
        assert_eq!(parse_arrow_type("string").expect("test"), DataType::Utf8);
        assert_eq!(parse_arrow_type("bool").expect("test"), DataType::Boolean);
        assert_eq!(
            parse_arrow_type("timestamp(us)").expect("test"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn parse_arrow_type_complex() {
        assert_eq!(
            parse_arrow_type("list<int64>").expect("list parses"),
            DataType::List(Arc::new(ArrowField::new_list_field(DataType::Int64, true)))
        );
        assert_eq!(
            parse_arrow_type("struct<name:utf8, scores:list<float64>>").expect("struct parses"),
            DataType::Struct(
                vec![
                    ArrowField::new("name", DataType::Utf8, true),
                    ArrowField::new(
                        "scores",
                        DataType::List(Arc::new(ArrowField::new_list_field(
                            DataType::Float64,
                            true
                        ))),
                        true,
                    ),
                ]
                .into()
            )
        );
        assert_eq!(
            parse_arrow_type("decimal(38, 10)").expect("decimal parses"),
            DataType::Decimal128(38, 10)
        );
        assert_eq!(
            parse_arrow_type("timestamp(us, UTC)").expect("timestamp with timezone parses"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn build_and_invoke_arithmetic_udf() {
        let d = decl("x + y", vec![("x", "int64"), ("y", "int64")], "int64");
        let udf = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect("builds");

        // Evaluate via ScalarUDFImpl::invoke_with_args
        let x: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let y: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(x), ColumnarValue::Array(y)],
            arg_fields: vec![
                Arc::new(ArrowField::new("x", DataType::Int64, true)),
                Arc::new(ArrowField::new("y", DataType::Int64, true)),
            ],
            number_rows: 3,
            return_field: Arc::new(ArrowField::new("out", DataType::Int64, true)),
            config_options: Arc::default(),
        };
        let result = udf.inner().invoke_with_args(args).expect("invokes");
        let array = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().expect("to_array"),
        };
        let as_i64 = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(as_i64.values(), &[11_i64, 22, 33]);
    }

    #[test]
    fn compatible_return_type_is_cast_to_declared_type() {
        let d = decl("x", vec![("x", "int32")], "int64");
        let udf = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect("builds");

        let x: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(x)],
            arg_fields: vec![Arc::new(ArrowField::new("x", DataType::Int32, true))],
            number_rows: 3,
            return_field: Arc::new(ArrowField::new("out", DataType::Int64, true)),
            config_options: Arc::default(),
        };
        let result = udf.inner().invoke_with_args(args).expect("invokes");
        let array = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().expect("to_array"),
        };
        assert_eq!(array.data_type(), &DataType::Int64);
        let as_i64 = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(as_i64.values(), &[1_i64, 2, 3]);
    }

    #[test]
    fn build_and_invoke_string_udf() {
        let d = decl("upper(s)", vec![("s", "utf8")], "utf8");
        let udf = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect("builds");

        let s: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(s)],
            arg_fields: vec![Arc::new(ArrowField::new("s", DataType::Utf8, true))],
            number_rows: 2,
            return_field: Arc::new(ArrowField::new("out", DataType::Utf8, true)),
            config_options: Arc::default(),
        };
        let result = udf.inner().invoke_with_args(args).expect("invokes");
        let array = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().expect("to_array"),
        };
        let as_str = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(as_str.value(0), "HELLO");
        assert_eq!(as_str.value(1), "WORLD");
    }

    #[test]
    fn build_and_invoke_list_identity_udf() {
        let list_type = DataType::List(Arc::new(ArrowField::new_list_field(DataType::Int64, true)));
        let d = decl("x", vec![("x", "list<int64>")], "list<int64>");
        let udf = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect("builds");

        let x: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
        ]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(x)],
            arg_fields: vec![Arc::new(ArrowField::new("x", list_type.clone(), true))],
            number_rows: 3,
            return_field: Arc::new(ArrowField::new("out", list_type.clone(), true)),
            config_options: Arc::default(),
        };
        let result = udf.inner().invoke_with_args(args).expect("invokes");
        let array = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().expect("to_array"),
        };
        assert_eq!(array.data_type(), &list_type);
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        assert_eq!(list.value_length(0), 2);
        assert!(list.is_null(1));
        assert_eq!(list.value_length(2), 1);
    }

    #[test]
    fn build_math_udf_float64() {
        // Haversine-ish snippet — exercises built-in math functions (cos, sin, acos, radians).
        let d = decl(
            "6371 * acos(cos(radians(lat1)) * cos(radians(lat2)) \
             + sin(radians(lat1)) * sin(radians(lat2)))",
            vec![("lat1", "float64"), ("lat2", "float64")],
            "float64",
        );
        let udf = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect("builds");

        // 0,0 to 0,0 → acos(1) = 0 → 0 km
        let lat1: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64]));
        let lat2: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(lat1), ColumnarValue::Array(lat2)],
            arg_fields: vec![
                Arc::new(ArrowField::new("lat1", DataType::Float64, true)),
                Arc::new(ArrowField::new("lat2", DataType::Float64, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(ArrowField::new("out", DataType::Float64, true)),
            config_options: Arc::default(),
        };
        let result = udf.inner().invoke_with_args(args).expect("invokes");
        let array = match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().expect("to_array"),
        };
        let as_f = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 array");
        let v = as_f.value(0);
        assert!(v.abs() < 1e-9, "expected ~0, got {v}");
    }

    #[test]
    fn missing_return_type_rejected() {
        let mut d = decl("x + 1", vec![("x", "int64")], "int64");
        d.signature.returns = None;
        let err = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect_err("no return");
        assert!(matches!(err, SqlBuildError::MissingReturnType));
    }

    #[test]
    fn unsupported_arg_type_rejected() {
        let d = decl("x", vec![("x", "not_a_type")], "int64");
        let err = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect_err("bad type");
        assert!(matches!(err, SqlBuildError::UnsupportedArrowType { .. }));
    }

    #[test]
    fn invalid_body_surfaces_parser_error() {
        let d = decl("this is not sql 😵", vec![("x", "int64")], "int64");
        let err = build_scalar_udf(&d, d.body.as_deref().expect("test")).expect_err("bad sql");
        assert!(matches!(err, SqlBuildError::ParseBody { .. }));
    }
}
