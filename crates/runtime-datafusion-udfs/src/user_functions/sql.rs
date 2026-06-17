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

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::array::{ArrayRef, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{
    Session, TableFunctionImpl, TableProvider, default_table_source::provider_as_source,
};
use datafusion::common::{Column, DFSchema, DataFusionError, Result as DataFusionResult, Spans};
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Subquery,
    TableScan, Volatility as DfVolatility,
    simplify::{ExprSimplifyResult, SimplifyContext},
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{PhysicalExpr, expressions::CastExpr};
use datafusion::prelude::{DataFrame, Expr, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::sql::{
    TableReference,
    parser::DFParser,
    sqlparser::{ast, dialect::PostgreSqlDialect},
};
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{
    Function, FunctionArg, FunctionReturns, FunctionTableArg, Volatility,
};
use util::session_state::builder_from_existing;

use crate::user_functions::args_inliner::inline_args_into_plan;

pub(crate) const SQL_TABLE_ARGS_TABLE_NAME: &str = "args";

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
        "table return schema is required for a SQL table function — set `signature.returns` to a list of output columns, e.g. `returns: [{{ name: value, type: int64 }}]`"
    ))]
    MissingTableReturnSchema,

    #[snafu(display(
        "scalar return type is required for a scalar SQL function — set `signature.returns` to a single Arrow type string, not a table column list"
    ))]
    ExpectedScalarReturnType,

    #[snafu(display(
        "table return schema is required for a SQL table function — set `signature.returns` to a list of output columns, not a scalar Arrow type string"
    ))]
    ExpectedTableReturnSchema,

    #[snafu(display(
        "unsupported or invalid Arrow type '{arrow_type}' for SQL UDF signature. \
        Use Arrow display types like `Int64`, `List(Int64)`, `Struct(\"name\": Utf8)`, \
        or Spicepod aliases like `int64`, `list<int64>`, `struct<name:utf8>`, `decimal(38,10)`."
    ))]
    UnsupportedArrowType { arrow_type: String },

    #[snafu(display("duplicate output column '{column}' in SQL table function return schema"))]
    DuplicateOutputColumn { column: String },

    #[snafu(display("duplicate input table '{table}' in SQL function signature"))]
    DuplicateInputTable { table: String },

    #[snafu(display(
        "SQL function input table name '{table}' conflicts with reserved scalar argument table name '{reserved}'"
    ))]
    ReservedInputTableName { table: String, reserved: String },

    #[snafu(display("failed to build schema for arguments: {source}"))]
    BuildSchema { source: DataFusionError },

    #[snafu(display("failed to build SQL table function argument table: {source}"))]
    BuildTableArgs { source: DataFusionError },

    #[snafu(display("failed to parse SQL table function body: {source}"))]
    ParseTableBody { source: DataFusionError },

    #[snafu(display("SQL table function body must be a single SELECT query: {details}"))]
    InvalidTableBody { details: String },

    #[snafu(display(
        "failed to parse function body as a SQL expression: {source}. \
        The body must be a single SQL expression using the function's argument names."
    ))]
    ParseBody { source: DataFusionError },

    #[snafu(display("failed to lower body to a physical expression: {source}"))]
    PlanExpression { source: DataFusionError },

    #[snafu(display("failed to plan SQL table function body: {source}"))]
    PlanTableBody { source: DataFusionError },

    #[snafu(display(
        "body expression evaluates to type {actual:?}, which is not coercible to declared return type {expected:?}"
    ))]
    ReturnTypeMismatch {
        expected: DataType,
        actual: DataType,
    },

    #[snafu(display(
        "SQL table function body schema does not match declared return schema: {details}"
    ))]
    ReturnSchemaMismatch { details: String },

    #[snafu(display("SQL function table argument schema does not match declaration: {details}"))]
    InputSchemaMismatch { details: String },
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
    if !decl.signature.tables.is_empty() {
        return build_scalar_table_arg_udf(decl, body);
    }

    let arg_specs = decl
        .signature
        .args
        .iter()
        .map(|a| Ok((a.name.clone(), parse_arrow_type(&a.arrow_type)?)))
        .collect::<Result<Vec<_>>>()?;

    let declared_return = match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Scalar(arrow_type)) => parse_arrow_type(arrow_type)?,
        Some(FunctionReturns::Table(_)) => return ExpectedScalarReturnTypeSnafu.fail(),
        None => return MissingReturnTypeSnafu.fail(),
    };

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

fn build_scalar_table_arg_udf(decl: &Function, body: &str) -> Result<Arc<ScalarUDF>> {
    validate_table_body_syntax(body)?;
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let table_args = table_arg_specs(&decl.signature.tables)?;
    let return_type = scalar_return_type(decl)?;
    let output_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        return_type.clone(),
        true,
    )]));
    let table_func = Arc::new(SqlTableFunc {
        name: decl.name.clone(),
        arg_schema,
        table_args,
        output_schema,
        body: body.to_string(),
    });
    let udf_impl = SqlScalarTableArgUdf {
        id: NEXT_UDF_ID.fetch_add(1, Ordering::Relaxed),
        name: decl.name.clone(),
        signature: Signature::variadic_any(map_volatility(decl.volatility)),
        return_type,
        table_func,
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

#[derive(Debug)]
struct SqlScalarTableArgUdf {
    id: u64,
    name: String,
    signature: Signature,
    return_type: DataType,
    table_func: Arc<SqlTableFunc>,
}

impl PartialEq for SqlScalarTableArgUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for SqlScalarTableArgUdf {}

impl Hash for SqlScalarTableArgUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for SqlScalarTableArgUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Execution(format!(
            "SQL scalar function '{}' with table arguments must be rewritten to a scalar subquery before execution",
            self.name
        )))
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult, DataFusionError> {
        let provider = self.table_func.call(&args)?;
        let table_source = provider_as_source(provider);
        let table_scan = TableScan::try_new(
            TableReference::bare(format!("{}_result", self.name)),
            table_source,
            None,
            vec![],
            None,
        )?;
        Ok(ExprSimplifyResult::Simplified(Expr::ScalarSubquery(
            Subquery {
                subquery: Arc::new(LogicalPlan::TableScan(table_scan)),
                outer_ref_columns: vec![],
                spans: Spans::new(),
            },
        )))
    }
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

/// Build a [`TableFunctionImpl`] from a SQL table-function declaration.
///
/// The SQL body is a full query. Function arguments are exposed to that query
/// as a one-row table named `args`, with one column per declared argument.
///
/// # Errors
///
/// Returns [`SqlBuildError`] when the return schema is missing or invalid, any
/// argument type is invalid, or the query cannot be planned against a typed
/// `args` table.
pub async fn build_table_udtf(decl: &Function, body: &str) -> Result<Arc<dyn TableFunctionImpl>> {
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let table_args = table_arg_specs(&decl.signature.tables)?;
    let output_schema = table_return_schema(decl)?;
    validate_table_body_syntax(body)?;

    let validation_args = typed_null_args(arg_schema.as_ref()).context(BuildTableArgsSnafu)?;
    let ctx = validation_context(Arc::clone(&arg_schema), &validation_args, &table_args)
        .context(BuildTableArgsSnafu)?;
    match ctx.sql(body).await {
        Ok(df) => {
            validate_output_schema(&decl.name, df.schema().as_arrow(), output_schema.as_ref())?;
        }
        Err(source) => {
            tracing::debug!(name = %decl.name, error = %source, "Deferring SQL table function output schema validation until execution");
        }
    }

    Ok(Arc::new(SqlTableFunc {
        name: decl.name.clone(),
        arg_schema,
        table_args,
        output_schema,
        body: body.to_string(),
    }))
}

#[derive(Clone, Debug)]
struct TableArgSpec {
    name: String,
    schema: SchemaRef,
}

#[derive(Clone, Debug)]
enum DynamicTableSource {
    Table(TableReference),
    Plan(Arc<LogicalPlan>),
}

#[derive(Clone, Debug)]
struct TableArgValue {
    name: String,
    schema: SchemaRef,
    source: DynamicTableSource,
}

#[derive(Clone)]
struct SqlTableFunc {
    name: String,
    arg_schema: SchemaRef,
    table_args: Vec<TableArgSpec>,
    output_schema: SchemaRef,
    body: String,
}

impl Debug for SqlTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlTableFunc")
            .field("name", &self.name)
            .field("arg_schema", &self.arg_schema)
            .field("table_args", &self.table_args)
            .field("output_schema", &self.output_schema)
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for SqlTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_args, scalar_exprs) = split_table_and_scalar_exprs(
            &self.name,
            &self.table_args,
            self.arg_schema.as_ref(),
            exprs,
        )?;
        let args = table_arg_values(&self.name, self.arg_schema.as_ref(), scalar_exprs)?;
        Ok(Arc::new(SqlTableProvider {
            name: self.name.clone(),
            arg_schema: Arc::clone(&self.arg_schema),
            table_args,
            schema: Arc::clone(&self.output_schema),
            body: self.body.clone(),
            args,
        }))
    }
}

#[derive(Debug)]
struct SqlTableProvider {
    name: String,
    arg_schema: SchemaRef,
    table_args: Vec<TableArgValue>,
    schema: SchemaRef,
    body: String,
    args: Vec<ScalarValue>,
}

#[async_trait::async_trait]
impl TableProvider for SqlTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let ctx = context_with_args_and_tables(
            Some(state),
            Arc::clone(&self.arg_schema),
            &self.args,
            &self.table_args,
            &self.name,
        )
        .await?;

        let (session_state, plan) = ctx.sql(&self.body).await?.into_parts();
        let inlined_plan = inline_args_into_plan(plan, self.arg_schema.as_ref(), &self.args)?;
        let mut df = DataFrame::new(session_state, inlined_plan);
        validate_output_schema(&self.name, df.schema().as_arrow(), self.schema.as_ref())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        if let Some(limit) = limit {
            df = df.limit(0, Some(limit))?;
        }
        if let Some(projection) = projection {
            df = project_dataframe(df, self.schema.as_ref(), projection)?;
        }
        df.create_physical_plan().await
    }
}

fn function_arg_schema(args: &[FunctionArg]) -> Result<SchemaRef> {
    let fields = args
        .iter()
        .map(|arg| {
            Ok(Field::new(
                &arg.name,
                parse_arrow_type(&arg.arrow_type)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn table_return_schema(decl: &Function) -> Result<SchemaRef> {
    let columns = match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Table(columns)) => columns,
        Some(FunctionReturns::Scalar(_)) => return ExpectedTableReturnSchemaSnafu.fail(),
        None => return MissingTableReturnSchemaSnafu.fail(),
    };

    let mut names = HashSet::with_capacity(columns.len());
    let fields = columns
        .iter()
        .map(|column| {
            if !names.insert(column.name.to_ascii_lowercase()) {
                return DuplicateOutputColumnSnafu {
                    column: column.name.clone(),
                }
                .fail();
            }
            Ok(Field::new(
                &column.name,
                parse_arrow_type(&column.arrow_type)?,
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(Schema::new(fields)))
}

fn scalar_return_type(decl: &Function) -> Result<DataType> {
    match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Scalar(arrow_type)) => parse_arrow_type(arrow_type),
        Some(FunctionReturns::Table(_)) => ExpectedScalarReturnTypeSnafu.fail(),
        None => MissingReturnTypeSnafu.fail(),
    }
}

fn table_arg_specs(tables: &[FunctionTableArg]) -> Result<Vec<TableArgSpec>> {
    let mut names = HashSet::with_capacity(tables.len());
    tables
        .iter()
        .map(|table| {
            if table.name.eq_ignore_ascii_case(SQL_TABLE_ARGS_TABLE_NAME) {
                return ReservedInputTableNameSnafu {
                    table: table.name.clone(),
                    reserved: SQL_TABLE_ARGS_TABLE_NAME.to_string(),
                }
                .fail();
            }
            if !names.insert(table.name.to_ascii_lowercase()) {
                return DuplicateInputTableSnafu {
                    table: table.name.clone(),
                }
                .fail();
            }
            Ok(TableArgSpec {
                name: table.name.clone(),
                schema: function_arg_schema(&table.columns)?,
            })
        })
        .collect()
}

fn typed_null_args(schema: &Schema) -> DataFusionResult<Vec<ScalarValue>> {
    schema
        .fields()
        .iter()
        .map(|field| ScalarValue::try_from(field.data_type()))
        .collect()
}

fn validate_table_body_syntax(body: &str) -> Result<()> {
    let statements = DFParser::parse_sql_with_dialect(body, &PostgreSqlDialect {})
        .context(ParseTableBodySnafu)?;
    if statements.len() != 1 {
        return InvalidTableBodySnafu {
            details: format!("expected one statement, got {}", statements.len()),
        }
        .fail();
    }

    let statement =
        statements
            .into_iter()
            .next()
            .ok_or_else(|| SqlBuildError::InvalidTableBody {
                details: "expected one statement, got 0".to_string(),
            })?;
    let is_query = match statement {
        datafusion::sql::parser::Statement::Statement(statement) => {
            matches!(*statement, ast::Statement::Query(_))
        }
        _ => false,
    };
    if !is_query {
        return InvalidTableBodySnafu {
            details: "expected a SELECT query".to_string(),
        }
        .fail();
    }

    Ok(())
}

fn table_arg_values(
    function_name: &str,
    schema: &Schema,
    exprs: &[Expr],
) -> DataFusionResult<Vec<ScalarValue>> {
    let fields = schema.fields();
    let mut values: Vec<Option<ScalarValue>> = vec![None; fields.len()];
    let mut positional_index = 0;

    for expr in exprs {
        let (parameter_name, scalar) = literal_arg(function_name, expr)?;
        let index = if let Some(name) = parameter_name {
            fields
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "SQL table function '{function_name}' has no argument named '{name}'"
                    ))
                })?
        } else {
            while positional_index < values.len() && values[positional_index].is_some() {
                positional_index += 1;
            }
            if positional_index >= fields.len() {
                return Err(DataFusionError::Plan(format!(
                    "SQL table function '{function_name}' expected {} arguments, got more",
                    fields.len()
                )));
            }
            let index = positional_index;
            positional_index += 1;
            index
        };

        if values[index].is_some() {
            return Err(DataFusionError::Plan(format!(
                "SQL table function '{function_name}' argument '{}' was provided more than once",
                fields[index].name()
            )));
        }
        values[index] = Some(cast_scalar_arg(&scalar, fields[index].data_type())?);
    }

    values
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            value.ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "SQL table function '{function_name}' missing required argument '{}'",
                    fields[idx].name()
                ))
            })
        })
        .collect()
}

fn split_table_and_scalar_exprs<'a>(
    function_name: &str,
    table_args: &[TableArgSpec],
    scalar_schema: &Schema,
    exprs: &'a [Expr],
) -> DataFusionResult<(Vec<TableArgValue>, &'a [Expr])> {
    if exprs.len() < table_args.len() {
        return Err(DataFusionError::Plan(format!(
            "SQL function '{function_name}' expected {} table argument(s) followed by {} scalar argument(s), got {} total argument(s)",
            table_args.len(),
            scalar_schema.fields().len(),
            exprs.len()
        )));
    }

    let scalar_exprs = &exprs[table_args.len()..];
    let table_values = table_args
        .iter()
        .zip(&exprs[..table_args.len()])
        .map(|(arg, expr)| {
            Ok(TableArgValue {
                name: arg.name.clone(),
                schema: Arc::clone(&arg.schema),
                source: dynamic_table_source_from_expr(function_name, &arg.name, expr)?,
            })
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    Ok((table_values, scalar_exprs))
}

fn dynamic_table_source_from_expr(
    function_name: &str,
    table_arg_name: &str,
    expr: &Expr,
) -> DataFusionResult<DynamicTableSource> {
    match expr {
        Expr::Column(column) => Ok(DynamicTableSource::Table(table_ref_from_column_expr(
            column,
        ))),
        Expr::Literal(ScalarValue::Utf8(Some(table)), _) => {
            Ok(DynamicTableSource::Table(TableReference::parse_str(table)))
        }
        Expr::ScalarSubquery(subquery) => {
            if !subquery.outer_ref_columns.is_empty() {
                return Err(DataFusionError::NotImplemented(format!(
                    "SQL function '{function_name}' does not support correlated dynamic table input for argument '{table_arg_name}'"
                )));
            }
            Ok(DynamicTableSource::Plan(Arc::clone(&subquery.subquery)))
        }
        other => Err(DataFusionError::Plan(format!(
            "SQL function '{function_name}' requires table argument '{table_arg_name}' to be a table reference or dynamic table input, got: {other:?}"
        ))),
    }
}

fn table_ref_from_column_expr(column: &Column) -> TableReference {
    let table: Arc<str> = column.name.clone().into();
    let schema = column.relation.as_ref().map(TableReference::table);
    let catalog = column.relation.as_ref().and_then(TableReference::schema);
    match (catalog, schema) {
        (None | Some(_), None) => TableReference::Bare { table },
        (None, Some(schema)) => TableReference::Partial {
            schema: schema.into(),
            table,
        },
        (Some(catalog), Some(schema)) => TableReference::Full {
            catalog: catalog.into(),
            schema: schema.into(),
            table,
        },
    }
}

fn literal_arg(
    function_name: &str,
    expr: &Expr,
) -> DataFusionResult<(Option<String>, ScalarValue)> {
    if let Expr::Literal(scalar, metadata) = expr {
        let parameter_name = metadata
            .as_ref()
            .and_then(|metadata| metadata.inner().get("spice.parameter_name"))
            .cloned();
        return Ok((parameter_name, scalar.clone()));
    }

    Err(DataFusionError::NotImplemented(format!(
        "SQL table function '{function_name}' currently supports literal arguments only; got {expr:?}"
    )))
}

fn cast_scalar_arg(value: &ScalarValue, data_type: &DataType) -> DataFusionResult<ScalarValue> {
    if matches!(value, ScalarValue::Null) {
        return ScalarValue::try_from(data_type);
    }
    value.cast_to(data_type)
}

fn context_with_args(
    state: Option<&dyn Session>,
    schema: SchemaRef,
    values: &[ScalarValue],
) -> DataFusionResult<SessionContext> {
    let ctx = if let Some(state) = state {
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "SQL table function execution requires a DataFusion SessionState".to_string(),
                )
            })?;
        SessionContext::new_with_state(builder_from_existing(state).build())
    } else {
        SessionContext::new()
    };
    let batch = args_record_batch(Arc::clone(&schema), values)?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let _ = ctx.deregister_table(SQL_TABLE_ARGS_TABLE_NAME);
    ctx.register_table(SQL_TABLE_ARGS_TABLE_NAME, Arc::new(table))?;
    Ok(ctx)
}

fn validation_context(
    arg_schema: SchemaRef,
    values: &[ScalarValue],
    table_args: &[TableArgSpec],
) -> DataFusionResult<SessionContext> {
    let ctx = context_with_args(None, arg_schema, values)?;
    for table_arg in table_args {
        let table = MemTable::try_new(
            Arc::clone(&table_arg.schema),
            vec![vec![RecordBatch::new_empty(Arc::clone(&table_arg.schema))]],
        )?;
        let _ = ctx.deregister_table(&table_arg.name);
        ctx.register_table(&table_arg.name, Arc::new(table))?;
    }
    Ok(ctx)
}

async fn context_with_args_and_tables(
    state: Option<&dyn Session>,
    arg_schema: SchemaRef,
    values: &[ScalarValue],
    table_args: &[TableArgValue],
    function_name: &str,
) -> DataFusionResult<SessionContext> {
    let ctx = context_with_args(state, arg_schema, values)?;
    for table_arg in table_args {
        let df = match &table_arg.source {
            DynamicTableSource::Table(table) => ctx.table(table.clone()).await?,
            DynamicTableSource::Plan(plan) => ctx.execute_logical_plan((**plan).clone()).await?,
        };
        validate_input_schema(
            function_name,
            &table_arg.name,
            df.schema().as_arrow(),
            table_arg.schema.as_ref(),
        )
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let _ = ctx.deregister_table(&table_arg.name);
        ctx.register_table(&table_arg.name, df.into_view())?;
    }
    Ok(ctx)
}

fn project_dataframe(
    df: DataFrame,
    schema: &Schema,
    projection: &[usize],
) -> DataFusionResult<DataFrame> {
    let columns = projection
        .iter()
        .map(|index| {
            schema
                .fields()
                .get(*index)
                .map(|field| field.name().as_str())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "SQL table function projection index {index} is out of bounds for schema with {} column(s)",
                        schema.fields().len()
                    ))
                })
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    df.select_columns(&columns)
}

fn args_record_batch(schema: SchemaRef, values: &[ScalarValue]) -> DataFusionResult<RecordBatch> {
    let arrays = values
        .iter()
        .map(|value| value.to_array_of_size(1))
        .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(DataFusionError::from)
}

fn validate_output_schema(function_name: &str, actual: &Schema, expected: &Schema) -> Result<()> {
    let actual_fields = actual.fields();
    let expected_fields = expected.fields();
    if actual_fields.len() != expected_fields.len() {
        return ReturnSchemaMismatchSnafu {
            details: format!(
                "expected {} column(s) [{}], got {} column(s) [{}] for function '{function_name}'",
                expected_fields.len(),
                schema_signature(expected),
                actual_fields.len(),
                schema_signature(actual)
            ),
        }
        .fail();
    }

    for (idx, (actual, expected)) in actual_fields.iter().zip(expected_fields.iter()).enumerate() {
        if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
            return ReturnSchemaMismatchSnafu {
                details: format!(
                    "column {idx} expected '{}: {:?}', got '{}: {:?}' for function '{function_name}'",
                    expected.name(),
                    expected.data_type(),
                    actual.name(),
                    actual.data_type()
                ),
            }
            .fail();
        }
    }

    Ok(())
}

fn validate_input_schema(
    function_name: &str,
    table_arg_name: &str,
    actual: &Schema,
    expected: &Schema,
) -> Result<()> {
    let actual_fields = actual.fields();
    let expected_fields = expected.fields();
    if actual_fields.len() != expected_fields.len() {
        return InputSchemaMismatchSnafu {
            details: format!(
                "table argument '{table_arg_name}' for function '{function_name}' expected {} column(s) [{}], got {} column(s) [{}]",
                expected_fields.len(),
                schema_signature(expected),
                actual_fields.len(),
                schema_signature(actual)
            ),
        }
        .fail();
    }

    for (idx, (actual, expected)) in actual_fields.iter().zip(expected_fields.iter()).enumerate() {
        if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
            return InputSchemaMismatchSnafu {
                details: format!(
                    "table argument '{table_arg_name}' for function '{function_name}' column {idx} expected '{}: {:?}', got '{}: {:?}'",
                    expected.name(),
                    expected.data_type(),
                    actual.name(),
                    actual.data_type()
                ),
            }
            .fail();
        }
    }

    Ok(())
}

fn schema_signature(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| format!("{}: {:?}", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
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
    use datafusion::prelude::{SessionContext, col, lit, scalar_subquery};
    use spicepod::component::function::{
        FunctionArg, FunctionKind, FunctionReturns, FunctionTableArg, Signature as YamlSignature,
    };
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
                tables: vec![],
                args: args
                    .into_iter()
                    .map(|(n, t)| FunctionArg {
                        name: n.into(),
                        arrow_type: t.into(),
                    })
                    .collect(),
                returns: Some(FunctionReturns::Scalar(ret.into())),
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

    fn table_decl(body: &str) -> Function {
        Function {
            name: "emit_pair".into(),
            from: "sql".into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Table,
            volatility: Volatility::Immutable,
            signature: YamlSignature {
                tables: vec![],
                args: vec![FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some(FunctionReturns::Table(vec![
                    FunctionArg {
                        name: "value".into(),
                        arrow_type: "int64".into(),
                    },
                    FunctionArg {
                        name: "doubled".into(),
                        arrow_type: "int64".into(),
                    },
                ])),
            },
            body: Some(body.into()),
            body_ref: None,
            metadata: HashMap::new(),
            params: HashMap::new(),
            depends_on: vec![],
            metrics: None,
            as_tool: false,
        }
    }

    fn dynamic_scalar_table_decl() -> Function {
        let mut decl = decl(
            "SELECT sum(value) + offset AS value FROM input CROSS JOIN args GROUP BY offset",
            vec![("offset", "int64")],
            "int64",
        );
        decl.name = "sum_values".into();
        decl.signature.tables = vec![FunctionTableArg {
            name: "input".into(),
            columns: vec![FunctionArg {
                name: "value".into(),
                arrow_type: "int64".into(),
            }],
        }];
        decl
    }

    fn dynamic_table_decl() -> Function {
        Function {
            name: "offset_values".into(),
            from: "sql".into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Table,
            volatility: Volatility::Immutable,
            signature: YamlSignature {
                tables: vec![FunctionTableArg {
                    name: "input".into(),
                    columns: vec![FunctionArg {
                        name: "value".into(),
                        arrow_type: "int64".into(),
                    }],
                }],
                args: vec![FunctionArg {
                    name: "offset".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some(FunctionReturns::Table(vec![FunctionArg {
                    name: "value".into(),
                    arrow_type: "int64".into(),
                }])),
            },
            body: Some("SELECT value + offset AS value FROM input CROSS JOIN args".into()),
            body_ref: None,
            metadata: HashMap::new(),
            params: HashMap::new(),
            depends_on: vec![],
            metrics: None,
            as_tool: false,
        }
    }

    fn register_numbers(ctx: &SessionContext) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
        )
        .expect("record batch");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        ctx.register_table("numbers", Arc::new(table))
            .expect("register table");
    }

    async fn filtered_numbers_expr(ctx: &SessionContext) -> Expr {
        let input_df = ctx
            .table("numbers")
            .await
            .expect("table exists")
            .filter(col("value").gt(lit(1_i64)))
            .expect("filters")
            .select(vec![col("value")])
            .expect("projects");
        scalar_subquery(Arc::new(input_df.into_unoptimized_plan()))
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

    #[tokio::test]
    async fn sql_table_udtf_registered_and_queried_via_sql() {
        use datafusion::prelude::SessionContext;

        let d = table_decl(
            "SELECT x AS value, x * 2 AS doubled FROM args \
             UNION ALL \
             SELECT x + 1 AS value, (x + 1) * 2 AS doubled FROM args",
        );
        let udtf = build_table_udtf(&d, d.body.as_deref().expect("test"))
            .await
            .expect("builds");

        let ctx = SessionContext::new();
        ctx.register_udtf(&d.name, udtf);
        let df = ctx
            .sql("SELECT value, doubled FROM emit_pair(4) ORDER BY value")
            .await
            .expect("sql compiles");
        let results = df.collect().await.expect("runs");

        assert_eq!(results.len(), 1);
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value int64");
        let doubled = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("doubled int64");
        assert_eq!(values.values(), &[4_i64, 5]);
        assert_eq!(doubled.values(), &[8_i64, 10]);
    }

    #[tokio::test]
    async fn sql_table_udtf_queried_via_dataframe_api() {
        use datafusion::prelude::SessionContext;

        let decl = table_decl(
            "SELECT x AS value, x * 2 AS doubled FROM args \
             UNION ALL \
             SELECT x + 1 AS value, (x + 1) * 2 AS doubled FROM args",
        );
        let udtf = build_table_udtf(&decl, decl.body.as_deref().expect("test"))
            .await
            .expect("builds");

        let ctx = SessionContext::new();
        ctx.register_udtf(&decl.name, udtf);
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[lit(4_i64)])
            .expect("creates table provider");
        ctx.register_table("emit_pair_result", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("emit_pair_result")
            .await
            .expect("table exists")
            .filter(col("doubled").gt(lit(0_i64)))
            .expect("filters")
            .sort_by(vec![col("value")])
            .expect("sorts")
            .select(vec![col("value"), col("doubled")])
            .expect("projects")
            .collect()
            .await
            .expect("runs");

        assert_eq!(results.len(), 1);
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value int64");
        let doubled = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("doubled int64");
        assert_eq!(values.values(), &[4_i64, 5]);
        assert_eq!(doubled.values(), &[8_i64, 10]);
    }

    #[tokio::test]
    async fn sql_table_udtf_can_query_caller_session_tables() {
        use datafusion::prelude::SessionContext;

        let mut d = table_decl(
            "SELECT numbers.value AS value, numbers.value * args.x AS doubled \
             FROM numbers CROSS JOIN args",
        );
        d.name = "scale_values".into();
        let udtf = build_table_udtf(&d, d.body.as_deref().expect("test"))
            .await
            .expect("builds even when caller tables are resolved at execution");

        let ctx = SessionContext::new();
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&table_schema),
            vec![Arc::new(Int64Array::from(vec![2_i64, 4])) as ArrayRef],
        )
        .expect("record batch");
        let table = MemTable::try_new(table_schema, vec![vec![batch]]).expect("mem table");
        ctx.register_table("numbers", Arc::new(table))
            .expect("register table");
        ctx.register_udtf(&d.name, udtf);

        let df = ctx
            .sql("SELECT value, doubled FROM scale_values(3) ORDER BY value")
            .await
            .expect("sql compiles");
        let results = df.collect().await.expect("runs");

        assert_eq!(results.len(), 1);
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value int64");
        let doubled = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("doubled int64");
        assert_eq!(values.values(), &[2_i64, 4]);
        assert_eq!(doubled.values(), &[6_i64, 12]);

        let projected = ctx
            .sql("SELECT value FROM scale_values(3) ORDER BY value")
            .await
            .expect("projected sql compiles")
            .collect()
            .await
            .expect("projected query runs");
        assert_eq!(projected[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn sql_scalar_udf_accepts_dynamic_table_arg_from_sql_subquery() {
        let decl = dynamic_scalar_table_decl();
        let udf = build_scalar_udf(&decl, decl.body.as_deref().expect("test")).expect("builds");

        let ctx = SessionContext::new();
        register_numbers(&ctx);
        ctx.register_udf(udf.as_ref().clone());

        let results = ctx
            .sql(
                "WITH filtered AS (SELECT value FROM numbers WHERE value > 1) \
                 SELECT sum_values((SELECT value FROM filtered), 10) AS total",
            )
            .await
            .expect("sql compiles")
            .collect()
            .await
            .expect("query runs");
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");
        assert_eq!(values.values(), &[15_i64]);
    }

    #[tokio::test]
    async fn sql_scalar_udf_accepts_dynamic_table_arg_via_dataframe_api() {
        let decl = dynamic_scalar_table_decl();
        let udf = build_scalar_udf(&decl, decl.body.as_deref().expect("test")).expect("builds");

        let ctx = SessionContext::new();
        register_numbers(&ctx);
        ctx.register_udf(udf.as_ref().clone());
        let input_expr = filtered_numbers_expr(&ctx).await;

        let results = ctx
            .table("numbers")
            .await
            .expect("table exists")
            .filter(udf.call(vec![input_expr, lit(10_i64)]).eq(lit(15_i64)))
            .expect("filters with scalar UDF")
            .sort_by(vec![col("value")])
            .expect("sorts")
            .select(vec![col("value")])
            .expect("projects")
            .collect()
            .await
            .expect("query runs");
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");
        assert_eq!(values.values(), &[1_i64, 2, 3]);
    }

    #[tokio::test]
    async fn sql_table_udtf_accepts_dynamic_table_arg_from_sql_subquery() {
        let decl = dynamic_table_decl();
        let udtf = build_table_udtf(&decl, decl.body.as_deref().expect("test"))
            .await
            .expect("builds");

        let ctx = SessionContext::new();
        register_numbers(&ctx);
        ctx.register_udtf(&decl.name, udtf);

        let results = ctx
            .sql(
                "WITH filtered AS (SELECT value FROM numbers WHERE value > 1) \
                 SELECT value FROM offset_values((SELECT value FROM filtered), 10) ORDER BY value",
            )
            .await
            .expect("sql compiles")
            .collect()
            .await
            .expect("query runs");
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");
        assert_eq!(values.values(), &[12_i64, 13]);
    }

    #[tokio::test]
    async fn sql_table_udtf_accepts_dynamic_table_arg_via_dataframe_api() {
        let decl = dynamic_table_decl();
        let udtf = build_table_udtf(&decl, decl.body.as_deref().expect("test"))
            .await
            .expect("builds");

        let ctx = SessionContext::new();
        register_numbers(&ctx);
        ctx.register_udtf(&decl.name, udtf);
        let input_expr = filtered_numbers_expr(&ctx).await;
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[input_expr, lit(10_i64)])
            .expect("creates table provider");
        ctx.register_table("offset_values_result", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("offset_values_result")
            .await
            .expect("table exists")
            .sort_by(vec![col("value")])
            .expect("sorts")
            .select(vec![col("value")])
            .expect("projects")
            .collect()
            .await
            .expect("query runs");
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");
        assert_eq!(values.values(), &[12_i64, 13]);
    }

    #[tokio::test]
    async fn sql_table_udtf_rejects_body_schema_mismatch() {
        let d = table_decl("SELECT x AS not_value, x * 2 AS doubled FROM args");
        let err = build_table_udtf(&d, d.body.as_deref().expect("test"))
            .await
            .expect_err("schema mismatch should fail");
        assert!(matches!(err, SqlBuildError::ReturnSchemaMismatch { .. }));
    }
}
