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

//! WebAssembly user-defined table functions.
//!
//! ABI:
//!   * Host serializes input table batches as an Arrow IPC stream.
//!   * Host serializes scalar function arguments as a one-row Arrow IPC stream.
//!   * Guest exports `memory`, `spice_alloc(len: i32) -> i32`,
//!     `spice_dealloc(ptr: i32, len: i32)`, and an entrypoint with signature
//!     `(input_ptr: i32, input_len: i32, args_ptr: i32, args_len: i32) -> i64`.
//!   * The return value packs `(output_ptr << 32) | output_len`, where output is
//!     an Arrow IPC stream matching `signature.returns`.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::io::Cursor;
#[cfg(feature = "wasm-functions-compile")]
use std::path::Path;
use std::path::PathBuf;
#[cfg(feature = "wasm-functions-compile")]
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::array::{ArrayRef, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use datafusion::catalog::{
    Session, TableFunctionImpl, TableProvider, default_table_source::provider_as_source,
};
use datafusion::common::{Column, DataFusionError, Result as DataFusionResult, Spans};
use datafusion::datasource::TableType;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Subquery,
    TableScan, Volatility as DfVolatility,
    simplify::{ExprSimplifyResult, SimplifyContext},
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{Function, FunctionArg, FunctionReturns, Volatility};
use util::session_state::builder_from_existing;
use wasmtime::{Config, Engine, Instance, Module, ResourceLimiter, Store, TypedFunc};

static NEXT_WASM_SCALAR_ID: AtomicU64 = AtomicU64::new(1);

const DEFAULT_ENTRYPOINT: &str = "spice_transform";
#[cfg(feature = "wasm-functions-compile")]
const DEFAULT_LANGUAGE: &str = "rust";
#[cfg(feature = "wasm-functions-compile")]
const DEFAULT_TARGET: &str = "wasm32-unknown-unknown";
const INPUT_TABLE_PARAM: &str = "input_table";
const MODULE_PARAM: &str = "module";
const SOURCE_PARAM: &str = "source";
const DEFAULT_WASM_FUEL: u64 = 100_000_000;
const DEFAULT_WASM_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_WASM_MAX_TABLE_ELEMENTS: usize = 1_000_000;

#[derive(Debug, Snafu)]
pub enum WasmBuildError {
    #[snafu(display(
        "WASM table functions require `signature.returns` to be a list of output columns"
    ))]
    MissingTableReturnSchema,

    #[snafu(display(
        "WASM scalar functions require `signature.returns` to be a scalar Arrow type"
    ))]
    MissingScalarReturnSchema,

    #[snafu(display(
        "WASM table functions require table return columns, not a scalar Arrow return type"
    ))]
    ExpectedTableReturnSchema,

    #[snafu(display(
        "WASM scalar functions require a scalar Arrow return type, not table return columns"
    ))]
    ExpectedScalarReturnSchema,

    #[snafu(display(
        "unsupported or invalid Arrow type '{arrow_type}' for WASM function signature"
    ))]
    UnsupportedArrowType { arrow_type: String },

    #[snafu(display("duplicate column '{column}' in WASM function schema"))]
    DuplicateColumn { column: String },

    #[snafu(display("WASM functions support at most one declared input table for now"))]
    TooManyInputTables,

    #[snafu(display(
        "WASM table function requires an input source: pass the declared table input as the first argument, set `params.input_table`, or provide SQL in `body` / `body_ref`"
    ))]
    MissingInputSource,

    #[snafu(display(
        "WASM table function has both `params.input_table` and SQL `body` / `body_ref`; provide exactly one input source"
    ))]
    ConflictingInputSource,

    #[snafu(display("WASM function requires one of `params.module` or `params.source`"))]
    MissingModuleOrSource,

    #[snafu(display(
        "WASM function has both `params.module` and `params.source`; provide exactly one"
    ))]
    ConflictingModuleSource,

    #[snafu(display("WASM param `{param}` must be a string"))]
    InvalidStringParam { param: String },

    #[snafu(display("WASM param `{param}` must be {expected}, got {got}"))]
    InvalidIntegerParam {
        param: String,
        expected: String,
        got: String,
    },

    #[snafu(display("unsupported WASM source language '{language}'. Supported languages: rust"))]
    UnsupportedLanguage { language: String },

    #[cfg(not(feature = "wasm-functions-compile"))]
    #[snafu(display(
        "compiling WASM source requires the `wasm-functions-compile` feature; use `params.module` with a precompiled .wasm artifact or rebuild with that feature"
    ))]
    SourceCompileDisabled,

    #[cfg(feature = "wasm-functions-compile")]
    #[snafu(display("failed to hash WASM source '{path}': {source}"))]
    HashSource {
        path: String,
        source: std::io::Error,
    },

    #[cfg(feature = "wasm-functions-compile")]
    #[snafu(display("failed to create WASM cache directory '{path}': {source}"))]
    CreateCacheDir {
        path: String,
        source: std::io::Error,
    },

    #[cfg(feature = "wasm-functions-compile")]
    #[snafu(display("failed to compile Rust source to WASM: {message}"))]
    CompileRust { message: String },

    #[cfg(feature = "wasm-functions-compile")]
    #[snafu(display("failed to read compiled WASM artifact directory '{path}': {source}"))]
    ReadArtifactDir {
        path: String,
        source: std::io::Error,
    },

    #[cfg(feature = "wasm-functions-compile")]
    #[snafu(display("failed to copy compiled WASM artifact from '{from}' to '{to}': {source}"))]
    CopyArtifact {
        from: String,
        to: String,
        source: std::io::Error,
    },

    #[cfg(feature = "wasm-functions-compile")]
    #[snafu(display("WASM source compilation task failed: {source}"))]
    CompileTask { source: tokio::task::JoinError },

    #[snafu(display("failed to read WASM module '{path}': {source}"))]
    ReadModule {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("failed to create WASM engine: {source}"))]
    CreateEngine { source: wasmtime::Error },

    #[snafu(display("failed to configure WASM execution fuel: {source}"))]
    SetFuel { source: wasmtime::Error },

    #[snafu(display("failed to compile WASM module '{path}': {source}"))]
    CompileModule {
        path: String,
        source: wasmtime::Error,
    },

    #[snafu(display("failed to instantiate WASM module: {source}"))]
    Instantiate { source: wasmtime::Error },

    #[snafu(display("WASM module is missing exported memory named `memory`"))]
    MissingMemory,

    #[snafu(display("failed to get WASM export '{name}': {source}"))]
    GetExport {
        name: String,
        source: wasmtime::Error,
    },

    #[snafu(display("WASM function call '{name}' failed: {source}"))]
    CallExport {
        name: String,
        source: wasmtime::Error,
    },

    #[snafu(display("failed to write WASM memory: {source}"))]
    MemoryWrite { source: wasmtime::MemoryAccessError },

    #[snafu(display("WASM module returned an invalid memory range ptr={ptr} len={len}"))]
    InvalidMemoryRange { ptr: u32, len: u32 },

    #[snafu(display("failed to serialize Arrow IPC stream for WASM function: {source}"))]
    EncodeArrow { source: arrow::error::ArrowError },

    #[snafu(display("failed to decode Arrow IPC stream returned by WASM function: {source}"))]
    DecodeArrow { source: arrow::error::ArrowError },

    #[snafu(display(
        "WASM table function output schema does not match declared return schema: {details}"
    ))]
    ReturnSchemaMismatch { details: String },
}

pub type Result<T, E = WasmBuildError> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
enum InputSource {
    Sql(String),
    Table(TableReference),
    Plan(Arc<LogicalPlan>),
}

#[derive(Clone)]
struct WasmRunner {
    engine: Engine,
    module: Arc<Module>,
    entrypoint: String,
    limits: WasmLimits,
}

impl Debug for WasmRunner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmRunner")
            .field("entrypoint", &self.entrypoint)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
struct WasmLimits {
    fuel: u64,
    max_memory_bytes: usize,
    max_table_elements: usize,
}

impl ResourceLimiter for WasmLimits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.max_memory_bytes)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.max_table_elements)
    }

    fn instances(&self) -> usize {
        1
    }

    fn memories(&self) -> usize {
        1
    }
}

/// Build a WASM table function from a declaration.
///
/// # Errors
///
/// Returns [`WasmBuildError`] if the declaration, module, source compilation,
/// or declared Arrow schemas are invalid.
pub async fn build_table_udtf(
    decl: &Function,
    input_sql: Option<String>,
) -> Result<Arc<dyn TableFunctionImpl>> {
    let output_schema = table_return_schema(decl)?;
    let table_func = build_table_func(decl, input_sql, output_schema).await?;
    Ok(table_func)
}

/// Build a WASM scalar function from a declaration.
///
/// The scalar form uses the same Arrow IPC ABI as WASM table functions, then
/// rewrites calls to scalar subqueries so `DataFusion` enforces one-row output.
///
/// # Errors
///
/// Returns [`WasmBuildError`] if the declaration, module, source compilation,
/// or declared Arrow schemas are invalid.
pub async fn build_scalar_udf(
    decl: &Function,
    input_sql: Option<String>,
) -> Result<Arc<ScalarUDF>> {
    let (return_type, output_schema) = scalar_return_schema(decl)?;
    let table_func = build_table_func(decl, input_sql, output_schema).await?;
    let udf_impl = WasmScalarTableArgUdf {
        id: NEXT_WASM_SCALAR_ID.fetch_add(1, Ordering::Relaxed),
        name: decl.name.clone(),
        signature: Signature::variadic_any(map_volatility(decl.volatility)),
        return_type,
        table_func,
    };
    Ok(Arc::new(ScalarUDF::from(udf_impl)))
}

async fn build_table_func(
    decl: &Function,
    input_sql: Option<String>,
    output_schema: SchemaRef,
) -> Result<Arc<WasmTableFunc>> {
    let config = WasmConfig::from_decl(decl)?;
    let input_schema = declared_input_schema(decl)?;
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let input_source = configured_input_source(&config, input_sql)?;
    if input_source.is_none() && input_schema.is_none() {
        return MissingInputSourceSnafu.fail();
    }
    #[cfg(feature = "wasm-functions-compile")]
    let module_path = resolve_module_path(&config).await?;
    #[cfg(not(feature = "wasm-functions-compile"))]
    let module_path = resolve_module_path(&config)?;
    let module_bytes = tokio::fs::read(&module_path)
        .await
        .context(ReadModuleSnafu {
            path: module_path.display().to_string(),
        })?;
    let limits = config.limits;
    let mut engine_config = Config::new();
    engine_config.consume_fuel(true);
    let engine = Engine::new(&engine_config).context(CreateEngineSnafu)?;
    let module = Module::from_binary(&engine, &module_bytes).context(CompileModuleSnafu {
        path: module_path.display().to_string(),
    })?;

    Ok(Arc::new(WasmTableFunc {
        name: decl.name.clone(),
        arg_schema,
        input_schema,
        output_schema,
        configured_input_source: input_source,
        runner: Arc::new(WasmRunner {
            engine,
            module: Arc::new(module),
            entrypoint: config.entrypoint,
            limits,
        }),
    }))
}

#[derive(Debug)]
struct WasmScalarTableArgUdf {
    id: u64,
    name: String,
    signature: Signature,
    return_type: DataType,
    table_func: Arc<WasmTableFunc>,
}

impl PartialEq for WasmScalarTableArgUdf {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for WasmScalarTableArgUdf {}

impl Hash for WasmScalarTableArgUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for WasmScalarTableArgUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Err(DataFusionError::Execution(format!(
            "WASM scalar function '{}' with table arguments must be rewritten to a scalar subquery before execution",
            self.name
        )))
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> DataFusionResult<ExprSimplifyResult> {
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

#[derive(Clone)]
struct WasmTableFunc {
    name: String,
    arg_schema: SchemaRef,
    input_schema: Option<SchemaRef>,
    output_schema: SchemaRef,
    configured_input_source: Option<InputSource>,
    runner: Arc<WasmRunner>,
}

impl Debug for WasmTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmTableFunc")
            .field("name", &self.name)
            .field("arg_schema", &self.arg_schema)
            .field("input_schema", &self.input_schema)
            .field("output_schema", &self.output_schema)
            .field("configured_input_source", &self.configured_input_source)
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for WasmTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (input_source, scalar_exprs) = self.input_source_and_scalar_exprs(exprs)?;
        let args = table_arg_values(&self.name, self.arg_schema.as_ref(), scalar_exprs)?;
        Ok(Arc::new(WasmTableProvider {
            name: self.name.clone(),
            arg_schema: Arc::clone(&self.arg_schema),
            input_schema: self.input_schema.as_ref().map(Arc::clone),
            output_schema: Arc::clone(&self.output_schema),
            input_source,
            runner: Arc::clone(&self.runner),
            args,
        }))
    }
}

impl WasmTableFunc {
    fn input_source_and_scalar_exprs<'a>(
        &self,
        exprs: &'a [Expr],
    ) -> DataFusionResult<(InputSource, &'a [Expr])> {
        let scalar_arg_count = self.arg_schema.fields().len();
        let declares_input_table = self.input_schema.is_some();

        if declares_input_table && exprs.len() == scalar_arg_count + 1 {
            if self.configured_input_source.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "WASM table function '{}' received a table as the first argument but also has a configured input source; use either the first table argument or `params.input_table` / SQL body, not both",
                    self.name
                )));
            }
            let input_source = table_input_source_from_expr(&self.name, &exprs[0])?;
            return Ok((input_source, &exprs[1..]));
        }

        if exprs.len() != scalar_arg_count {
            let expected = if declares_input_table {
                format!(
                    "{scalar_arg_count} scalar argument(s), or 1 table argument followed by {scalar_arg_count} scalar argument(s)"
                )
            } else {
                format!("{scalar_arg_count} scalar argument(s)")
            };
            return Err(DataFusionError::Plan(format!(
                "WASM table function '{}' expected {expected}, got {} argument(s)",
                self.name,
                exprs.len()
            )));
        }

        let Some(input_source) = self.configured_input_source.clone() else {
            return Err(DataFusionError::Plan(format!(
                "WASM table function '{}' requires a table input as the first argument, `params.input_table`, or SQL `body` / `body_ref`",
                self.name
            )));
        };

        Ok((input_source, exprs))
    }
}

#[derive(Debug)]
struct WasmTableProvider {
    name: String,
    arg_schema: SchemaRef,
    input_schema: Option<SchemaRef>,
    output_schema: SchemaRef,
    input_source: InputSource,
    runner: Arc<WasmRunner>,
    args: Vec<ScalarValue>,
}

#[async_trait::async_trait]
impl TableProvider for WasmTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
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
        let ctx = context_from_state(state)?;
        let (input_schema, input_batches) = execute_input(&ctx, &self.input_source).await?;
        if let Some(declared_schema) = &self.input_schema {
            validate_schema(&self.name, input_schema.as_ref(), declared_schema.as_ref())
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        }

        let args_batch = args_record_batch(Arc::clone(&self.arg_schema), &self.args)?;
        let input_ipc = encode_ipc(&input_schema, &input_batches)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let args_ipc = encode_ipc(&self.arg_schema, &[args_batch])
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let runner = Arc::clone(&self.runner);
        let output_ipc = tokio::task::spawn_blocking(move || runner.invoke(&input_ipc, &args_ipc))
            .await
            .map_err(|source| DataFusionError::Execution(source.to_string()))?
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let mut batches = decode_ipc(&output_ipc, Arc::clone(&self.output_schema))
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        for batch in &batches {
            validate_schema(
                &self.name,
                batch.schema().as_ref(),
                self.output_schema.as_ref(),
            )
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        }
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        if let Some(limit) = limit {
            truncate_batches(&mut batches, limit);
        }
        let memory_source = MemorySourceConfig::try_new(
            &[batches],
            Arc::clone(&self.output_schema),
            projection.cloned(),
        )?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(memory_source))))
    }
}

impl WasmRunner {
    fn invoke(&self, input_ipc: &[u8], args_ipc: &[u8]) -> Result<Vec<u8>> {
        let limits = self.limits;
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        store.set_fuel(limits.fuel).context(SetFuelSnafu)?;
        let instance = Instance::new(&mut store, &self.module, &[]).context(InstantiateSnafu)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(WasmBuildError::MissingMemory)?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "spice_alloc")
            .context(GetExportSnafu {
                name: "spice_alloc".to_string(),
            })?;
        let dealloc = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "spice_dealloc")
            .context(GetExportSnafu {
                name: "spice_dealloc".to_string(),
            })?;
        let entrypoint = instance
            .get_typed_func::<(i32, i32, i32, i32), i64>(&mut store, &self.entrypoint)
            .context(GetExportSnafu {
                name: self.entrypoint.clone(),
            })?;

        let input = write_guest_bytes(&mut store, &memory, &alloc, input_ipc)?;
        let args = write_guest_bytes(&mut store, &memory, &alloc, args_ipc)?;
        let packed = entrypoint
            .call(
                &mut store,
                (
                    input.ptr_i32()?,
                    input.len_i32()?,
                    args.ptr_i32()?,
                    args.len_i32()?,
                ),
            )
            .context(CallExportSnafu {
                name: self.entrypoint.clone(),
            })?;
        let output = GuestAllocation::from_packed(packed);
        let output_bytes = read_guest_bytes(&store, &memory, output)?;
        dealloc_unique(&mut store, &dealloc, &[input, args, output])?;
        Ok(output_bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct GuestAllocation {
    ptr: u32,
    len: u32,
}

impl GuestAllocation {
    fn from_packed(packed: i64) -> Self {
        let bytes = packed.to_be_bytes();
        Self {
            ptr: u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            len: u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        }
    }

    fn packed(ptr: i32, len: usize) -> Result<Self> {
        let len_for_error = u32::try_from(len).unwrap_or(u32::MAX);
        let ptr = u32::try_from(ptr).map_err(|_| WasmBuildError::InvalidMemoryRange {
            ptr: 0,
            len: len_for_error,
        })?;
        let len = u32::try_from(len)
            .map_err(|_| WasmBuildError::InvalidMemoryRange { ptr, len: u32::MAX })?;
        Ok(Self { ptr, len })
    }

    fn ptr_i32(self) -> Result<i32> {
        i32::try_from(self.ptr).map_err(|_| WasmBuildError::InvalidMemoryRange {
            ptr: self.ptr,
            len: self.len,
        })
    }

    fn len_i32(self) -> Result<i32> {
        i32::try_from(self.len).map_err(|_| WasmBuildError::InvalidMemoryRange {
            ptr: self.ptr,
            len: self.len,
        })
    }
}

fn write_guest_bytes(
    store: &mut Store<WasmLimits>,
    memory: &wasmtime::Memory,
    alloc: &TypedFunc<i32, i32>,
    bytes: &[u8],
) -> Result<GuestAllocation> {
    let len = i32::try_from(bytes.len()).map_err(|_| WasmBuildError::InvalidMemoryRange {
        ptr: 0,
        len: u32::MAX,
    })?;
    let ptr = alloc.call(&mut *store, len).context(CallExportSnafu {
        name: "spice_alloc".to_string(),
    })?;
    let allocation = GuestAllocation::packed(ptr, bytes.len())?;
    memory
        .write(&mut *store, allocation.ptr as usize, bytes)
        .context(MemoryWriteSnafu)?;
    Ok(allocation)
}

fn read_guest_bytes(
    store: &Store<WasmLimits>,
    memory: &wasmtime::Memory,
    allocation: GuestAllocation,
) -> Result<Vec<u8>> {
    let start = allocation.ptr as usize;
    let len = allocation.len as usize;
    let end = start
        .checked_add(len)
        .filter(|end| *end <= memory.data_size(store))
        .ok_or(WasmBuildError::InvalidMemoryRange {
            ptr: allocation.ptr,
            len: allocation.len,
        })?;
    Ok(memory.data(store)[start..end].to_vec())
}

fn dealloc_unique(
    store: &mut Store<WasmLimits>,
    dealloc: &TypedFunc<(i32, i32), ()>,
    allocations: &[GuestAllocation],
) -> Result<()> {
    let mut seen = HashSet::with_capacity(allocations.len());
    for allocation in allocations {
        if seen.insert(*allocation) {
            dealloc
                .call(&mut *store, (allocation.ptr_i32()?, allocation.len_i32()?))
                .context(CallExportSnafu {
                    name: "spice_dealloc".to_string(),
                })?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct WasmConfig {
    module: Option<PathBuf>,
    #[cfg(feature = "wasm-functions-compile")]
    source: Option<PathBuf>,
    #[cfg(feature = "wasm-functions-compile")]
    language: String,
    entrypoint: String,
    input_table: Option<String>,
    limits: WasmLimits,
    #[cfg(feature = "wasm-functions-compile")]
    cache_dir: Option<PathBuf>,
    #[cfg(feature = "wasm-functions-compile")]
    artifact: Option<String>,
    #[cfg(feature = "wasm-functions-compile")]
    target: String,
}

impl WasmConfig {
    fn from_decl(decl: &Function) -> Result<Self> {
        let module = param_path(&decl.params, MODULE_PARAM)?;
        let source = param_path(&decl.params, SOURCE_PARAM)?;
        if module.is_some() && source.is_some() {
            return ConflictingModuleSourceSnafu.fail();
        }
        if module.is_none() && source.is_none() {
            return MissingModuleOrSourceSnafu.fail();
        }

        #[cfg(feature = "wasm-functions-compile")]
        let language = {
            let language = param_string(&decl.params, "language")?
                .unwrap_or_else(|| DEFAULT_LANGUAGE.to_string())
                .to_ascii_lowercase();
            if source.is_some() && language != DEFAULT_LANGUAGE {
                return UnsupportedLanguageSnafu { language }.fail();
            }
            language
        };
        #[cfg(not(feature = "wasm-functions-compile"))]
        let _ = param_string(&decl.params, "language")?;

        Ok(Self {
            module,
            #[cfg(feature = "wasm-functions-compile")]
            source,
            #[cfg(feature = "wasm-functions-compile")]
            language,
            entrypoint: param_string(&decl.params, "entrypoint")?
                .unwrap_or_else(|| DEFAULT_ENTRYPOINT.to_string()),
            input_table: param_string(&decl.params, INPUT_TABLE_PARAM)?,
            limits: WasmLimits {
                fuel: param_u64(&decl.params, "fuel", DEFAULT_WASM_FUEL)?,
                max_memory_bytes: param_usize(
                    &decl.params,
                    "max_memory_bytes",
                    DEFAULT_WASM_MAX_MEMORY_BYTES,
                )?,
                max_table_elements: param_usize(
                    &decl.params,
                    "max_table_elements",
                    DEFAULT_WASM_MAX_TABLE_ELEMENTS,
                )?,
            },
            #[cfg(feature = "wasm-functions-compile")]
            cache_dir: param_path(&decl.params, "cache_dir")?,
            #[cfg(feature = "wasm-functions-compile")]
            artifact: param_string(&decl.params, "artifact")?,
            #[cfg(feature = "wasm-functions-compile")]
            target: param_string(&decl.params, "target")?
                .unwrap_or_else(|| DEFAULT_TARGET.to_string()),
        })
    }
}

fn param_string(params: &HashMap<String, Value>, key: &str) -> Result<Option<String>> {
    params
        .get(key)
        .map(|value| {
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                WasmBuildError::InvalidStringParam {
                    param: key.to_string(),
                }
            })
        })
        .transpose()
}

fn param_path(params: &HashMap<String, Value>, key: &str) -> Result<Option<PathBuf>> {
    Ok(param_string(params, key)?.map(PathBuf::from))
}

fn param_u64(params: &HashMap<String, Value>, key: &str, default: u64) -> Result<u64> {
    let Some(value) = params.get(key) else {
        return Ok(default);
    };
    let value = value
        .as_u64()
        .ok_or_else(|| WasmBuildError::InvalidIntegerParam {
            param: key.to_string(),
            expected: "a positive integer".to_string(),
            got: format!("{value}"),
        })?;
    if value == 0 {
        return Err(WasmBuildError::InvalidIntegerParam {
            param: key.to_string(),
            expected: "a positive integer".to_string(),
            got: value.to_string(),
        });
    }
    Ok(value)
}

fn param_usize(params: &HashMap<String, Value>, key: &str, default: usize) -> Result<usize> {
    let default = u64::try_from(default).map_err(|_| WasmBuildError::InvalidIntegerParam {
        param: key.to_string(),
        expected: "a positive integer that fits in u64".to_string(),
        got: default.to_string(),
    })?;
    let value = param_u64(params, key, default)?;
    usize::try_from(value).map_err(|_| WasmBuildError::InvalidIntegerParam {
        param: key.to_string(),
        expected: "a positive integer that fits in usize".to_string(),
        got: value.to_string(),
    })
}

fn configured_input_source(
    config: &WasmConfig,
    input_sql: Option<String>,
) -> Result<Option<InputSource>> {
    match (config.input_table.as_ref(), input_sql) {
        (Some(_), Some(_)) => ConflictingInputSourceSnafu.fail(),
        (Some(table), None) => Ok(Some(InputSource::Table(TableReference::parse_str(table)))),
        (None, Some(sql)) => Ok(Some(InputSource::Sql(sql))),
        (None, None) => Ok(None),
    }
}

fn table_input_source_from_expr(function_name: &str, expr: &Expr) -> DataFusionResult<InputSource> {
    match expr {
        Expr::Column(column) => Ok(InputSource::Table(table_ref_from_column_expr(column))),
        Expr::Literal(ScalarValue::Utf8(Some(table)), _) => {
            Ok(InputSource::Table(TableReference::parse_str(table)))
        }
        Expr::ScalarSubquery(subquery) => {
            if !subquery.outer_ref_columns.is_empty() {
                return Err(DataFusionError::NotImplemented(format!(
                    "WASM table function '{function_name}' does not support correlated dynamic table inputs"
                )));
            }
            Ok(InputSource::Plan(Arc::clone(&subquery.subquery)))
        }
        other => Err(DataFusionError::Plan(format!(
            "WASM table function '{function_name}' requires a table reference or dynamic table input as the first argument, got: {other:?}"
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

#[cfg(not(feature = "wasm-functions-compile"))]
fn resolve_module_path(config: &WasmConfig) -> Result<PathBuf> {
    if let Some(module) = &config.module {
        return Ok(module.clone());
    }

    SourceCompileDisabledSnafu.fail()
}

#[cfg(feature = "wasm-functions-compile")]
async fn resolve_module_path(config: &WasmConfig) -> Result<PathBuf> {
    if let Some(module) = &config.module {
        return Ok(module.clone());
    }

    let source = config
        .source
        .clone()
        .ok_or(WasmBuildError::MissingModuleOrSource)?;
    let compile_config = CompileConfig {
        source,
        cache_dir: config.cache_dir.clone(),
        artifact: config.artifact.clone(),
        target: config.target.clone(),
        language: config.language.clone(),
    };
    tokio::task::spawn_blocking(move || compile_rust_source(&compile_config))
        .await
        .context(CompileTaskSnafu)?
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

fn declared_input_schema(decl: &Function) -> Result<Option<SchemaRef>> {
    if decl.signature.tables.len() > 1 {
        return TooManyInputTablesSnafu.fail();
    }
    decl.signature
        .tables
        .first()
        .map(|table| function_arg_schema(&table.columns))
        .transpose()
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
                return DuplicateColumnSnafu {
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

fn scalar_return_schema(decl: &Function) -> Result<(DataType, SchemaRef)> {
    let return_type = match decl.signature.returns.as_ref() {
        Some(FunctionReturns::Scalar(arrow_type)) => parse_arrow_type(arrow_type)?,
        Some(FunctionReturns::Table(_)) => return ExpectedScalarReturnSchemaSnafu.fail(),
        None => return MissingScalarReturnSchemaSnafu.fail(),
    };
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        return_type.clone(),
        true,
    )]));
    Ok((return_type, schema))
}

fn map_volatility(volatility: Volatility) -> DfVolatility {
    match volatility {
        Volatility::Immutable => DfVolatility::Immutable,
        Volatility::Stable => DfVolatility::Stable,
        Volatility::Volatile => DfVolatility::Volatile,
    }
}

fn parse_arrow_type(s: &str) -> Result<DataType> {
    super::arrow_type::parse_arrow_type(s).map_err(|_| WasmBuildError::UnsupportedArrowType {
        arrow_type: s.to_string(),
    })
}

fn table_arg_values(
    function_name: &str,
    schema: &Schema,
    exprs: &[Expr],
) -> DataFusionResult<Vec<ScalarValue>> {
    let fields = schema.fields();
    if exprs.len() != fields.len() {
        return Err(DataFusionError::Plan(format!(
            "WASM table function '{function_name}' expected {} argument(s), got {}",
            fields.len(),
            exprs.len()
        )));
    }

    exprs
        .iter()
        .zip(fields.iter())
        .map(|(expr, field)| {
            let Expr::Literal(scalar, _) = expr else {
                return Err(DataFusionError::NotImplemented(format!(
                    "WASM table function '{function_name}' currently supports literal arguments only; got {expr:?}"
                )));
            };
            cast_scalar_arg(scalar, field.data_type())
        })
        .collect()
}

fn cast_scalar_arg(value: &ScalarValue, data_type: &DataType) -> DataFusionResult<ScalarValue> {
    if matches!(value, ScalarValue::Null) {
        return ScalarValue::try_from(data_type);
    }
    value.cast_to(data_type)
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

fn context_from_state(state: &dyn Session) -> DataFusionResult<SessionContext> {
    let state = state
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "WASM table function execution requires a DataFusion SessionState".to_string(),
            )
        })?;
    Ok(SessionContext::new_with_state(
        builder_from_existing(state).build(),
    ))
}

async fn execute_input(
    ctx: &SessionContext,
    source: &InputSource,
) -> DataFusionResult<(SchemaRef, Vec<RecordBatch>)> {
    let df = match source {
        InputSource::Sql(sql) => ctx.sql(sql).await?,
        InputSource::Table(table) => ctx.table(table.clone()).await?,
        InputSource::Plan(plan) => ctx.execute_logical_plan((**plan).clone()).await?,
    };
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    Ok((schema, batches))
}

fn encode_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema).context(EncodeArrowSnafu)?;
        for batch in batches {
            writer.write(batch).context(EncodeArrowSnafu)?;
        }
        writer.finish().context(EncodeArrowSnafu)?;
    }
    Ok(bytes)
}

fn decode_ipc(bytes: &[u8], empty_schema: SchemaRef) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(vec![RecordBatch::new_empty(empty_schema)]);
    }
    let reader = StreamReader::try_new(Cursor::new(bytes), None).context(DecodeArrowSnafu)?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .context(DecodeArrowSnafu)
}

fn validate_schema(function_name: &str, actual: &Schema, expected: &Schema) -> Result<()> {
    let actual_fields = actual.fields();
    let expected_fields = expected.fields();
    if actual_fields.len() != expected_fields.len() {
        return ReturnSchemaMismatchSnafu {
            details: format!(
                "expected {} column(s), got {} column(s) for function '{function_name}'",
                expected_fields.len(),
                actual_fields.len()
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

fn truncate_batches(batches: &mut Vec<RecordBatch>, limit: usize) {
    let mut remaining = limit;
    let mut keep = 0;
    for batch in batches.iter_mut() {
        if remaining == 0 {
            break;
        }
        let rows = batch.num_rows();
        if rows > remaining {
            *batch = batch.slice(0, remaining);
            keep += 1;
            break;
        }
        remaining -= rows;
        keep += 1;
    }
    batches.truncate(keep);
}

#[cfg(feature = "wasm-functions-compile")]
#[derive(Debug)]
struct CompileConfig {
    source: PathBuf,
    cache_dir: Option<PathBuf>,
    artifact: Option<String>,
    target: String,
    language: String,
}

#[cfg(feature = "wasm-functions-compile")]
fn compile_rust_source(config: &CompileConfig) -> Result<PathBuf> {
    if config.language != DEFAULT_LANGUAGE {
        return UnsupportedLanguageSnafu {
            language: config.language.clone(),
        }
        .fail();
    }
    let cache_dir = config
        .cache_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(".spice/wasm-cache"));
    std::fs::create_dir_all(&cache_dir).context(CreateCacheDirSnafu {
        path: cache_dir.display().to_string(),
    })?;

    let cache_key = source_hash(&config.source)?;
    let cached = cache_dir.join(format!("{cache_key}.wasm"));
    if cached.exists() {
        return Ok(cached);
    }

    if config.source.extension().and_then(std::ffi::OsStr::to_str) == Some("rs") {
        compile_rust_file(config, &cached)?;
        return Ok(cached);
    }

    let artifact = compile_cargo_project(config)?;
    std::fs::copy(&artifact, &cached).context(CopyArtifactSnafu {
        from: artifact.display().to_string(),
        to: cached.display().to_string(),
    })?;
    Ok(cached)
}

#[cfg(feature = "wasm-functions-compile")]
fn source_hash(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hash_path(path, &mut hasher)?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(feature = "wasm-functions-compile")]
fn hash_path(path: &Path, hasher: &mut blake3::Hasher) -> Result<()> {
    if path.is_file() {
        hasher.update(path.to_string_lossy().as_bytes());
        let bytes = std::fs::read(path).context(HashSourceSnafu {
            path: path.display().to_string(),
        })?;
        hasher.update(&bytes);
        return Ok(());
    }

    let mut entries = std::fs::read_dir(path)
        .context(HashSourceSnafu {
            path: path.display().to_string(),
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context(HashSourceSnafu {
            path: path.display().to_string(),
        })?;
    entries.sort_by_key(std::fs::DirEntry::path);
    for entry in entries {
        let entry_path = entry.path();
        if entry_path.is_dir()
            && entry_path
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|name| name == "target")
        {
            continue;
        }
        if entry_path.is_dir()
            || entry_path
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|ext| matches!(ext, "rs" | "toml" | "lock"))
        {
            hash_path(&entry_path, hasher)?;
        }
    }
    Ok(())
}

#[cfg(feature = "wasm-functions-compile")]
fn compile_rust_file(config: &CompileConfig, output: &Path) -> Result<()> {
    let result = Command::new("rustc")
        .arg("--edition")
        .arg("2024")
        .arg("--crate-type")
        .arg("cdylib")
        .arg("--target")
        .arg(&config.target)
        .arg("-O")
        .arg(&config.source)
        .arg("-o")
        .arg(output)
        .output()
        .map_err(|source| WasmBuildError::CompileRust {
            message: source.to_string(),
        })?;
    if !result.status.success() {
        return CompileRustSnafu {
            message: String::from_utf8_lossy(&result.stderr).to_string(),
        }
        .fail();
    }
    Ok(())
}

#[cfg(feature = "wasm-functions-compile")]
fn compile_cargo_project(config: &CompileConfig) -> Result<PathBuf> {
    let manifest = manifest_path(&config.source);
    let result = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg(&config.target)
        .arg("--manifest-path")
        .arg(&manifest)
        .output()
        .map_err(|source| WasmBuildError::CompileRust {
            message: source.to_string(),
        })?;
    if !result.status.success() {
        return CompileRustSnafu {
            message: String::from_utf8_lossy(&result.stderr).to_string(),
        }
        .fail();
    }

    let release_dir = manifest
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("target")
        .join(&config.target)
        .join("release");
    if let Some(artifact) = &config.artifact {
        return Ok(release_dir.join(artifact));
    }

    let mut wasm_files = std::fs::read_dir(&release_dir)
        .context(ReadArtifactDirSnafu {
            path: release_dir.display().to_string(),
        })?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(std::ffi::OsStr::to_str) == Some("wasm"))
        .collect::<Vec<_>>();
    wasm_files.sort();
    match wasm_files.as_slice() {
        [artifact] => Ok(artifact.clone()),
        [] => CompileRustSnafu {
            message: format!("no .wasm artifact found in {}", release_dir.display()),
        }
        .fail(),
        _ => CompileRustSnafu {
            message: "multiple .wasm artifacts found; set params.artifact".to_string(),
        }
        .fail(),
    }
}

#[cfg(feature = "wasm-functions-compile")]
fn manifest_path(source: &Path) -> PathBuf {
    if source.is_dir() {
        source.join("Cargo.toml")
    } else if source.file_name().and_then(std::ffi::OsStr::to_str) == Some("Cargo.toml") {
        source.to_path_buf()
    } else {
        source
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("Cargo.toml")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::common::Spans;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Subquery;
    use datafusion::prelude::{SessionContext, col, lit};
    use spicepod::component::function::{FunctionKind, FunctionTableArg, Signature, Volatility};
    use std::collections::HashMap;
    use std::path::Path;
    use tempfile::TempDir;

    fn identity_wasm(dir: &TempDir) -> PathBuf {
        let wasm = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (global $heap (mut i32) (i32.const 2048))
              (func (export "spice_alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.set $ptr
                global.get $heap
                local.get $len
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "spice_dealloc") (param i32) (param i32))
              (func (export "spice_transform")
                (param $input_ptr i32) (param $input_len i32) (param $args_ptr i32) (param $args_len i32)
                (result i64)
                local.get $input_ptr
                i64.extend_i32_u
                i64.const 32
                i64.shl
                local.get $input_len
                i64.extend_i32_u
                i64.or))
            "#,
        )
        .expect("valid wat");
        let path = dir.path().join("identity.wasm");
        std::fs::write(&path, wasm).expect("write wasm");
        path
    }

    fn looping_runner(fuel: u64) -> WasmRunner {
        let wasm = wat::parse_str(
            r#"
                        (module
                            (memory (export "memory") 1)
                            (func (export "spice_alloc") (param i32) (result i32)
                                i32.const 0)
                            (func (export "spice_dealloc") (param i32) (param i32))
                            (func (export "spice_transform")
                                (param i32) (param i32) (param i32) (param i32)
                                (result i64)
                                (loop $again
                                    br $again)
                                i64.const 0))
                        "#,
        )
        .expect("valid wat");
        let mut engine_config = Config::new();
        engine_config.consume_fuel(true);
        let engine = Engine::new(&engine_config).expect("engine");
        let module = Module::from_binary(&engine, &wasm).expect("module");
        WasmRunner {
            engine,
            module: Arc::new(module),
            entrypoint: DEFAULT_ENTRYPOINT.to_string(),
            limits: WasmLimits {
                fuel,
                max_memory_bytes: DEFAULT_WASM_MAX_MEMORY_BYTES,
                max_table_elements: DEFAULT_WASM_MAX_TABLE_ELEMENTS,
            },
        }
    }

    fn wasm_identity_decl(module: &Path) -> Function {
        let mut params = HashMap::new();
        params.insert(
            MODULE_PARAM.to_string(),
            Value::String(module.display().to_string()),
        );
        params.insert(
            INPUT_TABLE_PARAM.to_string(),
            Value::String("numbers".into()),
        );
        Function {
            name: "wasm_identity".into(),
            from: "wasm".into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Table,
            volatility: Volatility::Stable,
            signature: Signature {
                tables: vec![FunctionTableArg {
                    name: "input".into(),
                    columns: vec![FunctionArg {
                        name: "value".into(),
                        arrow_type: "int64".into(),
                    }],
                }],
                args: vec![],
                returns: Some(FunctionReturns::Table(vec![FunctionArg {
                    name: "value".into(),
                    arrow_type: "int64".into(),
                }])),
            },
            body: None,
            body_ref: None,
            metadata: HashMap::new(),
            params,
            depends_on: vec![],
            metrics: None,
            as_tool: false,
        }
    }

    fn wasm_scalar_identity_decl(module: &Path) -> Function {
        let mut decl = wasm_identity_decl(module);
        decl.name = "wasm_scalar_identity".into();
        decl.kind = FunctionKind::Scalar;
        decl.signature.returns = Some(FunctionReturns::Scalar("int64".into()));
        decl
    }

    async fn single_value_input_expr(ctx: &SessionContext) -> Expr {
        let input_df = ctx
            .table("numbers")
            .await
            .expect("table exists")
            .filter(col("value").eq(lit(2_i64)))
            .expect("filters")
            .select(vec![col("value")])
            .expect("projects");
        Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(input_df.into_unoptimized_plan()),
            outer_ref_columns: vec![],
            spans: Spans::new(),
        })
    }

    #[test]
    fn wasm_runner_interrupts_when_fuel_is_exhausted() {
        let runner = looping_runner(10);
        let err = runner.invoke(&[], &[]).expect_err("fuel exhaustion");
        assert!(matches!(err, WasmBuildError::CallExport { .. }));
    }

    #[tokio::test]
    async fn wasm_table_udtf_round_trips_arrow_ipc_from_input_table() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let decl = wasm_identity_decl(&module);
        let udtf = build_table_udtf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udtf(&decl.name, udtf);

        let results = ctx
            .sql("SELECT value FROM wasm_identity() ORDER BY value")
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
        assert_eq!(values.values(), &[1_i64, 2, 3]);
    }

    #[tokio::test]
    async fn wasm_table_udtf_round_trips_arrow_ipc_from_sql_body() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        decl.body = Some("SELECT value FROM numbers WHERE value > 1".into());
        let body = decl.body.clone();
        let udtf = build_table_udtf(&decl, body).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udtf(&decl.name, udtf);

        let results = ctx
            .sql("SELECT value FROM wasm_identity() ORDER BY value")
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
        assert_eq!(values.values(), &[2_i64, 3]);
    }

    #[tokio::test]
    async fn wasm_table_udtf_accepts_input_table_as_first_argument() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        decl.signature.args.push(FunctionArg {
            name: "unused".into(),
            arrow_type: "int64".into(),
        });
        let udtf = build_table_udtf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udtf(&decl.name, udtf);

        let results = ctx
            .sql("SELECT value FROM wasm_identity(numbers, 7) ORDER BY value")
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
        assert_eq!(values.values(), &[1_i64, 2, 3]);
    }

    #[tokio::test]
    async fn wasm_table_udtf_accepts_dynamic_input_from_sql_subquery() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        let udtf = build_table_udtf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udtf(&decl.name, udtf);

        let results = ctx
            .sql(
                "WITH filtered AS (SELECT value FROM numbers WHERE value > 1) \
                 SELECT value FROM wasm_identity((SELECT value FROM filtered)) ORDER BY value",
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
        assert_eq!(values.values(), &[2_i64, 3]);
    }

    #[tokio::test]
    async fn wasm_table_udtf_accepts_input_table_via_dataframe_api() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        decl.signature.args.push(FunctionArg {
            name: "unused".into(),
            arrow_type: "int64".into(),
        });
        let udtf = build_table_udtf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udtf(&decl.name, udtf);
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[col("numbers"), lit(7_i64)])
            .expect("creates table provider");
        ctx.register_table("wasm_identity_result", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("wasm_identity_result")
            .await
            .expect("table exists")
            .filter(col("value").gt(lit(0_i64)))
            .expect("filters")
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
    async fn wasm_table_udtf_accepts_dynamic_input_via_dataframe_api() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        decl.signature.args.push(FunctionArg {
            name: "unused".into(),
            arrow_type: "int64".into(),
        });
        let udtf = build_table_udtf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udtf(&decl.name, udtf);

        let input_df = ctx
            .table("numbers")
            .await
            .expect("table exists")
            .filter(col("value").gt(lit(1_i64)))
            .expect("filters")
            .select(vec![col("value")])
            .expect("projects");
        let input_expr = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(input_df.into_unoptimized_plan()),
            outer_ref_columns: vec![],
            spans: Spans::new(),
        });
        let provider = ctx
            .table_function(&decl.name)
            .expect("registered UDTF")
            .create_table_provider(&[input_expr, lit(7_i64)])
            .expect("creates table provider");
        ctx.register_table("wasm_identity_dynamic_result", provider)
            .expect("register UDTF result");

        let results = ctx
            .table("wasm_identity_dynamic_result")
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
        assert_eq!(values.values(), &[2_i64, 3]);
    }

    #[tokio::test]
    async fn wasm_scalar_udf_accepts_dynamic_input_from_sql_subquery() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_scalar_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        let udf = build_scalar_udf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udf(udf.as_ref().clone());

        let results = ctx
            .sql(
                "WITH filtered AS (SELECT value FROM numbers WHERE value = 2) \
                 SELECT wasm_scalar_identity((SELECT value FROM filtered)) AS value",
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
        assert_eq!(values.values(), &[2_i64]);
    }

    #[tokio::test]
    async fn wasm_scalar_udf_accepts_dynamic_input_via_dataframe_api() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let module = identity_wasm(&temp_dir);
        let mut decl = wasm_scalar_identity_decl(&module);
        decl.params.remove(INPUT_TABLE_PARAM);
        let udf = build_scalar_udf(&decl, None).await.expect("builds");

        let ctx = SessionContext::new();
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
        ctx.register_udf(udf.as_ref().clone());
        let input_expr = single_value_input_expr(&ctx).await;

        let results = ctx
            .table("numbers")
            .await
            .expect("table exists")
            .filter(udf.call(vec![input_expr]).eq(lit(2_i64)))
            .expect("filters")
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
}
