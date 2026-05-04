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
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{Function, FunctionArg, FunctionReturns};
use util::session_state::builder_from_existing;
use wasmtime::{Engine, Instance, Module, Store, TypedFunc};

const DEFAULT_ENTRYPOINT: &str = "spice_transform";
const DEFAULT_LANGUAGE: &str = "rust";
const DEFAULT_TARGET: &str = "wasm32-unknown-unknown";
const INPUT_TABLE_PARAM: &str = "input_table";
const MODULE_PARAM: &str = "module";
const SOURCE_PARAM: &str = "source";

#[derive(Debug, Snafu)]
pub enum WasmBuildError {
    #[snafu(display(
        "WASM table functions require `signature.returns` to be a list of output columns"
    ))]
    MissingTableReturnSchema,

    #[snafu(display(
        "WASM table functions require table return columns, not a scalar Arrow return type"
    ))]
    ExpectedTableReturnSchema,

    #[snafu(display(
        "unsupported or invalid Arrow type '{arrow_type}' for WASM function signature"
    ))]
    UnsupportedArrowType { arrow_type: String },

    #[snafu(display("duplicate column '{column}' in WASM function schema"))]
    DuplicateColumn { column: String },

    #[snafu(display("WASM functions support at most one declared input table for now"))]
    TooManyInputTables,

    #[snafu(display(
        "WASM table function requires exactly one input source: set `params.input_table` or provide SQL in `body` / `body_ref`"
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
    Table(String),
}

#[derive(Clone)]
struct WasmRunner {
    engine: Engine,
    module: Arc<Module>,
    entrypoint: String,
}

impl Debug for WasmRunner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmRunner")
            .field("entrypoint", &self.entrypoint)
            .finish_non_exhaustive()
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
    let config = WasmConfig::from_decl(decl)?;
    let input_schema = declared_input_schema(decl)?;
    let output_schema = table_return_schema(decl)?;
    let arg_schema = function_arg_schema(&decl.signature.args)?;
    let input_source = input_source(&config, input_sql)?;
    let module_path = resolve_module_path(&config).await?;
    let module_bytes = std::fs::read(&module_path).context(ReadModuleSnafu {
        path: module_path.display().to_string(),
    })?;
    let engine = Engine::default();
    let module = Module::from_binary(&engine, &module_bytes).context(CompileModuleSnafu {
        path: module_path.display().to_string(),
    })?;

    Ok(Arc::new(WasmTableFunc {
        name: decl.name.clone(),
        arg_schema,
        input_schema,
        output_schema,
        input_source,
        runner: Arc::new(WasmRunner {
            engine,
            module: Arc::new(module),
            entrypoint: config.entrypoint,
        }),
    }))
}

#[derive(Clone)]
struct WasmTableFunc {
    name: String,
    arg_schema: SchemaRef,
    input_schema: Option<SchemaRef>,
    output_schema: SchemaRef,
    input_source: InputSource,
    runner: Arc<WasmRunner>,
}

impl Debug for WasmTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmTableFunc")
            .field("name", &self.name)
            .field("arg_schema", &self.arg_schema)
            .field("input_schema", &self.input_schema)
            .field("output_schema", &self.output_schema)
            .field("input_source", &self.input_source)
            .finish_non_exhaustive()
    }
}

impl TableFunctionImpl for WasmTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let args = table_arg_values(&self.name, self.arg_schema.as_ref(), exprs)?;
        Ok(Arc::new(WasmTableProvider {
            name: self.name.clone(),
            arg_schema: Arc::clone(&self.arg_schema),
            input_schema: self.input_schema.as_ref().map(Arc::clone),
            output_schema: Arc::clone(&self.output_schema),
            input_source: self.input_source.clone(),
            runner: Arc::clone(&self.runner),
            args,
        }))
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

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
        let input_ipc = encode_ipc(Arc::clone(&input_schema), &input_batches)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let args_ipc = encode_ipc(Arc::clone(&self.arg_schema), &[args_batch])
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
        let mut store = Store::new(&self.engine, ());
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
                    input.ptr_i32(),
                    input.len_i32(),
                    args.ptr_i32(),
                    args.len_i32(),
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
        let packed = packed as u64;
        Self {
            ptr: (packed >> 32) as u32,
            len: (packed & u64::from(u32::MAX)) as u32,
        }
    }

    fn packed(ptr: i32, len: usize) -> Result<Self> {
        let ptr = u32::try_from(ptr).map_err(|_| WasmBuildError::InvalidMemoryRange {
            ptr: ptr as u32,
            len: u32::try_from(len).unwrap_or(u32::MAX),
        })?;
        let len = u32::try_from(len)
            .map_err(|_| WasmBuildError::InvalidMemoryRange { ptr, len: u32::MAX })?;
        Ok(Self { ptr, len })
    }

    fn ptr_i32(self) -> i32 {
        self.ptr as i32
    }

    fn len_i32(self) -> i32 {
        self.len as i32
    }
}

fn write_guest_bytes(
    store: &mut Store<()>,
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
    store: &Store<()>,
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
    store: &mut Store<()>,
    dealloc: &TypedFunc<(i32, i32), ()>,
    allocations: &[GuestAllocation],
) -> Result<()> {
    let mut seen = HashSet::with_capacity(allocations.len());
    for allocation in allocations {
        if seen.insert(*allocation) {
            dealloc
                .call(&mut *store, (allocation.ptr_i32(), allocation.len_i32()))
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
    source: Option<PathBuf>,
    language: String,
    entrypoint: String,
    input_table: Option<String>,
    cache_dir: Option<PathBuf>,
    artifact: Option<String>,
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

        let language = param_string(&decl.params, "language")?
            .unwrap_or_else(|| DEFAULT_LANGUAGE.to_string())
            .to_ascii_lowercase();
        if source.is_some() && language != DEFAULT_LANGUAGE {
            return UnsupportedLanguageSnafu { language }.fail();
        }

        Ok(Self {
            module,
            source,
            language,
            entrypoint: param_string(&decl.params, "entrypoint")?
                .unwrap_or_else(|| DEFAULT_ENTRYPOINT.to_string()),
            input_table: param_string(&decl.params, INPUT_TABLE_PARAM)?,
            cache_dir: param_path(&decl.params, "cache_dir")?,
            artifact: param_string(&decl.params, "artifact")?,
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

fn input_source(config: &WasmConfig, input_sql: Option<String>) -> Result<InputSource> {
    match (config.input_table.as_ref(), input_sql) {
        (Some(_), Some(_)) => ConflictingInputSourceSnafu.fail(),
        (Some(table), None) => Ok(InputSource::Table(table.clone())),
        (None, Some(sql)) => Ok(InputSource::Sql(sql)),
        (None, None) => MissingInputSourceSnafu.fail(),
    }
}

async fn resolve_module_path(config: &WasmConfig) -> Result<PathBuf> {
    if let Some(module) = &config.module {
        return Ok(module.clone());
    }

    #[cfg(not(feature = "wasm-functions-compile"))]
    {
        SourceCompileDisabledSnafu.fail()
    }

    #[cfg(feature = "wasm-functions-compile")]
    {
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
    let sql = match source {
        InputSource::Sql(sql) => sql.clone(),
        InputSource::Table(table) => format!("SELECT * FROM {table}"),
    };
    let df = ctx.sql(&sql).await?;
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;
    Ok((schema, batches))
}

fn encode_ipc(schema: SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).context(EncodeArrowSnafu)?;
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
            remaining = 0;
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
        .context(HashSourceSnafu {
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
    use datafusion::prelude::SessionContext;
    use spicepod::component::function::{FunctionKind, FunctionTableArg, Signature, Volatility};
    use std::collections::HashMap;
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
                args: vec![],
                tables: vec![FunctionTableArg {
                    name: "input".into(),
                    columns: vec![FunctionArg {
                        name: "value".into(),
                        arrow_type: "int64".into(),
                    }],
                }],
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
}
