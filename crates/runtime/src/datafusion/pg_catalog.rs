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

use std::{
    any::Any,
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, SchemaRef},
};
use async_trait::async_trait;
use data_components::DESCRIPTION_METADATA_KEY;
use datafusion::{
    catalog::CatalogProviderList,
    common::{DataFusionError, ScalarValue, TableReference, types::logical_string},
    logical_expr::{
        Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, TypeSignatureClass, Volatility,
        async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl},
    },
    prelude::SessionContext,
};

pub const OBJ_DESCRIPTION_UDF_NAME: &str = "obj_description";
pub const COL_DESCRIPTION_UDF_NAME: &str = "col_description";

static NEXT_COMMENT_UDF_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct CommentLookup {
    catalog_list: Arc<dyn CatalogProviderList>,
    default_catalog: String,
    default_schema: String,
}

impl CommentLookup {
    fn new(ctx: &SessionContext) -> Self {
        let state = ctx.state();
        let config_options = state.config_options();
        Self {
            catalog_list: Arc::clone(state.catalog_list()),
            default_catalog: config_options.catalog.default_catalog.clone(),
            default_schema: config_options.catalog.default_schema.clone(),
        }
    }

    fn resolve_table_name(&self, table_name: &str) -> TableKey {
        let resolved = TableReference::parse_str(table_name)
            .resolve(&self.default_catalog, &self.default_schema);
        TableKey {
            catalog: resolved.catalog.to_string(),
            schema: resolved.schema.to_string(),
            table: resolved.table.to_string(),
        }
    }

    async fn table_schema(
        &self,
        table_key: &TableKey,
        cache: &mut CommentLookupCache,
    ) -> Result<Option<SchemaRef>, DataFusionError> {
        if let Some(schema) = cache.schema_by_table.get(table_key) {
            return Ok(schema.clone());
        }

        let schema = self.load_table_schema(table_key).await?;
        cache
            .schema_by_table
            .insert(table_key.clone(), schema.clone());
        Ok(schema)
    }

    fn table_key_for_oid(&self, oid: u32, cache: &mut CommentLookupCache) -> Option<TableKey> {
        if let Some(table_key) = cache.table_by_oid.get(&oid) {
            return table_key.clone();
        }

        let table_key = self.find_table_key_for_oid(oid);
        cache.table_by_oid.insert(oid, table_key.clone());
        table_key
    }

    fn find_table_key_for_oid(&self, oid: u32) -> Option<TableKey> {
        let mut matching_table_key = None;

        for catalog_name in self.catalog_list.catalog_names() {
            let Some(catalog) = self.catalog_list.catalog(&catalog_name) else {
                continue;
            };
            for schema_name in catalog.schema_names() {
                let Some(schema_provider) = catalog.schema(&schema_name) else {
                    continue;
                };
                for table_name in schema_provider.table_names() {
                    let table_key = TableKey {
                        catalog: catalog_name.clone(),
                        schema: schema_name.clone(),
                        table: table_name,
                    };

                    if table_oid(&table_key) == oid {
                        if matching_table_key.is_some() {
                            return None;
                        }
                        matching_table_key = Some(table_key);
                    }
                }
            }
        }

        matching_table_key
    }

    async fn load_table_schema(
        &self,
        table_key: &TableKey,
    ) -> Result<Option<SchemaRef>, DataFusionError> {
        let Some(catalog) = self.catalog_list.catalog(&table_key.catalog) else {
            return Ok(None);
        };
        let Some(schema_provider) = catalog.schema(&table_key.schema) else {
            return Ok(None);
        };
        let Some(table) = schema_provider.table(&table_key.table).await? else {
            return Ok(None);
        };

        Ok(Some(table.schema()))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TableKey {
    catalog: String,
    schema: String,
    table: String,
}

enum ColumnSelector {
    Name(String),
    Position(u64),
}

#[derive(Default)]
struct CommentLookupCache {
    schema_by_table: HashMap<TableKey, Option<SchemaRef>>,
    table_by_oid: HashMap<u32, Option<TableKey>>,
}

pub fn register_postgres_comment_udfs(ctx: &SessionContext) {
    let lookup = CommentLookup::new(ctx);
    ctx.register_udf(ObjDescription::new(lookup.clone()).into_scalar_udf());
    ctx.register_udf(ColDescription::new(lookup).into_scalar_udf());
}

struct ObjDescription {
    id: u64,
    lookup: CommentLookup,
    signature: Signature,
}

impl ObjDescription {
    fn new(lookup: CommentLookup) -> Self {
        Self {
            id: NEXT_COMMENT_UDF_ID.fetch_add(1, Ordering::Relaxed),
            lookup,
            signature: obj_description_signature(),
        }
    }

    fn into_scalar_udf(self) -> ScalarUDF {
        AsyncScalarUDF::new(Arc::new(self)).into_scalar_udf()
    }

    async fn table_comment_for_row(
        &self,
        args: &[ArrayRef],
        row: usize,
        cache: &mut CommentLookupCache,
    ) -> Result<Option<String>, DataFusionError> {
        let Some(table_key) = table_key_from_obj_description_args(&self.lookup, args, row, cache)?
        else {
            return Ok(None);
        };
        let schema = self
            .lookup
            .table_schema(&table_key, cache)
            .await?
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Table '{}' not found", table_key.table))
            })?;

        Ok(schema.metadata().get(DESCRIPTION_METADATA_KEY).cloned())
    }
}

impl std::fmt::Debug for ObjDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjDescription")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ObjDescription {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ObjDescription {}

impl Hash for ObjDescription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for ObjDescription {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        OBJ_DESCRIPTION_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        match arg_types.len() {
            1..=3 => Ok(DataType::Utf8),
            count => Err(DataFusionError::Plan(format!(
                "{OBJ_DESCRIPTION_UDF_NAME} expects 1, 2, or 3 arguments, got {count}"
            ))),
        }
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Execution(format!(
            "{OBJ_DESCRIPTION_UDF_NAME} must be invoked asynchronously"
        )))
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for ObjDescription {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        validate_arg_count(OBJ_DESCRIPTION_UDF_NAME, args.args.len(), &[1, 2, 3])?;
        let arrays = args_to_arrays(&args)?;
        let mut cache = CommentLookupCache::default();
        let mut values = Vec::with_capacity(args.number_rows);

        for row in 0..args.number_rows {
            values.push(self.table_comment_for_row(&arrays, row, &mut cache).await?);
        }

        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}

struct ColDescription {
    id: u64,
    lookup: CommentLookup,
    signature: Signature,
}

impl ColDescription {
    fn new(lookup: CommentLookup) -> Self {
        Self {
            id: NEXT_COMMENT_UDF_ID.fetch_add(1, Ordering::Relaxed),
            lookup,
            signature: col_description_signature(),
        }
    }

    fn into_scalar_udf(self) -> ScalarUDF {
        AsyncScalarUDF::new(Arc::new(self)).into_scalar_udf()
    }

    async fn column_comment_for_row(
        &self,
        args: &[ArrayRef],
        row: usize,
        cache: &mut CommentLookupCache,
    ) -> Result<Option<String>, DataFusionError> {
        let Some((table_key, selector)) = col_description_args(&self.lookup, args, row, cache)?
        else {
            return Ok(None);
        };
        let schema = self
            .lookup
            .table_schema(&table_key, cache)
            .await?
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Table '{}' not found", table_key.table))
            })?;

        let comment = match selector {
            ColumnSelector::Name(name) => {
                let field = schema.field_with_name(&name).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Column '{}' not found in table '{}'",
                        name, table_key.table
                    ))
                })?;
                field.metadata().get(DESCRIPTION_METADATA_KEY).cloned()
            }
            ColumnSelector::Position(position) => {
                let index = position
                    .checked_sub(1)
                    .and_then(|i| usize::try_from(i).ok())
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Column position must be >= 1, got {position}"
                        ))
                    })?;
                let field = schema.fields().get(index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Column at position {position} does not exist in table '{}' ({} columns total)",
                        table_key.table,
                        schema.fields().len()
                    ))
                })?;
                field.metadata().get(DESCRIPTION_METADATA_KEY).cloned()
            }
        };

        Ok(comment)
    }
}

impl std::fmt::Debug for ColDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColDescription")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ColDescription {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ColDescription {}

impl Hash for ColDescription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ScalarUDFImpl for ColDescription {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        COL_DESCRIPTION_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        match arg_types.len() {
            2 | 4 => Ok(DataType::Utf8),
            count => Err(DataFusionError::Plan(format!(
                "{COL_DESCRIPTION_UDF_NAME} expects 2 or 4 arguments, got {count}"
            ))),
        }
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Execution(format!(
            "{COL_DESCRIPTION_UDF_NAME} must be invoked asynchronously"
        )))
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for ColDescription {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        validate_arg_count(COL_DESCRIPTION_UDF_NAME, args.args.len(), &[2, 4])?;
        let arrays = args_to_arrays(&args)?;
        let mut cache = CommentLookupCache::default();
        let mut values = Vec::with_capacity(args.number_rows);

        for row in 0..args.number_rows {
            values.push(
                self.column_comment_for_row(&arrays, row, &mut cache)
                    .await?,
            );
        }

        Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
    }
}

fn validate_arg_count(name: &str, count: usize, allowed: &[usize]) -> Result<(), DataFusionError> {
    if allowed.contains(&count) {
        return Ok(());
    }

    Err(DataFusionError::Execution(format!(
        "{name} received unsupported argument count {count}"
    )))
}

fn obj_description_signature() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Coercible(vec![integer_arg_coercion(), string_arg_coercion()]),
            TypeSignature::String(3),
            TypeSignature::String(2),
            TypeSignature::String(1),
        ],
        Volatility::Stable,
    )
}

fn col_description_signature() -> Signature {
    Signature::one_of(
        vec![
            TypeSignature::Coercible(vec![integer_arg_coercion(), integer_arg_coercion()]),
            TypeSignature::Coercible(vec![string_arg_coercion(), integer_arg_coercion()]),
            TypeSignature::Coercible(vec![
                string_arg_coercion(),
                string_arg_coercion(),
                string_arg_coercion(),
                integer_arg_coercion(),
            ]),
            TypeSignature::String(4),
            TypeSignature::String(2),
        ],
        Volatility::Stable,
    )
}

fn string_arg_coercion() -> Coercion {
    Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
}

fn integer_arg_coercion() -> Coercion {
    Coercion::new_exact(TypeSignatureClass::Integer)
}

fn args_to_arrays(args: &ScalarFunctionArgs) -> Result<Vec<ArrayRef>, DataFusionError> {
    args.args
        .iter()
        .map(|arg| arg.to_array(args.number_rows))
        .collect()
}

fn table_key_from_obj_description_args(
    lookup: &CommentLookup,
    args: &[ArrayRef],
    row: usize,
    cache: &mut CommentLookupCache,
) -> Result<Option<TableKey>, DataFusionError> {
    match args.len() {
        1 => table_identifier_arg(lookup, args, 0, row, cache),
        2 => {
            let Some(catalog) = string_arg(args, 1, row)? else {
                return Ok(None);
            };
            if !catalog.eq_ignore_ascii_case("pg_class") {
                return Ok(None);
            }
            table_identifier_arg(lookup, args, 0, row, cache)
        }
        3 => table_key_from_parts(args, row, 0, 1, 2),
        _ => Ok(None),
    }
}

fn col_description_args(
    lookup: &CommentLookup,
    args: &[ArrayRef],
    row: usize,
    cache: &mut CommentLookupCache,
) -> Result<Option<(TableKey, ColumnSelector)>, DataFusionError> {
    match args.len() {
        2 => {
            let Some(table_key) = table_identifier_arg(lookup, args, 0, row, cache)? else {
                return Ok(None);
            };
            let Some(selector) = column_selector_arg(args, 1, row)? else {
                return Ok(None);
            };
            Ok(Some((table_key, selector)))
        }
        4 => {
            let Some(table_key) = table_key_from_parts(args, row, 0, 1, 2)? else {
                return Ok(None);
            };
            let Some(selector) = column_selector_arg(args, 3, row)? else {
                return Ok(None);
            };
            Ok(Some((table_key, selector)))
        }
        _ => Ok(None),
    }
}

fn table_identifier_arg(
    lookup: &CommentLookup,
    args: &[ArrayRef],
    arg_index: usize,
    row: usize,
    cache: &mut CommentLookupCache,
) -> Result<Option<TableKey>, DataFusionError> {
    let scalar = ScalarValue::try_from_array(args[arg_index].as_ref(), row)?;
    match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Ok(Some(lookup.resolve_table_name(&value))),
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Null => Ok(None),
        other => table_oid_from_scalar(other)
            .map(|oid| oid.and_then(|table_oid| lookup.table_key_for_oid(table_oid, cache))),
    }
}

fn table_oid_from_scalar(scalar: ScalarValue) -> Result<Option<u32>, DataFusionError> {
    match scalar {
        ScalarValue::Int8(value) => Ok(value.and_then(|value| u32::try_from(value).ok())),
        ScalarValue::Int16(value) => Ok(value.and_then(|value| u32::try_from(value).ok())),
        ScalarValue::Int32(value) => Ok(value.and_then(|value| u32::try_from(value).ok())),
        ScalarValue::Int64(value) => Ok(value.and_then(|value| u32::try_from(value).ok())),
        ScalarValue::UInt8(value) => Ok(value.map(u32::from)),
        ScalarValue::UInt16(value) => Ok(value.map(u32::from)),
        ScalarValue::UInt32(value) => Ok(value),
        ScalarValue::UInt64(value) => Ok(value.and_then(|value| u32::try_from(value).ok())),
        other => Err(DataFusionError::Execution(format!(
            "expected UTF-8 string or integer table identifier argument, got {other:?}"
        ))),
    }
}

fn table_oid(table_key: &TableKey) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for part in [&table_key.catalog, &table_key.schema, &table_key.table] {
        for byte in part.as_bytes() {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(16_777_619);
        }
        hash = hash.wrapping_mul(16_777_619);
    }

    hash.max(1)
}

fn table_key_from_parts(
    args: &[ArrayRef],
    row: usize,
    catalog_index: usize,
    schema_index: usize,
    table_index: usize,
) -> Result<Option<TableKey>, DataFusionError> {
    let (Some(catalog), Some(schema), Some(table)) = (
        string_arg(args, catalog_index, row)?,
        string_arg(args, schema_index, row)?,
        string_arg(args, table_index, row)?,
    ) else {
        return Ok(None);
    };

    Ok(Some(TableKey {
        catalog,
        schema,
        table,
    }))
}

fn string_arg(
    args: &[ArrayRef],
    arg_index: usize,
    row: usize,
) -> Result<Option<String>, DataFusionError> {
    let scalar = ScalarValue::try_from_array(args[arg_index].as_ref(), row)?;
    match scalar {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Ok(value)
        }
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "expected UTF-8 string argument, got {other:?}"
        ))),
    }
}

fn column_selector_arg(
    args: &[ArrayRef],
    arg_index: usize,
    row: usize,
) -> Result<Option<ColumnSelector>, DataFusionError> {
    let scalar = ScalarValue::try_from_array(args[arg_index].as_ref(), row)?;
    match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Ok(Some(ColumnSelector::Name(value))),
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Null => Ok(None),
        ScalarValue::Int8(value) => Ok(value
            .and_then(|v| u64::try_from(v).ok())
            .map(ColumnSelector::Position)),
        ScalarValue::Int16(value) => Ok(value
            .and_then(|v| u64::try_from(v).ok())
            .map(ColumnSelector::Position)),
        ScalarValue::Int32(value) => Ok(value
            .and_then(|v| u64::try_from(v).ok())
            .map(ColumnSelector::Position)),
        ScalarValue::Int64(value) => Ok(value
            .and_then(|v| u64::try_from(v).ok())
            .map(ColumnSelector::Position)),
        ScalarValue::UInt8(value) => Ok(value.map(u64::from).map(ColumnSelector::Position)),
        ScalarValue::UInt16(value) => Ok(value.map(u64::from).map(ColumnSelector::Position)),
        ScalarValue::UInt32(value) => Ok(value.map(u64::from).map(ColumnSelector::Position)),
        ScalarValue::UInt64(value) => Ok(value.map(ColumnSelector::Position)),
        other => Err(DataFusionError::Execution(format!(
            "expected UTF-8 string or integer column selector argument, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use data_components::DESCRIPTION_METADATA_KEY;
    use datafusion::{
        assert_batches_eq,
        datasource::MemTable,
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;

    fn commented_schema() -> SchemaRef {
        let mut table_metadata = HashMap::new();
        table_metadata.insert(
            DESCRIPTION_METADATA_KEY.to_string(),
            "orders table".to_string(),
        );

        let mut id_metadata = HashMap::new();
        id_metadata.insert(
            DESCRIPTION_METADATA_KEY.to_string(),
            "stable row id".to_string(),
        );

        let mut customer_metadata = HashMap::new();
        customer_metadata.insert(
            DESCRIPTION_METADATA_KEY.to_string(),
            "customer name".to_string(),
        );

        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int32, false).with_metadata(id_metadata),
                Field::new("customer", DataType::Utf8, true).with_metadata(customer_metadata),
            ],
            table_metadata,
        ))
    }

    fn register_test_table(ctx: &SessionContext) {
        let batch = RecordBatch::try_new(
            commented_schema(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice"])) as ArrayRef,
            ],
        )
        .expect("test record batch should be valid");
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .expect("test memtable should be valid");
        ctx.register_table(TableReference::bare("orders"), Arc::new(table))
            .expect("test table should register");
    }

    #[tokio::test]
    async fn obj_description_returns_table_comment() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);

        let batches = ctx
            .sql("SELECT obj_description('orders', 'pg_class') AS table_comment")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+---------------+",
                "| table_comment |",
                "+---------------+",
                "| orders table  |",
                "+---------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn obj_description_accepts_table_oid() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);
        let table_oid = table_oid(&CommentLookup::new(&ctx).resolve_table_name("orders"));

        let batches = ctx
            .sql(&format!(
                "SELECT obj_description({table_oid}, 'pg_class') AS table_comment"
            ))
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+---------------+",
                "| table_comment |",
                "+---------------+",
                "| orders table  |",
                "+---------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn col_description_returns_column_comment_by_position() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);

        let batches = ctx
            .sql("SELECT col_description('orders', 1) AS column_comment")
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+----------------+",
                "| column_comment |",
                "+----------------+",
                "| stable row id  |",
                "+----------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn col_description_accepts_table_oid() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);
        let table_oid = table_oid(&CommentLookup::new(&ctx).resolve_table_name("orders"));

        let batches = ctx
            .sql(&format!(
                "SELECT col_description({table_oid}, 2) AS column_comment"
            ))
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+----------------+",
                "| column_comment |",
                "+----------------+",
                "| customer name  |",
                "+----------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn obj_description_errors_on_unknown_table() {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);

        let error = ctx
            .sql("SELECT obj_description('nonexistent_table')")
            .await
            .expect("planning should succeed")
            .collect()
            .await
            .expect_err("executing with unknown table should fail");

        assert!(
            error.to_string().contains("not found"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn col_description_errors_on_unknown_table() {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);

        let error = ctx
            .sql("SELECT col_description('nonexistent_table', 1)")
            .await
            .expect("planning should succeed")
            .collect()
            .await
            .expect_err("executing with unknown table should fail");

        assert!(
            error.to_string().contains("not found"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn col_description_errors_on_out_of_bounds_position() {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);

        let error = ctx
            .sql("SELECT col_description('orders', 999)")
            .await
            .expect("planning should succeed")
            .collect()
            .await
            .expect_err("executing with out-of-bounds column position should fail");

        assert!(
            error.to_string().contains("does not exist"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn col_description_errors_on_unknown_column_name() {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);

        let error = ctx
            .sql("SELECT col_description('orders', 'nonexistent_column')")
            .await
            .expect("planning should succeed")
            .collect()
            .await
            .expect_err("executing with unknown column name should fail");

        assert!(
            error.to_string().contains("not found"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn obj_description_rejects_invalid_arity_at_plan_time() {
        let ctx = SessionContext::new();
        register_postgres_comment_udfs(&ctx);

        let error = ctx
            .sql("SELECT obj_description(1)")
            .await
            .expect_err("invalid obj_description arity should fail during planning");

        assert!(
            error.to_string().contains("No function matches"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn comment_udfs_work_with_information_schema_rows() -> Result<(), DataFusionError> {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        register_postgres_comment_udfs(&ctx);
        register_test_table(&ctx);

        let table_batches = ctx
            .sql(
                "SELECT obj_description(table_catalog, table_schema, table_name) AS table_comment \
                 FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_name = 'orders'",
            )
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+---------------+",
                "| table_comment |",
                "+---------------+",
                "| orders table  |",
                "+---------------+",
            ],
            &table_batches
        );

        let column_batches = ctx
            .sql(
                "SELECT column_name, col_description(table_catalog, table_schema, table_name, column_name) AS column_comment \
                 FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'orders' \
                 ORDER BY column_name",
            )
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            &[
                "+-------------+----------------+",
                "| column_name | column_comment |",
                "+-------------+----------------+",
                "| customer    | customer name  |",
                "| id          | stable row id  |",
                "+-------------+----------------+",
            ],
            &column_batches
        );

        Ok(())
    }
}
