use arrow::array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableProvider;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use runtime_proto::UdtfArgs;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::cluster::datafusion::codec::udtf_args::UdtfArgsExt;
use crate::datafusion::udf::{UserFunctionInfo, user_function_infos};
use crate::execution_plan::UdtfExec;

/// UDTF name constant for `list_udfs`
pub const LIST_UDFS_UDTF_NAME: &str = "list_udfs";

pub struct ListUDFTableFunc {
    context: Arc<SessionContext>,
}

impl ListUDFTableFunc {
    pub fn new(context: Arc<SessionContext>) -> Self {
        Self { context }
    }
}

impl Debug for ListUDFTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListUDFTableFunc").finish()
    }
}

impl TableFunctionImpl for ListUDFTableFunc {
    fn call(&self, _exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let mut udf_names = self.context.udfs().into_iter().collect::<Vec<_>>();
        let mut seen = udf_names
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        for name in self.context.state().table_functions().keys() {
            if seen.insert(name.to_ascii_lowercase()) {
                udf_names.push(name.clone());
            }
        }
        udf_names.sort_by_key(|name| name.to_ascii_lowercase());
        let user_infos = user_function_infos();
        Ok(Arc::new(ListUDFTable::new(udf_names, user_infos)))
    }
}

/// The `TableProvider` produced by the `list_udfs()` UDTF.
///
/// Columns:
///   * `name` — function identifier (Utf8, NOT NULL)
///   * `source` — `"builtin"` for Spice / `DataFusion` built-ins, `"user"` for functions declared in a spicepod's `functions:` section
///   * `kind` — `"scalar"` or `"table"` for user-defined functions (NULL for built-ins whose kind we cannot cheaply introspect)
///   * `volatility` — `"immutable"` | `"stable"` | `"volatile"` (NULL for built-ins)
///   * `from` — source URI for user functions (`sql`, `http://...`, `https://...`, `wasm`), NULL for built-ins
///   * `description` — free-form description, NULL when not provided
#[derive(Debug)]
pub struct ListUDFTable {
    schema: SchemaRef,
    udf_names: Vec<String>,
    user_infos_by_name: HashMap<String, UserFunctionInfo>,
}

impl ListUDFTable {
    pub fn new(udf_names: Vec<String>, user_infos: Vec<UserFunctionInfo>) -> Self {
        let user_infos_by_name = user_infos
            .into_iter()
            .map(|i| (i.name.to_ascii_lowercase(), i))
            .collect::<HashMap<_, _>>();
        Self {
            schema: Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("source", DataType::Utf8, false),
                Field::new("kind", DataType::Utf8, true),
                Field::new("volatility", DataType::Utf8, true),
                Field::new("from", DataType::Utf8, true),
                Field::new("description", DataType::Utf8, true),
            ])),
            udf_names,
            user_infos_by_name,
        }
    }

    fn user_info_for_name(&self, name: &str) -> Option<&UserFunctionInfo> {
        self.user_infos_by_name.get(&name.to_ascii_lowercase())
    }
}

#[async_trait]
impl TableProvider for ListUDFTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut names = Vec::with_capacity(self.udf_names.len());
        let mut sources = Vec::with_capacity(self.udf_names.len());
        let mut kinds: Vec<Option<String>> = Vec::with_capacity(self.udf_names.len());
        let mut volatilities: Vec<Option<String>> = Vec::with_capacity(self.udf_names.len());
        let mut froms: Vec<Option<String>> = Vec::with_capacity(self.udf_names.len());
        let mut descriptions: Vec<Option<String>> = Vec::with_capacity(self.udf_names.len());

        for n in &self.udf_names {
            if let Some(info) = self.user_info_for_name(n) {
                names.push(info.name.clone());
                sources.push("user".to_string());
                kinds.push(Some(info.kind.clone()));
                volatilities.push(Some(info.volatility.clone()));
                froms.push(Some(info.from.clone()));
                descriptions.push(info.description.clone());
            } else {
                names.push(n.clone());
                sources.push("builtin".to_string());
                kinds.push(None);
                volatilities.push(None);
                froms.push(None);
                descriptions.push(None);
            }
        }

        let name_array = Arc::new(StringArray::from(names)) as ArrayRef;
        let source_array = Arc::new(StringArray::from(sources)) as ArrayRef;
        let kind_array = Arc::new(StringArray::from(kinds)) as ArrayRef;
        let volatility_array = Arc::new(StringArray::from(volatilities)) as ArrayRef;
        let from_array = Arc::new(StringArray::from(froms)) as ArrayRef;
        let description_array = Arc::new(StringArray::from(descriptions)) as ArrayRef;

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                name_array,
                source_array,
                kind_array,
                volatility_array,
                from_array,
                description_array,
            ],
        )?;

        let memory_source =
            MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&self.schema), None)?;

        let inner_exec = Arc::new(DataSourceExec::new(Arc::new(memory_source)));

        // Wrap in UdtfExec for distributed execution support.
        let udtf_exec = UdtfExec::new(UdtfArgs::list_udfs(), inner_exec);

        Ok(Arc::new(udtf_exec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_info_lookup_is_case_insensitive() {
        let table = ListUDFTable::new(
            vec!["MIXED_FN".to_string()],
            vec![UserFunctionInfo {
                name: "mixed_fn".to_string(),
                kind: "scalar".to_string(),
                volatility: "immutable".to_string(),
                from: "sql".to_string(),
                description: None,
            }],
        );

        let info = table
            .user_info_for_name("MIXED_FN")
            .expect("case-insensitive lookup should find user function metadata");
        assert_eq!(info.name, "mixed_fn");
    }
}
