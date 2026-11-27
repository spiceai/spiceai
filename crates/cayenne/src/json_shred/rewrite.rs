/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The rewrite of `json_get_*(<column>, '<key>', rest...)` over a Cayenne scan to
//! `json_get_*(<key column>, rest...)`, where the scan returns, per row, the text
//! of the value under `key` ([`super::top_level_value`]).
//!
//! The functions answer the same on that text as on the whole document (see the
//! parent module), so the rewrite changes no answer. What it changes is what the
//! scan reads: a file that stores a key's values is read for them instead of its
//! JSON column, and the JSON column leaves the scan when nothing else reads it.
//!
//! The rewritten scan is a [`JsonKeyScanTable`] over the table: its schema is the
//! table's followed by the key columns. Planning it asks the table for the key
//! columns through [`JsonKeyColumnScan`]; a table that cannot serve them at that
//! moment (an acceleration not ready and reading its source) is scanned for the
//! JSON columns instead, and the key columns are computed from them above the scan.
//! Both give the same rows.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    ColumnStatistics, Constraints, Result, ScalarValue, Statistics, TableReference,
    internal_datafusion_err, internal_err,
};
use datafusion::datasource::{DefaultTableSource, TableProvider, TableType};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Expr, Filter, LogicalPlan, Projection, ScalarUDF, TableProviderFilterPushDown, TableScan,
    TableScanBuilder,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_functions_json::udfs::{
    json_as_text_udf, json_get_array_udf, json_get_bool_udf, json_get_float_udf, json_get_int_udf,
    json_get_json_udf, json_get_str_udf, json_get_udf, json_length_udf,
};

use super::{
    JSON_TOP_LEVEL_VALUE_UDF, JsonKeyColumn, JsonKeyColumnScan, is_json_text_type,
    schema_with_json_keys,
};
use crate::provider::CayenneTableProvider;

/// Finds, in a table's provider, the table that serves its scans' key columns:
/// the provider itself when it is a Cayenne table, or the layer through which a
/// scan reads one in its place. `None` for any other table, which the rewrite then
/// leaves alone.
pub type JsonKeyScanResolver =
    Arc<dyn for<'a> Fn(&'a dyn TableProvider) -> Option<&'a dyn JsonKeyColumnScan> + Send + Sync>;

/// The functions the rewrite applies to, by identity: a user's function of the same
/// name is a different function and is never rewritten.
static REWRITTEN_FUNCTIONS: LazyLock<[Arc<ScalarUDF>; 9]> = LazyLock::new(|| {
    [
        json_get_udf(),
        json_get_str_udf(),
        json_get_int_udf(),
        json_get_float_udf(),
        json_get_bool_udf(),
        json_get_json_udf(),
        json_get_array_udf(),
        json_as_text_udf(),
        json_length_udf(),
    ]
});

/// Logical rule rewriting `f(<json column>, '<key>', rest...)` in a projection over
/// a Cayenne scan (through any filters between them) to `f(<key column>, rest...)`,
/// for `f` one of `json_get`, `json_get_str`, `json_get_int`, `json_get_float`,
/// `json_get_bool`, `json_get_json`, `json_get_array`, `json_as_text` and
/// `json_length`, where `<key>` is a non-NULL string literal that does not parse as
/// an integer and every argument after it is a non-NULL string or integer literal.
///
/// `DataFusion`'s leaf-expression extraction has already moved such calls into a
/// projection directly over their scan by the time this rule runs, which is why the
/// projection is the one shape it rewrites. Output column names are preserved. The
/// JSON column leaves the scan when no remaining expression, filter or pushed-down
/// scan filter reads it.
pub struct CayenneJsonKeyRewrite {
    resolver: JsonKeyScanResolver,
}

impl Default for CayenneJsonKeyRewrite {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CayenneJsonKeyRewrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneJsonKeyRewrite").finish()
    }
}

impl CayenneJsonKeyRewrite {
    /// The rule over scans whose provider is a [`CayenneTableProvider`].
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_resolver(Arc::new(|provider: &dyn TableProvider| {
            provider
                .downcast_ref::<CayenneTableProvider>()
                .map(|table| table as &dyn JsonKeyColumnScan)
        }))
    }

    /// The rule over scans whose provider `resolver` resolves.
    #[must_use]
    pub fn new_with_resolver(resolver: JsonKeyScanResolver) -> Self {
        Self { resolver }
    }

    fn rewrite_projection(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };
        match self.rewritten_projection(&projection)? {
            Some(rewritten) => Ok(Transformed::yes(LogicalPlan::Projection(rewritten))),
            None => Ok(Transformed::no(LogicalPlan::Projection(projection))),
        }
    }

    fn rewritten_projection(&self, projection: &Projection) -> Result<Option<Projection>> {
        let Some(scan) = scan_below(&projection.input) else {
            return Ok(None);
        };
        let Some(source) = scan.source.downcast_ref::<DefaultTableSource>() else {
            return Ok(None);
        };
        // A scan this rule already rewrote keeps its key columns and gains more.
        let (table, table_schema, mut keys) =
            match source.table_provider.downcast_ref::<JsonKeyScanTable>() {
                Some(rewritten) => (
                    Arc::clone(&rewritten.inner),
                    Arc::clone(&rewritten.table_schema),
                    rewritten.keys.to_vec(),
                ),
                None => (
                    Arc::clone(&source.table_provider),
                    source.table_provider.schema(),
                    Vec::new(),
                ),
            };
        if !(self.resolver)(table.as_ref()).is_some_and(JsonKeyColumnScan::serves_json_keys) {
            return Ok(None);
        }

        let names = NamePreserver::new_for_projection();
        let mut rewrote = false;
        let mut exprs = Vec::with_capacity(projection.expr.len());
        for expr in &projection.expr {
            let saved = names.save(expr);
            let rewritten = expr.clone().transform_up(|expr| {
                Ok(
                    match key_column_call(&expr, &scan.table_name, &table_schema, &mut keys) {
                        Some(call) => Transformed::yes(call),
                        None => Transformed::no(expr),
                    },
                )
            })?;
            rewrote |= rewritten.transformed;
            exprs.push(saved.restore(rewritten.data));
        }
        if !rewrote {
            return Ok(None);
        }
        // A table column named like a key column would make the scan's schema
        // ambiguous.
        if keys
            .iter()
            .any(|key| table_schema.field_with_name(key.name()).is_ok())
        {
            return Ok(None);
        }

        // What the rewritten plan still reads from the scan.
        let mut read: HashSet<&str> = HashSet::new();
        let mut filters = Vec::new();
        let mut node = projection.input.as_ref();
        while let LogicalPlan::Filter(filter) = node {
            filters.push(&filter.predicate);
            node = filter.input.as_ref();
        }
        for expr in exprs.iter().chain(filters).chain(&scan.filters) {
            read.extend(
                expr.column_refs()
                    .into_iter()
                    .map(|column| column.name.as_str()),
            );
        }
        let table_columns = table_schema.fields().len();
        let scanned: Vec<usize> = match &scan.projection {
            Some(projection) => projection.clone(),
            None => (0..scan.source.schema().fields().len()).collect(),
        };
        // Every table column still read must already be scanned.
        let scanned_names: HashSet<&str> = scan
            .projected_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        if read.iter().any(|name| {
            !scanned_names.contains(name) && !keys.iter().any(|key| key.name() == *name)
        }) {
            return Ok(None);
        }
        let mut new_projection: Vec<usize> = scanned
            .iter()
            .copied()
            .filter(|&index| {
                index < table_columns && read.contains(table_schema.field(index).name().as_str())
            })
            .collect();
        new_projection.extend(
            keys.iter()
                .enumerate()
                .filter(|(_, key)| read.contains(key.name()))
                .map(|(position, _)| table_columns + position),
        );

        let rewritten_table: Arc<dyn TableProvider> = Arc::new(JsonKeyScanTable::new(
            table,
            table_schema,
            keys,
            Arc::clone(&self.resolver),
        ));
        let new_scan = TableScanBuilder::new(
            scan.table_name.clone(),
            Arc::new(DefaultTableSource::new(rewritten_table)),
        )
        .with_projection(Some(new_projection))
        .with_filters(scan.filters.clone())
        .with_fetch(scan.fetch)
        .with_statistics_requests(scan.statistics_requests.clone())
        .build()?;
        let input = replace_scan(&projection.input, LogicalPlan::TableScan(new_scan))?;
        let rewritten = Projection::try_new(exprs, Arc::new(input))?;
        // The rewrite must not change what the projection outputs.
        if !same_fields(projection, &rewritten) {
            return Ok(None);
        }
        Ok(Some(rewritten))
    }
}

impl OptimizerRule for CayenneJsonKeyRewrite {
    fn name(&self) -> &'static str {
        "cayenne_json_key_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Walks the plan itself, subqueries included.
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // A DML statement reads its target through its own plan; leave it as written.
        if plan.exists(|node| {
            Ok(matches!(
                node,
                LogicalPlan::Dml(_) | LogicalPlan::Copy(_) | LogicalPlan::Ddl(_)
            ))
        })? {
            return Ok(Transformed::no(plan));
        }
        plan.transform_up_with_subqueries(|node| self.rewrite_projection(node))
    }
}

/// Where `item` is in `items`, appending it first when it is not there.
fn position_or_push<T: PartialEq>(items: &mut Vec<T>, item: T) -> usize {
    if let Some(position) = items.iter().position(|existing| *existing == item) {
        return position;
    }
    items.push(item);
    items.len() - 1
}

/// The scan under `plan`, through any filters.
fn scan_below(plan: &LogicalPlan) -> Option<&TableScan> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan),
        LogicalPlan::Filter(filter) => scan_below(&filter.input),
        _ => None,
    }
}

/// `plan` with the scan under its filters replaced by `scan`.
fn replace_scan(plan: &LogicalPlan, scan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan(_) => Ok(scan),
        LogicalPlan::Filter(filter) => Ok(LogicalPlan::Filter(Filter::try_new(
            filter.predicate.clone(),
            Arc::new(replace_scan(&filter.input, scan)?),
        )?)),
        _ => internal_err!("Expected a table scan under filters"),
    }
}

/// Whether two projections output the same columns: names, types, nullability and
/// metadata.
fn same_fields(before: &Projection, after: &Projection) -> bool {
    let (before, after) = (before.schema.as_ref(), after.schema.as_ref());
    before.fields().len() == after.fields().len()
        && before.iter().zip(after.iter()).all(
            |((before_qualifier, before_field), (after_qualifier, after_field))| {
                before_qualifier == after_qualifier && before_field == after_field
            },
        )
}

/// `expr` reading `scan_name`'s key column instead of its JSON column, registering
/// the key column in `keys`, when `expr` is a call the rule rewrites.
fn key_column_call(
    expr: &Expr,
    scan_name: &TableReference,
    table_schema: &Schema,
    keys: &mut Vec<JsonKeyColumn>,
) -> Option<Expr> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    if !REWRITTEN_FUNCTIONS
        .iter()
        .any(|function| function.as_ref() == call.func.as_ref())
    {
        return None;
    }
    let [Expr::Column(column), Expr::Literal(key, _), rest @ ..] = call.args.as_slice() else {
        return None;
    };
    if column
        .relation
        .as_ref()
        .is_some_and(|relation| relation != scan_name)
        || !rest.iter().all(is_path_literal)
    {
        return None;
    }
    let (ScalarValue::Utf8(Some(key))
    | ScalarValue::Utf8View(Some(key))
    | ScalarValue::LargeUtf8(Some(key))) = key
    else {
        return None;
    };
    let source = table_schema.index_of(&column.name).ok()?;
    if !is_json_text_type(table_schema.field(source).data_type()) {
        return None;
    }
    let key_column = JsonKeyColumn::new(source, &column.name, key)?;
    let position = position_or_push(keys, key_column);
    let mut args = Vec::with_capacity(rest.len() + 1);
    args.push(Expr::Column(datafusion::common::Column::new(
        Some(scan_name.clone()),
        keys[position].name(),
    )));
    args.extend(rest.iter().cloned());
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&call.func),
        args,
    )))
}

/// A path element the rewrite carries over unchanged: a non-NULL string or integer
/// literal, the forms the extraction was proven exact for.
fn is_path_literal(expr: &Expr) -> bool {
    let Expr::Literal(value, _) = expr else {
        return false;
    };
    !value.is_null() && (is_json_text_type(&value.data_type()) || value.data_type().is_integer())
}

/// A Cayenne table scanned with [`JsonKeyColumn`]s after its own columns: the
/// table the rewritten plan reads. Projection indices below the table's column
/// count select table columns; the rest select key columns.
pub struct JsonKeyScanTable {
    inner: Arc<dyn TableProvider>,
    /// The table's schema when the rewrite planned this scan. Columns a table
    /// gains later come after these, so these indices stay the table's.
    table_schema: SchemaRef,
    keys: Arc<[JsonKeyColumn]>,
    schema: SchemaRef,
    resolver: JsonKeyScanResolver,
}

impl fmt::Debug for JsonKeyScanTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonKeyScanTable")
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

/// Where an output column of a [`JsonKeyScanTable`] scan comes from in the plan it
/// is built on, whose columns are the table columns then the key columns.
enum ScanColumn {
    Table(usize),
    Key(usize),
}

impl JsonKeyScanTable {
    fn new(
        inner: Arc<dyn TableProvider>,
        table_schema: SchemaRef,
        keys: Vec<JsonKeyColumn>,
        resolver: JsonKeyScanResolver,
    ) -> Self {
        let mut fields: Vec<_> = table_schema.fields().iter().cloned().collect();
        fields.extend(keys.iter().map(JsonKeyColumn::field));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            table_schema.metadata().clone(),
        ));
        Self {
            inner,
            table_schema,
            keys: keys.into(),
            schema,
            resolver,
        }
    }

    /// The table this scan reads.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }

    /// The key columns this table adds to its table's columns.
    #[must_use]
    pub fn keys(&self) -> &[JsonKeyColumn] {
        &self.keys
    }

    /// The table columns `projection` selects, then `keys`: served by the table when
    /// it can, computed from its JSON columns otherwise.
    async fn scan_keys(
        &self,
        state: &dyn Session,
        projection: &[usize],
        keys: &[JsonKeyColumn],
        args: &ScanArgs<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filters = args.filters().unwrap_or(&[]);
        if let Some(table) = (self.resolver)(self.inner.as_ref())
            && table.serves_json_keys()
            && let Some(plan) = table
                .scan_with_json_keys(state, projection, keys, filters, args.limit())
                .await?
        {
            let expected = schema_with_json_keys(&self.table_schema, Some(projection), keys)?;
            let actual = plan.schema();
            let matches = actual.fields().len() == expected.fields().len()
                && actual
                    .fields()
                    .iter()
                    .zip(expected.fields())
                    .skip(projection.len())
                    .all(|(actual, expected)| {
                        actual.name() == expected.name()
                            && actual.data_type() == expected.data_type()
                    });
            if !matches {
                return internal_err!(
                    "A scan of table columns {projection:?} with JSON key columns {:?} returned columns {:?}",
                    keys.iter().map(JsonKeyColumn::name).collect::<Vec<_>>(),
                    actual
                        .fields()
                        .iter()
                        .map(|field| field.name())
                        .collect::<Vec<_>>()
                );
            }
            return Ok(plan);
        }
        self.computed_scan(state, projection, keys, args).await
    }

    /// The table columns `projection` selects and the JSON columns of `keys`, with
    /// the key columns computed from them above the scan.
    async fn computed_scan(
        &self,
        state: &dyn Session,
        projection: &[usize],
        keys: &[JsonKeyColumn],
        args: &ScanArgs<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut scanned = projection.to_vec();
        let mut sources = Vec::with_capacity(keys.len());
        for key in keys {
            if !key.matches(&self.table_schema) {
                return internal_err!(
                    "JSON key column '{}' does not match the table's schema",
                    key.name()
                );
            }
            let position = position_or_push(&mut scanned, key.source());
            sources.push(position);
        }
        let scan_args = ScanArgs::default()
            .with_projection(Some(&scanned))
            .with_filters(args.filters())
            .with_limit(args.limit())
            .with_statistics_requests(args.statistics_requests());
        let plan = self
            .inner
            .scan_with_args(state, scan_args)
            .await?
            .into_inner();
        let input = plan.schema();
        if input.fields().len() != scanned.len() {
            return internal_err!(
                "A scan of {} columns returned {}",
                scanned.len(),
                input.fields().len()
            );
        }
        let config = Arc::new(state.config_options().clone());
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(projection.len() + keys.len());
        for (index, field) in input.fields().iter().enumerate().take(projection.len()) {
            exprs.push((
                Arc::new(Column::new(field.name(), index)),
                field.name().clone(),
            ));
        }
        for (key, &position) in keys.iter().zip(&sources) {
            let json: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new(input.field(position).name(), position));
            let name: Arc<dyn PhysicalExpr> =
                Arc::new(Literal::new(ScalarValue::Utf8(Some(key.key().to_string()))));
            let value = ScalarFunctionExpr::try_new(
                Arc::clone(&JSON_TOP_LEVEL_VALUE_UDF),
                vec![json, name],
                &input,
                Arc::clone(&config),
            )?;
            exprs.push((Arc::new(value), key.name().to_string()));
        }
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }
}

#[async_trait]
impl TableProvider for JsonKeyScanTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        // Key columns follow the table's, so its constraints' indices still hold.
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let args = ScanArgs::default()
            .with_projection(projection.map(Vec::as_slice))
            .with_filters(Some(filters))
            .with_limit(limit);
        Ok(self.scan_with_args(state, args).await?.into_inner())
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let all: Vec<usize>;
        let projection = if let Some(projection) = args.projection() {
            projection
        } else {
            all = (0..self.schema.fields().len()).collect();
            &all
        };
        let table_columns = self.table_schema.fields().len();
        let mut table_projection = Vec::with_capacity(projection.len());
        let mut keys: Vec<JsonKeyColumn> = Vec::new();
        let mut columns = Vec::with_capacity(projection.len());
        for &index in projection {
            if index < table_columns {
                columns.push(ScanColumn::Table(table_projection.len()));
                table_projection.push(index);
                continue;
            }
            let key = self.keys.get(index - table_columns).ok_or_else(|| {
                internal_datafusion_err!(
                    "Projection index {index} is out of bounds for a scan of {} columns",
                    self.schema.fields().len()
                )
            })?;
            columns.push(ScanColumn::Key(position_or_push(&mut keys, key.clone())));
        }

        let plan = if keys.is_empty() {
            let table_args = ScanArgs::default()
                .with_projection(Some(&table_projection))
                .with_filters(args.filters())
                .with_limit(args.limit())
                .with_statistics_requests(args.statistics_requests());
            self.inner
                .scan_with_args(state, table_args)
                .await?
                .into_inner()
        } else {
            self.scan_keys(state, &table_projection, &keys, &args)
                .await?
        };

        // The plan holds the table columns, then the key columns; put them in the
        // order the projection asked for.
        let order: Vec<usize> = columns
            .iter()
            .map(|column| match column {
                ScanColumn::Table(position) => *position,
                ScanColumn::Key(position) => table_projection.len() + position,
            })
            .collect();
        if order.iter().copied().eq(0..plan.schema().fields().len()) {
            return Ok(ScanResult::new(plan));
        }
        let input = plan.schema();
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = order
            .iter()
            .map(|&position| {
                let name = input.field(position).name().clone();
                (
                    Arc::new(Column::new(&name, position)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect();
        Ok(ScanResult::new(Arc::new(ProjectionExec::try_new(
            exprs, plan,
        )?)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // A filter on a key column stays above the scan; the table's own filters
        // are the table's to decide.
        let reads_key = |filter: &Expr| {
            filter
                .column_refs()
                .iter()
                .any(|column| self.keys.iter().any(|key| key.name() == column.name))
        };
        let (table_filters, positions): (Vec<&Expr>, Vec<usize>) = filters
            .iter()
            .enumerate()
            .filter(|(_, filter)| !reads_key(filter))
            .map(|(position, filter)| (*filter, position))
            .unzip();
        let mut support = vec![TableProviderFilterPushDown::Unsupported; filters.len()];
        if !table_filters.is_empty() {
            let answers = self.inner.supports_filters_pushdown(&table_filters)?;
            if answers.len() != table_filters.len() {
                return internal_err!(
                    "Table answered filter pushdown for {} of {} filters",
                    answers.len(),
                    table_filters.len()
                );
            }
            for (position, answer) in positions.into_iter().zip(answers) {
                support[position] = answer;
            }
        }
        Ok(support)
    }

    fn statistics(&self) -> Option<Statistics> {
        let mut statistics = self.inner.statistics()?;
        let table_columns = self.table_schema.fields().len();
        statistics
            .column_statistics
            .resize(table_columns, ColumnStatistics::new_unknown());
        statistics
            .column_statistics
            .extend(self.keys.iter().map(|_| ColumnStatistics::new_unknown()));
        Some(statistics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_literal_strings_and_integers_are_carried_as_path() {
        assert!(is_path_literal(&Expr::Literal(
            ScalarValue::Utf8(Some("a".into())),
            None
        )));
        assert!(is_path_literal(&Expr::Literal(
            ScalarValue::Int64(Some(0)),
            None
        )));
        assert!(!is_path_literal(&Expr::Literal(
            ScalarValue::Utf8(None),
            None
        )));
        assert!(!is_path_literal(&Expr::Literal(
            ScalarValue::Float64(Some(0.0)),
            None
        )));
        assert!(!is_path_literal(&Expr::Column(
            datafusion::common::Column::from_name("a")
        )));
    }

    #[test]
    fn key_column_names_never_collide() {
        // Without the length prefix, ("a:b", "c") and ("a", "b:c") would share a name.
        assert_ne!(
            super::super::hidden_column_name("a:b", "c"),
            super::super::hidden_column_name("a", "b:c")
        );
        assert_eq!(
            super::super::hidden_column_name("attributes", "url.path"),
            "__cayenne_json_key:10:attributes:url.path"
        );
    }
}
