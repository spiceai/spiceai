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

//! Typed plan federation between a sidecar and an upstream Spice runtime.

use std::{fmt, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Fields, SchemaRef},
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    common::{Result, tree_node::TreeNodeRecursion},
    config::ConfigOptions,
    datasource::empty::EmptyTable,
    error::DataFusionError,
    execution::{TaskContext, context::SessionState},
    logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, TableScan},
    optimizer::{Analyzer, AnalyzerRule},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_federation::{
    FederatedPlanNode, FederatedTableProviderAdaptor, FederationAnalyzerForLogicalPlan,
    FederationPlanner, FederationProvider, get_table_source,
    sql::{MultiPartTableReference, SQLTableSource},
};
use datafusion_substrait::{
    extensions::Extensions,
    logical_plan::producer::{SubstraitProducer, from_table_scan},
    substrait::{
        proto::{
            Expression, ExtensionLeafRel, Plan, PlanRel, Rel, RelRoot, expression::RexType,
            plan_rel, read_rel::ReadType, rel::RelType,
        },
        version::version_with_producer,
    },
};
use flight_client::FlightClient;
use futures::StreamExt;
use prost::Message;
use runtime_request_context::{SPICE_TRACE_ID_HEADER, TRACEPARENT_HEADER};

use super::{
    FlightTable,
    delegated_search::{SEARCH_CALL_TYPE_URL, SearchCallNode},
    normalized_batches,
};

pub(super) fn federated_provider(table: Arc<FlightTable>) -> FederatedTableProviderAdaptor {
    let provider = Arc::new(SpiceFederation {
        client: table.client.clone(),
        context: table.client.federation_context(),
    });
    let source = Arc::new(SQLTableSource::new_with_schema(
        provider,
        table.table_reference.clone(),
        Arc::clone(&table.schema),
    ));
    FederatedTableProviderAdaptor::new_with_provider(source, table)
}

/// Resolve remote names at serialization time. All expression binding remains
/// against the original local schemas; Substrait refers to fields by position.
#[derive(Default)]
struct SpiceProducer {
    extensions: Extensions,
    direct_scan: bool,
}

impl SubstraitProducer for SpiceProducer {
    fn handle_extension(&mut self, plan: &Extension) -> Result<Box<Rel>> {
        // The only extension sent upstream is a delegated search's leaf.
        let Some(search) = plan.node.as_any().downcast_ref::<SearchCallNode>() else {
            return Err(DataFusionError::NotImplemented(format!(
                "'{}' is evaluated in the sidecar",
                plan.node.name()
            )));
        };
        Ok(Box::new(Rel {
            rel_type: Some(RelType::ExtensionLeaf(ExtensionLeafRel {
                common: None,
                detail: Some(pbjson_types::Any {
                    type_url: SEARCH_CALL_TYPE_URL.to_string(),
                    value: search.call.clone().into(),
                }),
            })),
        }))
    }

    fn handle_in_subquery(
        &mut self,
        _expression: &datafusion::logical_expr::expr::InSubquery,
        _schema: &datafusion::common::DFSchemaRef,
    ) -> Result<Expression> {
        // IN can become a NULL-aware anti/mark join during optimization.
        // The Substrait join codec cannot represent that NULL contract.
        Err(DataFusionError::NotImplemented(
            "IN subqueries are evaluated in the sidecar".into(),
        ))
    }

    fn handle_join(&mut self, join: &datafusion::logical_expr::Join) -> Result<Box<Rel>> {
        if join.null_aware {
            return Err(DataFusionError::NotImplemented(
                "NULL-aware joins are evaluated in the sidecar".into(),
            ));
        }
        datafusion_substrait::logical_plan::producer::from_join(self, join)
    }

    fn handle_expr(
        &mut self,
        expression: &Expr,
        schema: &datafusion::common::DFSchemaRef,
    ) -> Result<Expression> {
        let mut encoded = datafusion_substrait::logical_plan::producer::to_substrait_rex(
            self, expression, schema,
        )?;
        // Aliases delegate to an already-encoded child. Function calls retain
        // function identity; SQL operators retain operator identity.
        if !matches!(
            expression,
            Expr::Alias(_) | Expr::ScalarFunction(_) | Expr::HigherOrderFunction(_)
        ) && let Some(RexType::ScalarFunction(function)) = &mut encoded.rex_type
        {
            let name = self
                .extensions
                .functions
                .get(&function.function_reference)
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Substrait operator is missing its declaration".into(),
                    )
                })?
                .clone();
            if matches!(expression, Expr::BinaryExpr(_))
                && datafusion_substrait::logical_plan::consumer::name_to_op(&name).is_none()
            {
                return Err(DataFusionError::NotImplemented(format!(
                    "Operator '{name}' is evaluated in the sidecar",
                )));
            }
            if !name.starts_with("spice.operator.") {
                function.function_reference = self
                    .extensions
                    .register_function(&format!("spice.operator.{name}"));
            }
        }
        Ok(encoded)
    }

    fn handle_scalar_function(
        &mut self,
        function: &datafusion::logical_expr::expr::ScalarFunction,
        schema: &datafusion::common::DFSchemaRef,
    ) -> Result<Expression> {
        if runtime_udfs_api::is_user_function(function.func.name())
            || function.func.as_async().is_some()
            || matches!(function.func.name(), "embed" | "rerank")
        {
            return Err(DataFusionError::NotImplemented(format!(
                "Function '{}' is evaluated in the sidecar",
                function.func.name(),
            )));
        }
        let mut encoded = datafusion_substrait::logical_plan::producer::from_scalar_function(
            self, function, schema,
        )?;
        let Some(RexType::ScalarFunction(scalar)) = &mut encoded.rex_type else {
            return Err(DataFusionError::Internal(
                "Substrait scalar function producer returned an unexpected expression".into(),
            ));
        };
        // DataFusion's producer swaps log(base, value) for Substrait's
        // logb(value, base). Exact function binding uses DataFusion order.
        if function.func.name() == "log" && scalar.arguments.len() == 2 {
            scalar.arguments.swap(0, 1);
        }
        // Canonical aliases (isnan -> is_nan) may bind a different UDF.
        scalar.function_reference = self
            .extensions
            .register_function(&format!("spice.function.{}", function.func.name()));
        Ok(encoded)
    }

    fn register_function(&mut self, signature: String) -> u32 {
        self.extensions.register_function(&signature)
    }

    fn register_type(&mut self, name: String) -> u32 {
        self.extensions.register_type(&name)
    }

    fn get_extensions(self) -> Extensions {
        self.extensions
    }

    fn handle_table_scan(&mut self, scan: &TableScan) -> Result<Box<Rel>> {
        let names = if self.direct_scan {
            scan.table_name.to_vec()
        } else {
            let source = get_table_source(&scan.source)?.ok_or_else(|| {
                DataFusionError::Plan("Substrait fragment contains a local table".into())
            })?;
            let source = (source.as_ref() as &dyn std::any::Any)
                .downcast_ref::<SQLTableSource>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "Substrait fragment contains an unsupported table source".into(),
                    )
                })?;
            if source.provider.name() != "spice_substrait" {
                return Err(DataFusionError::Plan(
                    "Substrait fragment contains a different federation provider".into(),
                ));
            }
            match source.table_reference() {
                MultiPartTableReference::TableReference(reference) => reference.to_vec(),
                MultiPartTableReference::Multi(names) => {
                    names.iter().map(ToString::to_string).collect()
                }
            }
        };
        let mut rel = from_table_scan(self, &read_columns(scan)?)?;
        if let Some(RelType::Read(read)) = &mut rel.rel_type
            && let Some(ReadType::NamedTable(table)) = &mut read.read_type
        {
            table.names = names;
        }
        Ok(rel)
    }
}

/// `scan` over a source with only the columns it reads: its projection and the
/// columns its filters reference. The upstream matches a read's base schema to
/// its table by name, so a read never encodes a column it does not need, such
/// as an embedding column, whose fixed-size list type Substrait cannot express.
/// A scan of every column is returned unchanged.
fn read_columns(scan: &TableScan) -> Result<TableScan> {
    let Some(projection) = &scan.projection else {
        return Ok(scan.clone());
    };
    let source = scan.source.schema();
    let mut read: std::collections::BTreeSet<usize> = projection.iter().copied().collect();
    for filter in &scan.filters {
        for column in filter.column_refs() {
            if let Ok(index) = source.index_of(&column.name) {
                read.insert(index);
            }
        }
    }
    if read.len() == source.fields().len() {
        return Ok(scan.clone());
    }
    let read: Vec<usize> = read.into_iter().collect();
    let projection = projection
        .iter()
        .map(|index| {
            read.binary_search(index).map_err(|_| {
                DataFusionError::Internal("a projected column is missing from the read".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    datafusion::logical_expr::TableScanBuilder::new(
        scan.table_name.clone(),
        datafusion::datasource::provider_as_source(Arc::new(EmptyTable::new(Arc::new(
            source.project(&read)?,
        )))),
    )
    .with_projection(Some(projection))
    .with_filters(scan.filters.clone())
    .with_fetch(scan.fetch)
    .build()
}

/// The Substrait relation, root names and extensions for `plan`. Fails when any
/// part of the plan cannot be represented for the upstream.
fn produce(plan: &LogicalPlan, direct_scan: bool) -> Result<(Rel, Vec<String>, Extensions)> {
    let mut producer = SpiceProducer {
        direct_scan,
        ..Default::default()
    };
    let input = *producer.handle_plan(plan)?;
    // Preserve duplicate column names with distinct logical qualifiers. Building
    // a synthetic scan would discard those qualifiers and reject valid joins.
    let mut names = Vec::new();
    for field in plan.schema().fields() {
        flatten_root_names(field, false, &mut names)?;
    }
    Ok((input, names, producer.get_extensions()))
}

pub(super) fn encode_plan(plan: &LogicalPlan, direct_scan: bool) -> Result<Bytes> {
    let (input, names, extensions) = produce(plan, direct_scan)?;
    // Extension maps do not have a stable iteration order. Serialize anchors
    // deterministically so identical fragments have identical wire cache keys.
    let mut extensions: Vec<
        datafusion_substrait::substrait::proto::extensions::SimpleExtensionDeclaration,
    > = extensions.into();
    extensions.sort_by_key(|extension| {
        use datafusion_substrait::substrait::proto::extensions::simple_extension_declaration::MappingType;
        match &extension.mapping_type {
            Some(MappingType::ExtensionFunction(function)) => (0, function.function_anchor),
            Some(MappingType::ExtensionType(kind)) => (1, kind.type_anchor),
            Some(MappingType::ExtensionTypeVariation(variation)) => (2, variation.type_variation_anchor),
            None => (3, 0),
        }
    });
    Ok(Plan {
        version: Some(version_with_producer("spice")),
        relations: vec![PlanRel {
            rel_type: Some(plan_rel::RelType::Root(RelRoot {
                input: Some(input),
                names,
            })),
        }],
        extensions,
        ..Default::default()
    }
    .encode_to_vec()
    .into())
}

/// Substrait root names are depth-first struct names. List elements and map
/// entries are structural wrappers and do not contribute their own names.
fn flatten_root_names(field: &Field, skip_name: bool, names: &mut Vec<String>) -> Result<()> {
    if !skip_name {
        names.push(field.name().clone());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for field in fields {
                flatten_root_names(field, false, names)?;
            }
        }
        DataType::List(element) | DataType::LargeList(element) => {
            flatten_root_names(element, true, names)?;
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(DataFusionError::Plan(
                    "Substrait map entries must be a struct".into(),
                ));
            };
            if fields.len() != 2 {
                return Err(DataFusionError::Plan(
                    "Substrait map entries require key and value fields".into(),
                ));
            }
            for field in fields {
                flatten_root_names(field, true, names)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn table_names(plan: &LogicalPlan) -> Result<String> {
    let mut names = std::collections::BTreeSet::new();
    plan.apply_with_subqueries(|plan| {
        if let LogicalPlan::TableScan(scan) = plan {
            names.insert(scan.table_name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(names.into_iter().collect::<Vec<_>>().join(", "))
}

#[derive(Debug, Clone)]
struct SpiceFederation {
    client: FlightClient,
    context: String,
}

impl FederationProvider for SpiceFederation {
    fn name(&self) -> &'static str {
        "spice_substrait"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        // A failed conversion declines this candidate so the federation analyzer
        // can try smaller subtrees, keeping unsupported operators in the sidecar.
        // The analyzer asks at every node, so this skips the wire encoding.
        Some(if produce(plan, false).is_ok() {
            Arc::new(Analyzer::with_rules(vec![Arc::new(self.clone())])).into()
        } else {
            FederationAnalyzerForLogicalPlan::Unable
        })
    }
}

impl AnalyzerRule for SpiceFederation {
    fn name(&self) -> &'static str {
        "federate_spice_substrait"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(FederatedPlanNode::new(plan, Arc::new(self.clone()))),
        }))
    }
}

#[async_trait]
impl FederationPlanner for SpiceFederation {
    async fn plan_federation(
        &self,
        node: &FederatedPlanNode,
        session: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Run logical optimization in the sidecar without running its federation
        // analyzer again. Physical/distributed planning remains upstream.
        let plan = session
            .optimizer()
            .optimize(node.plan().clone(), session, |_, _| {})?;
        Ok(Arc::new(SubstraitExec::new(
            self.client.clone(),
            encode_plan(&plan, false)?,
            Arc::new(node.plan().schema().as_arrow().clone()),
            table_names(node.plan())?,
        )))
    }
}

pub(super) fn scan(
    table: &FlightTable,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let MultiPartTableReference::TableReference(reference) = table.table_reference.table_ref()
    else {
        return Err(DataFusionError::NotImplemented(
            "Substrait scan requires a table reference".into(),
        ));
    };
    let source = datafusion::datasource::provider_as_source(Arc::new(EmptyTable::new(Arc::clone(
        &table.schema,
    ))));
    let mut plan = LogicalPlanBuilder::scan_with_filters(
        reference.clone(),
        source,
        projection.cloned(),
        filters.to_vec(),
    )?;
    if let Some(limit) = limit {
        plan = plan.limit(0, Some(limit))?;
    }
    let plan = plan.build()?;
    let schema = Arc::new(plan.schema().as_arrow().clone());
    Ok(Arc::new(SubstraitExec::new(
        table.client.clone(),
        encode_plan(&plan, true)?,
        schema,
        table_names(&plan)?,
    )))
}

#[derive(Debug)]
pub(super) struct SubstraitExec {
    client: FlightClient,
    plan: Bytes,
    schema: SchemaRef,
    /// The schema Flight delivers when `schema` has view columns, which Flight
    /// expands to their large equivalents.
    wire_schema: Option<SchemaRef>,
    datasets: String,
    properties: Arc<PlanProperties>,
}

impl SubstraitExec {
    pub(super) fn new(
        client: FlightClient,
        plan: Bytes,
        schema: SchemaRef,
        datasets: String,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let wire_schema = arrow_tools::schema::expand_views_schema(&schema);
        let wire_schema = (wire_schema.fields() != schema.fields()).then(|| Arc::new(wire_schema));
        Self {
            client,
            plan,
            schema,
            wire_schema,
            datasets,
            properties,
        }
    }
}

/// Flight expands only top-level views. Reverse that exact representation
/// change; never coerce arbitrary remote types.
///
/// Top-level nullability is the remote's inference, not data: a remote
/// rewrite (checked integer arithmetic) can widen it. The planned nullability
/// applies once Arrow confirms a non-nullable column holds no nulls.
fn conform_batch(
    batch: RecordBatch,
    expected: &SchemaRef,
    wire: Option<&SchemaRef>,
) -> Result<RecordBatch> {
    let batch = match wire {
        Some(wire) if same_fields_except_nullability(batch.schema().fields(), wire.fields()) => {
            arrow_tools::schema::cast_view_columns(batch, expected)?
        }
        _ => batch,
    };
    if batch.schema().fields() == expected.fields() {
        return Ok(batch);
    }
    if !same_fields_except_nullability(batch.schema().fields(), expected.fields()) {
        return Err(DataFusionError::Execution(format!(
            "Remote Substrait result schema differs from the planned schema: expected {expected:?}, received {:?}",
            batch.schema(),
        )));
    }
    RecordBatch::try_new(Arc::clone(expected), batch.columns().to_vec()).map_err(|error| {
        DataFusionError::Execution(format!(
            "Remote Substrait result does not match the planned schema: {error}"
        ))
    })
}

fn same_fields_except_nullability(actual: &Fields, expected: &Fields) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.data_type() == expected.data_type()
                    && actual.metadata() == expected.metadata()
            })
}

impl DisplayAs for SubstraitExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SubstraitExec: plan_bytes={}", self.plan.len())
    }
}

impl ExecutionPlan for SubstraitExec {
    fn name(&self) -> &'static str {
        "SubstraitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal("SubstraitExec is a leaf".into()));
        }
        Ok(self)
    }

    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "SubstraitExec has one partition".into(),
            ));
        }
        let mut client = self.client.clone();
        let deadline = context
            .session_config()
            .get_extension::<runtime_request_context::QueryDeadline>();
        let mut propagated_trace_id = None;
        if let Some(request) = runtime_request_context::resolve_request_context(&context, false) {
            propagated_trace_id = request.settled_trace_id().cloned();
            if let Some(parent) = request.trace_parent() {
                client = client
                    .with_trace_header(TRACEPARENT_HEADER, &parent.to_string())
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
            if let Some(trace_id) = request.client_trace_id() {
                client = client
                    .with_trace_header(SPICE_TRACE_ID_HEADER, trace_id)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
        }
        let plan = self.plan.clone();
        let datasets = self.datasets.clone();
        let query = async move {
            let timeout = deadline.as_ref().map(|deadline| {
                deadline
                    .0
                    .saturating_duration_since(std::time::Instant::now())
            });
            client
                .query_substrait(plan, timeout, propagated_trace_id.as_deref())
                .await
                .map_err(|error| match error {
                    flight_client::Error::SubstraitUnsupported { source } => {
                        DataFusionError::Execution(format!(
                            "The upstream Spice runtime for dataset '{datasets}' does not support the required Substrait plan: {source}. Upgrade the upstream runtime, or restart this runtime with `--role server` to use SQL federation."
                        ))
                    }
                    other => DataFusionError::External(Box::new(other)),
                })
        };
        let schema = Arc::clone(&self.schema);
        let expected = Arc::clone(&schema);
        let wire = self.wire_schema.clone();
        let stream = normalized_batches(query, self.datasets.clone())
            .map(move |batch| conform_batch(batch?, &expected, wire.as_ref()));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };

    use super::{conform_batch, encode_plan};

    /// A read names only the columns it uses: its projection and the columns
    /// its filters reference. An embedding column the scan does not read is
    /// never encoded; reading it is still refused, since Substrait has no
    /// fixed-size list type.
    #[test]
    fn a_read_encodes_only_the_columns_it_uses() {
        use datafusion::{
            datasource::{empty::EmptyTable, provider_as_source},
            logical_expr::{LogicalPlanBuilder, col, lit},
        };
        use datafusion_substrait::substrait::proto::{
            Plan, plan_rel::RelType as PlanRelType, read_rel::ReadType, rel::RelType,
        };
        use prost::Message;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
            Field::new(
                "label_embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 64),
                true,
            ),
        ]));
        let scan = |projection: Option<Vec<usize>>| {
            LogicalPlanBuilder::scan_with_filters(
                "docs",
                provider_as_source(Arc::new(EmptyTable::new(Arc::clone(&schema)))),
                projection,
                vec![col("value").gt(lit(0_i64))],
            )
            .expect("scan")
            .build()
            .expect("plan")
        };

        let plan = Plan::decode(encode_plan(&scan(Some(vec![0])), true).expect("encodes"))
            .expect("decodes");
        let Some(PlanRelType::Root(root)) = &plan.relations[0].rel_type else {
            panic!("a root relation: {plan:?}");
        };
        let Some(RelType::Read(read)) = root
            .input
            .as_ref()
            .and_then(|input| input.rel_type.as_ref())
        else {
            panic!("a read: {root:?}");
        };
        assert!(
            matches!(read.read_type, Some(ReadType::NamedTable(_))),
            "{read:?}"
        );
        let base = read.base_schema.as_ref().expect("a base schema");
        assert_eq!(base.names, vec!["id".to_string(), "value".to_string()]);
        let select = read
            .projection
            .as_ref()
            .and_then(|mask| mask.select.as_ref());
        let fields: Vec<i32> = select
            .expect("a projection")
            .struct_items
            .iter()
            .map(|item| item.field)
            .collect();
        assert_eq!(fields, vec![0]);

        let error = encode_plan(&scan(Some(vec![0, 2])), true)
            .expect_err("Substrait has no fixed-size list type");
        assert!(error.to_string().contains("FixedSizeList"), "{error}");
    }

    fn batch(nullable: bool, values: Vec<Option<i64>>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("n", DataType::Int64, nullable)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(values))])
            .expect("valid batch")
    }

    #[test]
    fn conform_batch_takes_planned_nullability_when_the_data_allows_it() {
        let expected = batch(false, vec![]).schema();
        let conformed = conform_batch(batch(true, vec![Some(2), Some(3)]), &expected, None)
            .expect("no nulls in a non-nullable column");
        assert_eq!(conformed.schema(), expected);

        let error = conform_batch(batch(true, vec![Some(2), None]), &expected, None)
            .expect_err("a null in a non-nullable column");
        assert!(
            error.to_string().contains("declared as non-nullable"),
            "{error}"
        );
    }

    #[test]
    fn conform_batch_rejects_a_different_type() {
        let expected = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        let error = conform_batch(batch(true, vec![Some(2)]), &expected, None)
            .expect_err("Int64 is not Int32");
        assert!(
            error
                .to_string()
                .contains("differs from the planned schema"),
            "{error}"
        );
    }
}
