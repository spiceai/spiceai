/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Typed plan federation between a sidecar and an upstream Spice runtime.

use std::{fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_tools::map_entries::StreamNormalizer;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    catalog::Session,
    common::{Result, tree_node::TreeNodeRecursion},
    config::ConfigOptions,
    datasource::empty::EmptyTable,
    error::DataFusionError,
    execution::{TaskContext, context::SessionState},
    logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, TableScan, TableScanBuilder},
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
        proto::{Plan, PlanRel, Rel, RelRoot, plan_rel, read_rel::ReadType, rel::RelType},
        version::version_with_producer,
    },
};
use flight_client::FlightClient;
use futures::StreamExt;
use prost::Message;

use super::{FlightTable, map_query_stream_error};

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
    fn handle_scalar_function(
        &mut self,
        function: &datafusion::logical_expr::expr::ScalarFunction,
        schema: &datafusion::common::DFSchemaRef,
    ) -> Result<datafusion_substrait::substrait::proto::Expression> {
        if runtime_udfs_api::is_user_function(function.func.name())
            || function.func.as_async().is_some()
            || matches!(function.func.name(), "embed" | "rerank")
        {
            return Err(DataFusionError::NotImplemented(format!(
                "Function '{}' is evaluated in the sidecar",
                function.func.name(),
            )));
        }
        datafusion_substrait::logical_plan::producer::from_scalar_function(self, function, schema)
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
        let mut rel = from_table_scan(self, scan)?;
        if let Some(RelType::Read(read)) = &mut rel.rel_type
            && let Some(ReadType::NamedTable(table)) = &mut read.read_type
        {
            table.names = names;
        }
        Ok(rel)
    }
}

fn encode_plan(plan: &LogicalPlan, direct_scan: bool) -> Result<Bytes> {
    let mut producer = SpiceProducer {
        direct_scan,
        ..Default::default()
    };
    let input = *producer.handle_plan(plan)?;
    // A root uses the same flattened field names as a named read schema.
    // Reuse the producer's public read serializer, including nested names and
    // type registration, rather than duplicating its private schema codec.
    let output_schema = Arc::new(plan.schema().as_arrow().clone());
    let output = TableScanBuilder::new(
        datafusion::common::TableReference::bare("__spice_substrait_output"),
        datafusion::datasource::provider_as_source(Arc::new(EmptyTable::new(output_schema))),
    )
    .build()?;
    let Some(RelType::Read(read)) = from_table_scan(&mut producer, &output)?.rel_type else {
        return Err(DataFusionError::Internal(
            "Expected a Substrait read schema".into(),
        ));
    };
    let names = read
        .base_schema
        .ok_or_else(|| DataFusionError::Internal("Substrait output schema is missing".into()))?
        .names;
    let extensions = producer.get_extensions();
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
    fn name(&self) -> &str {
        "spice_substrait"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        // A failed conversion declines this candidate so the federation analyzer
        // can try smaller subtrees, keeping unsupported operators in the sidecar.
        Some(if encode_plan(plan, false).is_ok() {
            Arc::new(Analyzer::with_rules(vec![Arc::new(self.clone())])).into()
        } else {
            FederationAnalyzerForLogicalPlan::Unable
        })
    }
}

impl AnalyzerRule for SpiceFederation {
    fn name(&self) -> &str {
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
    _session: &dyn Session,
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
struct SubstraitExec {
    client: FlightClient,
    plan: Bytes,
    schema: SchemaRef,
    datasets: String,
    properties: Arc<PlanProperties>,
}

impl SubstraitExec {
    fn new(client: FlightClient, plan: Bytes, schema: SchemaRef, datasets: String) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            client,
            plan,
            schema,
            datasets,
            properties,
        }
    }
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
        if let Some(request) = runtime_request_context::resolve_request_context(&context, false) {
            if let Some(parent) = request.trace_parent() {
                client = client
                    .with_trace_header("traceparent", &parent.to_string())
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
            if let Some(trace_id) = request.settled_trace_id() {
                client = client
                    .with_trace_header("spice-trace-id", trace_id)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
            }
        }
        let plan = self.plan.clone();
        let schema = Arc::clone(&self.schema);
        let expected_schema = Arc::clone(&schema);
        let datasets = self.datasets.clone();
        let stream = try_stream! {
            let timeout = deadline.as_ref().map(|deadline| deadline.0.saturating_duration_since(std::time::Instant::now()));
            let mut stream = client.query_substrait(plan, timeout).await
                .map_err(|error| match error {
                    flight_client::Error::SubstraitUnsupported { source } => {
                        DataFusionError::Execution(format!(
                            "The upstream Spice runtime for dataset '{datasets}' does not support the required Substrait plan: {source}. Upgrade the upstream runtime, or set runtime.mode: server to use SQL federation."
                        ))
                    }
                    other => DataFusionError::External(Box::new(other)),
                })?;
            let mut normalizer = StreamNormalizer::new();
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(map_query_stream_error)?;
                let batch = normalizer.normalize(batch)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                if batch.schema().fields() != expected_schema.fields() {
                    Err(DataFusionError::Execution(format!(
                        "Remote Substrait result schema differs from the planned schema: expected {expected_schema:?}, received {:?}",
                        batch.schema(),
                    )))?;
                }
                yield batch;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
