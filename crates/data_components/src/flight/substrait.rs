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

use std::{borrow::Cow, fmt, sync::Arc, time::Duration};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Fields, SchemaRef},
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    common::{
        Result, ScalarValue,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
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
use flight_client::{
    FlightClient,
    parameterized_ticket::{
        PARAMETERIZED_SUBSTRAIT_VERSION, SPICE_SERVER_NAME, SQL_INFO_PARAMETERIZED_SUBSTRAIT,
    },
};
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

/// Which table reads a plan sent upstream may contain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Reads {
    /// Reads of tables federated to this upstream, renamed to their remote
    /// references. A local table is refused.
    #[default]
    Federated,
    /// The plan's own scan, already named by its remote reference.
    Direct,
    /// No table: a delegated search reads only its search leaf. Any scan, such
    /// as one inside a filter's subquery, is refused, so the upstream never
    /// resolves a name the sidecar did not declare.
    None,
}

/// Resolve remote names at serialization time. All expression binding remains
/// against the original local schemas; Substrait refers to fields by position.
#[derive(Default)]
struct SpiceProducer {
    extensions: Extensions,
    reads: Reads,
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
                    value: search.call.clone(),
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
        let names = if self.reads == Reads::None {
            return Err(DataFusionError::Plan(
                "A delegated search plan reads no table".into(),
            ));
        } else if self.reads == Reads::Direct {
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
        let read = read_columns(scan)?;
        let mut rel = from_table_scan(self, &read)?;
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
fn read_columns(scan: &TableScan) -> Result<Cow<'_, TableScan>> {
    let Some(projection) = &scan.projection else {
        return Ok(Cow::Borrowed(scan));
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
        return Ok(Cow::Borrowed(scan));
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
    .map(Cow::Owned)
}

/// The Substrait relation, root names and extensions for `plan`. Fails when any
/// part of the plan cannot be represented for the upstream.
pub(super) fn produce(plan: &LogicalPlan, reads: Reads) -> Result<(Rel, Vec<String>, Extensions)> {
    let mut producer = SpiceProducer {
        reads,
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

pub(super) fn encode_plan(plan: &LogicalPlan, reads: Reads) -> Result<Bytes> {
    let (input, names, extensions) = produce(plan, reads)?;
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
        Some(if produce(plan, Reads::Federated).is_ok() {
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
        let exec = SubstraitExec::new(
            self.client.clone(),
            encode_plan(&plan, Reads::Federated)?,
            Arc::new(node.plan().schema().as_arrow().clone()),
            table_names(node.plan())?,
        );
        Ok(Arc::new(match PlanParameters::of(plan)? {
            Some(parameters) => exec.with_parameters(parameters),
            None => exec,
        }))
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
        encode_plan(&plan, Reads::Direct)?,
        schema,
        table_names(&plan)?,
    )))
}

#[derive(Debug, Clone)]
pub(super) struct SubstraitExec {
    client: FlightClient,
    plan: Bytes,
    schema: SchemaRef,
    /// The schema Flight delivers when `schema` has view columns, which Flight
    /// expands to their large equivalents.
    wire_schema: Option<SchemaRef>,
    datasets: String,
    properties: Arc<PlanProperties>,
    /// Set when `plan` holds dynamic parameters (`$N`): a template, or, once
    /// [`SubstraitTemplate::bind`] has bound values to it, a bound plan whose
    /// `plan` carries them as `parameter_bindings`.
    parameters: Option<Arc<PlanParameters>>,
}

/// The dynamic parameters of a federated plan, and the values bound to them.
#[derive(Debug)]
struct PlanParameters {
    /// The optimized plan the template was encoded from, still holding its
    /// placeholders. For an upstream that does not take parameterized plans,
    /// the values are bound into it and it is encoded again, with literals.
    plan: Arc<LogicalPlan>,
    /// Each parameter's anchor (`$N` is `N - 1`) and the type the plan
    /// declares for it, by anchor.
    fields: Vec<(u32, DataType)>,
    /// The values bound to `fields`, in the same order; `None` for a template.
    values: Option<Vec<ScalarValue>>,
}

impl PlanParameters {
    /// The parameters of `plan`, or `None` when it has none. A placeholder
    /// that is not `$N`, or has no type, leaves the plan without parameters:
    /// such a plan cannot be bound exactly, and is sent as it is today.
    fn of(plan: LogicalPlan) -> Result<Option<Self>> {
        let declared = plan.get_parameter_fields()?;
        if declared.is_empty() {
            return Ok(None);
        }
        let mut fields = Vec::with_capacity(declared.len());
        for (id, field) in declared {
            let (Some(field), Some(anchor)) = (
                field,
                id.strip_prefix('$')
                    .and_then(|n| n.parse::<u32>().ok())
                    .and_then(|n| n.checked_sub(1)),
            ) else {
                return Ok(None);
            };
            fields.push((anchor, field.data_type().clone()));
        }
        fields.sort_by_key(|(anchor, _)| *anchor);
        Ok(Some(Self {
            plan: Arc::new(plan),
            fields,
            values: None,
        }))
    }

    /// The bound plan encoded with its values inlined as literals: the plan a
    /// sidecar sent before parameterized plans, for an upstream without them.
    fn inlined(&self) -> Result<Bytes> {
        let Some(values) = &self.values else {
            return Err(DataFusionError::Internal(
                "A Substrait template has no values to inline".into(),
            ));
        };
        let values: std::collections::HashMap<String, ScalarValue> = self
            .fields
            .iter()
            .zip(values)
            .map(|((anchor, _), value)| ((u64::from(*anchor) + 1).to_string(), value.clone()))
            .collect();
        let plan = Arc::unwrap_or_clone(Arc::clone(&self.plan)).with_param_values(values)?;
        encode_plan(&plan, Reads::Federated)
    }
}

/// How long a probe's finding that an upstream does or does not take
/// parameterized plans stands, and how long a failed probe (an unreachable
/// upstream, a refused credential) stands before the next query asks again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParameterizedProbeIntervals {
    pub answered: Duration,
    pub failed: Duration,
}

/// Ten minutes: an upstream upgraded in place, or rolled back, is sent the
/// plans it takes within that long. A failed probe is retried after 30 seconds.
const DEFAULT_PROBE_INTERVALS: ParameterizedProbeIntervals = ParameterizedProbeIntervals {
    answered: Duration::from_secs(600),
    failed: Duration::from_secs(30),
};

/// How long a probe may take; one that takes longer counts as failed.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

static PROBE_INTERVALS: std::sync::RwLock<ParameterizedProbeIntervals> =
    std::sync::RwLock::new(DEFAULT_PROBE_INTERVALS);

/// Sets the probe intervals of [`UpstreamCapability`] entries probed from now
/// on. For tests: an interval short enough to watch an upstream change. Not a
/// runtime setting.
#[doc(hidden)]
pub fn set_parameterized_probe_intervals_for_tests(intervals: ParameterizedProbeIntervals) {
    if let Ok(mut current) = PROBE_INTERVALS.write() {
        *current = intervals;
    }
}

/// What a probe of an upstream found, and how long it stands.
#[derive(Debug, Clone, Copy)]
struct UpstreamCapability {
    parameterized: bool,
    stands_for: Duration,
}

struct CapabilityExpiry;

impl moka::Expiry<String, UpstreamCapability> for CapabilityExpiry {
    fn expire_after_create(
        &self,
        _url: &String,
        capability: &UpstreamCapability,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(capability.stands_for)
    }
}

/// Whether each upstream, by URL, takes parameterized plans, as its
/// `GetSqlInfo` answered. Probed on the first parameterized plan for an
/// upstream, once however many queries arrive together, and again once the
/// finding expires.
static UPSTREAM_CAPABILITIES: std::sync::LazyLock<moka::future::Cache<String, UpstreamCapability>> =
    std::sync::LazyLock::new(|| {
        moka::future::Cache::builder()
            .max_capacity(1024)
            .expire_after(CapabilityExpiry)
            .build()
    });

/// Whether `client`'s upstream takes parameterized plans, probing it if no
/// finding for it stands.
async fn takes_parameterized_plans(client: &FlightClient) -> bool {
    let probe = async {
        let intervals = PROBE_INTERVALS
            .read()
            .map_or(DEFAULT_PROBE_INTERVALS, |intervals| *intervals);
        let info = [
            arrow_flight::sql::SqlInfo::FlightSqlServerName as u32,
            SQL_INFO_PARAMETERIZED_SUBSTRAIT,
        ];
        match client.get_sql_info(&info, Some(PROBE_TIMEOUT)).await {
            Ok(values) => UpstreamCapability {
                parameterized: advertises_parameterized_plans(&values),
                stands_for: intervals.answered,
            },
            Err(error) => {
                tracing::debug!(
                    "Unable to ask the upstream Spice runtime at {} whether it takes parameterized Substrait plans ({error}); sending plans with their values inlined",
                    client.url()
                );
                UpstreamCapability {
                    parameterized: false,
                    stands_for: intervals.failed,
                }
            }
        }
    };
    UPSTREAM_CAPABILITIES
        .get_with(client.url().to_string(), probe)
        .await
        .parameterized
}

/// Whether a `GetSqlInfo` answer advertises the parameterized Substrait
/// ticket this build sends. The code is custom, so it is read only from a
/// server that names itself Spice: any other server's code 10,000 means
/// whatever that server defines it to.
fn advertises_parameterized_plans(
    values: &std::collections::HashMap<u32, flight_client::SqlInfoValue>,
) -> bool {
    use arrow_flight::sql::SqlInfo;
    use flight_client::SqlInfoValue;
    let spice = matches!(
        values.get(&(SqlInfo::FlightSqlServerName as u32)),
        Some(SqlInfoValue::String(name)) if name == SPICE_SERVER_NAME
    );
    let version = match values.get(&SQL_INFO_PARAMETERIZED_SUBSTRAIT) {
        Some(SqlInfoValue::Int32(version)) => i64::from(*version),
        Some(SqlInfoValue::BigInt(version)) => *version,
        _ => 0,
    };
    spice && version >= i64::from(PARAMETERIZED_SUBSTRAIT_VERSION)
}

static METER: std::sync::LazyLock<opentelemetry::metrics::Meter> =
    std::sync::LazyLock::new(|| opentelemetry::global::meter("flight"));

/// Parameterized plans an upstream refused as unknown after advertising them,
/// each then sent with its values inlined.
static PARAMETERIZED_PLANS_REFUSED: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::LazyLock::new(|| {
        METER
            .u64_counter("flight_substrait_parameterized_refused")
            .with_description(
                "Parameterized Substrait plans an upstream refused as unknown after advertising them; each was resent with its values inlined.",
            )
            .build()
    });

/// [`PARAMETERIZED_PLANS_REFUSED`], readable in process.
static PARAMETERIZED_PLANS_REFUSED_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many parameterized plans an upstream refused as unknown after
/// advertising them, in this process.
#[must_use]
pub fn parameterized_plans_refused() -> u64 {
    PARAMETERIZED_PLANS_REFUSED_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}

/// A fully federated plan whose Substrait holds dynamic parameters, planned
/// once and bound to each request's values.
///
/// Holds the [`SubstraitExec`] a physical plan was planned around, and the
/// plan itself: binding replaces that leaf with one carrying the values and
/// rebuilds every node above it, so no two executions share a node's state.
#[derive(Debug, Clone)]
pub struct SubstraitTemplate {
    plan: Arc<dyn ExecutionPlan>,
    leaf: SubstraitExec,
}

impl SubstraitTemplate {
    /// The template in `plan`, when `plan` is a chain of single-input nodes
    /// over exactly one Substrait leaf whose plan holds unbound parameters.
    /// Anything else (a join of a local and a federated input, a second leaf,
    /// a leaf without parameters) is `None`.
    #[must_use]
    pub fn of(plan: &Arc<dyn ExecutionPlan>) -> Option<Self> {
        let mut node = plan;
        loop {
            let children = node.children();
            match children.as_slice() {
                [] => break,
                [child] => node = *child,
                _ => return None,
            }
        }
        let leaf = node.downcast_ref::<SubstraitExec>()?;
        let parameters = leaf.parameters.as_ref()?;
        if parameters.values.is_some() {
            return None;
        }
        Some(Self {
            plan: Arc::clone(plan),
            leaf: leaf.clone(),
        })
    }

    /// Each parameter the template's Substrait holds, as its `$N` number and
    /// the type the plan declares for it, in order. The optimizer may have
    /// removed a parameter the logical plan had; it is not listed.
    pub fn parameters(&self) -> impl Iterator<Item = (usize, &DataType)> {
        self.leaf
            .parameters
            .iter()
            .flat_map(|parameters| parameters.fields.iter())
            .map(|(anchor, data_type)| (*anchor as usize + 1, data_type))
    }

    /// The template's plan with `values` bound: `values[N - 1]` is the value
    /// of `$N`. Values for parameters the template does not hold are ignored.
    ///
    /// # Errors
    /// When a parameter has no value, a value's type is not exactly the type
    /// the plan declares, or a value cannot be written as a binding.
    pub fn bind(
        &self,
        values: &[ScalarValue],
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(parameters) = &self.leaf.parameters else {
            return Err(DataFusionError::Internal(
                "A Substrait template has no parameters".into(),
            ));
        };
        let mut bound = Vec::with_capacity(parameters.fields.len());
        for (anchor, data_type) in &parameters.fields {
            let id = u64::from(*anchor) + 1;
            let Some(value) = values.get(*anchor as usize) else {
                return Err(DataFusionError::Plan(format!(
                    "No value is bound to parameter ${id}"
                )));
            };
            if &value.data_type() != data_type {
                return Err(DataFusionError::Plan(format!(
                    "Parameter ${id} is declared as {data_type}, but bound to a {} value",
                    value.data_type()
                )));
            }
            bound.push(value.clone());
        }
        let bindings = super::substrait_parameters::parameter_bindings(
            parameters
                .fields
                .iter()
                .map(|(anchor, _)| *anchor)
                .zip(&bound),
            state,
        )?;
        // Concatenated Protobuf messages merge, and a repeated field appends:
        // the template's bytes followed by a message holding only the bindings
        // decode as the template with those bindings, without decoding it here.
        let mut plan = bytes::BytesMut::from(self.leaf.plan.as_ref());
        Plan {
            parameter_bindings: bindings,
            ..Default::default()
        }
        .encode(&mut plan)
        .map_err(|error| DataFusionError::Internal(error.to_string()))?;
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(SubstraitExec {
            plan: plan.freeze(),
            parameters: Some(Arc::new(PlanParameters {
                plan: Arc::clone(&parameters.plan),
                fields: parameters.fields.clone(),
                values: Some(bound),
            })),
            ..self.leaf.clone()
        });
        Arc::clone(&self.plan)
            .transform_up(|node| {
                if node.children().is_empty() {
                    Ok(Transformed::yes(Arc::clone(&leaf)))
                } else {
                    node.reset_state().map(Transformed::yes)
                }
            })
            .map(|transformed| transformed.data)
    }
}

/// The federation context of `source` when it is a table this runtime reads
/// from an upstream Spice runtime through Substrait plans: tables with the same
/// context are read from the same upstream, as the same principal.
#[must_use]
pub fn spice_substrait_context(
    source: &Arc<dyn datafusion::logical_expr::TableSource>,
) -> Option<String> {
    let provider = get_table_source(source).ok()??.federation_provider();
    if provider.name() == "spice_substrait" {
        provider.compute_context()
    } else {
        None
    }
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
            parameters: None,
        }
    }

    fn with_parameters(self, parameters: PlanParameters) -> Self {
        Self {
            parameters: Some(Arc::new(parameters)),
            ..self
        }
    }
}

/// Flight expands only top-level views. Reverse that exact representation
/// change; never coerce arbitrary remote types.
///
/// Top-level nullability is the remote's inference, not data: a remote
/// rewrite (checked integer arithmetic) can widen it. The planned nullability
/// applies once Arrow confirms a non-nullable column holds no nulls. The
/// planned descriptive field metadata applies too (see
/// [`same_fields_except_nullability`]).
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

/// Whether `actual` matches `expected` but for nullability and descriptive
/// field metadata. Metadata such as a source's `source_type` describes a
/// column without changing its data, and the remote need not send it back.
/// Arrow extension metadata does change what a column means, so it must match.
fn same_fields_except_nullability(actual: &Fields, expected: &Fields) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.data_type() == expected.data_type()
                    && extension_metadata(actual) == extension_metadata(expected)
            })
}

/// A field's Arrow extension metadata: the `ARROW:extension:*` keys.
fn extension_metadata(field: &Field) -> Vec<(&String, &String)> {
    let mut keys: Vec<_> = field
        .metadata()
        .iter()
        .filter(|(key, _)| key.starts_with("ARROW:extension:"))
        .collect();
    keys.sort();
    keys
}

impl DisplayAs for SubstraitExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SubstraitExec: plan_bytes={}", self.plan.len())?;
        if let Some(parameters) = &self.parameters {
            write!(f, ", parameters={}", parameters.fields.len())?;
        }
        Ok(())
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
        // Only a bound plan is sent parameterized; a template's placeholders
        // have no values, and are sent as they always were.
        let parameters = self
            .parameters
            .clone()
            .filter(|parameters| parameters.values.is_some());
        let query = async move {
            let timeout = || {
                deadline.as_ref().map(|deadline| {
                    deadline
                        .0
                        .saturating_duration_since(std::time::Instant::now())
                })
            };
            let map_error = |error| match error {
                flight_client::Error::SubstraitUnsupported { source } => {
                    DataFusionError::Execution(format!(
                        "The upstream Spice runtime for dataset '{datasets}' does not support the required Substrait plan: {source}. Upgrade the upstream runtime, or restart this runtime with `--role server` to use SQL federation."
                    ))
                }
                other => DataFusionError::External(Box::new(other)),
            };
            let plan = match parameters {
                None => plan,
                Some(parameters) => {
                    if takes_parameterized_plans(&client).await {
                        match client
                            .query_substrait_parameterized(
                                plan,
                                timeout(),
                                propagated_trace_id.as_deref(),
                            )
                            .await
                        {
                            // Advertised, then refused: such as a rolling
                            // upgrade, where the probe reached a new runtime
                            // and this query an old one. This query is resent
                            // inlined; the next is offered parameterized again.
                            Err(flight_client::Error::ParameterizedSubstraitUnsupported {
                                source,
                            }) => {
                                PARAMETERIZED_PLANS_REFUSED.add(1, &[]);
                                PARAMETERIZED_PLANS_REFUSED_COUNT
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                tracing::debug!(
                                    "The upstream Spice runtime at {} advertised parameterized Substrait plans but refused one ({source}); resending it with its values inlined",
                                    client.url()
                                );
                            }
                            result => return result.map_err(map_error),
                        }
                    }
                    parameters.inlined()?
                }
            };
            client
                .query_substrait(plan, timeout(), propagated_trace_id.as_deref())
                .await
                .map_err(map_error)
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

    use super::{Reads, conform_batch, encode_plan};

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

        let plan = Plan::decode(encode_plan(&scan(Some(vec![0])), Reads::Direct).expect("encodes"))
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

        let error = encode_plan(&scan(Some(vec![0, 2])), Reads::Direct)
            .expect_err("Substrait has no fixed-size list type");
        assert!(error.to_string().contains("FixedSizeList"), "{error}");
    }

    /// Code 10,000 is custom, so another server reporting it is not taken to
    /// accept parameterized plans; Spice reporting version 0 or no code does
    /// not either.
    #[test]
    fn only_spice_advertising_a_version_takes_parameterized_plans() {
        use flight_client::SqlInfoValue;
        use std::collections::HashMap;
        let answer = |name: &str, version: Option<SqlInfoValue>| {
            let mut values = HashMap::from([(0, SqlInfoValue::String(name.to_string()))]);
            if let Some(version) = version {
                values.insert(10_000, version);
            }
            super::advertises_parameterized_plans(&values)
        };
        assert!(answer("Spice.ai", Some(SqlInfoValue::Int32(1))));
        assert!(answer("Spice.ai", Some(SqlInfoValue::Int32(2))));
        assert!(!answer("Spice.ai", Some(SqlInfoValue::Int32(0))));
        assert!(!answer("Spice.ai", None));
        assert!(!answer("Some Other Server", Some(SqlInfoValue::Int32(1))));
        assert!(!answer("spice.ai", Some(SqlInfoValue::Int32(1))));
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

    /// Descriptive metadata the remote does not send back, such as a source's
    /// `source_type`, is restored from the plan; an extension type is not
    /// something a remote may drop or change.
    #[test]
    fn conform_batch_restores_descriptive_metadata_but_not_extension_types() {
        use std::collections::HashMap;
        let with = |metadata: &[(&str, &str)]| {
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true).with_metadata(
                    metadata
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect::<HashMap<_, _>>(),
                ),
            ]))
        };
        let planned = with(&[("source_type", "bigint")]);
        let conformed = conform_batch(batch(true, vec![Some(2), None]), &planned, None)
            .expect("metadata alone is not a mismatch");
        assert_eq!(conformed.schema(), planned);

        let extension = with(&[("ARROW:extension:name", "arrow.uuid")]);
        let error = conform_batch(batch(true, vec![Some(2)]), &extension, None)
            .expect_err("an extension type the remote dropped is a mismatch");
        assert!(
            error
                .to_string()
                .contains("differs from the planned schema"),
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
