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

//! `FlightSQL` `CommandStatementSubstraitPlan` handlers.
//!
//! Per the `FlightSQL` spec, clients submit a serialized [`substrait::proto::Plan`]
//! wrapped in a [`SubstraitPlan`] message. The runtime decodes the plan, lowers
//! it to a `DataFusion` [`LogicalPlan`] via `datafusion-substrait`, and then runs
//! it through the same execution path as a SQL statement.

use std::{collections::HashMap, sync::Arc};

use crate::datafusion::flight_session_extension::FlightSessionExtension;
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
};
use bytes::Bytes;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    SubstraitConsumer, from_substrait_plan_with_consumer,
};
use datafusion_substrait::substrait::proto::{DynamicParameterBinding, Plan};
use prost::Message;
use tonic::{Request, Response, Status};

use super::substrait_consumer::SpiceSubstraitConsumer;
use crate::datafusion::query::{Query, QueryBuilder};
use crate::datafusion::request_context_extension::get_current_datafusion;
use crate::datafusion::sql_validator::validate_sql_query_read_only;
use crate::flight::{
    Service, handle_datafusion_error, is_auth_read_only,
    metrics::track_flight_request,
    to_tonic_err,
    util::{attach_cache_metadata, set_flightsql_protocol},
};
use crate::{datafusion::DataFusion, flight::handle_query_error};
use runtime_request_context::{AsyncMarker, RequestContext};
use telemetry::timing::{TimeMeasurement, TimedStream};

/// Decodes the Substrait protobuf payload out of a
/// [`sql::CommandStatementSubstraitPlan`] and computes a stable cache key for
/// the plan bytes. Split from [`decode_plan`] so the validation/decoding
/// rules can be unit-tested without spinning up a `DataFusion` session.
fn decode_plan_proto(cmd: &sql::CommandStatementSubstraitPlan) -> Result<(Plan, String), Status> {
    let substrait = cmd.plan.as_ref().ok_or_else(|| {
        Status::invalid_argument("CommandStatementSubstraitPlan.plan is required")
    })?;

    if substrait.plan.is_empty() {
        return Err(Status::invalid_argument(
            "CommandStatementSubstraitPlan.plan.plan must not be empty",
        ));
    }

    let proto = Plan::decode(substrait.plan.as_ref())
        .map_err(|e| Status::invalid_argument(format!("Failed to decode Substrait plan: {e}")))?;

    // Cache key namespaced so it cannot collide with a SQL query string.
    let cache_key = format!("substrait:{}", sha256_hex(substrait.plan.as_ref()));
    Ok((proto, cache_key))
}

/// Lowers the wire representation of a Substrait plan to a `DataFusion`
/// [`LogicalPlan`]. Returns a synthetic cache key derived from the plan bytes
/// so that identical Substrait plans share results-cache entries.
async fn decode_plan(
    cmd: &sql::CommandStatementSubstraitPlan,
    datafusion: &Arc<DataFusion>,
) -> Result<(LogicalPlan, String, SessionState), Status> {
    let (proto, cache_key) = decode_plan_proto(cmd)?;
    let session = datafusion.ctx.state();
    let plan = lower_plan(&proto, &session).await?;
    Ok((plan, cache_key, session))
}

/// The extensions `proto` declares, refusing any the consumer cannot honour.
fn plan_extensions(proto: &Plan) -> Result<Extensions, Status> {
    let extensions = Extensions::try_from(&proto.extensions).map_err(map_substrait_error)?;
    // `from_substrait_plan` refuses these too; the consumer cannot see them.
    if !extensions.type_variations.is_empty() {
        return Err(map_substrait_error(DataFusionError::NotImplemented(
            "Type variation extensions are not supported".to_string(),
        )));
    }
    Ok(extensions)
}

async fn lower_plan(proto: &Plan, session: &SessionState) -> Result<LogicalPlan, Status> {
    let extensions = plan_extensions(proto)?;
    let consumer = SpiceSubstraitConsumer::new(&extensions, session);
    from_substrait_plan_with_consumer(&consumer, proto)
        .await
        .map_err(map_substrait_error)
}

/// Decodes `cmd` for a request. A sessionless request also returns the session
/// snapshot the plan was decoded against, for execution to reuse; a stateful
/// Flight request keeps its own session-specific execution settings.
async fn decode_request(
    cmd: &sql::CommandStatementSubstraitPlan,
    context: &RequestContext,
    datafusion: &Arc<DataFusion>,
) -> Result<(LogicalPlan, String, Option<SessionState>), Status> {
    let (plan, cache_key, session) = decode_plan(cmd, datafusion).await?;
    let session = context
        .extension::<FlightSessionExtension>()
        .is_none()
        .then_some(session);
    Ok((plan, cache_key, session))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // `write!` formats directly into the preallocated `String` without
        // an intermediate allocation per byte.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn map_substrait_error(e: DataFusionError) -> Status {
    match e {
        DataFusionError::Substrait(msg) => Status::invalid_argument(msg),
        DataFusionError::NotImplemented(msg) => {
            Status::unimplemented(format!("Substrait feature not supported: {msg}"))
        }
        other => handle_datafusion_error(&other),
    }
}

/// Get a `FlightInfo` for executing a Substrait plan.
pub(crate) async fn get_flight_info(
    cmd: sql::CommandStatementSubstraitPlan,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info_substrait_plan");
    let _start = track_flight_request("get_flight_info", Some("statement_substrait_plan")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let (plan, cache_key, session) = decode_request(&cmd, &context, &datafusion).await?;

    let read_only = is_auth_read_only(&context);
    if read_only {
        validate_sql_query_read_only(&plan)
            .map_err(|e| Status::permission_denied(format!("Write access denied. {e}")))?;
    }

    let query =
        QueryBuilder::from_substrait_plan(plan, cache_key, session, Arc::clone(&datafusion))
            .build();
    let (dataset_schema, _) = query
        .get_schema()
        .await
        .map_err(|e| handle_datafusion_error(&e))?;
    let dataset_schema = arrow_tools::schema::expand_views_schema(&dataset_schema);

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: cmd.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .try_with_schema(&dataset_schema)
        .map_err(to_tonic_err)?
        .with_descriptor(fd);

    Ok(Response::new(info))
}

/// Execute a Substrait plan and stream the results.
pub(crate) async fn do_get(
    cmd: sql::CommandStatementSubstraitPlan,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = track_flight_request("do_get", Some("statement_substrait_plan")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let (plan, cache_key, session) = decode_request(&cmd, &context, &datafusion).await?;
    stream_plan(plan, cache_key, session, &context, &datafusion, start).await
}

/// Execute a parameterized Substrait plan
/// ([`flight_client::parameterized_ticket`]) and stream the results.
pub(crate) async fn do_get_parameterized(
    payload: Bytes,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start =
        track_flight_request("do_get", Some("statement_substrait_plan_parameterized")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let (plan, cache_key, session, _) =
        decode_parameterized_request(&payload, &context, &datafusion).await?;
    stream_plan(plan, cache_key, session, &context, &datafusion, start).await
}

async fn stream_plan(
    plan: LogicalPlan,
    cache_key: String,
    session: Option<SessionState>,
    context: &Arc<RequestContext>,
    datafusion: &Arc<DataFusion>,
    start: TimeMeasurement,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let query_result = run_plan(plan, cache_key, session, context, datafusion).await?;

    let ipc_write_options = Service::ipc_write_options_for_context(context)?;
    let (output, from_cache) = Service::query_result_to_flight_stream(
        query_result,
        ipc_write_options,
        datafusion.cpu_runtime().cloned(),
        &datafusion.ctx.runtime_env().memory_pool,
        Arc::clone(context),
    );
    let timed_output = TimedStream::new(output, move || start);

    let mut response =
        Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    attach_cache_metadata(&mut response, from_cache, context);
    Ok(response)
}

async fn run_plan(
    plan: LogicalPlan,
    cache_key: String,
    session: Option<SessionState>,
    context: &RequestContext,
    datafusion: &Arc<DataFusion>,
) -> Result<cache::result::query::QueryResult, Status> {
    if is_auth_read_only(context) {
        validate_sql_query_read_only(&plan)
            .map_err(|e| Status::permission_denied(format!("Write access denied. {e}")))?;
    }

    QueryBuilder::from_substrait_plan(plan, cache_key, session, Arc::clone(datafusion))
        .build()
        .run()
        .await
        .map_err(handle_query_error)
}

/// A parameterized plan split into its template and the values bound to it.
struct ParameterizedPlan {
    /// The plan with its `parameter_bindings` cleared.
    template: Plan,
    /// Names the template: the same template has the same digest whatever
    /// values a request binds to it.
    template_digest: String,
    /// Sorted by anchor; no anchor appears twice.
    bindings: Vec<DynamicParameterBinding>,
    /// The results-cache key: the template digest and the bound values, so two
    /// requests share a cached result only when both match.
    results_key: String,
}

/// Where a parameterized request's template plan came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateSource {
    /// Found in the plans cache, decoded by an earlier request.
    Cached,
    Decoded,
}

fn decode_parameterized_proto(payload: &[u8]) -> Result<ParameterizedPlan, Status> {
    if payload.is_empty() {
        return Err(Status::invalid_argument(
            "The parameterized Substrait plan must not be empty",
        ));
    }
    let mut template = Plan::decode(payload)
        .map_err(|e| Status::invalid_argument(format!("Failed to decode Substrait plan: {e}")))?;
    let mut bindings = std::mem::take(&mut template.parameter_bindings);
    bindings.sort_by_key(|binding| binding.parameter_anchor);
    if let Some(pair) = bindings
        .windows(2)
        .find(|pair| pair[0].parameter_anchor == pair[1].parameter_anchor)
    {
        return Err(Status::invalid_argument(format!(
            "The Substrait plan binds parameter ${} more than once",
            u64::from(pair[0].parameter_anchor) + 1
        )));
    }

    // Re-encoded rather than hashed as received: the payload holds the values,
    // and the template's digest must not change with them.
    let template_digest = sha256_hex(&template.encode_to_vec());
    // Each binding's own encoding names its anchor, type and value; distinct
    // values therefore never share a key. An equal value written two ways only
    // costs a cache miss.
    let mut values = Vec::new();
    for binding in &bindings {
        binding.encode_length_delimited(&mut values).map_err(|e| {
            Status::internal(format!(
                "Failed to encode a Substrait parameter binding: {e}"
            ))
        })?;
    }
    let results_key = format!("substrait-params:{template_digest}:{}", sha256_hex(&values));

    Ok(ParameterizedPlan {
        template,
        template_digest,
        bindings,
        results_key,
    })
}

/// Decodes a parameterized plan for a request and binds its values. The
/// template is lowered once per digest and kept in the plans cache, under a key
/// SQL text cannot produce; the values are bound per request.
///
/// As for [`decode_request`], a sessionless request also returns the session
/// snapshot the values were read against, for execution to reuse.
async fn decode_parameterized_request(
    payload: &[u8],
    context: &RequestContext,
    datafusion: &Arc<DataFusion>,
) -> Result<(LogicalPlan, String, Option<SessionState>, TemplateSource), Status> {
    let parameterized = decode_parameterized_proto(payload)?;
    let session = datafusion.ctx.state();

    // `None` for a stateful Flight session, whose plans stay off the shared
    // cache (see `Query::shared_plans_cache_key`).
    let plan_key = Query::shared_plans_cache_key(
        datafusion,
        &format!("substrait-template:{}", parameterized.template_digest),
        context,
    );
    let cached = if let Some(key) = plan_key.as_ref()
        && let Some(plans) = datafusion.plans_cache_provider()
    {
        plans.get_raw_key(&key.as_u64()).await
    } else {
        None
    };
    let (template, source) = if let Some(plan) = cached {
        (Arc::unwrap_or_clone(plan), TemplateSource::Cached)
    } else {
        let plan = lower_plan(&parameterized.template, &session).await?;
        if let Some(key) = plan_key.as_ref()
            && !matches!(plan, LogicalPlan::Ddl(_))
        {
            datafusion.cache_plan_planned_in(&session, key, &plan).await;
        }
        (plan, TemplateSource::Decoded)
    };

    let values = binding_values(&parameterized, &session).await?;
    let plan = bind_values(template, values)?;
    let session = context
        .extension::<FlightSessionExtension>()
        .is_none()
        .then_some(session);
    Ok((plan, parameterized.results_key, session, source))
}

/// Reads each binding's literal the way the plan's own literals are read.
async fn binding_values(
    parameterized: &ParameterizedPlan,
    session: &SessionState,
) -> Result<Vec<(u32, ScalarValue)>, Status> {
    let extensions = plan_extensions(&parameterized.template)?;
    let consumer = SpiceSubstraitConsumer::new(&extensions, session);
    let mut values = Vec::with_capacity(parameterized.bindings.len());
    for binding in &parameterized.bindings {
        let id = u64::from(binding.parameter_anchor) + 1;
        let literal = binding.value.as_ref().ok_or_else(|| {
            Status::invalid_argument(format!("The binding for parameter ${id} has no value"))
        })?;
        match consumer
            .consume_literal(literal)
            .await
            .map_err(map_substrait_error)?
        {
            Expr::Literal(value, _) => values.push((binding.parameter_anchor, value)),
            other => {
                return Err(Status::invalid_argument(format!(
                    "The binding for parameter ${id} is not a literal: {other}"
                )));
            }
        }
    }
    Ok(values)
}

/// Binds `values` to `template`'s placeholders, refusing a set that does not
/// match them exactly: `LogicalPlan::with_param_values` ignores a value no
/// placeholder names, and a value of another type than its placeholder
/// declares would be coerced rather than compared as sent.
fn bind_values(
    template: LogicalPlan,
    values: Vec<(u32, ScalarValue)>,
) -> Result<LogicalPlan, Status> {
    let fields = template
        .get_parameter_fields()
        .map_err(map_substrait_error)?;
    let mut bound = HashMap::with_capacity(values.len());
    for (anchor, value) in values {
        let name = (u64::from(anchor) + 1).to_string();
        let Some(field) = fields.get(&format!("${name}")) else {
            return Err(Status::invalid_argument(format!(
                "The Substrait plan binds parameter ${name}, but has no such parameter"
            )));
        };
        if let Some(field) = field
            && field.data_type() != &value.data_type()
        {
            return Err(Status::invalid_argument(format!(
                "The Substrait plan declares parameter ${name} as {}, but binds a {} value to it",
                field.data_type(),
                value.data_type()
            )));
        }
        bound.insert(name, value);
    }
    let mut unbound: Vec<_> = fields
        .keys()
        .filter(|id| !bound.contains_key(id.trim_start_matches('$')))
        .cloned()
        .collect();
    if !unbound.is_empty() {
        unbound.sort();
        return Err(Status::invalid_argument(format!(
            "The Substrait plan has no binding for parameter {}",
            unbound.join(", ")
        )));
    }
    if bound.is_empty() {
        return Ok(template);
    }
    template
        .with_param_values(ParamValues::from(bound))
        .map_err(map_substrait_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::SubstraitPlan;
    use bytes::Bytes;
    use datafusion_substrait::substrait::proto::Version;
    use prost::Message;
    use tonic::Code;

    fn cmd(plan: Option<SubstraitPlan>) -> sql::CommandStatementSubstraitPlan {
        sql::CommandStatementSubstraitPlan {
            plan,
            transaction_id: None,
        }
    }

    #[tokio::test]
    async fn decode_plan_executes_a_varchar_literal() -> Result<(), anyhow::Error> {
        use datafusion_substrait::{
            logical_plan::producer::to_substrait_plan,
            substrait::proto::{
                expression::{
                    RexType,
                    literal::{LiteralType, VarChar},
                },
                plan_rel::RelType as PlanRelType,
                rel::RelType,
            },
        };

        let df = Arc::new(
            DataFusion::builder(
                runtime_status::RuntimeStatus::new(),
                Arc::new(crate::dataaccelerator::AcceleratorEngineRegistry::default()),
                tokio::runtime::Handle::current(),
            )
            .build(),
        );
        let plan = df
            .ctx
            .sql("SELECT 'EUROPE' AS region")
            .await?
            .into_optimized_plan()?;
        let mut proto = to_substrait_plan(&plan, &df.ctx.state())?;
        let Some(PlanRelType::Root(root)) = proto.relations[0].rel_type.as_mut() else {
            panic!("expected a root relation");
        };
        let Some(RelType::Project(project)) =
            root.input.as_mut().and_then(|rel| rel.rel_type.as_mut())
        else {
            panic!("expected a projection");
        };
        let Some(RexType::Literal(literal)) = project.expressions[0].rex_type.as_mut() else {
            panic!("expected a string literal");
        };
        literal.literal_type = Some(LiteralType::VarChar(VarChar {
            value: "EUROPE".to_string(),
            length: 25,
        }));
        let (decoded, _, _) = decode_plan(
            &cmd(Some(SubstraitPlan {
                plan: Bytes::from(proto.encode_to_vec()),
                version: "0.62.0".to_string(),
            })),
            &df,
        )
        .await?;
        let batches = df
            .ctx
            .execute_logical_plan(decoded)
            .await?
            .collect()
            .await?;
        let expected = arrow::array::RecordBatch::try_from_iter([(
            "region",
            Arc::new(arrow::array::StringArray::from(vec!["EUROPE"])) as arrow::array::ArrayRef,
        )])?;
        assert_eq!(batches, vec![expected]);
        Ok(())
    }

    /// Guards the spiceai/datafusion patch that reads a Substrait string field
    /// over a string column of any width (`docs/dev/fork_patches.md`). Spice
    /// serves a file dataset's strings as `LargeUtf8`, and a producer other than
    /// `DataFusion` declares them `string` or `varchar` with no width; without the
    /// patch, `decode_plan` refuses every such plan, because the declared `Utf8`
    /// field differs from the table's `LargeUtf8` column.
    #[tokio::test]
    async fn decode_plan_reads_a_large_string_column_a_plan_declares_as_string()
    -> Result<(), anyhow::Error> {
        use arrow::array::{ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use datafusion_substrait::{
            logical_plan::producer::to_substrait_plan,
            substrait::proto::{
                ReadRel, Rel,
                plan_rel::RelType as PlanRelType,
                rel::RelType,
                r#type::{Kind, VarChar},
            },
        };

        fn region(names: ArrayRef) -> Result<Arc<MemTable>, anyhow::Error> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("r_regionkey", DataType::Int32, false),
                Field::new("r_name", names.data_type().clone(), false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![0, 1, 3])), names],
            )?;
            Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
        }

        fn read_rel(rel: &mut Rel) -> Option<&mut ReadRel> {
            match rel.rel_type.as_mut()? {
                RelType::Read(read) => Some(read),
                RelType::Project(project) => read_rel(project.input.as_mut()?),
                RelType::Filter(filter) => read_rel(filter.input.as_mut()?),
                _ => None,
            }
        }

        // The producer's catalog declares `r_name` as `Utf8`, which DataFusion
        // writes as a Substrait `string` with no width.
        let producer = SessionContext::new();
        producer.register_table(
            "region",
            region(Arc::new(StringArray::from(vec![
                "AFRICA", "AMERICA", "EUROPE",
            ])))?,
        )?;
        let plan = producer
            .sql("SELECT r_name FROM region WHERE r_regionkey = 3")
            .await?
            .into_optimized_plan()?;
        let as_string = *to_substrait_plan(&plan, &producer.state())?;

        // The same plan as Isthmus writes it: `r_name` declared `varchar(25)`.
        let mut as_varchar = as_string.clone();
        let Some(PlanRelType::Root(root)) = as_varchar.relations[0].rel_type.as_mut() else {
            panic!("expected a root relation");
        };
        let read = root
            .input
            .as_mut()
            .and_then(read_rel)
            .expect("the plan reads region");
        let schema = read.base_schema.as_mut().expect("a read has a base schema");
        let r_name = schema
            .names
            .iter()
            .position(|name| name == "r_name")
            .expect("the base schema names r_name");
        let r_name_type = &mut schema
            .r#struct
            .as_mut()
            .expect("the base schema has a struct")
            .types[r_name];
        let Some(Kind::String(string)) = r_name_type.kind.as_ref() else {
            panic!("expected r_name as a Substrait string, got {r_name_type:?}");
        };
        r_name_type.kind = Some(Kind::Varchar(VarChar {
            length: 25,
            type_variation_reference: 0,
            nullability: string.nullability,
        }));

        // Spice serves the same table with `LargeUtf8` strings.
        let df = Arc::new(
            DataFusion::builder(
                crate::status::RuntimeStatus::new(),
                Arc::new(crate::dataaccelerator::AcceleratorEngineRegistry::default()),
                tokio::runtime::Handle::current(),
            )
            .build(),
        );
        df.ctx.register_table(
            "region",
            region(Arc::new(LargeStringArray::from(vec![
                "AFRICA", "AMERICA", "EUROPE",
            ])))?,
        )?;

        let expected = RecordBatch::try_from_iter([(
            "r_name",
            Arc::new(LargeStringArray::from(vec!["EUROPE"])) as ArrayRef,
        )])?;
        for (declared, proto) in [("string", as_string), ("varchar", as_varchar)] {
            let (decoded, _, _) = decode_plan(
                &cmd(Some(SubstraitPlan {
                    plan: Bytes::from(proto.encode_to_vec()),
                    version: "0.62.0".to_string(),
                })),
                &df,
            )
            .await
            .map_err(|status| anyhow::anyhow!("r_name declared {declared}: {status}"))?;
            let batches = df
                .ctx
                .execute_logical_plan(decoded)
                .await?
                .collect()
                .await?;
            assert_eq!(
                batches,
                vec![expected.clone()],
                "r_name declared {declared}"
            );
        }
        Ok(())
    }

    /// The session registers `datafusion-spark`'s `like` and `ilike`, and
    /// `datafusion-substrait` resolves a Substrait `like` to a same-named UDF
    /// first. Unless `decode_plan` lowers it to `Expr::Like`, the unparser sends
    /// a `DuckDB` acceleration `like(a, b)`, which `DuckDB` has no function for
    /// (regression test for #15290). A pattern with a backslash must also carry
    /// `ESCAPE '\'`: `DuckDB` has no default escape character, so without it the
    /// pushed-down `LIKE` matches different rows than `DataFusion` evaluates.
    #[tokio::test]
    async fn decode_plan_lowers_a_substrait_like_to_a_like_expression() -> Result<(), anyhow::Error>
    {
        use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::sql::unparser::{Unparser, dialect::DuckDBDialect};
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;

        let df = Arc::new(
            DataFusion::builder(
                crate::status::RuntimeStatus::new(),
                Arc::new(crate::dataaccelerator::AcceleratorEngineRegistry::default()),
                tokio::runtime::Handle::current(),
            )
            .build(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "LARGE BRASS",
                    "SMALL TIN",
                    "PROMO brass",
                    "a_b",
                    r"a\_b",
                    "50%",
                    "50x",
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![15, 15, 3, 4, 5, 6, 7])),
            ],
        )?;
        df.ctx.register_table(
            "part",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
        )?;

        // (query, expected rows, whether the `DuckDB` SQL must name the escape)
        let queries = [
            (
                "SELECT p_type FROM part WHERE p_size = 15 AND p_type LIKE '%BRASS' ORDER BY p_type",
                "+-------------+\n| p_type      |\n+-------------+\n| LARGE BRASS |\n+-------------+",
                false,
            ),
            (
                "SELECT sum(CASE WHEN p_type LIKE 'PROMO%' THEN p_size ELSE 0 END) AS promo FROM part",
                "+-------+\n| promo |\n+-------+\n| 3     |\n+-------+",
                false,
            ),
            (
                "SELECT p_type FROM part WHERE p_type ILIKE '%brass' AND p_type NOT LIKE 'LARGE%' ORDER BY p_type",
                "+-------------+\n| p_type      |\n+-------------+\n| PROMO brass |\n+-------------+",
                false,
            ),
            (
                "SELECT p_type FROM part WHERE p_size IN (SELECT p_size FROM part WHERE p_type LIKE 'L%') ORDER BY p_type",
                "+-------------+\n| p_type      |\n+-------------+\n| LARGE BRASS |\n| SMALL TIN   |\n+-------------+",
                false,
            ),
            (
                r"SELECT p_type FROM part WHERE p_type LIKE '%a\_b%' ORDER BY p_type",
                "+--------+\n| p_type |\n+--------+\n| a_b    |\n+--------+",
                true,
            ),
            (
                r"SELECT p_type FROM part WHERE p_type LIKE '50\%' ORDER BY p_type",
                "+--------+\n| p_type |\n+--------+\n| 50%    |\n+--------+",
                true,
            ),
            // The bytes of `50\%`: a cast pattern is not known to be backslash-free.
            (
                "SELECT p_type FROM part WHERE p_type LIKE CAST(X'35305C25' AS VARCHAR) ORDER BY p_type",
                "+--------+\n| p_type |\n+--------+\n| 50%    |\n+--------+",
                true,
            ),
        ];
        for (sql, expected, names_escape) in queries {
            let plan = df.ctx.sql(sql).await?.into_unoptimized_plan();
            let proto = to_substrait_plan(&plan, &df.ctx.state())?;
            let (decoded, _, _) = decode_plan(
                &cmd(Some(SubstraitPlan {
                    plan: Bytes::from(proto.encode_to_vec()),
                    version: "0.63.0".to_string(),
                })),
                &df,
            )
            .await
            .map_err(|status| anyhow::anyhow!("{sql}: {status}"))?;

            let duckdb_sql = Unparser::new(&DuckDBDialect::new())
                .plan_to_sql(&decoded)?
                .to_string();
            assert!(
                !duckdb_sql.contains("like(") && duckdb_sql.contains("LIKE"),
                "{sql} unparsed for DuckDB as: {duckdb_sql}"
            );
            assert_eq!(
                duckdb_sql.contains(r"ESCAPE '\'"),
                names_escape,
                "{sql} unparsed for DuckDB as: {duckdb_sql}"
            );

            let batches = df
                .ctx
                .execute_logical_plan(decoded)
                .await?
                .collect()
                .await?;
            assert_eq!(
                arrow::util::pretty::pretty_format_batches(&batches)?.to_string(),
                expected,
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn decode_plan_proto_missing_plan_returns_invalid_argument() {
        let err = decode_plan_proto(&cmd(None)).expect_err("missing plan must error");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("plan is required"), "{err}");
    }

    #[test]
    fn decode_plan_proto_empty_bytes_returns_invalid_argument() {
        let err = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: Bytes::new(),
            version: String::new(),
        })))
        .expect_err("empty plan must error");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("must not be empty"), "{err}");
    }

    #[test]
    fn decode_plan_proto_invalid_bytes_returns_invalid_argument() {
        // 0x0a = field 1, wire-type 2 (length-delimited); 0x10 = "length 16
        // bytes follow" — but no further bytes are supplied, forcing prost
        // into a buffer-underflow decode error.
        let err = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: Bytes::from_static(&[0x0a, 0x10]),
            version: String::new(),
        })))
        .expect_err("invalid bytes must error");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("Failed to decode Substrait plan"),
            "{err}"
        );
    }

    #[test]
    fn decode_plan_proto_returns_namespaced_cache_key() {
        let plan = Plan {
            version: Some(Version {
                minor_number: 62,
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = Bytes::from(plan.encode_to_vec());
        let (_proto, key) = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: bytes.clone(),
            version: String::new(),
        })))
        .expect("valid empty Plan should decode");
        assert!(key.starts_with("substrait:"));
        // Same bytes yield the same key.
        let (_proto2, key2) = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: bytes,
            version: String::new(),
        })))
        .expect("decode");
        assert_eq!(key, key2);
    }

    /// A runtime with the results cache keyed by plan, as a `plan` cache key
    /// configures it, and a plans cache.
    fn cached_runtime() -> Result<Arc<DataFusion>, anyhow::Error> {
        use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use cache::{Caching, QueryResultsCacheProvider, SimpleCache};
        use datafusion::datasource::MemTable;
        use spicepod::component::caching::{CacheKeyType, SQLResultsCacheConfig};

        let results = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                cache_key_type: CacheKeyType::Plan,
                ..Default::default()
            },
            Box::new([]),
        )?;
        let plans = Arc::new(SimpleCache::new(
            512,
            std::time::Duration::from_hours(1),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ));
        let df = Arc::new(
            DataFusion::builder(
                crate::status::RuntimeStatus::new(),
                Arc::new(crate::dataaccelerator::AcceleratorEngineRegistry::default()),
                tokio::runtime::Handle::current(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(results))
                    .with_plans_cache(plans),
            ))
            .build(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )?;
        df.ctx
            .register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
        Ok(df)
    }

    /// The Substrait template of `sql`, planned unbound, so each `$N` is a
    /// dynamic parameter typed by inference.
    async fn template_of(df: &DataFusion, sql: &str) -> Result<Plan, anyhow::Error> {
        use datafusion_substrait::logical_plan::producer::to_substrait_plan;
        let plan = df.ctx.sql(sql).await?.into_unoptimized_plan();
        Ok(*to_substrait_plan(&plan, &df.ctx.state())?)
    }

    fn payload(
        df: &DataFusion,
        template: &Plan,
        values: &[datafusion::common::ScalarValue],
    ) -> Result<Vec<u8>, anyhow::Error> {
        let mut plan = template.clone();
        data_components::flight::substrait_parameters::bind_parameters(
            &mut plan,
            values,
            &df.ctx.state(),
        )?;
        Ok(plan.encode_to_vec())
    }

    fn plan_keyed_context() -> Arc<RequestContext> {
        use runtime_request_context::{CacheControl, CacheKeyType, Protocol};
        Arc::new(
            RequestContext::builder(Protocol::FlightSQL)
                .with_cache_control(CacheControl::Cache(CacheKeyType::Default))
                .build(),
        )
    }

    /// Runs `payload` as `do_get_parameterized` runs it, and returns its rows,
    /// how the results cache answered, and where its template came from.
    async fn run_parameterized(
        df: &Arc<DataFusion>,
        payload: &[u8],
    ) -> Result<(String, cache::result::CacheStatus, TemplateSource), anyhow::Error> {
        let context = plan_keyed_context();
        Arc::clone(&context)
            .scope(async {
                let (plan, key, session, source) =
                    decode_parameterized_request(payload, &context, df).await?;
                let result = run_plan(plan, key, session, &context, df).await?;
                let status = result.cache_status;
                let batches = result.collect_batches().await?;
                Ok::<_, anyhow::Error>((
                    arrow::util::pretty::pretty_format_batches(&batches)?.to_string(),
                    status,
                    source,
                ))
            })
            .await
    }

    /// One template sent with different values returns each value's rows, with
    /// the results cache on: were the values left out of its key, the second
    /// value would be served the first value's cached rows. The template is
    /// lowered once and then served from the plans cache.
    #[tokio::test]
    async fn parameterized_plan_binds_each_requests_values() -> Result<(), anyhow::Error> {
        use cache::result::CacheStatus;
        use datafusion::common::ScalarValue;

        let df = cached_runtime()?;
        let template = template_of(&df, "SELECT v FROM t WHERE k = $1 LIMIT 1").await?;
        let one = "+-----+\n| v   |\n+-----+\n| one |\n+-----+";
        let two = "+-----+\n| v   |\n+-----+\n| two |\n+-----+";

        let a = payload(&df, &template, &[ScalarValue::Int64(Some(1))])?;
        let b = payload(&df, &template, &[ScalarValue::Int64(Some(2))])?;
        assert_eq!(
            run_parameterized(&df, &a).await?,
            (
                one.to_string(),
                CacheStatus::CacheMiss,
                TemplateSource::Decoded
            )
        );
        assert_eq!(
            run_parameterized(&df, &b).await?,
            (
                two.to_string(),
                CacheStatus::CacheMiss,
                TemplateSource::Cached
            )
        );
        // The results cache is in use: the first value's rows are now a hit.
        assert_eq!(
            run_parameterized(&df, &a).await?,
            (
                one.to_string(),
                CacheStatus::CacheHit,
                TemplateSource::Cached
            )
        );

        // A different template is decoded, not served the first one's plan.
        let other = template_of(&df, "SELECT v FROM t WHERE k > $1 ORDER BY k").await?;
        let c = payload(&df, &other, &[ScalarValue::Int64(Some(2))])?;
        assert_eq!(
            run_parameterized(&df, &c).await?,
            (
                "+-------+\n| v     |\n+-------+\n| three |\n+-------+".to_string(),
                CacheStatus::CacheMiss,
                TemplateSource::Decoded
            )
        );
        Ok(())
    }

    /// The bindings must cover the plan's parameters exactly, with the types it
    /// declares; anything else is refused rather than run.
    #[tokio::test]
    async fn parameterized_plan_refuses_bindings_that_do_not_match_its_parameters()
    -> Result<(), anyhow::Error> {
        use datafusion::common::ScalarValue;

        let df = cached_runtime()?;
        let two_params = template_of(&df, "SELECT v FROM t WHERE k = $1 OR k = $2").await?;
        let one_param = template_of(&df, "SELECT v FROM t WHERE k = $1").await?;
        let mut duplicated = one_param.clone();
        data_components::flight::substrait_parameters::bind_parameters(
            &mut duplicated,
            &[ScalarValue::Int64(Some(1))],
            &df.ctx.state(),
        )?;
        duplicated
            .parameter_bindings
            .push(duplicated.parameter_bindings[0].clone());

        let cases = [
            (
                payload(&df, &two_params, &[ScalarValue::Int64(Some(1))])?,
                "The Substrait plan has no binding for parameter $2",
            ),
            (
                payload(
                    &df,
                    &one_param,
                    &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
                )?,
                "The Substrait plan binds parameter $2, but has no such parameter",
            ),
            (
                payload(&df, &one_param, &[ScalarValue::Utf8(Some("1".to_string()))])?,
                "The Substrait plan declares parameter $1 as Int64, but binds a Utf8 value to it",
            ),
            (
                duplicated.encode_to_vec(),
                "The Substrait plan binds parameter $1 more than once",
            ),
        ];
        for (payload, expected) in cases {
            let context = plan_keyed_context();
            let status = decode_parameterized_request(&payload, &context, &df)
                .await
                .map(|_| ())
                .expect_err(expected);
            assert_eq!(status.code(), Code::InvalidArgument, "{expected}");
            assert_eq!(status.message(), expected);
        }
        Ok(())
    }

    /// An upstream that predates the parameterized ticket finds no Flight SQL
    /// command in it and answers `Unimplemented` from `Command::Unknown`, which
    /// is what a client falls back on. `do_get` must recognise the ticket before
    /// that dispatch for the same reason.
    #[test]
    fn parameterized_ticket_is_not_a_flight_sql_command() {
        use arrow_flight::sql::{Any, Command};
        use flight_client::parameterized_ticket::PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL;
        let any = Any {
            type_url: PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL.to_string(),
            value: Bytes::from_static(b"plan"),
        };
        assert!(matches!(
            Command::try_from(any),
            Ok(Command::Unknown(any)) if any.type_url == PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL
        ));
    }

    #[test]
    fn sha256_hex_is_lowercase_64_chars() {
        let out = sha256_hex(b"");
        assert_eq!(out.len(), 64);
        assert!(
            out.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }
}
