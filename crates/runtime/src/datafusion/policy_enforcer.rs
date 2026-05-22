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

//! Cedar policy enforcement for SQL queries.
//!
//! Walks the logical plan to identify the tables and operations being performed,
//! then checks each (principal, action, resource) triple against the policy engine.

use std::{
    any::Any,
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;

use datafusion::{
    catalog::{
        Session, TableProvider,
        default_table_source::{provider_as_source, source_as_provider},
    },
    common::{
        Column, Constraints, DFSchema, Statistics,
        tree_node::{Transformed, TransformedResult, TreeNodeRecursion},
    },
    datasource::TableType,
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{
        Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown,
        TableScan, TableSource, dml::InsertOp,
    },
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
use runtime_policy::{
    AccessPlan, AuthzDecision, ColumnMask, PolicyEngine, TagMask, entities::SpiceResource,
    request::SpiceAction,
};
use runtime_request_context::{AsyncMarker, RequestContext};
use tracing::{Instrument, Span};

const POLICY_AUDIT_TASK: &str = "policy_audit";

#[derive(Clone)]
pub(crate) struct PolicyTableProvider {
    table_name: TableReference,
    inner: Arc<dyn TableProvider>,
    policy_engine: Arc<PolicyEngine>,
}

impl PolicyTableProvider {
    #[must_use]
    pub(crate) fn new(
        table_name: TableReference,
        inner: Arc<dyn TableProvider>,
        policy_engine: Arc<PolicyEngine>,
    ) -> Self {
        Self {
            table_name,
            inner,
            policy_engine,
        }
    }

    #[must_use]
    pub(crate) fn inner(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.inner)
    }
}

impl std::fmt::Debug for PolicyTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyTableProvider")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for PolicyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        let Some(principal) = request_context.auth_principal() else {
            trace_policy_skip(
                "provider_scan",
                SpiceAction::READ,
                &self.table_name.to_string(),
                "no_principal",
            );
            return self.inner.scan(state, projection, filters, limit).await;
        };

        let Some(session) = state.as_any().downcast_ref::<SessionState>() else {
            return Err(DataFusionError::Plan(
                "Failed to enforce fine-grained policy: DataFusion session state is unavailable"
                    .to_string(),
            ));
        };

        let authorization_span = policy_audit_span(
            "provider_scan",
            SpiceAction::QUERY,
            &self.table_name.to_string(),
            self.table_name.catalog(),
            self.table_name.schema(),
            principal,
        );
        let query_decision = evaluate_query_authorization(
            &self.policy_engine,
            principal,
            &self.table_name,
            &authorization_span,
        )
        .await;
        if is_explicit_deny(&query_decision) {
            return Err(authorization_denied_error(
                SpiceAction::QUERY,
                &self.table_name,
                &query_decision,
                &authorization_span,
            ));
        }

        let read_span = policy_audit_span(
            "provider_scan",
            SpiceAction::READ,
            &self.table_name.to_string(),
            self.table_name.catalog(),
            self.table_name.schema(),
            principal,
        );
        let access_plan =
            evaluate_access_plan(&self.policy_engine, principal, &self.table_name, &read_span)
                .await?;

        if let Some(access_plan) = access_plan {
            if access_plan.is_noop() {
                trace_read_access_plan(&read_span, "allow", &access_plan);
                return self.inner.scan(state, projection, filters, limit).await;
            }

            let parsed = match parse_access_plan_for_schema(
                &self.table_name,
                &self.inner.schema(),
                &access_plan,
                session,
            ) {
                Ok(parsed) => parsed,
                Err(err) => {
                    tracing::error!(target: "task_history", parent: &read_span, "{err}");
                    return Err(err);
                }
            };
            trace_read_access_plan(&read_span, "allow", &access_plan);

            let secured = build_secured_scan_plan(
                &self.table_name,
                &self.inner,
                projection,
                filters.to_vec(),
                limit,
                &parsed,
            )?;
            return state.create_physical_plan(&secured).await;
        }

        if !query_decision.is_allowed() {
            return Err(authorization_denied_error(
                SpiceAction::QUERY,
                &self.table_name,
                &query_decision,
                &authorization_span,
            ));
        }

        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(
        &self,
        state: &dyn Session,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}

#[must_use]
pub(crate) fn wrap_policy_table_provider(
    table_name: TableReference,
    inner: Arc<dyn TableProvider>,
    policy_engine: Option<&Arc<PolicyEngine>>,
) -> Arc<dyn TableProvider> {
    match policy_engine {
        Some(policy_engine) if !is_policy_table_provider(&inner) => Arc::new(
            PolicyTableProvider::new(table_name, inner, Arc::clone(policy_engine)),
        ),
        _ => inner,
    }
}

#[must_use]
pub(crate) fn unwrap_policy_table_provider(
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    if let Some(policy_provider) = provider.as_any().downcast_ref::<PolicyTableProvider>() {
        policy_provider.inner()
    } else {
        provider
    }
}

#[must_use]
fn is_policy_table_provider(provider: &Arc<dyn TableProvider>) -> bool {
    provider.as_any().is::<PolicyTableProvider>()
}

fn policy_table_source_provider(
    source: &Arc<dyn TableSource>,
) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
    source_as_provider(source)
        .map(|provider| is_policy_table_provider(&provider).then_some(provider))
}

/// Enforce Cedar policies on a SQL query plan.
///
/// This first performs the existing coarse dataset authorization checks, then
/// rewrites table scans with SQL row filters and column masks from `read` policy
/// annotations. The rewrite happens before execution and before any downstream
/// node can observe unauthorized rows or field values.
pub async fn enforce_query_plan(
    plan: LogicalPlan,
    policy_engine: &Arc<PolicyEngine>,
    session: &SessionState,
) -> Result<LogicalPlan, DataFusionError> {
    authorize_query_plan(&plan, policy_engine).await?;
    apply_read_access_plans(plan, policy_engine, session).await
}

/// Authorize a SQL query's logical plan against the Cedar policy engine.
///
/// Walks the plan to find all referenced tables and operations, then checks
/// each against the policy engine using the current request's principal.
///
/// Returns `Ok(())` if all operations are authorized, or an error describing
/// which resource/action was denied.
pub async fn authorize_query_plan(
    plan: &LogicalPlan,
    policy_engine: &Arc<PolicyEngine>,
) -> Result<(), DataFusionError> {
    let request_context = RequestContext::current(AsyncMarker::new().await);
    let Some(principal) = request_context.auth_principal() else {
        // No principal = unauthenticated request. The auth layer already
        // decided whether to allow or deny unauthenticated requests.
        // If we got here, the auth layer allowed it (e.g. no auth configured).
        trace_policy_skip("authorization", SpiceAction::QUERY, "", "no_principal");
        return Ok(());
    };

    // Collect all (action, table) pairs from the plan.
    let mut checks: Vec<(&str, String, Option<String>, Option<String>)> = Vec::new();

    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::Dml(dml) => {
                let action = if let datafusion::logical_expr::WriteOp::Insert(_) = &dml.op {
                    SpiceAction::INSERT
                } else if matches!(&dml.op, datafusion::logical_expr::WriteOp::Delete) {
                    SpiceAction::DELETE
                } else {
                    SpiceAction::UPDATE
                };
                checks.push((
                    action,
                    dml.table_name.table().to_string(),
                    dml.table_name.catalog().map(ToString::to_string),
                    dml.table_name.schema().map(ToString::to_string),
                ));
            }
            LogicalPlan::TableScan(scan) => {
                checks.push((
                    SpiceAction::QUERY,
                    scan.table_name.table().to_string(),
                    scan.table_name.catalog().map(ToString::to_string),
                    scan.table_name.schema().map(ToString::to_string),
                ));
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    // Evaluate each check against the policy engine.
    for (action, table_name, catalog, schema) in checks {
        let audit_span = policy_audit_span(
            "authorization",
            action,
            &table_name,
            catalog.as_deref(),
            schema.as_deref(),
            principal,
        );
        let resource = SpiceResource::Dataset {
            name: table_name.clone(),
            catalog: catalog.clone(),
            schema: schema.clone(),
        };

        let decision = async {
            let mut decision = policy_engine
                .is_authorized(principal, action, &resource)
                .await;
            if action == SpiceAction::QUERY
                && matches!(&decision, AuthzDecision::Deny { reasons } if reasons.is_empty())
            {
                let read_decision = policy_engine
                    .is_authorized(principal, SpiceAction::READ, &resource)
                    .await;
                if read_decision.is_allowed() {
                    decision = read_decision;
                }
            }
            decision
        }
        .instrument(audit_span.clone())
        .await;

        if !decision.is_allowed() {
            let reasons = match &decision {
                AuthzDecision::Deny { reasons } if !reasons.is_empty() => {
                    format!(" (policies: {})", reasons.join(", "))
                }
                _ => String::new(),
            };
            trace_policy_decision(&audit_span, "deny", &authz_policy_ids(&decision));
            let err = DataFusionError::Plan(format!(
                "Authorization denied: action '{action}' on dataset '{table_name}' is not permitted for this user{reasons}",
            ));
            tracing::error!(target: "task_history", parent: &audit_span, "{err}");
            return Err(err);
        }

        trace_policy_decision(&audit_span, "allow", &authz_policy_ids(&decision));
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct ParsedAccessPlan {
    row_filters: Vec<Expr>,
    column_masks: HashMap<String, Expr>,
}

async fn evaluate_access_plan(
    policy_engine: &Arc<PolicyEngine>,
    principal: &AuthPrincipalRef,
    table_name: &TableReference,
    audit_span: &Span,
) -> Result<Option<AccessPlan>, DataFusionError> {
    let resource = resource_for_table(table_name);
    let access_plan = match policy_engine
        .evaluate_read_access(principal, &resource)
        .instrument(audit_span.clone())
        .await
    {
        Ok(access_plan) => access_plan,
        Err(err) => {
            let err = DataFusionError::Plan(format!(
                "Failed to evaluate fine-grained policy for dataset '{table_name}': {err}"
            ));
            tracing::error!(target: "task_history", parent: audit_span, "{err}");
            return Err(err);
        }
    };

    if !access_plan.allowed {
        if access_plan.policy_ids.is_empty() {
            trace_policy_skip_on_span(audit_span, "no_matching_read_policy");
            return Ok(None);
        }

        trace_read_access_plan(audit_span, "deny", &access_plan);
        let err = DataFusionError::Plan(format!(
            "Authorization denied: action '{}' on dataset '{}' is not permitted for this user (policies: {})",
            SpiceAction::READ,
            table_name,
            access_plan.policy_ids.join(", ")
        ));
        tracing::error!(target: "task_history", parent: audit_span, "{err}");
        return Err(err);
    }

    Ok(Some(access_plan))
}

async fn evaluate_query_authorization(
    policy_engine: &Arc<PolicyEngine>,
    principal: &AuthPrincipalRef,
    table_name: &TableReference,
    audit_span: &Span,
) -> AuthzDecision {
    let resource = resource_for_table(table_name);
    let decision = policy_engine
        .is_authorized(principal, SpiceAction::QUERY, &resource)
        .instrument(audit_span.clone())
        .await;
    trace_policy_decision(
        audit_span,
        if decision.is_allowed() {
            "allow"
        } else {
            "deny"
        },
        &authz_policy_ids(&decision),
    );
    decision
}

fn is_explicit_deny(decision: &AuthzDecision) -> bool {
    matches!(decision, AuthzDecision::Deny { reasons } if !reasons.is_empty())
}

fn authorization_denied_error(
    action: &str,
    table_name: &TableReference,
    decision: &AuthzDecision,
    audit_span: &Span,
) -> DataFusionError {
    let reasons = match decision {
        AuthzDecision::Deny { reasons } if !reasons.is_empty() => {
            format!(" (policies: {})", reasons.join(", "))
        }
        _ => String::new(),
    };
    let err = DataFusionError::Plan(format!(
        "Authorization denied: action '{action}' on dataset '{table_name}' is not permitted for this user{reasons}",
    ));
    tracing::error!(target: "task_history", parent: audit_span, "{err}");
    err
}

fn resource_for_table(table_name: &TableReference) -> SpiceResource {
    SpiceResource::Dataset {
        name: table_name.table().to_string(),
        catalog: table_name.catalog().map(ToString::to_string),
        schema: table_name.schema().map(ToString::to_string),
    }
}

async fn apply_read_access_plans(
    plan: LogicalPlan,
    policy_engine: &Arc<PolicyEngine>,
    session: &SessionState,
) -> Result<LogicalPlan, DataFusionError> {
    let request_context = RequestContext::current(AsyncMarker::new().await);
    let Some(principal) = request_context.auth_principal() else {
        trace_policy_skip("read_access", SpiceAction::READ, "", "no_principal");
        return Ok(plan);
    };

    let mut scans = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            scans.push(scan.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut parsed_by_table = HashMap::new();
    for scan in scans {
        let table_name = scan.table_name.to_string();
        let audit_span = policy_audit_span(
            "read_access",
            SpiceAction::READ,
            &table_name,
            scan.table_name.catalog(),
            scan.table_name.schema(),
            principal,
        );

        let Some(access_plan) =
            evaluate_access_plan(policy_engine, principal, &scan.table_name, &audit_span).await?
        else {
            continue;
        };

        if access_plan.is_noop() {
            trace_read_access_plan(&audit_span, "allow", &access_plan);
            continue;
        }

        if policy_table_source_provider(&scan.source)?.is_none() {
            let err = DataFusionError::Plan(format!(
                "Policy enforcement bypass detected for dataset '{}': table provider is not policy-wrapped",
                scan.table_name
            ));
            tracing::error!(target: "task_history", parent: &audit_span, "{err}");
            return Err(err);
        }

        let parsed = match parse_access_plan_for_schema(
            &scan.table_name,
            &scan.source.schema(),
            &access_plan,
            session,
        ) {
            Ok(parsed) => parsed,
            Err(err) => {
                tracing::error!(target: "task_history", parent: &audit_span, "{err}");
                return Err(err);
            }
        };
        trace_read_access_plan(&audit_span, "allow", &access_plan);
        parsed_by_table.insert(table_name, parsed);
    }

    if parsed_by_table.is_empty() {
        return Ok(plan);
    }

    plan.transform_up_with_subqueries(|node| {
        let LogicalPlan::TableScan(scan) = &node else {
            return Ok(Transformed::no(node));
        };
        let Some(parsed) = parsed_by_table.get(&scan.table_name.to_string()) else {
            return Ok(Transformed::no(node));
        };

        rewrite_table_scan(scan, parsed)
    })
    .data()
}

fn parse_access_plan_for_schema(
    table_name: &TableReference,
    source_schema: &SchemaRef,
    access_plan: &AccessPlan,
    session: &SessionState,
) -> Result<ParsedAccessPlan, DataFusionError> {
    let df_schema = DFSchema::try_from_qualified_schema(table_name.clone(), source_schema)?;

    let mut row_filters = Vec::with_capacity(access_plan.row_filters.len());
    for row_filter in &access_plan.row_filters {
        let expr = session.create_logical_expr(row_filter, &df_schema)?;
        let (_, field) = expr.to_field(&df_schema)?;
        if field.data_type() != &DataType::Boolean {
            return Err(DataFusionError::Plan(format!(
                "Policy row filter for dataset '{}' must return Boolean, got {}",
                table_name,
                field.data_type()
            )));
        }
        row_filters.push(expr);
    }

    let mut mask_expressions_by_column = HashMap::with_capacity(access_plan.column_masks.len());
    for mask in &access_plan.column_masks {
        if source_schema.field_with_name(&mask.column).is_err() {
            return Err(DataFusionError::Plan(format!(
                "Policy column mask for dataset '{}' references unknown column '{}'",
                table_name, mask.column
            )));
        }

        insert_mask_expression(
            table_name,
            &mut mask_expressions_by_column,
            &mask.column,
            &mask.expression,
        )?;
    }

    for tag_mask in &access_plan.tag_masks {
        expand_tag_mask(
            table_name,
            source_schema,
            &mut mask_expressions_by_column,
            tag_mask,
        )?;
    }

    let mut column_masks = HashMap::with_capacity(mask_expressions_by_column.len());
    for (column, expression) in mask_expressions_by_column {
        let field = source_schema.field_with_name(&column).map_err(|_| {
            DataFusionError::Plan(format!(
                "Policy column mask for dataset '{table_name}' references unknown column '{column}'"
            ))
        })?;

        let expr = session.create_logical_expr(&expression, &df_schema)?;
        let (_, mask_field) = expr.to_field(&df_schema)?;
        if mask_field.data_type() != field.data_type() {
            return Err(DataFusionError::Plan(format!(
                "Policy column mask for '{}.{}' must return {}, got {}",
                table_name,
                column,
                field.data_type(),
                mask_field.data_type()
            )));
        }

        column_masks.insert(column, expr);
    }

    Ok(ParsedAccessPlan {
        row_filters,
        column_masks,
    })
}

fn insert_mask_expression(
    table_name: &TableReference,
    masks: &mut HashMap<String, String>,
    column: &str,
    expression: &str,
) -> Result<(), DataFusionError> {
    if let Some(existing) = masks.get(column)
        && existing != expression
    {
        return Err(DataFusionError::Plan(format!(
            "Conflicting policy column masks for '{table_name}.{column}'"
        )));
    }

    masks.insert(column.to_string(), expression.to_string());
    Ok(())
}

fn expand_tag_mask(
    table_name: &TableReference,
    source_schema: &SchemaRef,
    masks: &mut HashMap<String, String>,
    tag_mask: &TagMask,
) -> Result<(), DataFusionError> {
    for field in source_schema.fields() {
        if field_has_tag(field, &tag_mask.tag) {
            insert_mask_expression(table_name, masks, field.name(), &tag_mask.expression)?;
        }
    }
    Ok(())
}

fn field_has_tag(field: &Field, tag: &str) -> bool {
    field
        .metadata()
        .get("tags")
        .into_iter()
        .chain(field.metadata().get("tag"))
        .flat_map(|value| metadata_tags(value))
        .any(|candidate| candidate == tag)
}

fn metadata_tags(value: &str) -> Vec<String> {
    if let Ok(tags) = serde_json::from_str::<Vec<String>>(value) {
        return tags;
    }

    if let Ok(tag) = serde_json::from_str::<String>(value) {
        return vec![tag];
    }

    value
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn rewrite_table_scan(
    scan: &TableScan,
    parsed: &ParsedAccessPlan,
) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    let Some(source) = policy_table_source_provider(&scan.source)? else {
        return Err(DataFusionError::Plan(format!(
            "Policy enforcement bypass detected for dataset '{}': table provider is not policy-wrapped",
            scan.table_name
        )));
    };
    let source = unwrap_policy_table_provider(source);
    build_secured_scan_plan(
        &scan.table_name,
        &source,
        scan.projection.as_ref(),
        scan.filters.clone(),
        scan.fetch,
        parsed,
    )
    .map(Transformed::yes)
}

fn build_secured_scan_plan(
    table_name: &TableReference,
    source: &Arc<dyn TableProvider>,
    projection: Option<&Vec<usize>>,
    mut filters: Vec<Expr>,
    fetch: Option<usize>,
    parsed: &ParsedAccessPlan,
) -> Result<LogicalPlan, DataFusionError> {
    let policy_filters = parsed.row_filters.clone();
    filters.extend(policy_filters.clone());
    let source_schema = source.schema();
    let scan_projection = projection
        .map(|projection| required_scan_projection(&source_schema, projection, &filters, parsed))
        .transpose()?;

    let mut secured = LogicalPlanBuilder::scan_with_filters_fetch(
        table_name.clone(),
        provider_as_source(Arc::clone(source)),
        scan_projection,
        filters,
        fetch,
    )?
    .build()?;

    for policy_filter in policy_filters {
        secured = LogicalPlanBuilder::from(secured)
            .filter(policy_filter)?
            .build()?;
    }

    if !parsed.column_masks.is_empty() || projection.is_some() {
        let output_projection = projection
            .cloned()
            .unwrap_or_else(|| (0..source_schema.fields().len()).collect());
        let projection_exprs = output_projection
            .iter()
            .map(|idx| {
                let field = source_schema.field(*idx);
                parsed
                    .column_masks
                    .get(field.name())
                    .map_or_else(
                        || {
                            Expr::Column(Column::new(
                                Some(table_name.clone()),
                                field.name().clone(),
                            ))
                        },
                        Clone::clone,
                    )
                    .alias(field.name().clone())
            })
            .collect::<Vec<_>>();
        secured = LogicalPlanBuilder::from(secured)
            .project(projection_exprs)?
            .alias(table_name.clone())?
            .build()?;
    }

    Ok(secured)
}

fn required_scan_projection(
    source_schema: &SchemaRef,
    output_projection: &[usize],
    filters: &[Expr],
    parsed: &ParsedAccessPlan,
) -> Result<Vec<usize>, DataFusionError> {
    let mut required = output_projection.iter().copied().collect::<BTreeSet<_>>();

    for filter in filters {
        add_expression_columns_to_projection(source_schema, filter, &mut required)?;
    }

    for idx in output_projection {
        let field = source_schema.field(*idx);
        if let Some(mask) = parsed.column_masks.get(field.name()) {
            add_expression_columns_to_projection(source_schema, mask, &mut required)?;
        }
    }

    Ok(required.into_iter().collect())
}

fn add_expression_columns_to_projection(
    source_schema: &SchemaRef,
    expr: &Expr,
    projection: &mut BTreeSet<usize>,
) -> Result<(), DataFusionError> {
    for column in expr.column_refs() {
        let Some(idx) = source_schema
            .fields()
            .iter()
            .position(|field| field.name() == &column.name)
        else {
            return Err(DataFusionError::Plan(format!(
                "Policy expression references unknown column '{}'",
                column.name
            )));
        };
        projection.insert(idx);
    }

    Ok(())
}

fn policy_audit_span(
    phase: &'static str,
    action: &str,
    dataset: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    principal: &AuthPrincipalRef,
) -> Span {
    let input = format!("{phase}:{action}:{dataset}");
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        POLICY_AUDIT_TASK,
        input = %input
    );
    tracing::info!(
        target: "task_history",
        parent: &span,
        policy_phase = %phase,
        policy_action = %action,
        dataset = %dataset,
        catalog = %catalog.unwrap_or_default(),
        schema = %schema.unwrap_or_default(),
        principal = %principal_name(principal),
        "labels"
    );
    span
}

fn trace_policy_skip(phase: &'static str, action: &str, dataset: &str, reason: &str) {
    let input = format!("{phase}:{action}:{dataset}");
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        POLICY_AUDIT_TASK,
        input = %input
    );
    tracing::info!(
        target: "task_history",
        parent: &span,
        policy_phase = %phase,
        policy_action = %action,
        dataset = %dataset,
        decision = "skip",
        reason = %reason,
        "labels"
    );
}

fn trace_policy_skip_on_span(span: &Span, reason: &str) {
    tracing::info!(
        target: "task_history",
        parent: span,
        decision = "skip",
        reason = %reason,
        "labels"
    );
}

fn trace_policy_decision(span: &Span, decision: &str, policy_ids: &str) {
    tracing::info!(
        target: "task_history",
        parent: span,
        decision = %decision,
        policy_ids = %policy_ids,
        "labels"
    );
}

fn trace_read_access_plan(span: &Span, decision: &str, access_plan: &AccessPlan) {
    tracing::info!(
        target: "task_history",
        parent: span,
        decision = %decision,
        policy_ids = %access_plan.policy_ids.join(","),
        row_filters = %sql_values_label(&access_plan.row_filters),
        column_masks = %column_masks_label(&access_plan.column_masks),
        tag_masks = %tag_masks_label(&access_plan.tag_masks),
        row_filter_count = access_plan.row_filters.len(),
        column_mask_count = access_plan.column_masks.len() + access_plan.tag_masks.len(),
        "labels"
    );
}

fn authz_policy_ids(decision: &AuthzDecision) -> String {
    match decision {
        AuthzDecision::Allow => String::new(),
        AuthzDecision::Deny { reasons } => reasons.join(","),
    }
}

fn principal_name(principal: &AuthPrincipalRef) -> String {
    principal.identity_context().map_or_else(
        || principal.username().to_string(),
        |identity| identity.user_id.clone(),
    )
}

fn sql_values_label(values: &[String]) -> String {
    values
        .iter()
        .map(|value| task_history_label_value(value))
        .collect::<Vec<_>>()
        .join(";")
}

fn column_masks_label(masks: &[ColumnMask]) -> String {
    masks
        .iter()
        .map(|mask| {
            format!(
                "{}={}",
                mask.column,
                task_history_label_value(&mask.expression)
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn tag_masks_label(masks: &[TagMask]) -> String {
    masks
        .iter()
        .map(|mask| {
            format!(
                "tag:{}={}",
                mask.tag,
                task_history_label_value(&mask.expression)
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn task_history_label_value(value: &str) -> String {
    value.replace(['\n', '\r', '\t'], " ")
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::assert_batches_eq;
    use datafusion::catalog::MemTable;
    use datafusion::common::TableReference;
    use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
    use datafusion::prelude::{SessionContext, col, lit};
    use runtime_auth::{AuthPrincipal, AuthRequestContext, identity::IdentityContext};
    use runtime_datafusion_udfs::{role_check::CurrentUserHasRoleUdf, user::UserUdf};
    use runtime_policy::PolicyEngine;
    use runtime_policy::engine::parse_policies;
    use runtime_request_context::{Protocol, RequestContext};

    use super::*;

    struct TestPrincipal {
        identity: IdentityContext,
    }

    impl AuthPrincipal for TestPrincipal {
        fn username(&self) -> &str {
            &self.identity.user_id
        }
        fn groups(&self) -> &[&str] {
            &[]
        }
        fn identity_context(&self) -> Option<&IdentityContext> {
            Some(&self.identity)
        }
    }

    fn make_engine(cedar: &str) -> Arc<PolicyEngine> {
        let ps = parse_policies(cedar).expect("valid cedar policy");
        Arc::new(PolicyEngine::new(ps).expect("engine should build"))
    }

    fn make_request_context(user_id: &str, roles: Vec<String>) -> Arc<RequestContext> {
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());
        let principal: runtime_auth::AuthPrincipalRef = Arc::new(TestPrincipal {
            identity: IdentityContext::new(user_id).with_roles(roles),
        });
        ctx.set_auth_principal(principal)
            .expect("set auth principal");
        ctx
    }

    fn make_scan_plan(table_name: &str) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table_source = Arc::new(LogicalTableSource::new(schema));
        LogicalPlanBuilder::scan(TableReference::bare(table_name), table_source, None)
            .expect("scan plan")
            .build()
            .expect("build plan")
    }

    fn make_patient_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("physician_id", DataType::Utf8, false),
            Field::new("ssn", DataType::Utf8, true).with_metadata(HashMap::from([(
                "tags".to_string(),
                serde_json::json!(["pii"]).to_string(),
            )])),
        ]))
    }

    fn make_patient_table_source() -> Arc<dyn TableProvider> {
        let schema = make_patient_schema();
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table should be created"))
    }

    fn make_patient_data_table_source() -> Arc<dyn TableProvider> {
        let schema = make_patient_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice", "bob", "alice"])),
                Arc::new(StringArray::from(vec![
                    "111-11-1111",
                    "222-22-2222",
                    "333-33-3333",
                ])),
            ],
        )
        .expect("patient batch should be created");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"))
    }

    fn make_policy_dataframe_context(engine: &Arc<PolicyEngine>) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(UserUdf::new().into());
        ctx.register_udf(CurrentUserHasRoleUdf::new().into());
        let table_source = wrap_policy_table_provider(
            TableReference::bare("patients"),
            make_patient_data_table_source(),
            Some(engine),
        );
        ctx.register_table("patients", table_source)
            .expect("register patients table");
        ctx
    }

    fn make_patient_scan_plan(projection: Option<Vec<usize>>) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("physician_id", DataType::Utf8, false),
            Field::new("ssn", DataType::Utf8, true),
        ]));
        let table_source = Arc::new(LogicalTableSource::new(schema));
        LogicalPlanBuilder::scan(TableReference::bare("patients"), table_source, projection)
            .expect("scan plan")
            .build()
            .expect("build plan")
    }

    fn make_wrapped_patient_scan_plan(
        engine: &Arc<PolicyEngine>,
        projection: Option<Vec<usize>>,
    ) -> LogicalPlan {
        let table_source = wrap_policy_table_provider(
            TableReference::bare("patients"),
            make_patient_table_source(),
            Some(engine),
        );
        LogicalPlanBuilder::scan(
            TableReference::bare("patients"),
            provider_as_source(table_source),
            projection,
        )
        .expect("scan plan")
        .build()
        .expect("build plan")
    }

    async fn run_authz(
        engine: &Arc<PolicyEngine>,
        plan: &LogicalPlan,
        user_id: &str,
        roles: Vec<String>,
    ) -> Result<(), datafusion::error::DataFusionError> {
        let ctx = make_request_context(user_id, roles);
        ctx.scope(authorize_query_plan(plan, engine)).await
    }

    async fn run_enforce(
        engine: &Arc<PolicyEngine>,
        plan: LogicalPlan,
        user_id: &str,
        roles: Vec<String>,
    ) -> Result<LogicalPlan, datafusion::error::DataFusionError> {
        let ctx = make_request_context(user_id, roles);
        let session = SessionContext::new().state();
        ctx.scope(enforce_query_plan(plan, engine, &session)).await
    }

    #[tokio::test]
    async fn test_default_allow_permits_query() {
        let engine = make_engine(r"permit(principal, action, resource);");
        let plan = make_scan_plan("sales");

        let result = run_authz(&engine, &plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect("expected Ok");
    }

    #[tokio::test]
    async fn test_forbid_overrides_permit() {
        let engine = make_engine(
            r#"
            permit(principal, action, resource);
            forbid(
                principal,
                action == Spice::Action::"query",
                resource == Spice::Dataset::"pii_table"
            );
            "#,
        );

        let pii_plan = make_scan_plan("pii_table");
        let result = run_authz(&engine, &pii_plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect_err("expected Err");
        let err_msg = result.expect_err("should be denied").to_string();
        assert!(
            err_msg.contains("Authorization denied"),
            "expected denial message, got: {err_msg}"
        );

        // Other tables should still be allowed
        let ok_plan = make_scan_plan("sales");
        let result = run_authz(&engine, &ok_plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect("expected Ok");
    }

    #[tokio::test]
    async fn test_role_based_access() {
        let engine = make_engine(
            r#"
            permit(
                principal in Spice::Role::"analyst",
                action == Spice::Action::"query",
                resource
            );
            "#,
        );
        let plan = make_scan_plan("reports");

        // analyst role: allowed
        let result = run_authz(&engine, &plan, "alice", vec!["analyst".into()]).await;
        result.as_ref().expect("expected Ok");

        // guest role: denied
        let result = run_authz(&engine, &plan, "bob", vec!["guest".into()]).await;
        result.as_ref().expect_err("expected Err");
    }

    #[tokio::test]
    async fn test_read_action_permits_query_scan() {
        let engine = make_engine(
            r#"
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let plan = make_patient_scan_plan(None);

        let result = run_authz(&engine, &plan, "alice", vec!["physician".into()]).await;
        result.as_ref().expect("expected Ok");
    }

    #[tokio::test]
    async fn test_policy_row_filter_is_injected_into_scan() {
        let engine = make_engine(
            r#"
            permit(principal, action == Spice::Action::"query", resource);

            @row_filter("physician_id = 'alice'")
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let plan = make_wrapped_patient_scan_plan(&engine, Some(vec![0]));

        let enforced = run_enforce(&engine, plan, "alice", vec!["physician".into()])
            .await
            .expect("policy rewrite should succeed");
        let plan_debug = format!("{enforced:?}");
        assert!(
            plan_debug.contains("physician_id"),
            "expected policy filter in plan, got: {plan_debug}"
        );
        assert!(
            plan_debug.contains("projection: Some([0, 1])"),
            "expected secured scan to push down only requested and policy columns, got: {plan_debug}"
        );
    }

    #[tokio::test]
    async fn test_policy_mask_type_mismatch_fails_closed() {
        let engine = make_engine(
            r#"
            permit(principal, action == Spice::Action::"query", resource);

            @mask_ssn("1")
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let plan = make_wrapped_patient_scan_plan(&engine, None);

        let err = run_enforce(&engine, plan, "alice", vec!["physician".into()])
            .await
            .expect_err("mask type mismatch should fail");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("must return Utf8"),
            "expected type mismatch error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_policy_tag_mask_is_injected_into_scan() {
        let engine = make_engine(
            r#"
            permit(principal, action == Spice::Action::"query", resource);

            @mask_tag_pii("'***'")
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let plan = make_wrapped_patient_scan_plan(&engine, None);

        let enforced = run_enforce(&engine, plan, "alice", vec!["physician".into()])
            .await
            .expect("policy rewrite should succeed");
        let plan_debug = format!("{enforced:?}");
        assert!(
            plan_debug.contains("***"),
            "expected policy mask in plan, got: {plan_debug}"
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_applies_row_filter_and_column_mask() {
        let engine = make_engine(
            r#"
            @row_filter("physician_id = current_user_id()")
            @mask_ssn("'***'")
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["physician".into()]);

        let batches = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .select(vec![col("id"), col("ssn")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect("dataframe collect should succeed");

        assert_batches_eq!(
            &[
                "+----+-----+",
                "| id | ssn |",
                "+----+-----+",
                "| 1  | *** |",
                "| 3  | *** |",
                "+----+-----+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_composes_user_filter_with_policy_filter() {
        let engine = make_engine(
            r#"
            @row_filter("physician_id = current_user_id()")
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["physician".into()]);

        let batches = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .filter(col("id").eq(lit(3_i64)))
                    .expect("filter dataframe")
                    .select(vec![col("id"), col("physician_id")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect("dataframe collect should succeed");

        assert_batches_eq!(
            &[
                "+----+--------------+",
                "| id | physician_id |",
                "+----+--------------+",
                "| 3  | alice        |",
                "+----+--------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_applies_tag_mask() {
        let engine = make_engine(
            r#"
            @mask_tag_pii("'MASKED'")
            permit(
                principal in Spice::Role::"physician",
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["physician".into()]);

        let batches = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .select(vec![col("id"), col("ssn")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect("dataframe collect should succeed");

        assert_batches_eq!(
            &[
                "+----+--------+",
                "| id | ssn    |",
                "+----+--------+",
                "| 1  | MASKED |",
                "| 2  | MASKED |",
                "| 3  | MASKED |",
                "+----+--------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_query_permit_without_read_policy_delegates() {
        let engine = make_engine(
            r#"
            permit(
                principal in Spice::Role::"analyst",
                action == Spice::Action::"query",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["analyst".into()]);

        let batches = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .select(vec![col("id"), col("ssn")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect("dataframe collect should succeed");

        assert_batches_eq!(
            &[
                "+----+-------------+",
                "| id | ssn         |",
                "+----+-------------+",
                "| 1  | 111-11-1111 |",
                "| 2  | 222-22-2222 |",
                "| 3  | 333-33-3333 |",
                "+----+-------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_query_forbid_denies_scan() {
        let engine = make_engine(
            r#"
            forbid(
                principal,
                action == Spice::Action::"query",
                resource == Spice::Dataset::"patients"
            );

            permit(
                principal,
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["physician".into()]);

        let err = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .select(vec![col("id")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect_err("query forbid should deny DataFrame scan");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Authorization denied: action 'query'"),
            "expected query authorization denial, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_read_forbid_overrides_query_permit() {
        let engine = make_engine(
            r#"
            permit(
                principal,
                action == Spice::Action::"query",
                resource == Spice::Dataset::"patients"
            );

            forbid(
                principal,
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["physician".into()]);

        let err = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .select(vec![col("id")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect_err("read forbid should deny DataFrame scan");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Authorization denied: action 'read'"),
            "expected read authorization denial, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_dataframe_api_policy_can_use_current_user_has_role() {
        let engine = make_engine(
            r#"
            @row_filter("current_user_has_role('auditor')")
            permit(
                principal,
                action == Spice::Action::"read",
                resource == Spice::Dataset::"patients"
            );
            "#,
        );
        let ctx = make_policy_dataframe_context(&engine);
        let request_context = make_request_context("alice", vec!["auditor".into()]);

        let batches = request_context
            .scope(async {
                ctx.table("patients")
                    .await
                    .expect("patients dataframe")
                    .select(vec![col("id")])
                    .expect("select dataframe columns")
                    .collect()
                    .await
            })
            .await
            .expect("dataframe collect should succeed");

        assert_batches_eq!(
            &[
                "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "+----+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_no_principal_skips_authz() {
        let engine = make_engine(r"forbid(principal, action, resource);");
        let plan = make_scan_plan("anything");

        // No principal set — should pass (auth layer decides unauthenticated access)
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());
        let result = ctx.scope(authorize_query_plan(&plan, &engine)).await;
        result.as_ref().expect("expected Ok");
    }
}
