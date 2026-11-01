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

//! Search delegated from a sidecar to the upstream Spice runtime that holds a
//! table's embeddings and search indexes.
//!
//! A sidecar's Spice table has no local index, so `vector_search` and
//! `text_search` over it are sent to the upstream as one Substrait plan: an
//! `ExtensionLeafRel` carrying the search call (a [`SEARCH_CALL_TYPE_URL`]
//! message), with any filter, projection and limit pushed into the scan above
//! it. The upstream runs the search against its own indexes and returns the
//! table's columns, in table order, followed by [`SEARCH_SCORE_COLUMN`] when the
//! call includes scores. That contract fixes the result schema, so the sidecar
//! plans the search without a round trip.

use std::{cmp::Ordering, fmt, sync::Arc};

use arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DFSchema, DFSchemaRef, Result},
    datasource::TableType,
    logical_expr::{
        Expr, Extension, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown,
        UserDefinedLogicalNodeCore, expr_rewriter::unnormalize_cols, ident, utils::conjunction,
    },
    physical_plan::ExecutionPlan,
};
use datafusion_federation::sql::MultiPartTableReference;
use flight_client::FlightClient;
use spice_table::LayerWalk;

use super::{
    FlightTable,
    substrait::{Reads, SubstraitExec, encode_plan, produce},
};

/// The `ExtensionLeafRel` detail type of a delegated search: an encoded
/// `spice.protobuf.UdtfArgs` holding one `vector_search` or `text_search` call
/// against the upstream's table.
pub const SEARCH_CALL_TYPE_URL: &str = "type.googleapis.com/spice.protobuf.UdtfArgs";

/// The score column a delegated search appends when its call includes scores.
pub const SEARCH_SCORE_COLUMN: &str = "_score";

/// The leaf of a delegated search plan. Encoded as an `ExtensionLeafRel`;
/// never executed locally.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SearchCallNode {
    pub(super) call: Bytes,
    schema: DFSchemaRef,
}

impl PartialOrd for SearchCallNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.call.partial_cmp(&other.call)
    }
}

impl UserDefinedLogicalNodeCore for SearchCallNode {
    fn name(&self) -> &'static str {
        "SpiceSearchCall"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpiceSearchCall")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(self.clone())
    }
}

/// A `vector_search` or `text_search` the upstream Spice runtime runs.
#[derive(Debug)]
struct DelegatedSearchTable {
    client: FlightClient,
    call: Bytes,
    schema: SchemaRef,
    dataset: String,
}

impl DelegatedSearchTable {
    /// The leaf plan of this search, with `filters`, `projection` and `limit`
    /// applied above it.
    fn plan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<LogicalPlan> {
        let leaf = LogicalPlan::Extension(Extension {
            node: Arc::new(SearchCallNode {
                call: self.call.clone(),
                schema: Arc::new(DFSchema::try_from(Arc::clone(&self.schema))?),
            }),
        });
        let mut plan = LogicalPlanBuilder::from(leaf);
        // A scan's filters arrive qualified with the table function's alias; the
        // search leaf's schema is unqualified.
        if let Some(predicate) = conjunction(unnormalize_cols(filters.iter().cloned())) {
            plan = plan.filter(predicate)?;
        }
        if let Some(projection) = projection {
            plan = plan.project(
                projection
                    .iter()
                    .map(|index| ident(self.schema.field(*index).name())),
            )?;
        }
        if let Some(limit) = limit {
            plan = plan.limit(0, Some(limit))?;
        }
        plan.build()
    }
}

#[async_trait]
impl TableProvider for DelegatedSearchTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // A filter the upstream can evaluate runs there, before rows cross the
        // network; anything else (a sidecar-only function) runs here. Producing
        // the Substrait relation decides it, without the wire encoding.
        Ok(filters
            .iter()
            .map(|filter| {
                if self
                    .plan(None, &[(*filter).clone()], None)
                    .and_then(|plan| produce(&plan, Reads::None))
                    .is_ok()
                {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.plan(projection, filters, limit)?;
        let schema = Arc::clone(plan.schema().inner());
        Ok(Arc::new(SubstraitExec::new(
            self.client.clone(),
            encode_plan(&plan, Reads::None)?,
            schema,
            self.dataset.clone(),
        )))
    }
}

/// The Spice table a sidecar reads `provider` from, when it delegates search to
/// that table's upstream: a Spice table read through Substrait.
fn delegating_table(provider: &dyn TableProvider) -> Option<&FlightTable> {
    // `Passthrough` stops at an acceleration: a search delegated past it would
    // read the whole upstream table, not the rows and columns the accelerated
    // dataset holds (its `refresh_sql`, for one), so an accelerated dataset is
    // searched locally or not at all.
    spice_table::find_concrete::<FlightTable>(provider, LayerWalk::Passthrough)
        .filter(|table| table.substrait)
}

/// The upstream's schema for `provider`'s table, when search over `provider` is
/// delegated to the upstream; `None` when it is searched locally or not at all.
#[must_use]
pub fn delegated_search_schema(provider: &dyn TableProvider) -> Option<SchemaRef> {
    delegating_table(provider).map(|table| Arc::clone(&table.schema))
}

/// A search over `provider`'s upstream table, run by the upstream Spice
/// runtime, or `None` when `provider` does not read a Spice table through
/// Substrait (a sidecar's Spice dataset).
///
/// `call` receives the upstream's name for the table and returns the encoded
/// [`SEARCH_CALL_TYPE_URL`] message. `include_score` must match that call.
#[must_use]
pub fn delegated_search(
    provider: &dyn TableProvider,
    dataset: &str,
    include_score: bool,
    call: impl FnOnce(&str) -> Vec<u8>,
) -> Option<Arc<dyn TableProvider>> {
    let table = delegating_table(provider)?;
    let remote = match table.table_reference.table_ref() {
        MultiPartTableReference::TableReference(reference) => reference.to_quoted_string(),
        MultiPartTableReference::Multi(_) => return None,
    };
    let mut schema = SchemaBuilder::from(table.schema.as_ref());
    if include_score {
        schema.push(Field::new(SEARCH_SCORE_COLUMN, DataType::Float64, false));
    }
    Some(Arc::new(DelegatedSearchTable {
        client: table.client.clone(),
        call: Bytes::from(call(&remote)),
        schema: Arc::new(schema.finish()),
        dataset: dataset.to_string(),
    }))
}
