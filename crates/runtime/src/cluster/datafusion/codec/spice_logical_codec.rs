/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

use crate::Runtime;
use crate::cluster::datafusion::codec::udtf_args::{
    RrfArgs, TextSearchArgs, UdtfArgs, UdtfArgsExt, VectorSearchArgs,
};
use crate::embeddings::udtf::{
    VectorSearchTableFunc, VectorSearchTableFuncArgs, VectorSearchUDTFProvider,
};
use crate::search::full_text::udtf::{TextSearchTableFunc, TextSearchTableFuncArgs};
use crate::search::rrf::ReciprocalRankFusion;
use crate::udtfs::{ListUDFTable, ListUDFTableFunc};
use arrow_schema::SchemaRef;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, ScalarValue, TableReference, exec_err};
use datafusion::execution::TaskContext;
use datafusion::sql::TableReference as SqlTableReference;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{Extension, LogicalPlan, ScalarUDF};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use prost::Message;
use runtime_proto::rrf_nested_query::Query;
use runtime_proto::udtf_args::Args;
use search::provider::{SearchQueryProvider, UdtfSource};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Serialization support for custom Spice logical nodes
pub struct SpiceLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
    runtime: Option<Arc<Runtime>>,
}

impl Debug for SpiceLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpiceLogicalCodec")
    }
}

impl SpiceLogicalCodec {
    #[must_use]
    pub fn new_with_runtime(runtime: Arc<Runtime>) -> Arc<dyn LogicalExtensionCodec> {
        Arc::new(Self {
            inner: Arc::new(BallistaLogicalExtensionCodec::default()),
            runtime: Some(runtime),
        })
    }

    #[must_use]
    pub fn new_codec() -> Arc<dyn LogicalExtensionCodec> {
        Arc::new(Self {
            inner: Arc::new(BallistaLogicalExtensionCodec::default()),
            runtime: None,
        })
    }

    fn runtime(&self) -> Result<Arc<Runtime>> {
        self.runtime.clone().ok_or(DataFusionError::Execution(
            "SpiceLogicalCodec did not bind a Runtime handle. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues".to_string(),
        ))
    }

    fn limit_from_u64(value: u64) -> Result<usize> {
        usize::try_from(value).map_err(|_| {
            DataFusionError::Plan(format!("UDTF limit value {value} exceeds usize range."))
        })
    }

    /// Reconstructs a UDTF-produced `TableProvider` by re-invoking the UDTF with
    /// the serialized arguments.
    pub(crate) fn invoke_udtf(
        udtf_args: UdtfArgs,
        runtime: &Arc<Runtime>,
    ) -> Result<Arc<dyn TableProvider>> {
        use datafusion::catalog::TableFunctionImpl;

        let Some(args) = udtf_args.args else {
            return exec_err!("UDTF args missing inner args field");
        };

        match args {
            Args::ListUdfs(_) => {
                let udtf = ListUDFTableFunc::new(Arc::clone(&runtime.df.ctx));
                udtf.call(&[])
            }
            Args::TextSearch(text_args) => {
                let udtf = TextSearchTableFunc::new(Arc::downgrade(&runtime.df));
                let exprs = TextSearchTableFunc::to_expr(&TextSearchTableFuncArgs {
                    tbl: SqlTableReference::parse_str(&text_args.table),
                    query: text_args.query,
                    column: text_args.column,
                    limit: text_args.limit.map(Self::limit_from_u64).transpose()?,
                    include_score: text_args.include_score,
                });
                udtf.call(&exprs)
            }
            Args::VectorSearch(vector_args) => {
                let udtf = VectorSearchTableFunc::new(
                    Arc::downgrade(&runtime.df),
                    HashMap::new(), // explicit_pks - will be inferred from table
                );
                let exprs = VectorSearchTableFunc::to_expr(&VectorSearchTableFuncArgs {
                    tbl: SqlTableReference::parse_str(&vector_args.table),
                    query: vector_args.query,
                    column: vector_args.column,
                    limit: vector_args.limit.map(Self::limit_from_u64).transpose()?,
                    include_score: vector_args.include_score,
                });
                udtf.call(&exprs)
            }
            Args::Rrf(rrf_args) => Self::invoke_rrf(&rrf_args, runtime),
        }
    }

    /// Reconstructs an RRF (Reciprocal Rank Fusion) `TableProvider` by re-invoking
    /// the nested search UDTFs and then the RRF UDTF.
    fn invoke_rrf(rrf_args: &RrfArgs, runtime: &Arc<Runtime>) -> Result<Arc<dyn TableProvider>> {
        use datafusion::catalog::TableFunctionImpl;
        use datafusion::logical_expr::expr::FieldMetadata;
        use datafusion::prelude::Expr;
        use std::collections::BTreeMap;

        let mut exprs: Vec<Expr> = Vec::new();

        // Convert nested queries to expressions
        for nested in &rrf_args.queries {
            let Some(query) = &nested.query else {
                return exec_err!("RRF nested query missing query field");
            };

            let (search_exprs, rank_weight) = match query {
                Query::TextSearch(ts) => {
                    let Some(args) = &ts.args else {
                        return exec_err!("TextSearch nested query missing args");
                    };
                    let text_exprs = TextSearchTableFunc::to_expr(&TextSearchTableFuncArgs {
                        tbl: SqlTableReference::parse_str(&args.table),
                        query: args.query.clone(),
                        column: args.column.clone(),
                        limit: args.limit.map(Self::limit_from_u64).transpose()?,
                        include_score: args.include_score,
                    });
                    (text_exprs, ts.rank_weight)
                }
                Query::VectorSearch(vs) => {
                    let Some(args) = &vs.args else {
                        return exec_err!("VectorSearch nested query missing args");
                    };
                    let vector_exprs = VectorSearchTableFunc::to_expr(&VectorSearchTableFuncArgs {
                        tbl: SqlTableReference::parse_str(&args.table),
                        query: args.query.clone(),
                        column: args.column.clone(),
                        limit: args.limit.map(Self::limit_from_u64).transpose()?,
                        include_score: args.include_score,
                    });
                    (vector_exprs, vs.rank_weight)
                }
            };

            // Add rank_weight as a named parameter if specified
            let mut final_exprs = search_exprs;
            if let Some(weight) = rank_weight {
                let meta = FieldMetadata::new(BTreeMap::from([(
                    "spice.parameter_name".to_string(),
                    "rank_weight".to_string(),
                )]));
                let weight_expr = Expr::Literal(ScalarValue::Float64(Some(weight)), Some(meta));
                final_exprs.push(weight_expr);
            }
            exprs.extend(final_exprs);
        }

        // Add RRF named parameters
        if let Some(k) = rrf_args.k {
            exprs.push(Self::named_literal("k", ScalarValue::Float64(Some(k))));
        }
        if let Some(join_key) = &rrf_args.join_key {
            exprs.push(Self::named_literal(
                "join_key",
                ScalarValue::Utf8(Some(join_key.clone())),
            ));
        }
        if let Some(time_column) = &rrf_args.time_column {
            exprs.push(Self::named_literal(
                "time_column",
                ScalarValue::Utf8(Some(time_column.clone())),
            ));
        }
        if let Some(recency_decay) = &rrf_args.recency_decay {
            exprs.push(Self::named_literal(
                "recency_decay",
                ScalarValue::Utf8(Some(recency_decay.clone())),
            ));
        }
        if let Some(decay_constant) = rrf_args.decay_constant {
            exprs.push(Self::named_literal(
                "decay_constant",
                ScalarValue::Float64(Some(decay_constant)),
            ));
        }
        if let Some(decay_scale_secs) = rrf_args.decay_scale_secs {
            exprs.push(Self::named_literal(
                "decay_scale_secs",
                ScalarValue::Float64(Some(decay_scale_secs)),
            ));
        }
        if let Some(decay_window_secs) = rrf_args.decay_window_secs {
            exprs.push(Self::named_literal(
                "decay_window_secs",
                ScalarValue::Float64(Some(decay_window_secs)),
            ));
        }

        // Invoke the RRF UDTF
        let rrf_udtf = ReciprocalRankFusion::from_ctx(&runtime.df.ctx);
        rrf_udtf.call(&exprs)
    }

    /// Creates a literal expression with `spice.parameter_name` metadata.
    fn named_literal(name: &str, value: ScalarValue) -> datafusion::prelude::Expr {
        use datafusion::logical_expr::expr::FieldMetadata;
        use std::collections::BTreeMap;

        let meta = FieldMetadata::new(BTreeMap::from([(
            "spice.parameter_name".to_string(),
            name.to_string(),
        )]));
        datafusion::prelude::Expr::Literal(value, Some(meta))
    }
}

impl LogicalExtensionCodec for SpiceLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        if let Ok(ext) = self.inner.try_decode(buf, inputs, ctx) {
            return Ok(ext);
        }

        let name = serde_json::from_slice::<String>(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        exec_err!(
            "SpiceLogicalCodec does not support {}. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues",
            name.as_str()
        )
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        if matches!(self.inner.try_encode(node, buf), Ok(())) {
            return Ok(());
        }

        let node_name = serde_json::to_vec(node.node.name())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        buf.extend_from_slice(&node_name[..]);
        Ok(())
    }

    // Look up the table ref in the context or reconstruct UDTF-produced providers
    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let runtime = self.runtime()?;

        // Try to deserialize as UDTF args first (using protobuf)
        if !buf.is_empty()
            && let Ok(udtf_args) = UdtfArgs::decode(buf)
        {
            return Self::invoke_udtf(udtf_args, &runtime);
        }

        // Fall back to regular table lookup
        if let Some(table_provider) = runtime.df.get_table_sync(table_ref) {
            return Ok(table_provider);
        }

        exec_err!(
            "SpiceLogicalCodec could not resolve table reference {}. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues",
            table_ref
        )
    }

    // Encode UDTF-produced table providers for distributed execution (using protobuf)
    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let any = node.as_any();

        // Check for ListUDFTable
        if any.downcast_ref::<ListUDFTable>().is_some() {
            let args = UdtfArgs::list_udfs();
            buf.extend_from_slice(&args.encode_to_vec());
            return Ok(());
        }

        // Check for SearchQueryProvider (text_search/vector_search via index)
        if let Some(search_provider) = any.downcast_ref::<SearchQueryProvider>()
            && let Some(source) = &search_provider.udtf_source
        {
            let args = match source {
                UdtfSource::TextSearch {
                    table,
                    query,
                    column,
                    limit,
                    include_score,
                } => UdtfArgs::text_search(TextSearchArgs {
                    table: table.clone(),
                    query: query.clone(),
                    column: column.clone(),
                    limit: limit.map(|l| l as u64),
                    include_score: *include_score,
                }),
                UdtfSource::VectorSearch {
                    table,
                    query,
                    column,
                    limit,
                    include_score,
                } => UdtfArgs::vector_search(VectorSearchArgs {
                    table: table.clone(),
                    query: query.clone(),
                    column: column.clone(),
                    limit: limit.map(|l| l as u64),
                    include_score: *include_score,
                }),
            };
            buf.extend_from_slice(&args.encode_to_vec());
            return Ok(());
        }

        // Check for VectorSearchUDTFProvider (vector_search without index)
        if let Some(vector_provider) = any.downcast_ref::<VectorSearchUDTFProvider>() {
            let provider_args = vector_provider.args();
            let args = UdtfArgs::vector_search(VectorSearchArgs {
                table: provider_args.tbl.to_string(),
                query: provider_args.query.clone(),
                column: provider_args.column.clone(),
                limit: provider_args.limit.map(|l| l as u64),
                include_score: provider_args.include_score,
            });
            buf.extend_from_slice(&args.encode_to_vec());
            return Ok(());
        }

        // Check for ReciprocalRankFusion (rrf)
        if let Some(rrf_provider) = any.downcast_ref::<ReciprocalRankFusion>()
            && let Some(source) = &rrf_provider.rrf_source
        {
            let args = UdtfArgs::rrf(source.clone());
            buf.extend_from_slice(&args.encode_to_vec());
            return Ok(());
        }

        // Fall through for regular tables - no-op
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.runtime()?.df.ctx.udf(name)
    }
}
