/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use arrow::{array::RecordBatch, datatypes::Schema};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    datasource::{MemTable, TableProvider},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::sqlparser::ast::Expr,
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
};

use futures::{Stream, StreamExt};
use snafu::ResultExt;
use std::sync::Arc;

use crate::generation::{CandidateGeneration, InternalSnafu, Result};

/// [`PostApplyCandidateGeneration`] applies filter predicate and retrieves additional columns to a
///  [`CandidateGeneration`] that may not have full support.
pub struct PostApplyCandidateGeneration {
    table_provider: Arc<dyn TableProvider>,
    inner: Arc<dyn CandidateGeneration>,
    primary_key: Vec<String>,
}

/// Temporary table names used internally in SQL.
static TABLE_PROVIDER_TABLE_NAME: &str = "table_provider";
static CANDIDATE_GENERATION_TABLE_NAME: &str = "candidate_generation";

impl PostApplyCandidateGeneration {
    pub fn new(
        table: Arc<dyn TableProvider>,
        inner: Arc<dyn CandidateGeneration>,
        primary_key: Vec<String>,
    ) -> Self {
        Self {
            table_provider: table,
            inner,
            primary_key,
        }
    }

    /// Return the subset of `filters` that are supported by the underlying FTS.
    fn supported_underlying_filters<'a>(&self, filters: &'a [&'a Expr]) -> Result<Vec<&'a Expr>> {
        let underlying_filters = self
            .inner
            .supports_filters_pushdown(filters)?
            .into_iter()
            .zip(filters.iter())
            .filter_map(|(supported, &filter)| if supported { Some(filter) } else { None })
            .collect::<Vec<_>>();

        Ok(underlying_filters)
    }

    /// Return the subset of `projections` that are supported by the underlying FTS.
    fn supported_underlying_projection<'a>(
        &self,
        projections: &[&'a Expr],
    ) -> Result<Vec<&'a Expr>> {
        let supported = self.inner.supports_columns(projections)?;
        let underlying_projection = supported
            .into_iter()
            .zip(projections.iter())
            .filter_map(|(supported, &expr)| if supported { Some(expr) } else { None })
            .collect::<Vec<_>>();

        Ok(underlying_projection)
    }

    async fn augment_stream(
        &self,
        stream: SendableRecordBatchStream,
        remaining_filters: &[&Expr],
        remaining_projection: &[&Expr],
    ) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::new();
        let _ = ctx
            .register_table("table", Arc::clone(&self.table_provider))
            .boxed()
            .context(InternalSnafu)?;

        let (new_schema, strm) = Self::apply_postprocessing(
            stream,
            ctx,
            self.primary_key.clone(),
            remaining_filters
                .iter()
                .cloned()
                .map(|e| e.clone())
                .collect(),
            remaining_projection
                .iter()
                .cloned()
                .map(|e| e.clone())
                .collect(),
        )
        .await;

        Ok(Box::pin(RecordBatchStreamAdapter::new(new_schema, strm)))
    }

    /// For each [`RecordBatch`] provided, add the remaining columns (joining on primary keys), and apply the appropriate fiters.
    async fn apply_postprocessing(
        mut stream: SendableRecordBatchStream,
        ctx: SessionContext,
        primary_key: Vec<String>,
        remaining_filters: Vec<Expr>,
        remaining_projection: Vec<Expr>,
    ) -> (
        Arc<Schema>,
        impl Stream<Item = Result<RecordBatch, DataFusionError>>,
    ) {
        let mut schema = stream.schema().clone();
        let s = stream! {
            while let Some(item) = stream.next().await {
                let batch = item?;
                let schema = batch.schema();
                let t = match MemTable::try_new(schema.clone(), vec![vec![batch]]) {
                    Ok(t) => t,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };

                if let Err(e) = ctx.register_table(CANDIDATE_GENERATION_TABLE_NAME, Arc::new(t)) {
                    yield Err(e);
                    continue;
                };

                let mut cols: Vec<_> = schema.fields.iter().map(|f| f.name().clone()).collect();
                cols.append(&mut remaining_projection.iter().map(|s| format!("t.{s}")).collect::<Vec<_>>());

                let df = match ctx.sql(format!(
                    "SELECT {proj} \n\
                    FROM {TABLE_PROVIDER_TABLE_NAME} \n\
                    JOIN {CANDIDATE_GENERATION_TABLE_NAME} t ON {primary_key_join} \n\
                    WHERE {cond}",
                    proj = cols.join(", "),
                    primary_key_join = primary_key
                        .iter()
                        .map(|pk| format!("t.{pk} = {TABLE_PROVIDER_TABLE_NAME}.{pk}"))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                    cond = remaining_filters
                        .iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>()
                        .join(" AND ")
                ).as_str()).await {
                    Ok(df) => df,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };
                match df.collect().await {
                    Ok(batches) => {
                        for batch in batches {
                            yield Ok(batch);
                        }
                    }
                    Err(e) => {
                        yield Err(e);
                    }
                }
            }
        };

        // Peek at the first item to infer the schema.
        let mut s = Box::pin(s.peekable());

        match s.as_mut().peek().await {
            Some(Ok(batch)) => schema = batch.schema().clone(),
            Some(Err(e)) => {
                tracing::warn!(
                    "Failed to infer schema while augmenting search candidate stream: {e}. Using as is"
                );
            }
            None => {}
        };
        (schema, s)
    }
}

#[async_trait]
impl CandidateGeneration for PostApplyCandidateGeneration {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> Result<SendableRecordBatchStream> {
        let underlying_filters = self.supported_underlying_filters(opt_filters)?;
        let underlying_projection = self.supported_underlying_projection(addition_projection)?;

        let underlying = self
            .inner
            .search(
                query,
                underlying_filters.as_slice(),
                underlying_projection.as_slice(),
                limit,
            )
            .await?;

        let unapplied_filters: Vec<&Expr> = opt_filters
            .iter()
            .filter(|&&f| !underlying_filters.contains(&f))
            .cloned()
            .collect::<Vec<_>>();

        let unapplied_projection: Vec<&Expr> = addition_projection
            .iter()
            .filter(|&&p| !underlying_projection.contains(&p))
            .cloned()
            .collect::<Vec<_>>();

        // If there are no unapplied filters or projections, we can return the underlying stream directly.
        if unapplied_filters.is_empty() && unapplied_projection.is_empty() {
            return Ok(underlying);
        }

        self.augment_stream(
            underlying,
            unapplied_filters.as_slice(),
            unapplied_projection.as_slice(),
        )
        .await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<bool>> {
        Ok((0..filters.len()).map(|_| true).collect::<Vec<_>>())
    }

    fn supports_columns(&self, _projection: &[&Expr]) -> Result<Vec<bool>> {
        Ok(vec![])
    }

    fn value_derived_from(&self) -> String {
        self.inner.value_derived_from()
    }
}
