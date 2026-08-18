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

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use crate::aggregation::from_single_input;
use crate::{
    SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME, VectorSearchGenerationResult,
    collect_batches,
};

use super::{
    AggregationResult, CandidateAggregation, DatafusionSnafu, Error,
    InconsistentAggregationResultSnafu, Result, additional_columns_of_schema,
    ith_search_value_column, result_from_table, verify_schema_compatibility,
};

use arrow::array::{Array, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan, LogicalPlanBuilder};
use datafusion::logical_expr::{JoinType, Operator, binary_expr, col, lit};
use datafusion::prelude::{SessionContext, coalesce};
use datafusion::sql::TableReference;
use snafu::ResultExt;

/// Relative Score Fusion (RSF) combines multiple scored sets of search results using the
/// *magnitude* of each score, not only its rank.
///
/// Every input's scores are min-max normalised onto `[0, 1]`, and the normalised scores are
/// combined as a weighted linear sum:
/// ```text
/// score_a   = w_i * norm_i(a) + w_j * norm_j(a) + ...
/// norm_i(a) = (score_i(a) - min_i) / (max_i - min_i)
/// ```
/// A candidate an input did not return contributes `0` for that input.
///
/// This is the complement of [`super::reciprocal_rank::ReciprocalRankFusion`], which reads
/// only rank: RRF gives a near-exact match and a barely-relevant one the same fused score as
/// long as they hold the same rank in their input, because the underlying scores never reach
/// the sum. RSF preserves that distance. The trade-off is the usual one for score-based
/// fusion — it assumes each input's scores are comparable *within* that input, which
/// normalisation makes true per input but cannot make true across inputs; that is what the
/// weights are for.
///
/// Unlike RRF this aggregation does not widen the candidate pool ([`CandidateAggregation::candidate_pool_size`]):
/// the normalisation window is exactly the candidate set each input contributes, so a wider
/// pool would move `min_i`/`max_i` and change the fused scores rather than only extending
/// them.
pub struct RelativeScoreFusion {
    weights: Vec<f64>,
}

/// Weight applied to an input for which no explicit weight is configured.
pub const DEFAULT_RELATIVE_SCORE_WEIGHT: f64 = 1.0;

/// Column carrying each input's normalised score while the inputs are being fused.
///
/// Internal to the fused plan: it is added to the in-memory candidate tables and never
/// projected into [`AggregationResult::data`].
const NORMALISED_SCORE_COLUMN_NAME: &str = "_relative_score_norm";

impl RelativeScoreFusion {
    /// Fuses every input with an equal weight of [`DEFAULT_RELATIVE_SCORE_WEIGHT`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            weights: Vec::new(),
        }
    }

    /// Fuses the i-th input with the i-th weight, so callers can favour one retrieval leg
    /// (e.g. vector similarity) over another (e.g. full-text).
    ///
    /// Inputs past the end of `weights` fall back to [`DEFAULT_RELATIVE_SCORE_WEIGHT`], so a
    /// short list weights a prefix rather than silently zeroing the remaining inputs.
    #[must_use]
    pub fn with_weights(weights: Vec<f64>) -> Self {
        Self { weights }
    }

    fn weight(&self, index: usize) -> f64 {
        self.weights
            .get(index)
            .copied()
            .unwrap_or(DEFAULT_RELATIVE_SCORE_WEIGHT)
    }
}

impl Default for RelativeScoreFusion {
    fn default() -> Self {
        Self::new()
    }
}

/// The affine transform that maps one input's scores onto `[0, 1]`.
///
/// Applying it as `(score - offset) * scale + bias` keeps the degenerate case in the
/// coefficients instead of in a branch, so every candidate of every input goes through the
/// same arithmetic.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreScale {
    offset: f64,
    scale: f64,
    bias: f64,
}

impl ScoreScale {
    /// Builds the transform for an input whose finite scores span `[min, max]`.
    ///
    /// An empty span — a single candidate, or every candidate tied — carries no relative
    /// order, so every candidate normalises to `1.0`: each is equally the best that input
    /// offered. Normalising a tied input to `0.0` instead would drop it out of the weighted
    /// sum entirely, which is not the same statement as "these all matched equally well".
    #[must_use]
    pub fn from_bounds(min: f64, max: f64) -> Self {
        let span = max - min;
        if span.is_finite() && span > 0.0 {
            Self {
                offset: min,
                scale: 1.0 / span,
                bias: 0.0,
            }
        } else {
            Self {
                offset: min,
                scale: 0.0,
                bias: 1.0,
            }
        }
    }

    /// The transform for an input whose scores carry no usable information; every candidate
    /// normalises to `0.0` and the input contributes nothing to the sum.
    #[must_use]
    pub fn unscored() -> Self {
        Self {
            offset: 0.0,
            scale: 0.0,
            bias: 0.0,
        }
    }

    /// Normalises one score. A non-finite score is treated as unscored rather than allowed to
    /// poison the fused sum for that candidate.
    #[must_use]
    pub fn apply(&self, score: f64) -> f64 {
        if !score.is_finite() {
            return 0.0;
        }
        (score - self.offset) * self.scale + self.bias
    }
}

/// Returns the span of the finite values in `scores`, or `None` when none are finite.
#[must_use]
pub fn finite_bounds(scores: &[f64]) -> Option<(f64, f64)> {
    scores
        .iter()
        .copied()
        .filter(|s| s.is_finite())
        .fold(None, |acc, s| match acc {
            None => Some((s, s)),
            Some((min, max)) => Some((min.min(s), max.max(s))),
        })
}

/// Returns the [`ScoreScale`] that min-max normalises `scores`.
#[must_use]
pub fn score_scale(scores: &[f64]) -> ScoreScale {
    match finite_bounds(scores) {
        Some((min, max)) => ScoreScale::from_bounds(min, max),
        None => ScoreScale::unscored(),
    }
}

/// Min-max normalises `scores` onto `[0, 1]`.
#[must_use]
pub fn min_max_normalize(scores: &[f64]) -> Vec<f64> {
    let scale = score_scale(scores);
    scores.iter().map(|s| scale.apply(*s)).collect()
}

/// Fuses scored candidate lists into one weighted score per key.
///
/// Each list is min-max normalised independently, then the i-th list is added into the total
/// with `weights[i]`, falling back to [`DEFAULT_RELATIVE_SCORE_WEIGHT`] past the end of
/// `weights`. Keys absent from a list contribute nothing for that list.
#[must_use]
pub fn relative_score_fusion_scores<K, L, I>(scored_lists: I, weights: &[f64]) -> HashMap<K, f64>
where
    K: Eq + Hash,
    L: IntoIterator<Item = (K, f64)>,
    I: IntoIterator<Item = L>,
{
    let mut fused: HashMap<K, f64> = HashMap::new();
    for (list_index, scored_list) in scored_lists.into_iter().enumerate() {
        let weight = weights
            .get(list_index)
            .copied()
            .unwrap_or(DEFAULT_RELATIVE_SCORE_WEIGHT);

        let entries: Vec<(K, f64)> = scored_list.into_iter().collect();
        let raw: Vec<f64> = entries.iter().map(|(_, score)| *score).collect();
        let scale = score_scale(raw.as_slice());

        for (key, score) in entries {
            let contribution = weight * scale.apply(score);
            fused
                .entry(key)
                .and_modify(|total| *total += contribution)
                .or_insert(contribution);
        }
    }
    fused
}

/// Reads a batch's [`SEARCH_SCORE_COLUMN_NAME`] column as `f64`, treating a null as unscored.
fn batch_scores(batch: &RecordBatch) -> Result<Vec<f64>> {
    let column = batch
        .column_by_name(SEARCH_SCORE_COLUMN_NAME)
        .ok_or_else(|| Error::CandidateMissingRequiredColumn {
            col: SEARCH_SCORE_COLUMN_NAME.to_string(),
        })?;

    let scores = arrow::compute::cast(column, &DataType::Float64)
        .boxed()
        .context(InconsistentAggregationResultSnafu)?;

    let scores = scores
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::InconsistentAggregationResult {
            source: Box::from(format!(
                "'{SEARCH_SCORE_COLUMN_NAME}' could not be read as a floating point score"
            )),
        })?;

    Ok((0..scores.len())
        .map(|i| {
            if scores.is_null(i) {
                f64::NAN
            } else {
                scores.value(i)
            }
        })
        .collect())
}

/// Appends [`NORMALISED_SCORE_COLUMN_NAME`] to every batch of one input.
///
/// The normalisation window is the whole input, so the scale is computed across all of its
/// batches before any batch is rewritten.
fn with_normalised_scores(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    let per_batch_scores = batches
        .iter()
        .map(batch_scores)
        .collect::<Result<Vec<_>>>()?;

    let all_scores: Vec<f64> = per_batch_scores.iter().flatten().copied().collect();
    let scale = score_scale(all_scores.as_slice());

    batches
        .into_iter()
        .zip(per_batch_scores)
        .map(|(batch, scores)| {
            let normalised = Float64Array::from_iter_values(scores.iter().map(|s| scale.apply(*s)));

            let mut fields: Vec<Field> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.push(Field::new(
                NORMALISED_SCORE_COLUMN_NAME,
                DataType::Float64,
                false,
            ));

            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(normalised));

            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .boxed()
                .context(InconsistentAggregationResultSnafu)
        })
        .collect()
}

#[async_trait]
impl CandidateAggregation for RelativeScoreFusion {
    async fn aggregate(
        &self,
        mut data: Vec<VectorSearchGenerationResult>,
        primary_key: Vec<Column>,
        limit: usize,
    ) -> Result<AggregationResult> {
        let num_inputs = data.len();
        // Handle 0, or 1 candidates.
        if num_inputs <= 1 {
            return data
                .pop()
                .map(|d| from_single_input(d, primary_key))
                .ok_or(Error::NoCandidatesGenerated);
        }

        if primary_key.is_empty() {
            return Err(Error::NoPrimaryKey);
        }

        let schemas = data.iter().map(|d| d.data.schema()).collect::<Vec<_>>();
        let () = verify_schema_compatibility(schemas.as_slice())?;

        let ctx = SessionContext::new();
        let mut table_names: Vec<TableReference> = Vec::with_capacity(num_inputs);

        let mut additional_columns = HashSet::new();
        let mut matches: HashMap<String, Vec<String>> = HashMap::new();

        let mut i = 0;
        for VectorSearchGenerationResult {
            data: stream,
            derived_from,
        } in data
        {
            let schema = stream.schema();
            additional_columns.extend(additional_columns_of_schema(
                &schema,
                primary_key.as_slice(),
            ));

            let mut batches = collect_batches(stream).await.context(DatafusionSnafu)?;
            batches.retain(|batch| batch.num_rows() > 0);

            // If data is empty, don't use.
            if batches.is_empty() {
                continue;
            }

            matches
                .get_mut(derived_from.as_str())
                .map(|v| v.push(ith_search_value_column(i)))
                .unwrap_or_else(|| {
                    matches.insert(derived_from.clone(), vec![ith_search_value_column(i)]);
                });

            let batches = with_normalised_scores(batches)?;
            let normalised_schema = batches
                .first()
                .map(RecordBatch::schema)
                .ok_or(Error::NoCandidatesGenerated)?;

            let table_name = TableReference::bare(format!("search_candidates_{i}"));
            table_names.push(table_name.clone());
            let table =
                MemTable::try_new(normalised_schema, vec![batches]).context(DatafusionSnafu)?;
            let _ = ctx
                .register_table(table_name, Arc::new(table))
                .context(DatafusionSnafu)?;

            i += 1;
        }

        let primary_key_str: Vec<String> = primary_key
            .iter()
            .map(datafusion::prelude::Column::flat_name)
            .collect();

        // Now that we've filtered empty generation data, again check for <=1 inputs.
        if table_names.len() <= 1 {
            let tbl = table_names.pop().ok_or(Error::NoCandidatesGenerated)?;
            let match_keys: Vec<_> = matches.keys().cloned().collect();

            return result_from_table(
                &ctx,
                &tbl,
                match_keys.first().ok_or(Error::NoCandidatesGenerated)?,
                primary_key_str.as_slice(),
            )
            .await;
        }

        let additional_columns = additional_columns.into_iter().collect::<Vec<_>>();

        let weights: Vec<f64> = (0..table_names.len()).map(|i| self.weight(i)).collect();

        let plan = relative_score_fusion_plan(
            &ctx,
            table_names.as_slice(),
            primary_key.as_slice(),
            additional_columns.as_slice(),
            weights.as_slice(),
            limit,
        )
        .await
        .context(DatafusionSnafu)?;

        tracing::debug!("Running relative score fusion logical plan: {plan:?}");
        let data = ctx
            .execute_logical_plan(plan)
            .await
            .context(DatafusionSnafu)?
            .execute_stream()
            .await
            .context(DatafusionSnafu)?;

        Ok(AggregationResult {
            data,
            primary_key: primary_key_str,
            data_columns: additional_columns
                .iter()
                .map(datafusion::prelude::Column::flat_name)
                .collect(),
            matches,
        })
    }
}

/// Generates the [`LogicalPlan`] that fuses already-normalised candidate tables.
///
/// The tables must already carry [`NORMALISED_SCORE_COLUMN_NAME`] (see
/// [`with_normalised_scores`]); this function only joins them and sums the weighted
/// normalised scores.
async fn relative_score_fusion_plan(
    ctx: &SessionContext,
    tables: &[TableReference],
    primary_key: &[Column],
    additional_columns: &[Column],
    weights: &[f64],
    limit: usize,
) -> datafusion::error::Result<LogicalPlan> {
    let mut scoped_plans: Vec<(TableReference, LogicalPlan)> = Vec::with_capacity(tables.len());

    for table_name in tables {
        let table = ctx.table(table_name.clone()).await?;
        let scoped = LogicalPlanBuilder::from(table.into_unoptimized_plan())
            .alias(table_name.clone())?
            .build()?;

        scoped_plans.push((table_name.clone(), scoped));
    }

    let (first_table_name, first_plan) = scoped_plans.first().ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "No tables provided for relative score fusion".to_string(),
        )
    })?;

    let mut builder = LogicalPlanBuilder::from(first_plan.clone());
    let mut joined_table_names = vec![first_table_name.clone()];

    // FULL OUTER JOIN remaining tables on primary key columns, so a candidate only one input
    // returned still reaches the fused result.
    for (table_name, plan) in scoped_plans.iter().skip(1) {
        let on_exprs = primary_key.iter().map(|pk| {
            let mut left_key_parts = joined_table_names
                .iter()
                .map(|joined_table| col(pk.clone().with_relation(joined_table.clone())))
                .collect::<Vec<_>>();
            let left_key = if left_key_parts.len() == 1 {
                left_key_parts.swap_remove(0)
            } else {
                coalesce(left_key_parts)
            };

            left_key.eq(col(pk.clone().with_relation(table_name.clone())))
        });

        builder = builder.join_on(plan.clone(), JoinType::Full, on_exprs)?;
        joined_table_names.push(table_name.clone());
    }

    // SUM(COALESCE(weight * normalised_score, 0)) across all tables. The COALESCE is what
    // makes a candidate missing from an input score 0 for it rather than null out the sum.
    let fused_score = scoped_plans
        .iter()
        .enumerate()
        .map(|(i, (table_name, _))| {
            let normalised = col(Column::new(
                Some(table_name.clone()),
                NORMALISED_SCORE_COLUMN_NAME,
            ));
            let weight = weights
                .get(i)
                .copied()
                .unwrap_or(DEFAULT_RELATIVE_SCORE_WEIGHT);
            coalesce(vec![
                binary_expr(lit(weight), Operator::Multiply, normalised),
                lit(0.0),
            ])
        })
        .reduce(|acc, expr| binary_expr(acc, Operator::Plus, expr))
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "No tables to compute relative score fusion score".to_string(),
            )
        })?;

    let fused_score_final = fused_score.alias(SEARCH_SCORE_COLUMN_NAME);

    let value_cols: Vec<LogicalExpr> = scoped_plans
        .iter()
        .enumerate()
        .map(|(i, (table_name, _))| {
            col(Column::new(
                Some(table_name.clone()),
                SEARCH_VALUE_COLUMN_NAME,
            ))
            .alias(ith_search_value_column(i))
        })
        .collect();

    let coalesced_cols: Vec<LogicalExpr> = [primary_key, additional_columns]
        .concat()
        .iter()
        .map(|col_name| {
            let col_refs: Vec<LogicalExpr> = scoped_plans
                .iter()
                .map(|(table_name, _)| col(col_name.clone().with_relation(table_name.clone())))
                .collect();
            coalesce(col_refs).alias(col_name.to_string())
        })
        .collect();

    let projection: Vec<LogicalExpr> =
        [vec![fused_score_final], value_cols, coalesced_cols].concat();

    builder = builder.project(projection)?;

    // Sort by score descending, then by primary key ascending for deterministic ordering on ties.
    let mut sort_exprs = vec![col(SEARCH_SCORE_COLUMN_NAME).sort(false, false)];
    sort_exprs.extend(
        primary_key
            .iter()
            .map(|pk| col(pk.clone()).sort(true, true)),
    );
    builder = builder.sort(sort_exprs)?.limit(0, Some(limit))?;

    builder.build()
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, RecordBatch, StringArray};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    use crate::aggregation::reciprocal_rank::{DEFAULT_RRF_K, reciprocal_rank_fusion_scores};

    use super::*;

    const EPSILON: f64 = 1e-9;

    fn assert_close(actual: f64, expected: f64, what: &str) {
        assert!(
            (actual - expected).abs() < EPSILON,
            "expected {what} to be {expected}, got {actual}"
        );
    }

    #[test]
    fn min_max_normalize_maps_the_span_onto_the_unit_interval() {
        let normalised = min_max_normalize(&[1.0, 3.0, 2.0]);
        assert_close(normalised[0], 0.0, "the minimum");
        assert_close(normalised[1], 1.0, "the maximum");
        assert_close(normalised[2], 0.5, "the midpoint");
    }

    #[test]
    fn min_max_normalize_treats_a_tied_input_as_all_best() {
        // No relative order to preserve: every candidate is equally the best this input
        // offered, and must keep contributing to the weighted sum.
        assert_eq!(min_max_normalize(&[5.0, 5.0, 5.0]), vec![1.0, 1.0, 1.0]);
        assert_eq!(min_max_normalize(&[7.0]), vec![1.0]);
        assert_eq!(min_max_normalize(&[]), Vec::<f64>::new());
    }

    #[test]
    fn min_max_normalize_excludes_non_finite_scores_from_the_span() {
        let normalised = min_max_normalize(&[1.0, f64::NAN, 3.0, f64::INFINITY]);

        // The span is [1, 3] — the non-finite entries neither widen it nor poison it.
        assert_close(normalised[0], 0.0, "the finite minimum");
        assert_close(normalised[2], 1.0, "the finite maximum");
        assert_close(normalised[1], 0.0, "a NaN score");
        assert_close(normalised[3], 0.0, "an infinite score");
    }

    #[test]
    fn finite_bounds_reports_no_span_without_finite_scores() {
        assert_eq!(finite_bounds(&[1.0, 5.0, 3.0]), Some((1.0, 5.0)));
        assert_eq!(finite_bounds(&[f64::NAN, f64::INFINITY]), None);
        assert_eq!(finite_bounds(&[]), None);
    }

    /// The behaviour this aggregation exists for: a candidate ranked second in *both* inputs,
    /// but scored near the top of each, is a stronger result than one input's best that the
    /// other input ranked last. Reciprocal rank fusion cannot express that — it only ever sees
    /// ranks 1, 2, 3 — so it ranks the consensus candidate below both, while relative score
    /// fusion keeps the score distance and puts it first.
    #[test]
    fn relative_score_fusion_ranks_by_score_distance_where_rrf_ranks_by_position() {
        let vector_leg = vec![("A", 1.00), ("B", 0.90), ("C", 0.00)];
        let keyword_leg = vec![("C", 1.00), ("B", 0.95), ("A", 0.00)];

        let fused =
            relative_score_fusion_scores(vec![vector_leg.clone(), keyword_leg.clone()], &[]);

        assert_close(fused["A"], 1.0, "A's fused score");
        assert_close(fused["B"], 1.85, "B's fused score");
        assert_close(fused["C"], 1.0, "C's fused score");
        assert!(
            fused["B"] > fused["A"] && fused["B"] > fused["C"],
            "expected the candidate scored near the top of both inputs to win, got {fused:?}"
        );

        // Same inputs, ranks only: B is rank 2 twice, which loses to rank 1 + rank 3.
        let ranked = reciprocal_rank_fusion_scores(
            vec![
                vector_leg.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                keyword_leg.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            ],
            DEFAULT_RRF_K,
        );
        assert!(
            ranked["B"] < ranked["A"] && ranked["B"] < ranked["C"],
            "expected rank-only fusion to rank the consensus candidate last, got {ranked:?}"
        );
    }

    #[test]
    fn relative_score_fusion_scores_weights_each_input() {
        let lists = vec![
            vec![("A", 1.00), ("B", 0.90), ("C", 0.00)],
            vec![("C", 1.00), ("B", 0.95), ("A", 0.00)],
        ];

        // Silencing the second input leaves the first input's normalised order.
        let fused = relative_score_fusion_scores(lists.clone(), &[1.0, 0.0]);
        assert_close(fused["A"], 1.0, "A under a silenced second input");
        assert_close(fused["B"], 0.9, "B under a silenced second input");
        assert_close(fused["C"], 0.0, "C under a silenced second input");

        // A short weight list weights a prefix; the rest fall back to the default weight.
        let partial = relative_score_fusion_scores(lists, &[2.0]);
        assert_close(
            partial["B"],
            2.0 * 0.9 + 0.95,
            "B under a prefix weight list",
        );
    }

    #[test]
    fn relative_score_fusion_scores_ignores_a_candidate_missing_from_an_input() {
        let fused = relative_score_fusion_scores(
            vec![vec![("A", 2.0), ("B", 1.0)], vec![("A", 8.0), ("C", 4.0)]],
            &[],
        );

        assert_close(fused["A"], 2.0, "A, top of both inputs");
        assert_close(
            fused["B"],
            0.0,
            "B, bottom of one input and absent from the other",
        );
        assert_close(
            fused["C"],
            0.0,
            "C, bottom of one input and absent from the other",
        );
    }

    fn stream_from_batch(batch: RecordBatch) -> SendableRecordBatchStream {
        stream_from_batches(vec![batch])
    }

    fn stream_from_batches(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches
            .first()
            .expect("candidate stream should contain at least one batch")
            .schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    fn candidate_batch(scores: Vec<f64>, values: Vec<&str>, ids: Vec<&str>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                SEARCH_SCORE_COLUMN_NAME,
                Arc::new(Float64Array::from(scores)) as _,
            ),
            (
                SEARCH_VALUE_COLUMN_NAME,
                Arc::new(StringArray::from(values)) as _,
            ),
            ("id", Arc::new(StringArray::from(ids)) as _),
        ])
        .expect("valid record batch")
    }

    fn candidate_input(batch: RecordBatch) -> VectorSearchGenerationResult {
        VectorSearchGenerationResult {
            data: stream_from_batch(batch),
            derived_from: "body".to_string(),
        }
    }

    /// Returns `(primary key, fused score)` per row, in the order the aggregation returned them.
    async fn fused_rows(result: AggregationResult) -> Vec<(String, f64)> {
        assert_eq!(
            result.primary_key,
            vec!["id".to_string()],
            "expected the fused result to carry the primary key through"
        );

        let batches = collect_batches(result.data)
            .await
            .expect("should collect fused batches");

        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("id")
                .expect("fused result should carry the primary key column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("the primary key should read back as a string column");
            let scores = batch
                .column_by_name(SEARCH_SCORE_COLUMN_NAME)
                .expect("fused result should carry the score column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("the fused score should read back as a float column");

            for i in 0..batch.num_rows() {
                rows.push((ids.value(i).to_string(), scores.value(i)));
            }
        }
        rows
    }

    #[tokio::test]
    async fn relative_score_fusion_orders_the_fused_result_by_score_distance() {
        let result = RelativeScoreFusion::new()
            .aggregate(
                vec![
                    candidate_input(candidate_batch(
                        vec![1.00, 0.90, 0.00],
                        vec!["A-vector", "B-vector", "C-vector"],
                        vec!["A", "B", "C"],
                    )),
                    candidate_input(candidate_batch(
                        vec![1.00, 0.95, 0.00],
                        vec!["C-keyword", "B-keyword", "A-keyword"],
                        vec!["C", "B", "A"],
                    )),
                ],
                vec![Column::from_name("id")],
                10,
            )
            .await
            .expect("relative score fusion should succeed");

        let rows = fused_rows(result).await;
        let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();

        assert_eq!(
            ids,
            vec!["B", "A", "C"],
            "expected the candidate scored near the top of both inputs first, got {rows:?}"
        );
        assert_close(rows[0].1, 1.85, "B's fused score");
        assert_close(rows[1].1, 1.0, "A's fused score");
        assert_close(rows[2].1, 1.0, "C's fused score");
    }

    #[tokio::test]
    async fn relative_score_fusion_merges_documents_missing_from_the_first_input() {
        let result = RelativeScoreFusion::new()
            .aggregate(
                vec![
                    candidate_input(candidate_batch(vec![10.0], vec!["A-from-s0"], vec!["A"])),
                    candidate_input(candidate_batch(
                        vec![9.0, 8.0],
                        vec!["B-from-s1", "D-from-s1"],
                        vec!["B", "D"],
                    )),
                    candidate_input(candidate_batch(
                        vec![7.0, 6.0],
                        vec!["B-from-s2", "E-from-s2"],
                        vec!["B", "E"],
                    )),
                ],
                vec![Column::from_name("id")],
                10,
            )
            .await
            .expect("relative score fusion should succeed");

        let rows = fused_rows(result).await;
        let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();

        // B is top of the two inputs that returned it; A is the only (so best) candidate of
        // its input; D and E are the bottom of theirs and absent from the others.
        assert_eq!(ids, vec!["B", "A", "D", "E"], "got {rows:?}");
        assert_close(rows[0].1, 2.0, "B's fused score");
        assert_close(rows[1].1, 1.0, "A's fused score");
        assert_close(rows[2].1, 0.0, "D's fused score");
        assert_close(rows[3].1, 0.0, "E's fused score");
    }

    /// The normalisation window is the whole input, not one batch: a candidate's score must
    /// normalise against every score its input returned, however the input chose to batch them.
    #[tokio::test]
    async fn relative_score_fusion_normalises_across_all_batches_of_an_input() {
        let split = VectorSearchGenerationResult {
            data: stream_from_batches(vec![
                candidate_batch(vec![], vec![], vec![]),
                candidate_batch(vec![10.0], vec!["A-from-s0"], vec!["A"]),
                candidate_batch(
                    vec![6.0, 2.0],
                    vec!["B-from-s0", "C-from-s0"],
                    vec!["B", "C"],
                ),
            ]),
            derived_from: "body".to_string(),
        };

        let result = RelativeScoreFusion::new()
            .aggregate(
                vec![
                    split,
                    candidate_input(candidate_batch(
                        vec![1.0, 0.0],
                        vec!["C-from-s1", "A-from-s1"],
                        vec!["C", "A"],
                    )),
                ],
                vec![Column::from_name("id")],
                10,
            )
            .await
            .expect("relative score fusion should succeed");

        let rows = fused_rows(result).await;
        let scored: HashMap<String, f64> = rows.iter().cloned().collect();

        // First input spans [2, 10]: A -> 1.0, B -> 0.5, C -> 0.0. Second spans [0, 1]:
        // C -> 1.0, A -> 0.0. A batch boundary between A and B must not change any of them.
        assert_close(scored["A"], 1.0, "A's fused score");
        assert_close(scored["B"], 0.5, "B's fused score");
        assert_close(scored["C"], 1.0, "C's fused score");
        assert!(
            rows.iter().any(|(id, _)| id == "B"),
            "expected a candidate that only the later batch carried, got {rows:?}"
        );
    }

    #[tokio::test]
    async fn relative_score_fusion_weights_favour_one_input() {
        let inputs = || {
            vec![
                candidate_input(candidate_batch(
                    vec![1.00, 0.90, 0.00],
                    vec!["A-vector", "B-vector", "C-vector"],
                    vec!["A", "B", "C"],
                )),
                candidate_input(candidate_batch(
                    vec![1.00, 0.95, 0.00],
                    vec!["C-keyword", "B-keyword", "A-keyword"],
                    vec!["C", "B", "A"],
                )),
            ]
        };

        let result = RelativeScoreFusion::with_weights(vec![1.0, 0.0])
            .aggregate(inputs(), vec![Column::from_name("id")], 10)
            .await
            .expect("relative score fusion should succeed");

        let rows = fused_rows(result).await;
        let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();

        // Silencing the keyword input leaves the vector input's own normalised order.
        assert_eq!(ids, vec!["A", "B", "C"], "got {rows:?}");
        assert_close(rows[0].1, 1.0, "A's fused score");
        assert_close(rows[1].1, 0.9, "B's fused score");
        assert_close(rows[2].1, 0.0, "C's fused score");
    }

    #[tokio::test]
    async fn relative_score_fusion_returns_a_single_input_directly() {
        let result = RelativeScoreFusion::new()
            .aggregate(
                vec![candidate_input(candidate_batch(
                    vec![3.0, 1.0],
                    vec!["A-only", "B-only"],
                    vec!["A", "B"],
                ))],
                vec![Column::from_name("id")],
                10,
            )
            .await
            .expect("a single input should pass through");

        let rows = fused_rows(result).await;
        let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();

        // Nothing to fuse against, so the input's own scores are returned unnormalised.
        assert_eq!(ids, vec!["A", "B"], "got {rows:?}");
        assert_close(rows[0].1, 3.0, "A's score");
        assert_close(rows[1].1, 1.0, "B's score");
    }

    #[tokio::test]
    async fn relative_score_fusion_requires_a_primary_key_to_fuse_inputs() {
        let err = RelativeScoreFusion::new()
            .aggregate(
                vec![
                    candidate_input(candidate_batch(vec![1.0], vec!["A-s0"], vec!["A"])),
                    candidate_input(candidate_batch(vec![1.0], vec!["A-s1"], vec!["A"])),
                ],
                vec![],
                10,
            )
            .await
            .expect_err("fusing without a primary key should be rejected");

        assert!(
            matches!(err, Error::NoPrimaryKey),
            "expected a user-facing missing primary key error, got {err:?}"
        );
        assert!(
            err.is_user_error(),
            "missing primary key is the user's to fix"
        );
    }

    #[tokio::test]
    async fn relative_score_fusion_rejects_candidates_without_a_score_column() {
        let unscored = RecordBatch::try_from_iter(vec![
            (
                SEARCH_VALUE_COLUMN_NAME,
                Arc::new(StringArray::from(vec!["A-s0"])) as _,
            ),
            ("id", Arc::new(StringArray::from(vec!["A"])) as _),
        ])
        .expect("valid record batch");

        let err = RelativeScoreFusion::new()
            .aggregate(
                vec![
                    VectorSearchGenerationResult {
                        data: stream_from_batch(unscored),
                        derived_from: "body".to_string(),
                    },
                    candidate_input(candidate_batch(vec![1.0], vec!["A-s1"], vec!["A"])),
                ],
                vec![Column::from_name("id")],
                10,
            )
            .await
            .expect_err("a candidate stream without a score column should be rejected");

        assert!(
            matches!(err, Error::CandidateMissingRequiredColumn { .. }),
            "expected a missing required column error, got {err:?}"
        );
    }

    #[test]
    fn relative_score_fusion_keeps_the_user_visible_limit_as_the_candidate_pool() {
        // Unlike RRF, the normalisation window is the candidate set itself, so the pool is
        // not widened past the requested limit.
        assert_eq!(RelativeScoreFusion::new().candidate_pool_size(10), 10);
    }
}
