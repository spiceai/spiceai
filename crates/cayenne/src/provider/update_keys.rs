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

//! An `UPDATE` that gives a row a primary key another row keeps fails and changes
//! nothing, as in PostgreSQL (#14576), rather than one row replacing the other.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::row::{RowConverter, SortField};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit};
use futures::TryStreamExt;

/// Checks the rows an `UPDATE` will write, before it deletes anything: no new key
/// is NULL, is shared by two updated rows, or is the key of a row the statement
/// leaves in place.
#[derive(Debug)]
pub(crate) struct UpdateKeyCheck {
    dataset: String,
    primary_key: Vec<String>,
    /// The keys of the rows the statement does not update.
    untouched: LogicalPlan,
    state: SessionState,
}

impl UpdateKeyCheck {
    /// The check for an `UPDATE` of `scan` that sets the `assigned` columns on the
    /// rows matching `filter`, keyed on `primary_key`; `None` when no assignment
    /// sets a key column, so every updated row keeps its own key.
    pub(crate) fn new(
        dataset: &str,
        scan: LogicalPlan,
        primary_key: &[String],
        assigned: &[&str],
        filter: Option<Expr>,
        state: SessionState,
    ) -> Result<Option<Self>> {
        if !primary_key
            .iter()
            .any(|column| assigned.contains(&column.as_str()))
        {
            return Ok(None);
        }
        // A row whose filter is NULL is not updated, so it keeps its key.
        let untouched = LogicalPlanBuilder::from(scan)
            .filter(filter.unwrap_or_else(|| lit(true)).is_not_true())?
            .project(primary_key.iter().map(|column| col(column.as_str())))?
            .build()?;
        Ok(Some(Self {
            dataset: dataset.to_string(),
            primary_key: primary_key.to_vec(),
            untouched,
            state,
        }))
    }
}

#[async_trait::async_trait]
impl data_components::update::UpdateValidator for UpdateKeyCheck {
    async fn validate(&self, rows: &[RecordBatch], context: Arc<TaskContext>) -> Result<()> {
        let Some(schema) = rows.first().map(RecordBatch::schema) else {
            return Ok(());
        };
        let indices = self
            .primary_key
            .iter()
            .map(|column| schema.index_of(column))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let converter = RowConverter::new(
            indices
                .iter()
                .map(|&index| SortField::new(schema.field(index).data_type().clone()))
                .collect(),
        )?;

        // The updated rows by new key, and the rows whose new key is NULL.
        let mut copies: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut null = 0_u64;
        for batch in rows {
            let keys: Vec<_> = indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect();
            let encoded = converter.convert_columns(&keys)?;
            for row in 0..batch.num_rows() {
                if keys.iter().any(|key| key.is_null(row)) {
                    null += 1;
                } else {
                    *copies
                        .entry(encoded.row(row).as_ref().to_vec())
                        .or_default() += 1;
                }
            }
        }
        if null > 0 {
            return Err(DataFusionError::Execution(null_key_message(
                &self.dataset,
                &self.primary_key,
                null,
            )));
        }

        let mut stored = 0_u64;
        let mut untouched = execute_stream(
            self.state.create_physical_plan(&self.untouched).await?,
            context,
        )?;
        while let Some(batch) = untouched.try_next().await? {
            let encoded = converter.convert_columns(batch.columns())?;
            for row in 0..batch.num_rows() {
                if let Some(count) = copies.remove(encoded.row(row).as_ref()) {
                    stored += count;
                }
            }
        }
        let among = copies.values().filter(|&&count| count > 1).sum::<u64>();
        if stored > 0 || among > 0 {
            return Err(DataFusionError::Execution(key_collision_message(
                &self.dataset,
                &self.primary_key,
                stored,
                among,
            )));
        }
        Ok(())
    }
}

/// The key `primary_key` names, as a message quotes it.
fn quoted_key(primary_key: &[String]) -> String {
    match primary_key {
        [column] => format!("'{column}'"),
        columns => format!("'({})'", columns.join(", ")),
    }
}

fn rows(count: u64) -> String {
    if count == 1 {
        "1 row".to_string()
    } else {
        format!("{count} rows")
    }
}

/// The error for an `UPDATE` of `dataset` that would give `stored` rows the key of a
/// row it does not update, and `among` rows the key of another updated row.
pub(crate) fn key_collision_message(
    dataset: &str,
    primary_key: &[String],
    stored: u64,
    among: u64,
) -> String {
    let key = quoted_key(primary_key);
    if stored > 0 {
        let verb = if stored == 1 { "matches" } else { "match" };
        format!(
            "Failed to update dataset '{dataset}': the new {key} of {} {verb} a key already stored, so nothing was changed.",
            rows(stored)
        )
    } else {
        format!(
            "Failed to update dataset '{dataset}': {} would get the same new {key}, so nothing was changed.",
            rows(among)
        )
    }
}

/// The error for an `UPDATE` of `dataset` that would give `null` rows a NULL key.
pub(crate) fn null_key_message(dataset: &str, primary_key: &[String], null: u64) -> String {
    let verb = if null == 1 { "is" } else { "are" };
    format!(
        "Failed to update dataset '{dataset}': the new {} of {} {verb} NULL, and a primary key cannot be NULL, so nothing was changed.",
        quoted_key(primary_key),
        rows(null)
    )
}

#[cfg(test)]
mod tests {
    use super::{key_collision_message, null_key_message};

    #[test]
    fn the_message_names_the_dataset_the_key_and_the_rows() {
        let id = ["id".to_string()];
        assert_eq!(
            key_collision_message("events", &id, 1, 0),
            "Failed to update dataset 'events': the new 'id' of 1 row matches a key already stored, so nothing was changed."
        );
        assert_eq!(
            key_collision_message("events", &id, 3, 2),
            "Failed to update dataset 'events': the new 'id' of 3 rows match a key already stored, so nothing was changed."
        );
        assert_eq!(
            key_collision_message("events", &["region".to_string(), "id".to_string()], 0, 2),
            "Failed to update dataset 'events': 2 rows would get the same new '(region, id)', so nothing was changed."
        );
        assert_eq!(
            null_key_message("events", &id, 1),
            "Failed to update dataset 'events': the new 'id' of 1 row is NULL, and a primary key cannot be NULL, so nothing was changed."
        );
    }
}
