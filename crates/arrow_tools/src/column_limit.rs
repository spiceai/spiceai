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

//! The widest schema a dataset's source may report.

use std::fmt;

/// The most top-level columns a dataset's source schema may have.
///
/// Spice's cost per column is not linear everywhere. Inferring a file's schema
/// ends in `arrow_schema::Schema::try_merge`, which searches the fields merged so
/// far by name for every field it adds, so a single file of `n` columns costs
/// `O(n²)` before the dataset registers; and every query against the table plans
/// over its whole schema. A source reporting an extreme column count therefore
/// costs CPU out of all proportion to its size — a 0.9 MB CSV of 100,000 columns
/// pins a core for about a minute while loading.
///
/// 10,000 is the per-table column limit of `BigQuery`, and well above those of
/// `PostgreSQL` (1,600), `MySQL` (4,096), `SQL Server` (1,024) and `Oracle` (1,000),
/// so real tables fit under it. At this width a debug build still loads a CSV in
/// about a second and answers `SELECT count(*)` in about 5 seconds; one more column
/// is refused in a fraction of a second, instead of the minute a 100,000-column
/// file spends loading and the further half minute that query spends planning.
pub const MAX_SCHEMA_COLUMNS: usize = 10_000;

/// A source schema wider than [`MAX_SCHEMA_COLUMNS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooManyColumns {
    /// The number of columns the source reported.
    pub columns: usize,
    /// The limit it exceeds.
    pub limit: usize,
}

impl fmt::Display for TooManyColumns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "The source schema has {} columns, more than the {} columns a dataset can have. Reduce the number of columns the source provides, for example by splitting it into narrower files or tables. See: https://spiceai.org/docs/components/data-connectors",
            self.columns, self.limit
        )
    }
}

impl std::error::Error for TooManyColumns {}

/// Fails when `columns` exceeds [`MAX_SCHEMA_COLUMNS`].
///
/// # Errors
///
/// Returns [`TooManyColumns`] when `columns` is greater than [`MAX_SCHEMA_COLUMNS`].
pub fn ensure_within_column_limit(columns: usize) -> Result<(), TooManyColumns> {
    if columns > MAX_SCHEMA_COLUMNS {
        return Err(TooManyColumns {
            columns,
            limit: MAX_SCHEMA_COLUMNS,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_limit_is_inclusive() {
        assert_eq!(ensure_within_column_limit(0), Ok(()));
        assert_eq!(ensure_within_column_limit(MAX_SCHEMA_COLUMNS), Ok(()));
        assert_eq!(
            ensure_within_column_limit(MAX_SCHEMA_COLUMNS + 1),
            Err(TooManyColumns {
                columns: MAX_SCHEMA_COLUMNS + 1,
                limit: MAX_SCHEMA_COLUMNS,
            })
        );
    }

    #[test]
    fn the_error_names_the_width_the_limit_and_the_fix() {
        let err =
            ensure_within_column_limit(100_000).expect_err("100,000 columns is over the limit");
        assert_eq!(
            err.to_string(),
            "The source schema has 100000 columns, more than the 10000 columns a dataset can have. \
Reduce the number of columns the source provides, for example by splitting it into narrower files \
or tables. See: https://spiceai.org/docs/components/data-connectors"
        );
    }
}
