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

//! TPC-H tables generated in-process by `tpchgen`, for runs at a scale factor
//! the suite ships no data for (`--scale-factor`).
//!
//! `tpchgen` reproduces the TPC's reference `dbgen` row for row. The suite's
//! SF 0.01 CSVs come from `DuckDB`'s `dbgen` port instead, which agrees on every
//! key, number and date but generates different comments and addresses, so
//! the suite's goldens do not describe generated rows: a generated run
//! compares against goldens computed on those same rows (`--write-data`,
//! `scripts/generate_expected.py`).
//!
//! Every column takes the suite's Isthmus type from `schema.rs`, and three
//! conversions carry that:
//!
//! - `l_quantity` is an integer count in `tpchgen` and `decimal(15,2)` in the
//!   plans, so it is scaled by 100 rather than widened; widened, every
//!   `sum(l_quantity)` would come back a hundred times too small.
//! - `TPCHDecimal` holds hundredths in an `i64`, which is `Decimal128(15, 2)`'s
//!   raw value as it stands, with no floating point in between.
//! - Keys are `i64` in `tpchgen` and `i32` in the plans. A key past `i32::MAX`
//!   (`o_orderkey` above scale factor ~357) is a `KeyOutOfRange` error, never a
//!   wrapped value.

use std::fmt::{Display, Write as _};
use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayBuilder, ArrayRef, Date32Builder, Decimal128Builder, Int32Builder, StringBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt, ensure};
use tokio::task::JoinSet;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

use crate::error::{self, Result};
use crate::schema::{TPCH_TABLES, TpchTable, schema_for};

/// Rows per generated batch: `DataFusion`'s default batch size.
const BATCH_ROWS: usize = 8192;

/// One TPC-H table at a scale factor, split into the partitions it was
/// generated in. Partition order is row order.
pub struct GeneratedTable {
    pub table: TpchTable,
    pub schema: SchemaRef,
    pub partitions: Vec<Vec<RecordBatch>>,
}

impl GeneratedTable {
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.partitions
            .iter()
            .flatten()
            .map(RecordBatch::num_rows)
            .sum()
    }
}

/// Reject a `--scale-factor` that is not a positive finite number.
///
/// Called from `main` immediately after parse: `0` and `-1` map to
/// `expected/sf0` and `expected/sf-1`, which are not committed, so a
/// missing-directory check would fire first and hide this error.
pub(crate) fn require_positive_scale_factor(scale_factor: f64) -> Result<()> {
    ensure!(
        scale_factor.is_finite() && scale_factor > 0.0,
        error::InvalidScaleFactorSnafu { scale_factor }
    );
    Ok(())
}

/// Generate all eight TPC-H tables at `scale_factor`, each split `parts` ways
/// and generated in parallel.
///
/// `nation` and `region` are always one part: their generators ignore the
/// part arguments and return every row, so splitting them would repeat rows.
pub async fn generate(scale_factor: f64, parts: usize) -> Result<Vec<GeneratedTable>> {
    require_positive_scale_factor(scale_factor)?;
    let parts = i32::try_from(parts.max(1)).unwrap_or(i32::MAX);

    let mut tasks = JoinSet::new();
    let mut tables = Vec::with_capacity(TPCH_TABLES.len());
    for (index, table) in TPCH_TABLES.iter().copied().enumerate() {
        let schema = schema_for(table.file_stem).context(error::UnknownTableSnafu {
            name: table.file_stem,
            test_id: String::new(),
        })?;
        let part_count = if has_fixed_rows(table) { 1 } else { parts };
        for part in 1..=part_count {
            let schema = Arc::clone(&schema);
            tasks.spawn_blocking(move || {
                generate_part(table, &schema, scale_factor, part, part_count)
                    .map(|batches| (index, part, batches))
            });
        }
        tables.push(GeneratedTable {
            table,
            schema,
            partitions: Vec::new(),
        });
    }

    let mut generated: Vec<Vec<(i32, Vec<RecordBatch>)>> = std::iter::repeat_with(Vec::new)
        .take(tables.len())
        .collect();
    while let Some(joined) = tasks.join_next().await {
        let (index, part, batches) = joined.context(error::GenerateTaskSnafu)??;
        if let Some(parts) = generated.get_mut(index) {
            parts.push((part, batches));
        }
    }
    for (table, mut parts) in tables.iter_mut().zip(generated) {
        parts.sort_unstable_by_key(|(part, _)| *part);
        table.partitions = parts.into_iter().map(|(_, batches)| batches).collect();
    }
    Ok(tables)
}

/// Write each table as `<dir>/<table>.csv` in the suite's `data/` layout:
/// pipe-delimited, no header, rows in generation order.
pub fn write_csv(tables: &[GeneratedTable], dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).context(error::WriteFileSnafu { path: dir })?;
    for table in tables {
        let path = dir.join(format!("{}.csv", table.table.file_stem));
        let file = File::create(&path).context(error::WriteFileSnafu { path: &path })?;
        let mut writer = arrow::csv::WriterBuilder::new()
            .with_delimiter(b'|')
            .with_header(false)
            .build(BufWriter::new(file));
        for batch in table.partitions.iter().flatten() {
            writer
                .write(batch)
                .context(error::WriteCsvSnafu { path: &path })?;
        }
        writer
            .into_inner()
            .flush()
            .context(error::WriteFileSnafu { path: &path })?;
    }
    Ok(())
}

fn has_fixed_rows(table: TpchTable) -> bool {
    matches!(table.file_stem, "nation" | "region")
}

/// One part of one table as record batches, columns in `schema.rs` order.
fn generate_part(
    table: TpchTable,
    schema: &SchemaRef,
    scale_factor: f64,
    part: i32,
    part_count: i32,
) -> Result<Vec<RecordBatch>> {
    let mut rows = RowBuilder::new(table, schema)?;
    let (sf, n) = (scale_factor, part_count);
    match table.file_stem {
        "region" => {
            for r in RegionGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.r_regionkey),
                    Cell::Text(&r.r_name),
                    Cell::Text(&r.r_comment),
                ])?;
            }
        }
        "nation" => {
            for r in NationGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.n_nationkey),
                    Cell::Text(&r.n_name),
                    Cell::Key(r.n_regionkey),
                    Cell::Text(&r.n_comment),
                ])?;
            }
        }
        "part" => {
            for r in PartGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.p_partkey),
                    Cell::Text(&r.p_name),
                    Cell::Text(&r.p_mfgr),
                    Cell::Text(&r.p_brand),
                    Cell::Text(&r.p_type),
                    Cell::Int(r.p_size),
                    Cell::Text(&r.p_container),
                    Cell::Money(r.p_retailprice.into_inner()),
                    Cell::Text(&r.p_comment),
                ])?;
            }
        }
        "supplier" => {
            for r in SupplierGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.s_suppkey),
                    Cell::Text(&r.s_name),
                    Cell::Text(&r.s_address),
                    Cell::Key(r.s_nationkey),
                    Cell::Text(&r.s_phone),
                    Cell::Money(r.s_acctbal.into_inner()),
                    Cell::Text(&r.s_comment),
                ])?;
            }
        }
        "partsupp" => {
            for r in PartSuppGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.ps_partkey),
                    Cell::Key(r.ps_suppkey),
                    Cell::Int(r.ps_availqty),
                    Cell::Money(r.ps_supplycost.into_inner()),
                    Cell::Text(&r.ps_comment),
                ])?;
            }
        }
        "customer" => {
            for r in CustomerGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.c_custkey),
                    Cell::Text(&r.c_name),
                    Cell::Text(&r.c_address),
                    Cell::Key(r.c_nationkey),
                    Cell::Text(&r.c_phone),
                    Cell::Money(r.c_acctbal.into_inner()),
                    Cell::Text(&r.c_mktsegment),
                    Cell::Text(&r.c_comment),
                ])?;
            }
        }
        "orders" => {
            for r in OrderGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.o_orderkey),
                    Cell::Key(r.o_custkey),
                    Cell::Text(&r.o_orderstatus),
                    Cell::Money(r.o_totalprice.into_inner()),
                    Cell::Date(r.o_orderdate.to_unix_epoch()),
                    Cell::Text(&r.o_orderpriority),
                    Cell::Text(&r.o_clerk),
                    Cell::Int(r.o_shippriority),
                    Cell::Text(&r.o_comment),
                ])?;
            }
        }
        "lineitem" => {
            for r in LineItemGenerator::new(sf, part, n) {
                rows.append(&[
                    Cell::Key(r.l_orderkey),
                    Cell::Key(r.l_partkey),
                    Cell::Key(r.l_suppkey),
                    Cell::Int(r.l_linenumber),
                    Cell::Quantity(r.l_quantity),
                    Cell::Money(r.l_extendedprice.into_inner()),
                    Cell::Money(r.l_discount.into_inner()),
                    Cell::Money(r.l_tax.into_inner()),
                    Cell::Text(&r.l_returnflag),
                    Cell::Text(&r.l_linestatus),
                    Cell::Date(r.l_shipdate.to_unix_epoch()),
                    Cell::Date(r.l_commitdate.to_unix_epoch()),
                    Cell::Date(r.l_receiptdate.to_unix_epoch()),
                    Cell::Text(&r.l_shipinstruct),
                    Cell::Text(&r.l_shipmode),
                    Cell::Text(&r.l_comment),
                ])?;
            }
        }
        other => {
            return error::UnknownTableSnafu {
                name: other,
                test_id: String::new(),
            }
            .fail();
        }
    }
    rows.finish()
}

/// One generated value, tagged with the column kind it must land in.
enum Cell<'a> {
    /// A `tpchgen` `i64` key for an `i32` column.
    Key(i64),
    Int(i32),
    /// Hundredths, the raw value of a `decimal(15,2)`.
    Money(i64),
    /// A whole-unit count for a `decimal(15,2)` column, which stores hundredths.
    Quantity(i64),
    /// Days since the Unix epoch.
    Date(i32),
    Text(&'a dyn Display),
}

enum Column {
    Int32(Int32Builder),
    Decimal(Decimal128Builder),
    Date32(Date32Builder),
    Utf8(StringBuilder),
}

impl Column {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Column::Int32(b) => Arc::new(b.finish()),
            Column::Decimal(b) => Arc::new(b.finish()),
            Column::Date32(b) => Arc::new(b.finish()),
            Column::Utf8(b) => Arc::new(b.finish()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Column::Int32(b) => b.len(),
            Column::Decimal(b) => b.len(),
            Column::Date32(b) => b.len(),
            Column::Utf8(b) => b.len(),
        }
    }
}

/// Appends rows column by column and cuts a batch every `BATCH_ROWS` rows.
struct RowBuilder {
    table: TpchTable,
    schema: SchemaRef,
    columns: Vec<Column>,
    batches: Vec<RecordBatch>,
}

impl RowBuilder {
    fn new(table: TpchTable, schema: &SchemaRef) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int32 => Ok(Column::Int32(Int32Builder::with_capacity(BATCH_ROWS))),
                DataType::Decimal128(..) => Ok(Column::Decimal(
                    Decimal128Builder::with_capacity(BATCH_ROWS)
                        .with_data_type(field.data_type().clone()),
                )),
                DataType::Date32 => Ok(Column::Date32(Date32Builder::with_capacity(BATCH_ROWS))),
                DataType::Utf8 => Ok(Column::Utf8(StringBuilder::new())),
                other => error::UnsupportedColumnTypeSnafu {
                    table: table.file_stem,
                    column: field.name(),
                    data_type: other.to_string(),
                }
                .fail(),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            table,
            schema: Arc::clone(schema),
            columns,
            batches: Vec::new(),
        })
    }

    fn append(&mut self, cells: &[Cell<'_>]) -> Result<()> {
        ensure!(
            cells.len() == self.columns.len(),
            error::RowWidthSnafu {
                table: self.table.file_stem,
                actual: cells.len(),
                expected: self.columns.len(),
            }
        );
        for ((column, cell), field) in self
            .columns
            .iter_mut()
            .zip(cells)
            .zip(self.schema.fields().iter())
        {
            match (column, cell) {
                (Column::Int32(b), Cell::Key(value)) => {
                    let key = i32::try_from(*value)
                        .ok()
                        .context(error::KeyOutOfRangeSnafu {
                            table: self.table.file_stem,
                            column: field.name(),
                            value: *value,
                        })?;
                    b.append_value(key);
                }
                (Column::Int32(b), Cell::Int(value)) => b.append_value(*value),
                (Column::Decimal(b), Cell::Money(value)) => b.append_value(i128::from(*value)),
                // Widened before scaling, so no `i64` count can overflow.
                (Column::Decimal(b), Cell::Quantity(units)) => {
                    b.append_value(i128::from(*units) * 100);
                }
                (Column::Date32(b), Cell::Date(days)) => b.append_value(*days),
                (Column::Utf8(b), Cell::Text(value)) => {
                    write!(b, "{value}").ok().context(error::FormatTextSnafu {
                        table: self.table.file_stem,
                        column: field.name(),
                    })?;
                    // Completes the value `write!` streamed into the builder.
                    b.append_value("");
                }
                _ => {
                    return error::CellTypeMismatchSnafu {
                        table: self.table.file_stem,
                        column: field.name(),
                        data_type: field.data_type().to_string(),
                    }
                    .fail();
                }
            }
        }
        if self.columns.first().is_some_and(|c| c.len() >= BATCH_ROWS) {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.columns.first().is_none_or(|c| c.len() == 0) {
            return Ok(());
        }
        let arrays = self.columns.iter_mut().map(Column::finish).collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays).context(
            error::BuildBatchSnafu {
                table: self.table.file_stem,
            },
        )?;
        self.batches.push(batch);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<RecordBatch>> {
        self.flush()?;
        Ok(self.batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows per table at SF 0.01, as the suite's `metadata.yaml` and the TPC-H
    /// specification's cardinalities state them.
    const SF_001_ROWS: &[(&str, usize)] = &[
        ("region", 5),
        ("nation", 25),
        ("part", 2_000),
        ("supplier", 100),
        ("partsupp", 8_000),
        ("customer", 1_500),
        ("orders", 15_000),
        ("lineitem", 60_175),
    ];

    fn table<'a>(tables: &'a [GeneratedTable], stem: &str) -> &'a GeneratedTable {
        tables
            .iter()
            .find(|t| t.table.file_stem == stem)
            .expect("generated table present")
    }

    fn csv_lines(generated: &GeneratedTable) -> Vec<String> {
        let mut out = Vec::new();
        {
            let mut writer = arrow::csv::WriterBuilder::new()
                .with_delimiter(b'|')
                .with_header(false)
                .build(&mut out);
            for batch in generated.partitions.iter().flatten() {
                writer.write(batch).expect("write csv");
            }
        }
        String::from_utf8(out)
            .expect("utf-8 csv")
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[tokio::test]
    async fn sf_001_matches_the_suite_cardinalities() {
        let tables = generate(0.01, 3).await.expect("generate SF 0.01");
        assert_eq!(tables.len(), TPCH_TABLES.len());
        for (stem, rows) in SF_001_ROWS {
            assert_eq!(table(&tables, stem).num_rows(), *rows, "{stem} row count");
        }
    }

    /// `nation` and `region` generators ignore the part arguments, so a split
    /// run must not repeat their rows once per part.
    #[tokio::test]
    async fn fixed_tables_are_not_repeated_per_part() {
        let tables = generate(0.01, 4).await.expect("generate SF 0.01");
        assert_eq!(table(&tables, "nation").num_rows(), 25);
        assert_eq!(table(&tables, "nation").partitions.len(), 1);
        assert_eq!(table(&tables, "region").num_rows(), 5);
    }

    /// A split generation yields the rows of a single-part generation, in
    /// the same order, so the CSV a golden was computed on and the tables a
    /// run registers cannot diverge on partitioning.
    #[tokio::test]
    async fn split_generation_equals_single_part_generation() {
        let single = generate(0.01, 1).await.expect("single part");
        let split = generate(0.01, 5).await.expect("five parts");
        for stem in ["lineitem", "orders", "partsupp"] {
            assert!(table(&split, stem).partitions.len() > 1, "{stem} is split");
            assert_eq!(
                csv_lines(table(&single, stem)),
                csv_lines(table(&split, stem)),
                "{stem} rows"
            );
        }
    }

    /// Every column but the comment of the suite's first SF 0.01 `lineitem`
    /// and `orders` rows, which both `dbgen`s agree on: `l_quantity` as
    /// `17.00` (scaled, not widened), money at two places, and ISO dates. The
    /// comments are where `DuckDB`'s port and the reference `dbgen` differ.
    #[tokio::test]
    async fn first_rows_match_the_suite_csvs_before_the_comment() {
        let tables = generate(0.01, 2).await.expect("generate SF 0.01");
        let lineitem = &csv_lines(table(&tables, "lineitem"))[0];
        assert!(
            lineitem.starts_with(
                "1|1552|93|1|17.00|24710.35|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|"
            ),
            "{lineitem}"
        );
        let orders = &csv_lines(table(&tables, "orders"))[0];
        assert!(
            orders.starts_with("1|370|O|172799.49|1996-01-02|5-LOW|Clerk#000000951|0|"),
            "{orders}"
        );
    }

    #[tokio::test]
    async fn rejects_a_scale_factor_that_is_not_positive() {
        for scale_factor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let err = generate(scale_factor, 1)
                .await
                .err()
                .expect("invalid scale factor");
            assert!(
                matches!(err, error::Error::InvalidScaleFactor { .. }),
                "{scale_factor}: {err}"
            );
        }
    }

    /// A key past `i32::MAX` is an error naming the column, never a wrapped
    /// `i32` that would silently join the wrong rows.
    #[test]
    fn key_past_i32_is_an_error() {
        let region = TPCH_TABLES
            .iter()
            .copied()
            .find(|t| t.file_stem == "region")
            .expect("region is catalogued");
        let schema = schema_for("region").expect("region schema");
        let mut rows = RowBuilder::new(region, &schema).expect("builder");
        let err = rows
            .append(&[
                Cell::Key(i64::from(i32::MAX) + 1),
                Cell::Text(&"AFRICA"),
                Cell::Text(&"comment"),
            ])
            .expect_err("key out of range");
        assert!(
            matches!(err, error::Error::KeyOutOfRange { ref column, value, .. } if column == "R_REGIONKEY" && value == i64::from(i32::MAX) + 1),
            "{err}"
        );
    }
}
