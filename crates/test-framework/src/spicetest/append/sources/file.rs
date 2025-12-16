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
use std::path::{Path, PathBuf};

use anyhow::Result;
use duckdb::Connection;
use std::fmt::Write;
use tokio::fs;
use tonic::async_trait;

use crate::{
    queries::{QuerySet, TableWithTimeColumn},
    spicetest::append::worker::AppendConfig,
};

use super::AppendableSource;

// Large offset for retention test data primary keys to avoid conflicts with real data.
// TPC-H SF=1 generates max ~6M rows, so 1 billion offset is safe.
const TEMP_DATA_KEY_OFFSET: i64 = 1_000_000_000;

// Retention timestamp: NOW() - 1 day + 1 minute
// With retention_period=1d, this data will expire ~1 minute after insertion.
// On first refresh, there's no max timestamp so ALL data is loaded (including this old data).
// Retention should be configured to 1 day for tests using this data.
// Using TIMESTAMPTZ to ensure timezone is preserved.
const RETENTION_TIMESTAMP_EXPR: &str =
    "(current_timestamp - INTERVAL '1 day' + INTERVAL '1 minute')::TIMESTAMPTZ";

/// TPC-H table name to primary key column mapping for retention data offset.
const TPCH_PRIMARY_KEYS: &[(&str, &str)] = &[
    ("customer", "c_custkey"),
    ("orders", "o_orderkey"),
    ("lineitem", "l_orderkey"),
    ("part", "p_partkey"),
    ("partsupp", "ps_partkey"),
    ("supplier", "s_suppkey"),
    ("nation", "n_nationkey"),
    ("region", "r_regionkey"),
];

/// Returns the primary key column for a TPC-H table, if known.
fn tpch_primary_key(table_name: &str) -> Option<&'static str> {
    TPCH_PRIMARY_KEYS
        .iter()
        .find(|(t, _)| *t == table_name)
        .map(|(_, pk)| *pk)
}

/// Generates SQL for TPC-H initial setup (step 0).
/// Unlike `generate_tpch_sql`, this creates fresh tables rather than appending to existing ones.
fn generate_tpch_setup_sql(
    load_steps: u16,
    generate_retention_data: bool,
    tables: &[TableWithTimeColumn],
    temp_directory: &Path,
) -> String {
    let mut sql = format!(
        "INSTALL tpch;
         LOAD tpch;
         BEGIN;
         CALL dbgen(sf=1, children={load_steps}, step=0);\n"
    );

    // Generate retention test data during initial setup so it's included in first load
    if generate_retention_data {
        writeln!(
            &mut sql,
            "CALL dbgen(sf=1, children={load_steps}, step=0, suffix='_retention');"
        )
        .ok();
    }

    for TableWithTimeColumn { name, column } in tables {
        let parquet_path = temp_directory.join(format!("{name}.parquet"));

        // Add timestamp column with current timestamp for main data
        writeln!(
            &mut sql,
            "ALTER TABLE {name} ADD COLUMN {column} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;"
        )
        .ok();

        // Retention test data: offset primary keys + old timestamp for deletion testing
        if generate_retention_data {
            writeln!(
                sql,
                "ALTER TABLE {name}_retention ADD COLUMN {column} TIMESTAMPTZ;"
            )
            .ok();
            // Offset primary keys and set old timestamp in single UPDATE
            if let Some(pk_col) = tpch_primary_key(name) {
                writeln!(
                    sql,
                    "UPDATE {name}_retention SET {pk_col} = {pk_col} + {TEMP_DATA_KEY_OFFSET}, {column} = {RETENTION_TIMESTAMP_EXPR};"
                )
                .ok();
            } else {
                writeln!(
                    sql,
                    "UPDATE {name}_retention SET {column} = {RETENTION_TIMESTAMP_EXPR};"
                )
                .ok();
            }

            writeln!(
                sql,
                "INSERT INTO {name} SELECT * FROM {name}_retention;
                DROP TABLE {name}_retention;"
            )
            .ok();
        }

        writeln!(
            &mut sql,
            "COPY {name} TO '{}' (FORMAT 'parquet');",
            parquet_path.to_string_lossy()
        )
        .ok();
    }

    sql += "COMMIT;";
    sql
}

/// Generates SQL for TPC-H append data generation (step 1+).
fn generate_tpch_sql(
    load_steps: u16,
    load_index: u16,
    generate_conflict_data: bool,
    tables: &[TableWithTimeColumn],
    temp_directory: &Path,
) -> String {
    let mut sql = format!(
        "INSTALL tpch;
         LOAD tpch;
         BEGIN;
         CALL dbgen(sf=1, children={load_steps}, step={load_index}, suffix='_new');\n"
    );

    // Generate conflict data: next step's data with SAME primary keys (will conflict on refresh)
    if generate_conflict_data {
        let next_step = load_index + 1;
        writeln!(
            &mut sql,
            "CALL dbgen(sf=1, children={load_steps}, step={next_step}, suffix='_conflict');"
        )
        .ok();
    }

    for TableWithTimeColumn { name, column } in tables {
        let parquet_path = temp_directory.join(format!("{name}.parquet"));

        // Insert the current step's data with current timestamp
        write!(
            &mut sql,
            "ALTER TABLE {name}_new ADD COLUMN {column} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
                         INSERT INTO {name} SELECT * FROM {name}_new;
                         DROP TABLE {name}_new;\n"
        )
        .ok();

        // Conflict data: same primary keys, will be handled by ON CONFLICT
        if generate_conflict_data {
            write!(&mut sql, "ALTER TABLE {name}_conflict ADD COLUMN {column} TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;
                             INSERT INTO {name} SELECT * FROM {name}_conflict;
                             DROP TABLE {name}_conflict;\n").ok();
        }

        writeln!(
            &mut sql,
            "COPY {name} TO '{}' (FORMAT 'parquet');",
            parquet_path.to_string_lossy()
        )
        .ok();
    }

    sql += "COMMIT;";
    sql
}

/// Generates SQL for TPC-DS append data generation.
fn generate_tpcds_sql(load_steps: u16, load_index: u16, tables: &[TableWithTimeColumn]) -> String {
    let mut sql = "BEGIN;\n".to_string();

    for TableWithTimeColumn { name, column } in tables {
        write!(
            &mut sql,
            "INSERT INTO {name} SELECT *, CURRENT_TIMESTAMP AS {column}
                         FROM {name}_gen
                         LIMIT (SELECT COUNT(*) / {load_steps} FROM {name}_gen)
                         OFFSET (SELECT COUNT(*) / {load_steps} * {load_index} FROM {name}_gen);
                         COPY {name} TO '{name}.parquet' (FORMAT 'parquet');\n"
        )
        .ok();
    }

    sql += "COMMIT;";
    sql
}

/// Generates SQL for `ClickBench` append data generation.
fn generate_clickbench_sql(load_steps: u16, load_index: u16) -> String {
    format!(
        "BEGIN;
         INSERT INTO hits_delayed SELECT *, CURRENT_TIMESTAMP AS created_at
         FROM hits
         LIMIT (SELECT COUNT(*) / {load_steps} FROM hits)
         OFFSET (SELECT COUNT(*) / {load_steps} * {load_index} FROM hits);
         COPY hits_delayed TO 'hits_delayed.parquet' (FORMAT 'parquet');
         COMMIT;"
    )
}

pub(crate) struct FileAppendableSource {
    dest_db_file: PathBuf,
    tables: Vec<TableWithTimeColumn>,
}

impl FileAppendableSource {
    pub fn new(config: &AppendConfig) -> Self {
        let dest_db_file = config
            .temp_directory
            .join(format!("./{}_append.db", config.query_set));
        let tables = config.query_set.append_time_columns();

        Self {
            dest_db_file,
            tables,
        }
    }
}

#[async_trait]
impl AppendableSource for FileAppendableSource {
    async fn setup(&self, config: &AppendConfig) -> Result<()> {
        if fs::try_exists(&self.dest_db_file).await? {
            fs::remove_file(&self.dest_db_file).await?;
        }

        for TableWithTimeColumn { name, .. } in &self.tables {
            let parquet_path = config.temp_directory.join(format!("{name}.parquet"));
            if fs::try_exists(&parquet_path).await? {
                fs::remove_file(&parquet_path).await?;
            }
        }

        let dest_db_file = self.dest_db_file.clone();
        let query_set = config.query_set.clone();
        let load_steps = config.load_steps;
        let tables = self.tables.clone();
        let temp_directory = config.temp_directory.clone();
        let generate_retention_data = config.with_retention_data;

        tokio::task::spawn_blocking(move || {
            let dest_conn = Connection::open(&dest_db_file)?;
            println!("Loading initial data for {query_set} benchmark suite (with retention: {generate_retention_data})");
            match query_set {
                QuerySet::Tpch => {
                    let sql = generate_tpch_setup_sql(
                        load_steps,
                        generate_retention_data,
                        &tables,
                        &temp_directory,
                    );
                    dest_conn.execute_batch(&sql)?;
                }
                QuerySet::Tpcds => {
                    let mut setup_sql = "INSTALL tpcds;
                             LOAD tpcds;
                             BEGIN;
                             CALL dsdgen(sf=1, suffix='_gen');\n"
                        .to_string();

                    for TableWithTimeColumn { name, column } in &tables {
                        // DuckDB's TPCDS generation doesn't support partitioning and generating in steps
                        // Instead, generate the whole dataset and load it with incrementally increasing OFFSET and LIMIT
                        write!(&mut setup_sql,
                            "CREATE TABLE {name} AS SELECT * FROM {name}_gen WHERE 1=0;
                             ALTER TABLE {name} ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                             INSERT INTO {name} SELECT *, CURRENT_TIMESTAMP AS {column} FROM {name}_gen
                             LIMIT (SELECT COUNT(*) / {load_steps} FROM {name}_gen) OFFSET 0;
                             COPY {name} TO '{name}.parquet' (FORMAT 'parquet');\n"
                        ).ok();
                    }

                    setup_sql += "COMMIT;";

                    dest_conn.execute_batch(&setup_sql)?;
                }
                QuerySet::Clickbench => {
                    // import the parquet file into the database so we can use it for OFFSET delayed loading
                    // limit to 40 million rows because the file connector goes OOM with the full file
                    let setup_sql = "BEGIN;
                                     CREATE TABLE hits AS SELECT * FROM read_parquet('hits.parquet') LIMIT 40000000;
                                     CREATE TABLE hits_delayed AS SELECT * FROM hits WHERE 1=0;
                                     ALTER TABLE hits_delayed ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                                     COMMIT;";

                    dest_conn.execute_batch(setup_sql)?;
                }
                QuerySet::Scenario { .. } | QuerySet::ParameterizedTpch => unimplemented!("Appendable file source is not implemented for Scenario or Parameterized TPC-H query sets"),
            }

            drop(dest_conn);

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to spawn blocking task: {e}"))??;

        Ok(())
    }

    async fn generate(&self, config: &AppendConfig, load_index: u16) -> Result<()> {
        // If conflict testing is enabled and not the last step, also generate next step's data
        // This creates conflicts that should be resolved by the next append operation
        let generate_conflict_test_data =
            config.with_conflict_data && load_index < config.load_steps - 1;

        println!(
            "Loading append data (with conflict: {generate_conflict_test_data}) {query_set} benchmark suite - {load_index}/{load_steps}",
            query_set = config.query_set,
            load_steps = config.load_steps,
            load_index = load_index + 1, // display index is 1-based
        );

        if generate_conflict_test_data && !matches!(config.query_set, QuerySet::Tpch) {
            anyhow::bail!("Generating conflict test data is only supported for TPC-H datasets");
        }

        let dest_db_file = self.dest_db_file.clone();
        let query_set = config.query_set.clone();
        let load_steps = config.load_steps;
        let tables = self.tables.clone();
        let temp_directory = config.temp_directory.clone();

        tokio::task::spawn_blocking(move || {
            let dest_conn = Connection::open(&dest_db_file)?;

            let sql = match query_set {
                QuerySet::Tpch => generate_tpch_sql(
                    load_steps,
                    load_index,
                    generate_conflict_test_data,
                    &tables,
                    &temp_directory,
                ),
                QuerySet::Tpcds => generate_tpcds_sql(load_steps, load_index, &tables),
                QuerySet::Clickbench => generate_clickbench_sql(load_steps, load_index),
                QuerySet::Scenario { .. } | QuerySet::ParameterizedTpch => {
                    unimplemented!("Appendable file source is not implemented for Scenario or Parameterized query sets")
                }
            };

            dest_conn.execute_batch(&sql)?;

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to spawn blocking task: {e}"))??;

        Ok(())
    }

    async fn teardown(&self, _worker: &AppendConfig) -> Result<()> {
        Ok(())
    }
}
