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

use std::path::PathBuf;

use anyhow::Result;
use duckdb::Connection;
use tokio::fs;
use tonic::async_trait;

use crate::{
    queries::{QuerySet, TableWithTimeColumn},
    spicetest::append::worker::AppendConfig,
};

use super::AppendableSource;

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
        let query_set = config.query_set;
        let load_steps = config.load_steps;
        let tables = self.tables.clone();
        let temp_directory = config.temp_directory.clone();

        tokio::task::spawn_blocking(move || {
            let dest_conn = Connection::open(&dest_db_file)?;
            println!(
                "Loading initial data for {} benchmark suite",
                query_set
            );
            match query_set {
                QuerySet::Tpch => {
                    let mut sql = format!(
                        "INSTALL tpch;
                         LOAD tpch;
                         BEGIN;
                         CALL dbgen(sf=1, children={load_steps}, step=0);\n",
                        load_steps = load_steps
                    );

                    for TableWithTimeColumn { name, column } in &tables {
                        let parquet_path = temp_directory.join(format!("{name}.parquet"));
                        sql += &format!(
                                    "ALTER TABLE {name} ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                             COPY {name} TO '{parquet_path}' (FORMAT 'parquet');\n", parquet_path = parquet_path.to_string_lossy());
                    }

                    sql += "COMMIT;";

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
                        setup_sql += &format!(
                            "CREATE TABLE {name} AS SELECT * FROM {name}_gen WHERE 1=0;
                             ALTER TABLE {name} ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                             INSERT INTO {name} SELECT *, CURRENT_TIMESTAMP AS {column} FROM {name}_gen
                             LIMIT (SELECT COUNT(*) / {load_steps} FROM {name}_gen) OFFSET 0;
                             COPY {name} TO '{name}.parquet' (FORMAT 'parquet');\n",
                            load_steps = load_steps
                        );
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
                QuerySet::ParameterizedTpch => todo!(),
            }

            drop(dest_conn);

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("Failed to spawn blocking task: {e}"))??;

        Ok(())
    }

    async fn generate(&self, config: &AppendConfig, load_index: u16) -> Result<()> {
        println!(
            "Loading append data for {query_set} benchmark suite - {load_index}/{load_steps}",
            query_set = config.query_set,
            load_steps = config.load_steps,
            load_index = load_index + 1, // display index is 1-based
        );

        let dest_db_file = self.dest_db_file.clone();
        let query_set = config.query_set;
        let load_steps = config.load_steps;
        let tables = self.tables.clone();
        let temp_directory = config.temp_directory.clone();

        tokio::task::spawn_blocking(move || {
            let dest_conn = Connection::open(&dest_db_file)?;

            match query_set {
                QuerySet::Tpch => {
                    let mut sql = format!(
                        "INSTALL tpch;
                         LOAD tpch;
                         BEGIN;
                         CALL dbgen(sf=1, children={load_steps}, step={load_index}, suffix='_new');\n",
                        load_steps = load_steps
                    );

                    for TableWithTimeColumn { name, column } in &tables {
                        let parquet_path = temp_directory.join(format!("{name}.parquet"));
                        sql += &format!("ALTER TABLE {name}_new ADD COLUMN {column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                                         INSERT INTO {name} SELECT * FROM {name}_new;
                                         DROP TABLE {name}_new;
                                         COPY {name} TO '{parquet_path}' (FORMAT 'parquet');\n", parquet_path = parquet_path.to_string_lossy());
                    }

                    sql += "COMMIT;";

                    dest_conn.execute_batch(&sql)?;
                }
                QuerySet::Tpcds => {
                    let mut sql = "BEGIN;\n".to_string();

                    for TableWithTimeColumn { name, column } in &tables {
                        sql += &format!("INSERT INTO {name} SELECT *, CURRENT_TIMESTAMP AS {column}
                                         FROM {name}_gen
                                         LIMIT (SELECT COUNT(*) / {load_steps} FROM {name}_gen)
                                         OFFSET (SELECT COUNT(*) / {load_steps} * {load_index} FROM {name}_gen);
                                         COPY {name} TO '{name}.parquet' (FORMAT 'parquet');\n",
                                load_steps = load_steps);
                    }

                    sql += "COMMIT;";

                    dest_conn.execute_batch(&sql)?;
                }
                QuerySet::Clickbench => {
                    let sql = format!("BEGIN;
                                       INSERT INTO hits_delayed SELECT *, CURRENT_TIMESTAMP AS created_at
                                       FROM hits
                                       LIMIT (SELECT COUNT(*) / {load_steps} FROM hits)
                                       OFFSET (SELECT COUNT(*) / {load_steps} * {load_index} FROM hits);
                                       COPY hits_delayed TO 'hits_delayed.parquet' (FORMAT 'parquet');
                                       COMMIT;",
                                                    load_steps = load_steps);

                    dest_conn.execute_batch(&sql)?;
                }
                QuerySet::ParameterizedTpch => todo!(),
            }

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
