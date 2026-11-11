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

//! Mode B: the product path. `spiced` serves the TPC-H tables as Spicepod
//! datasets — accelerated the way `--acceleration-engine`, `--acceleration-mode`
//! and `--layout` ask, or federated from parquet — and runs each suite plan sent
//! as a `FlightSQL` `CommandStatementSubstraitPlan`
//! (`crates/runtime/src/flight/flightsql/statement_substrait_plan.rs`:
//! `get_flight_info` / `do_get` → `from_substrait_plan` → `QueryBuilder`).
//! Answers are compared with the same goldens as Mode A.
//!
//! The suite's Isthmus plans read uppercase names and Spice registers datasets
//! under lowercase ones, so the tables are written with lowercase names and
//! each plan's read names are lowercased to match ([`crate::plan_names`]).

use std::fs::File;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightDescriptor;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementSubstraitPlan, ProstMessageExt, SubstraitPlan};
use bytes::Bytes;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion_substrait::substrait::proto::{Plan, Version};
use futures::TryStreamExt;
use prost::Message;
use snafu::ResultExt;
use test_framework::{
    app::AppBuilder,
    constants::FLIGHT_URL,
    layout::{Layout, apply_layout, benchmark_tables},
    queries::QuerySet,
    spiced::{SpicedInstance, StartRequest},
    spicepod::{
        acceleration::{Acceleration, Mode},
        component::dataset::Dataset,
    },
    spicepod_utils::from_app,
};
use tonic::transport::Channel;

use crate::compare::TableData;
use crate::datagen::GeneratedTable;
use crate::error::{self, Result};
use crate::mode_a::{batches_to_table, case_result, unverifiable};
use crate::plan_names::lowercase_read_names;
use crate::report::{CaseResult, TestStatus};
use crate::suite::LoadedCase;

pub const ENGINE_NAME: &str = "Spice FlightSQL";

/// IBM v0.1.1 q01/q22 plans declare this Substrait release. Used when the
/// plan bytes do not decode to a `Plan.version` (or decode as `0.0.0`).
pub const SUITE_SUBSTRAIT_VERSION: &str = "0.81.0";

/// `FlightSQL` `SubstraitPlan.version` is the Substrait release so a
/// consumer can accept or reject the payload. Prefer the version embedded
/// in the decoded plan; fall back to the pinned suite's declared release.
#[must_use]
pub fn substrait_version_from_plan(plan_bytes: &[u8]) -> String {
    match Plan::decode(plan_bytes) {
        Ok(plan) => plan
            .version
            .as_ref()
            .map(format_substrait_version)
            .filter(|version| version != "0.0.0")
            .unwrap_or_else(|| SUITE_SUBSTRAIT_VERSION.to_string()),
        Err(_) => SUITE_SUBSTRAIT_VERSION.to_string(),
    }
}

#[must_use]
pub fn format_substrait_version(version: &Version) -> String {
    format!(
        "{}.{}.{}",
        version.major_number, version.minor_number, version.patch_number
    )
}

/// Build the exact `FlightSQL` command `spiced` decodes in
/// `statement_substrait_plan`.
#[must_use]
pub fn command_statement_substrait_plan(plan_bytes: &[u8]) -> CommandStatementSubstraitPlan {
    CommandStatementSubstraitPlan {
        plan: Some(SubstraitPlan {
            plan: Bytes::copy_from_slice(plan_bytes),
            version: substrait_version_from_plan(plan_bytes),
        }),
        transaction_id: None,
    }
}

/// Protobuf payload for `FlightDescriptor::new_cmd(...)`.
#[must_use]
pub fn command_bytes(plan_bytes: &[u8]) -> Vec<u8> {
    command_statement_substrait_plan(plan_bytes)
        .as_any()
        .encode_to_vec()
}

/// How `spiced` serves the TPC-H tables.
pub struct ServingOptions {
    pub spiced_path: PathBuf,
    /// How each dataset is accelerated; `None` serves the parquet files
    /// federated.
    pub acceleration: Option<AccelerationOptions>,
    pub ready_wait: Duration,
}

pub struct AccelerationOptions {
    pub engine: String,
    pub mode: Mode,
    pub layout: Option<Layout>,
}

impl ServingOptions {
    /// What the report names as the engine, e.g.
    /// `Spice FlightSQL (cayenne, file, layout primary_key,sort)`.
    #[must_use]
    pub fn engine_description(&self) -> String {
        match &self.acceleration {
            None => format!("{ENGINE_NAME} (federated parquet)"),
            Some(acceleration) => {
                let mode = match acceleration.mode {
                    Mode::Memory => "memory",
                    _ => "file",
                };
                match &acceleration.layout {
                    None => format!("{ENGINE_NAME} ({}, {mode})", acceleration.engine),
                    Some(layout) => {
                        format!(
                            "{ENGINE_NAME} ({}, {mode}, layout {layout})",
                            acceleration.engine
                        )
                    }
                }
            }
        }
    }
}

/// A `spiced` serving the TPC-H tables, stopped when dropped.
pub struct SpicedEngine {
    spiced: SpicedInstance,
    client: FlightServiceClient<Channel>,
}

impl SpicedEngine {
    /// Write `tables` into `data_dir` as parquet, start `spiced` on them as
    /// `options` asks, and connect to its Flight endpoint.
    pub async fn start(
        options: &ServingOptions,
        tables: &[GeneratedTable],
        data_dir: &Path,
    ) -> Result<Self> {
        write_parquet(tables, data_dir)?;
        let mut datasets: Vec<Dataset> = tables
            .iter()
            .map(|table| {
                dataset(
                    table.table.file_stem,
                    data_dir,
                    options.acceleration.as_ref(),
                )
            })
            .collect();
        if let Some(AccelerationOptions {
            layout: Some(layout),
            ..
        }) = &options.acceleration
        {
            let keys = benchmark_tables(&QuerySet::Tpch).ok_or_else(|| {
                serve_error("the TPC-H layout keys are missing from test_framework::layout")
            })?;
            let applied = apply_layout(&mut datasets, keys, layout)
                .map_err(|e| serve_error(format!("layout '{layout}': {e:#}")))?;
            for dataset in applied {
                println!("  layout {dataset}");
            }
        }
        let app = datasets
            .into_iter()
            .fold(
                AppBuilder::new("substrait-compliance"),
                AppBuilder::with_dataset,
            )
            .build();
        let request = StartRequest::new(options.spiced_path.clone(), from_app(app))
            .map_err(|e| serve_error(format!("prepare spiced: {e:#}")))?;
        let mut spiced = SpicedInstance::start(request)
            .await
            .map_err(|e| serve_error(format!("start spiced: {e:#}")))?;
        spiced
            .wait_for_ready(options.ready_wait)
            .await
            .map_err(|e| serve_error(format!("wait for spiced to load the tables: {e:#}")))?;
        let channel = Channel::from_static(FLIGHT_URL)
            .connect()
            .await
            .map_err(|e| serve_error(format!("connect to {FLIGHT_URL}: {e}")))?;
        Ok(Self {
            spiced,
            client: FlightServiceClient::new(channel),
        })
    }

    /// The `spiced` version, for the report.
    #[must_use]
    pub fn version(&self) -> String {
        self.spiced.version().to_string()
    }

    /// Run each case `iterations` times, comparing every execution with the
    /// golden. A case passes only when every execution does; the first that
    /// does not decides its result. Repeating catches answers that change once
    /// the first execution has warmed a cache or an index.
    pub async fn run_suite(
        &mut self,
        cases: &[&LoadedCase],
        iterations: NonZeroU32,
    ) -> Vec<CaseResult> {
        let mut results = Vec::with_capacity(cases.len());
        for case in cases {
            let start = Instant::now();
            if case.expected.is_none() {
                results.push(unverifiable(case, start));
                continue;
            }
            let mut result = case_result(case, start, self.execute(case).await);
            for iteration in 2..=iterations.get() {
                if result.status != TestStatus::Passed {
                    break;
                }
                result = case_result(case, start, self.execute(case).await);
                if result.status != TestStatus::Passed {
                    result.error_message = result.error_message.map(|message| {
                        format!(
                            "execution {iteration} of {iterations}, after {} that passed: {message}",
                            iteration - 1
                        )
                    });
                }
            }
            results.push(result);
        }
        results
    }

    async fn execute(&mut self, case: &LoadedCase) -> std::result::Result<TableData, String> {
        let mut plan = Plan::decode(case.plan_bytes.as_slice()).map_err(|e| {
            format!(
                "Failed to decode Substrait plan {}: {e}",
                case.plan_path.display()
            )
        })?;
        lowercase_read_names(&mut plan);
        let descriptor = FlightDescriptor::new_cmd(command_bytes(&plan.encode_to_vec()));
        let info = self
            .client
            .get_flight_info(descriptor)
            .await
            .map_err(|status| format!("GetFlightInfo: {}", status.message()))?
            .into_inner();
        let schema = info
            .clone()
            .try_decode_schema()
            .map_err(|e| format!("FlightInfo schema: {e}"))?;
        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket = endpoint
                .ticket
                .ok_or_else(|| "FlightInfo endpoint carries no ticket".to_string())?;
            let stream = self
                .client
                .do_get(ticket)
                .await
                .map_err(|status| format!("DoGet: {}", status.message()))?
                .into_inner();
            let mut decoded = FlightRecordBatchStream::new_from_flight_data(
                stream.map_err(|status| FlightError::Tonic(Box::new(status))),
            );
            while let Some(batch) = decoded
                .try_next()
                .await
                .map_err(|e| format!("DoGet stream: {e}"))?
            {
                batches.push(batch);
            }
        }
        Ok(batches_to_table(&batches, &schema))
    }
}

impl Drop for SpicedEngine {
    fn drop(&mut self) {
        if let Err(e) = self.spiced.stop() {
            eprintln!("Failed to stop spiced: {e:#}");
        }
    }
}

fn serve_error(detail: impl Into<String>) -> error::Error {
    error::ModeBServeSnafu {
        detail: detail.into(),
    }
    .build()
}

/// The dataset that serves `stem`'s parquet file, accelerated as `acceleration`
/// asks.
fn dataset(stem: &str, data_dir: &Path, acceleration: Option<&AccelerationOptions>) -> Dataset {
    let path = data_dir.join(format!("{stem}.parquet"));
    let mut dataset = Dataset::new(format!("file:{}", path.display()), stem);
    if let Some(acceleration) = acceleration {
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some(acceleration.engine.clone()),
            mode: acceleration.mode.clone(),
            ..Acceleration::default()
        });
    }
    dataset
}

/// Write each table as `<dir>/<table>.parquet`, rows in generation order, with
/// its column names lowercased the way Spice resolves them.
pub fn write_parquet(tables: &[GeneratedTable], dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).context(error::WriteFileSnafu { path: dir })?;
    for table in tables {
        let schema = Arc::new(Schema::new(
            table
                .schema
                .fields()
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_name(field.name().to_lowercase())
                })
                .collect::<Vec<_>>(),
        ));
        let path = dir.join(format!("{}.parquet", table.table.file_stem));
        let file = File::create(&path).context(error::WriteFileSnafu { path: &path })?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .context(error::WriteParquetSnafu { path: &path })?;
        for batch in table.partitions.iter().flatten() {
            let batch = RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                .map_err(|e| {
                    serve_error(format!("rename the columns of {}: {e}", path.display()))
                })?;
            writer
                .write(&batch)
                .context(error::WriteParquetSnafu { path: &path })?;
        }
        writer
            .close()
            .context(error::WriteParquetSnafu { path: &path })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_bytes_are_a_non_empty_any_payload() {
        let bytes = command_bytes(&[0x0a, 0x00]);
        assert!(!bytes.is_empty(), "FlightSQL Any payload must not be empty");
        let cmd = command_statement_substrait_plan(&[0x0a, 0x00]);
        let plan = cmd.plan.expect("plan must be present");
        assert_eq!(plan.plan.as_ref(), &[0x0a, 0x00]);
        // `[0x0a, 0x00]` is not a versioned Plan; use the pinned suite release.
        assert_eq!(plan.version, SUITE_SUBSTRAIT_VERSION);
    }

    #[test]
    fn flight_sql_version_comes_from_the_decoded_plan() {
        let proto = Plan {
            version: Some(Version {
                major_number: 0,
                minor_number: 81,
                patch_number: 0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let bytes = proto.encode_to_vec();
        let cmd = command_statement_substrait_plan(&bytes);
        let plan = cmd.plan.expect("plan must be present");
        assert_eq!(plan.version, "0.81.0");
        assert_eq!(substrait_version_from_plan(&bytes), "0.81.0");
    }

    #[test]
    fn undecodable_plan_uses_the_pinned_suite_version() {
        assert_eq!(
            substrait_version_from_plan(&[0xff, 0x00]),
            SUITE_SUBSTRAIT_VERSION
        );
        assert_eq!(SUITE_SUBSTRAIT_VERSION, "0.81.0");
    }
}
