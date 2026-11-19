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

//! An executor serves a peer's shuffle fetch only from its shuffle work
//! directory, including when `shuffle_location` is a relative path: a fetch is
//! judged by the file it would open, so parent components that climb out of
//! the executor's current directory are refused on both fetch paths, while
//! shuffle output under the work directory is served whether it is named
//! relative or absolute.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{self, FetchPartition, action::ActionType};
use futures::TryStreamExt;
use prost::Message;
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use spicepod::component::dataset::Dataset;
use tonic::transport::Channel;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;
use super::peer_identity::connect_verifying;

const NAMES_CSV: &str = include_str!("../acceleration/data/names.csv");

/// A `FetchPartition` for the file at `path`, encoded as both fetch paths carry
/// it: the `IO_BLOCK_TRANSPORT` action body and the `do_get` ticket.
fn fetch_partition(path: &str) -> Vec<u8> {
    protobuf::Action {
        action_type: Some(ActionType::FetchPartition(FetchPartition {
            job_id: "probe-job".to_string(),
            path: path.to_string(),
            ..Default::default()
        })),
        settings: Vec::new(),
    }
    .encode_to_vec()
}

/// The bytes an executor serves for `path` over `IO_BLOCK_TRANSPORT`.
async fn fetch_block(channel: &Channel, path: &str) -> Result<Vec<u8>, tonic::Status> {
    let stream = FlightServiceClient::new(channel.clone())
        .do_action(Action {
            r#type: "IO_BLOCK_TRANSPORT".to_string(),
            body: fetch_partition(path).into(),
        })
        .await?
        .into_inner();
    let chunks: Vec<arrow_flight::Result> = stream.try_collect().await?;
    Ok(chunks.into_iter().flat_map(|chunk| chunk.body).collect())
}

/// The record batches an executor serves for `path` over `do_get`.
async fn fetch_batches(channel: &Channel, path: &str) -> Result<Vec<RecordBatch>, FlightError> {
    let stream = FlightServiceClient::new(channel.clone())
        .do_get(Ticket {
            ticket: fetch_partition(path).into(),
        })
        .await?
        .into_inner()
        .map_err(FlightError::from);
    FlightRecordBatchStream::new_from_flight_data(stream)
        .try_collect()
        .await
}

/// Writes one batch with a single `marker` column as an Arrow IPC stream, the
/// shuffle file format `do_get` decodes.
fn write_marker_stream(path: &Path, marker: &str) -> Result<RecordBatch, anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "marker",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![marker]))],
    )?;
    let mut writer = StreamWriter::try_new(std::fs::File::create(path)?, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(batch)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relative_shuffle_location_serves_fetches_only_from_its_work_directory()
-> Result<(), anyhow::Error> {
    crate::utils::require_process_per_test(
        "this test sets the process's current directory, which a relative \
         shuffle_location resolves against, and installs the process-global meter \
         provider to observe shuffle transfers",
    );
    let registry = &*crate::utils::PROMETHEUS;
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            // The executor runs in `<base>/node/cwd` with `shuffle_location:
            // shuffle`, so its work directory is `<base>/node/cwd/shuffle`.
            // Markers sit two directories above its current directory, in a
            // directory named like the work directory.
            let base = tempfile::tempdir()?;
            let base_path = base.path().canonicalize()?;
            let cwd = base_path.join("node").join("cwd");
            std::fs::create_dir_all(&cwd)?;
            std::env::set_current_dir(&cwd)?;
            let outside = base_path.join("shuffle");
            std::fs::create_dir_all(&outside)?;
            std::fs::write(outside.join("marker.txt"), b"outside the work directory")?;
            write_marker_stream(&outside.join("marker.arrow"), "outside the work directory")?;

            let csv = base_path.join("names.csv");
            std::fs::write(&csv, NAMES_CSV)?;
            let mut app = AppBuilder::new("relative_shuffle_location")
                .with_dataset(Dataset::new(format!("file:{}", csv.display()), "names"))
                .build();
            app.runtime
                .params
                .insert("shuffle_location".to_string(), "shuffle".to_string());
            app.runtime
                .caching
                .sql_results
                .get_or_insert_default()
                .enabled = false;
            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;

                // (1) A distributed query reads its shuffle through the
                //     executor's Flight server, by the relative paths Ballista
                //     wrote under the relative work directory, and returns the
                //     rows the scheduler computes alone.
                {
                    let state = harness.scheduler.datafusion().ctx.state_ref();
                    let mut state = state.write();
                    let config = state.config_mut();
                    *config = config
                        .clone()
                        .with_ballista_shuffle_reader_force_remote_read(true);
                }
                let sql = "SELECT city, count(*) AS n, sum(score) AS total FROM names GROUP BY city ORDER BY city";
                let expected = harness.query(sql).await?;
                let actual = harness
                    .query_distributed(sql, "relative_shuffle_location_query")
                    .await?;
                assert!(expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
                assert_eq!(
                    arrow::util::pretty::pretty_format_batches(&actual)?.to_string(),
                    arrow::util::pretty::pretty_format_batches(&expected)?.to_string(),
                );
                assert!(
                    crate::utils::metric_value(registry, "executor_shuffle_read_remote_rows", &[])
                        > 0.0,
                    "the executor must serve nonempty shuffle partitions through Flight"
                );
                assert!(
                    cwd.join("shuffle").is_dir(),
                    "Ballista writes shuffle output under the relative work directory"
                );

                // A cluster peer's connection to the executor's Flight server.
                let pki = harness
                    .cluster_pki()
                    .expect("harness runs with cluster mTLS");
                let endpoints = harness.executor_flight_endpoints();
                let [endpoint] = endpoints.as_slice() else {
                    anyhow::bail!("expected one executor endpoint, got {endpoints:?}");
                };
                let channel =
                    connect_verifying(pki, "executor1", endpoint.parse()?, "127.0.0.1").await?;

                // (2) A file under the work directory, named relative to the
                //     executor's current directory as Ballista names it, is
                //     served on both fetch paths, so the refusals below are the
                //     check and not an unreachable file.
                let inside = cwd.join("shuffle").join("probe-job").join("1").join("0");
                std::fs::create_dir_all(&inside)?;
                std::fs::write(inside.join("data.arrow"), b"inside the work directory")?;
                let marker = write_marker_stream(&inside.join("stream.arrow"), "inside")?;
                assert_eq!(
                    fetch_block(&channel, "shuffle/probe-job/1/0/data.arrow").await?,
                    b"inside the work directory",
                );
                assert_eq!(
                    fetch_batches(&channel, "shuffle/probe-job/1/0/stream.arrow").await?,
                    vec![marker.clone()],
                );

                // (3) Parent components that climb out of the current directory
                //     are refused before any byte is read, on both fetch paths.
                //     Every outcome is collected first, so a failure lists each
                //     path that was not refused.
                let mut outcomes = Vec::new();
                for path in [
                    "../../shuffle/marker.txt",
                    "shuffle/../../../shuffle/marker.txt",
                ] {
                    outcomes.push(match fetch_block(&channel, path).await {
                        Ok(served) => format!(
                            "IO_BLOCK_TRANSPORT {path}: served {:?}",
                            String::from_utf8_lossy(&served)
                        ),
                        Err(refused) => format!("IO_BLOCK_TRANSPORT {path}: {:?}", refused.code()),
                    });
                }
                for path in [
                    "../../shuffle/marker.arrow",
                    "shuffle/../../../shuffle/marker.arrow",
                ] {
                    outcomes.push(match fetch_batches(&channel, path).await {
                        Ok(served) => format!(
                            "do_get {path}: served {}",
                            arrow::util::pretty::pretty_format_batches(&served)?
                                .to_string()
                                .replace('\n', " ")
                        ),
                        Err(FlightError::Tonic(refused)) => {
                            format!("do_get {path}: {:?}", refused.code())
                        }
                        Err(other) => format!("do_get {path}: {other:?}"),
                    });
                }
                assert_eq!(
                    outcomes,
                    [
                        "IO_BLOCK_TRANSPORT ../../shuffle/marker.txt: PermissionDenied",
                        "IO_BLOCK_TRANSPORT shuffle/../../../shuffle/marker.txt: PermissionDenied",
                        "do_get ../../shuffle/marker.arrow: PermissionDenied",
                        "do_get shuffle/../../../shuffle/marker.arrow: PermissionDenied",
                    ],
                    "every fetch outside the work directory is refused"
                );

                // (4) The same files named by absolute path are served too: the
                //     request and the relative work directory are compared as
                //     the absolute paths they open.
                let absolute = inside.join("data.arrow").display().to_string();
                assert_eq!(
                    fetch_block(&channel, &absolute).await?,
                    b"inside the work directory",
                    "{absolute}"
                );
                let absolute = inside.join("stream.arrow").display().to_string();
                assert_eq!(fetch_batches(&channel, &absolute).await?, vec![marker]);
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}
