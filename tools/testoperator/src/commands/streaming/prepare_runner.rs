/*
Copyright 2026 The Spice.ai OSS Authors

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

//! One-time stream preparation for the streaming ingestion benchmark.
//!
//! Creates a persistent Kafka topic or Debezium-tracked PostgreSQL table,
//! produces `--rows` data rows, and appends a permanent `benchmark-ready`
//! marker.  Run this once; the `streaming-ingestion` command then replays
//! the stream on every benchmark execution.

use test_framework::anyhow::Result;

use super::sources::{DebeziumIngestionSource, IngestionSource, KafkaIngestionSource};
use super::sources::debezium::DebeziumConfig;
use super::sources::kafka::{KafkaConfig, MARKER_EVENT_TYPE};
use crate::args::PrepareIngestionStreamArgs;
use crate::args::streaming::StreamMode;

/// Prepare the persistent ingestion stream (run once before benchmarks).
pub async fn run_prepare_stream(args: &PrepareIngestionStreamArgs) -> Result<()> {
    println!("=== Prepare Ingestion Stream ===");
    println!("Mode:  {}", args.stream_mode);
    println!("Topic: {}", args.topic);
    println!("Rows:  {}", args.rows);

    let mut source: Box<dyn IngestionSource> = match args.stream_mode {
        StreamMode::Debezium => {
            let config = DebeziumConfig::from_env()?;
            Box::new(DebeziumIngestionSource::new(config, args.topic.clone()))
        }
        StreamMode::Kafka => {
            let config = KafkaConfig::from_env();
            Box::new(KafkaIngestionSource::new(config, args.topic.clone(), args.batch_size))
        }
    };

    println!("\n[1/3] Creating stream infrastructure...");
    source.prepare().await?;

    println!("\n[2/3] Producing {} data rows...", args.rows);
    source.produce_rows(args.rows).await?;

    println!("\n[3/3] Writing permanent marker (event_type={MARKER_EVENT_TYPE})...");
    source.produce_marker(MARKER_EVENT_TYPE).await?;

    println!("\nStream ready.");
    println!("  source_from: {}", source.source_from_field());
    println!("  bootstrap:   {}", source.kafka_bootstrap_servers());
    println!("\nRun benchmarks with:");
    println!(
        "  testoperator run streaming-ingestion \\\n    --stream-mode {} \\\n    --topic {} \\\n    --rows {} \\\n    --spicepod-path <spicepod>",
        args.stream_mode, args.topic, args.rows
    );

    Ok(())
}
