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

//! Spicepod JSON Schema Generator
//!
//! This tool generates a JSON Schema for the Spicepod specification, enriched with
//! connector-specific parameter schemas for data connectors, accelerators, and
//! catalog connectors.

mod collector;
mod enricher;
mod transform;

use collector::{
    collect_catalog_connectors, collect_data_accelerators, collect_data_connectors,
    collect_model_sources,
};
use enricher::enrich_params_schema;
use schemars::schema_for;
use spicepod::spec::SpicepodDefinition;
use std::env;
use std::fs::File;
use std::io::Write;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <output_filename>", args[0]);
        std::process::exit(1);
    }

    let output_filename = &args[1];

    // Generate base schema from SpicepodDefinition
    let mut schema = schema_for!(SpicepodDefinition);

    // Collect parameter specs from all registered connectors, accelerators, and model sources
    let data_connectors = collect_data_connectors();
    let data_accelerators = collect_data_accelerators();
    let catalog_connectors = collect_catalog_connectors();
    let model_sources = collect_model_sources();

    // Log what we collected
    eprintln!(
        "Collected {} data connectors, {} data accelerators, {} catalog connectors, {} model sources",
        data_connectors.len(),
        data_accelerators.len(),
        catalog_connectors.len(),
        model_sources.len()
    );

    // Enrich the schema with connector-specific parameter definitions
    enrich_params_schema(
        &mut schema,
        &data_connectors,
        &data_accelerators,
        &catalog_connectors,
        &model_sources,
    );

    // Serialize to JSON
    let Ok(json_schema) = serde_json::to_string_pretty(&schema) else {
        eprintln!("Unable to serialize schema");
        std::process::exit(1);
    };

    // Write to file
    let Ok(mut file) = File::create(output_filename) else {
        eprintln!("Unable to create file {output_filename}");
        std::process::exit(1);
    };

    if file.write_all(json_schema.as_bytes()).is_err() {
        eprintln!("Unable to write to file {output_filename}");
        std::process::exit(1);
    }

    eprintln!("Schema written to {output_filename}");
}
