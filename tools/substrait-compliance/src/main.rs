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

//! Substrait compliance harness for spiceai.
//!
//! Mode A (default): run IBM TPC-H plans through the spiceai DataFusion
//! fork's Substrait consumer. Report-only — a low pass rate does not
//! fail the process unless `--fail-below` is set.
//!
//! Mode B: stub. Prints the FlightSQL product-path approach and writes
//! a SKIPPED report.

mod compare;
mod error;
mod mode_a;
mod mode_b;
mod report;
mod suite;
mod tpch;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use suite::Suite;

use crate::error::Result;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    A,
    B,
}

#[derive(Parser, Debug)]
#[command(
    name = "substrait-compliance",
    about = "IBM Substrait compliance harness for Spice"
)]
struct Args {
    /// `a` = DataFusion consumer baseline; `b` = FlightSQL product-path stub
    #[arg(long, value_enum, default_value = "a")]
    mode: Mode,

    /// IBM suite directory (contains `metadata.yaml`, `plans/`, `data/`, `expected/`)
    #[arg(long)]
    suite_dir: Option<PathBuf>,

    /// Write the JSON report to this path
    #[arg(long)]
    output: Option<PathBuf>,

    /// Run a single TPC-H query id (e.g. `q01`)
    #[arg(long)]
    query: Option<String>,

    /// Print Mode A / Mode B approach text and exit
    #[arg(long)]
    print_approach: bool,

    /// Exit 1 if pass rate is below this percent. Unset = report-only.
    #[arg(long)]
    fail_below: Option<f64>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<ExitCode> {
    let args = Args::parse();

    if args.print_approach {
        println!("{}", mode_a::approach());
        println!("{}", mode_b::approach());
        return Ok(ExitCode::SUCCESS);
    }

    let suite_dir = args
        .suite_dir
        .ok_or_else(|| error::Error::MissingSuitePath {
            path: "--suite-dir is required unless --print-approach is set".to_string(),
        })?;
    let suite = Suite::load(&suite_dir)?;
    eprintln!(
        "Loaded suite '{}' v{} ({} cases) from {} — {}",
        suite.name,
        suite.version,
        suite.cases.len(),
        suite_dir.display(),
        suite.description
    );

    let report = match args.mode {
        Mode::A => mode_a::run(&suite, args.query.as_deref()).await?,
        Mode::B => mode_b::run_stub(&suite)?,
    };

    report.print_summary();

    if let Some(path) = args.output.as_ref() {
        report.write_json(path)?;
        eprintln!("Wrote {}", path.display());
    }

    if let Some(threshold) = args.fail_below
        && report.pass_rate_pct < threshold
    {
        eprintln!(
            "pass rate {:.1}% is below --fail-below {threshold}",
            report.pass_rate_pct
        );
        return Ok(ExitCode::from(2));
    }

    Ok(ExitCode::SUCCESS)
}
