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
use std::process::exit;

use app::spicepod::Spicepod;
use clap::Parser;
use snafu::ErrorCompat;

#[derive(Parser, Debug)]
#[command(
    name = "spicepod-validator",
    about = "Validates a spicepod.yaml file using Spice runtime validation logic",
    version
)]
struct Args {
    /// Path to spicepod.yaml file or directory containing it
    path: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match validate_spicepod(args.path).await {
        Ok(()) => exit(0),
        Err(e) => {
            eprintln!("{e}");
            if let Some(backtrace) = ErrorCompat::backtrace(&e) {
                eprintln!("\nBacktrace:\n{backtrace}");
            }
            exit(1);
        }
    }
}

async fn validate_spicepod(path: PathBuf) -> Result<(), app::spicepod::Error> {
    // This uses the same validation logic as the runtime
    // Spicepod::load internally validates:
    // - YAML syntax and schema
    // - Component references
    // - Duplicate component names
    // - Reserved keywords
    // - Dependencies

    // Determine if we should use load (directory/spicepod.yaml) or load_exact (any file)
    if let Ok(file_info) = tokio::fs::metadata(&path).await {
        if file_info.is_file() {
            // For files, use load_exact which can handle any filename
            Spicepod::load_exact(&path).await?;
        } else {
            // For directories, use regular load which looks for spicepod.yaml
            Spicepod::load(&path).await?;
        }
    } else {
        // If metadata fails, try load_exact and let it produce the proper error
        Spicepod::load_exact(&path).await?;
    }

    Ok(())
}
