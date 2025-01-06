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

use clap::Parser;
use std::path::PathBuf;
use test_framework::{anyhow, rustls};

mod tests;
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the spicepod.yaml file
    #[arg(short('p'), long)]
    spicepod_path: PathBuf,

    /// Path to the spiced binary
    #[arg(short, long)]
    spiced_path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let args = Args::parse();

    tests::throughput::run(&args).await?;
    Ok(())
}
