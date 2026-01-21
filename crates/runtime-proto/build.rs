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

use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let proto_files = std::fs::read_dir("proto")?
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "proto"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();

    if !proto_files.is_empty() {
        tonic_prost_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos(&proto_files, &[PathBuf::from("proto")])?;
    }

    Ok(())
}
