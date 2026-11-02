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

fn main() {
    // The streaming datasets and their verification load the installed libduckdb
    // through ADBC, and the runtime this binary links can also bring in the
    // bundled DuckDB: export nothing from this package's binaries on macOS, for
    // the reason `bin/spiced/build.rs` gives.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-Wl,-exported_symbol,__mh_execute_header");
    }
}
