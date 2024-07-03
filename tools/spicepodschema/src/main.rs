/*
Copyright 2024 The Spice.ai OSS Authors

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

use schemars::schema_for;
use spicepod::spec::SpicepodDefinition;
use std::fs::File;
use std::io::Write;

fn main() {
    let schema = schema_for!(SpicepodDefinition);
    let json_schema = serde_json::to_string_pretty(&schema).unwrap();

    let mut file = File::create("spicepod_schema.json").expect("Unable to create file");
    file.write_all(json_schema.as_bytes())
        .expect("Unable to write data");
}
