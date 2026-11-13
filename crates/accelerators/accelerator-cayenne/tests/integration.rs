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

//! Integration tests for the Cayenne accelerator, in one binary so the crate's
//! dependency graph links once (`docs/dev/integration_test_layout.md`).

#![expect(
    clippy::expect_used,
    reason = "set-up and helpers outside `#[test]` functions fail fast with `expect`"
)]

mod lookup_index_catch_up;
mod partitioned_keyed_writes;
mod partitioned_lookup_index;
mod snapshot_restore_over_leftovers;
mod source_ack_cancellation;
mod stored_data_files;
mod stored_partitioning;
