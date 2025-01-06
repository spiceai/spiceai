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

use std::sync::Arc;

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch};
use datafusion::assert_batches_eq;
use runtime::{status, Runtime};
use spicepod::component::dataset::Dataset;

use crate::{
    get_test_datafusion, init_tracing, run_query_and_check_results, utils::test_request_context,
    ValidateFn,
};

#[cfg(feature = "mysql")]
mod mysql;
