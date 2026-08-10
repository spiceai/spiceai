/*
Copyright 2025 The Spice.ai OSS Authors

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

//! DataFusion optimizer machinery that turns an indexed [`spice_table::SpiceTable`]
//! scan into an index-served plan.

use snafu::prelude::*;

pub mod analyzer;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Index table scans should have only one input. Received {input_len} inputs."))]
    MultipleInputs { input_len: usize },

    #[snafu(display(
        "Index table scans should have no expressions. Received {expr_len} expressions."
    ))]
    NoExpressions { expr_len: usize },
}
