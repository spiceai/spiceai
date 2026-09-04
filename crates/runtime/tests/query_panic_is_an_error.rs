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

//! A query whose execution panics must reach the caller as an error, never as a
//! successful empty result — which is what a client cannot tell apart from "no rows
//! matched", and what a pipeline would record as fact.
//!
//! The unit tests beside `RuntimeDriverStream` pin the channel/handle ordering that
//! produced the empty success. This one covers the other half: that no layer between
//! the driver stream and the caller — `run_with_managed_runtime`, `QueryResult`, the
//! stream adapters — converts that error back into a success. It drives the real
//! `Query::run` path, so it fails if any of them ever does.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/13876>.
//!
//! This lives in its own test binary because it registers a deliberately panicking
//! UDF on the session context, which must not be visible to any other test.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use futures::StreamExt;
use runtime::Runtime;
use runtime_async::ManagedTokioRuntime;

/// A UDF that panics when it is executed. Planning succeeds — the panic happens on the
/// execution thread, which is the shape this test needs and the shape a kernel hitting
/// an `unreachable!()` produces.
///
/// A real panicking expression was available (`concat` over an untyped NULL literal, the
/// one #13876 was found through) but is deliberately not used: it is a bug in its own
/// right, tracked as #13877, so a test built on it would break the moment it is fixed.
#[derive(Debug, Hash, PartialEq, Eq)]
struct PanickingUdf {
    signature: Signature,
}

impl PanickingUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for PanickingUdf {
    fn name(&self) -> &'static str {
        "spice_test_panic"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        panic!("spice_test_panic: deliberate panic from a scalar UDF");
    }
}

#[tokio::test]
async fn a_panicking_query_is_an_error_not_an_empty_success() {
    let rt = Arc::new(Runtime::builder().build().await);

    // `Query::run` only takes the offloaded driver path when a CPU runtime is set, which
    // is what `spiced` does at startup. Without this the query runs inline and never
    // reaches `RuntimeDriverStream` at all — the test would pass while covering nothing.
    rt.datafusion().set_cpu_runtime(
        ManagedTokioRuntime::try_new().expect("create a CPU runtime for the query driver"),
    );
    rt.datafusion()
        .ctx
        .register_udf(ScalarUDF::from(PanickingUdf::new()));

    // Several rows over several partitions, which is the shape the empty success was
    // measured on: most partitions finish empty and only one carries the panicking row.
    const SQL: &str =
        "SELECT spice_test_panic(x) AS z FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8)) t(x)";

    // Repeated because the failure this guards is an ordering race: on the pre-fix code
    // roughly a third of runs returned the empty success and the rest returned the error,
    // so a single run could pass by luck.
    for attempt in 1..=20 {
        let outcome: Result<Vec<_>, DataFusionError> =
            match rt.datafusion().query_builder(SQL).build().run().await {
                // A planning failure would mean the UDF never executed, so this test would
                // be asserting nothing at all — the failure mode it is most likely to rot
                // into. Refuse it rather than counting it as the correct outcome.
                Err(err) => {
                    let msg = err.to_string();
                    assert!(
                        !msg.contains("Error during planning"),
                        "attempt {attempt}: the query failed to plan, so execution was never \
                         reached and this test covers nothing: {msg}"
                    );
                    continue;
                }
                Ok(result) => result.data.collect::<Vec<_>>().await.into_iter().collect(),
            };

        match outcome {
            Err(_) => {}
            Ok(batches) => {
                let rows: usize = batches
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum();
                panic!(
                    "attempt {attempt}: a query whose execution panicked returned success with \
                     {} batch(es) and {rows} row(s); a client cannot tell this apart from a query \
                     that legitimately matched no rows",
                    batches.len()
                );
            }
        }
    }
}
