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

//! `query_active_count` must stay raised for exactly as long as a request has a
//! query running, in whatever order those queries finish.
//!
//! Several independent top-level queries can share one `RequestContext` — a
//! search fans out one query per table and per embedding column, and NSQL
//! samples up to ten datasets with `buffer_unordered`, which yields in
//! completion order by construction. Those siblings are not dropped
//! last-in-first-out, which breaks both ways of anchoring the release on a
//! single guard: the guard that raised the count can strand its decrement when
//! it is not the last one out, leaving the series permanently high (the
//! instrument is a cumulative up/down counter, so the drift never recovers), or
//! it can release as soon as it finishes, reporting an idle runtime while its
//! siblings are still running.
//!
//! So the assertions below check the reading *between* drops, not only after
//! the last one — a test that looks only at the end passes under both failures.
//!
//! Regression test for <https://github.com/spiceai/spiceai/issues/12883>.
//!
//! This lives in its own test binary because the meter install is global: the
//! instrument is a `LazyLock` over `global::meter(..)`, so it binds to whichever
//! provider is installed when it is first touched, and a count assertion is only
//! stable when no other test is recording on the same series.

use std::sync::{Arc, LazyLock};

use opentelemetry::global;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
use runtime::datafusion::query::QueryActiveGuard;
use runtime_request_context::{Protocol, RequestContext};

/// How wide a fan-out to exercise beyond the two-query minimum. Every guard
/// after the first is a nested one, so the count must stay at exactly one
/// however many there are.
const FAN_OUT: usize = 8;

/// The one `MeterProvider` for this binary, installed before any instrument
/// exists.
static PROMETHEUS: LazyLock<prometheus::Registry> =
    LazyLock::new(install_prometheus_meter_provider);

/// Installs a `MeterProvider` backed by a scrapable Prometheus registry, as
/// `spiced` does for the `--metrics` endpoint.
fn install_prometheus_meter_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()
        .expect("to build the prometheus exporter");

    let provider = SdkMeterProvider::builder()
        .with_resource(Resource::builder().build())
        .with_reader(exporter)
        .build();
    global::set_meter_provider(provider);

    registry
}

/// Asserts `query_active_count` sits exactly `expected` above `baseline`.
///
/// `#[track_caller]` so a failure names the drop it followed. Every assertion
/// here reads the same instrument, and several of them expect the same value,
/// so without it a panic points at this line and leaves the reader to guess
/// which of a dozen call sites produced it.
#[track_caller]
fn assert_raised_by(registry: &prometheus::Registry, baseline: f64, expected: f64) {
    let observed = query_active_count(registry) - baseline;
    assert!(
        (observed - expected).abs() < f64::EPSILON,
        "query_active_count is {observed} above baseline, expected {expected}"
    );
}

/// The value on `query_active_count`, summed over every label set.
///
/// An up/down counter is not monotonic, so it exports as a Prometheus gauge.
fn query_active_count(registry: &prometheus::Registry) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "query_active_count")
        .flat_map(prometheus::proto::MetricFamily::get_metric)
        .map(|metric| metric.get_gauge().value())
        .sum()
}

#[test]
fn query_active_count_returns_to_baseline_whatever_order_queries_finish_in() {
    let registry = &*PROMETHEUS;
    let context = Arc::new(RequestContext::builder(Protocol::Http).build());

    // Assertions are deltas rather than absolute values, so reading the
    // instrument before it has been touched (which reports 0.0, since an
    // unrecorded family is absent rather than zero) is the right baseline.
    let baseline = query_active_count(registry);

    // Two sibling top-level queries sharing one request context, finishing in
    // the order they started. This is the case that leaked.
    let first = QueryActiveGuard::new(Arc::clone(&context));
    let second = QueryActiveGuard::new(Arc::clone(&context));
    assert_raised_by(registry, baseline, 1.0);
    drop(first);
    // The reading between the two drops is the half of this that a
    // "returns to baseline" assertion cannot see: the request is still busy,
    // so releasing here would report an idle runtime while a query runs.
    assert_raised_by(registry, baseline, 1.0);
    drop(second);
    assert_raised_by(registry, baseline, 0.0);

    // The count unwound with them, so the query after them is recognised as
    // the start of a new busy period and is counted again.
    let next = QueryActiveGuard::new(Arc::clone(&context));
    assert_raised_by(registry, baseline, 1.0);
    drop(next);
    assert_raised_by(registry, baseline, 0.0);

    // A genuinely nested query is the last-in-first-out case, and must keep
    // being counted once for the request as a whole.
    let outer = QueryActiveGuard::new(Arc::clone(&context));
    let inner = QueryActiveGuard::new(Arc::clone(&context));
    assert_raised_by(registry, baseline, 1.0);
    drop(inner);
    assert_raised_by(registry, baseline, 1.0);
    drop(outer);
    assert_raised_by(registry, baseline, 0.0);

    // A wider fan-out, again finishing oldest first. Checking before every
    // drop covers the same ground as checking after: the request stays busy
    // until the last one leaves, and the check after the last drop is the one
    // below the loop.
    let fan_out: Vec<QueryActiveGuard> = (0..FAN_OUT)
        .map(|_| QueryActiveGuard::new(Arc::clone(&context)))
        .collect();
    for guard in fan_out {
        assert_raised_by(registry, baseline, 1.0);
        drop(guard);
    }
    assert_raised_by(registry, baseline, 0.0);

    // Two requests are counted independently, and one finishing does not
    // release the other's count.
    let other = Arc::new(RequestContext::builder(Protocol::Http).build());
    let held = QueryActiveGuard::new(Arc::clone(&context));
    let elsewhere = QueryActiveGuard::new(Arc::clone(&other));
    assert_raised_by(registry, baseline, 2.0);
    drop(held);
    assert_raised_by(registry, baseline, 1.0);
    drop(elsewhere);
    assert_raised_by(registry, baseline, 0.0);
}
