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

use std::sync::LazyLock;

use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter},
};

/// Boundaries for the `s3_vectors_*_latency` histograms, in milliseconds.
///
/// Sized for a round trip to the `S3Vectors` API rather than for in-process work: the floor sits
/// at 1ms because no network call answers faster, and the 5-75ms boundaries resolve the band a
/// query against a small index is answered in.
///
/// A histogram needs a boundary wherever its observations land. A quantile interpolated inside a
/// single bucket is a function of the requested percentile alone, so a p50 drawn from one bucket
/// spanning 0-100ms reports 50ms whatever the real latency is, and a p90 reports 90ms.
///
/// The coarser boundaries above 100ms are `le` series that operator dashboards read, so they are
/// fixed. Adding a boundary only subdivides a bucket and leaves every series meaning what it
/// means; moving or dropping one silently redefines a published series.
fn latency_histogram_boundaries() -> Vec<f64> {
    let sub_hundred = [0.0, 1.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 75.0];
    let hundreds = (1..10).map(|i| 100.0 * f64::from(i));
    let half_seconds = (1..20).map(|i| 500.0 + 500.0 * f64::from(i));
    let seconds = (1..10).map(|i| 10000.0 + 1000.0 * f64::from(i));

    sub_hundred
        .into_iter()
        .chain(hundreds)
        .chain(half_seconds)
        .chain(seconds)
        .collect()
}

/// A macro to standardise the API-level metrics recorded for each `S3Vectors` operation.
macro_rules! generate_s3vectors_metrics {
    ($prefix:literal, $name:ident) => {
        pub mod $name {
            use super::*;

            static METER: LazyLock<Meter> =
                LazyLock::new(|| global::meter(concat!("s3_vectors_", $prefix)));

            pub static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!("s3_vectors_", $prefix, "_requests"))
                    .with_description("Number of requests to this operation.")
                    .build()
            });

            pub static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
                METER
                    .u64_counter(concat!("s3_vectors_", $prefix, "_errors"))
                    .with_description("Number of errors returned from this operation.")
                    .build()
            });

            pub static LATENCY: LazyLock<Histogram<f64>> = LazyLock::new(|| {
                METER
                    .f64_histogram(concat!("s3_vectors_", $prefix, "_latency"))
                    .with_description("Total duration of operation, in milliseconds.")
                    .with_boundaries(latency_histogram_boundaries())
                    .build()
            });
        }
    };
}

generate_s3vectors_metrics!("create_index", create_index);
generate_s3vectors_metrics!("create_vector_bucket", create_vector_bucket);
generate_s3vectors_metrics!("delete_index", delete_index);
generate_s3vectors_metrics!("delete_vector_bucket", delete_vector_bucket);
generate_s3vectors_metrics!("delete_vector_bucket_policy", delete_vector_bucket_policy);
generate_s3vectors_metrics!("delete_vectors", delete_vectors);
generate_s3vectors_metrics!("get_vector_bucket_policy", get_vector_bucket_policy);
generate_s3vectors_metrics!("get_index", get_index);
generate_s3vectors_metrics!("get_vector_bucket", get_vector_bucket);
generate_s3vectors_metrics!("get_vectors", get_vectors);
generate_s3vectors_metrics!("list_indexes", list_indexes);
generate_s3vectors_metrics!("list_vector_buckets", list_vector_buckets);
generate_s3vectors_metrics!("list_vectors", list_vectors);
generate_s3vectors_metrics!("put_vector_bucket_policy", put_vector_bucket_policy);
generate_s3vectors_metrics!("put_vectors", put_vectors);
generate_s3vectors_metrics!("query_vectors", query_vectors);

#[cfg(test)]
mod tests {
    use super::latency_histogram_boundaries;

    /// The coarse boundaries operator dashboards read as `le` series on these histograms.
    fn published_le_boundaries() -> Vec<f64> {
        let hundreds = (0..10).map(|i| 100.0 * f64::from(i));
        let half_seconds = (1..20).map(|i| 500.0 + 500.0 * f64::from(i));
        let seconds = (1..10).map(|i| 10000.0 + 1000.0 * f64::from(i));

        hundreds.chain(half_seconds).chain(seconds).collect()
    }

    /// Membership by total order, so a boundary is located without comparing floats for equality.
    fn holds_boundary(bounds: &[f64], value: f64) -> bool {
        bounds.iter().any(|bound| bound.total_cmp(&value).is_eq())
    }

    /// A quantile is only as precise as the bucket its observations land in. If nothing separates
    /// 0 from 100ms, an operation answered in 10-40ms shares a bucket with one answered instantly,
    /// and interpolating inside that single bucket reports the requested percentile rather than a
    /// latency.
    ///
    /// Regression test for #12698.
    #[test]
    fn latency_boundaries_resolve_below_a_hundred_milliseconds() {
        let bounds = latency_histogram_boundaries();
        let resolved: Vec<f64> = bounds
            .iter()
            .copied()
            .filter(|&bound| bound > 0.0 && bound < 100.0)
            .collect();

        assert!(
            resolved.len() >= 4,
            "operations faster than 100ms need more than one bucket to draw a quantile from, got \
             boundaries {resolved:?}"
        );

        let in_small_index_band = resolved
            .iter()
            .filter(|&&bound| (10.0..=40.0).contains(&bound))
            .count();
        assert!(
            in_small_index_band >= 3,
            "a query against a small index is answered in 10-40ms, which should not collapse into \
             one bucket, got boundaries {resolved:?}"
        );
    }

    /// `with_boundaries` requires an ordered set, and a cumulative `le` series is only monotonic
    /// if the boundaries it is keyed by increase.
    #[test]
    fn latency_boundaries_are_strictly_increasing() {
        let bounds = latency_histogram_boundaries();

        assert!(
            bounds.windows(2).all(|pair| pair[0] < pair[1]),
            "the latency boundaries must be strictly increasing, got {bounds:?}"
        );
    }

    /// Subdividing a bucket leaves every `le` series meaning what it means; dropping or moving a
    /// boundary silently redefines one that operator dashboards read.
    #[test]
    fn latency_boundaries_keep_every_published_le_series() {
        let bounds = latency_histogram_boundaries();

        for published in published_le_boundaries() {
            assert!(
                holds_boundary(&bounds, published),
                "boundary {published} is gone, which redefines the le={published} series"
            );
        }
    }
}
