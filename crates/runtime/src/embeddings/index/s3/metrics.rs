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
                    .with_boundaries(
                        [
                            (0..10).map(|i| 100.0 * f64::from(i)).collect::<Vec<_>>(),
                            (1..20)
                                .map(|i| 500.0 + 500.0 * f64::from(i))
                                .collect::<Vec<_>>(),
                            (1..10)
                                .map(|i| 10000.0 + 1000.0 * f64::from(i))
                                .collect::<Vec<_>>(),
                        ]
                        .concat(),
                    )
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
