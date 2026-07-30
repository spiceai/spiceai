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

use super::{Counter, Gauge, LazyLock, Meter, global};

pub static CATALOGS_METER: LazyLock<Meter> = LazyLock::new(|| global::meter("catalog"));

pub static LOAD_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CATALOGS_METER
        .u64_counter("catalog_load_errors")
        .with_description("Number of errors loading the catalog provider.")
        .build()
});

pub static STATUS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CATALOGS_METER
            .u64_gauge("catalog_load_state")
            .with_description("Status of the catalog provider. 0=Initializing, 1=Ready, 2=Disabled, 3=Error, 4=Refreshing, 5=ShuttingDown.")
            .build()
});

/// Number of relations a CDC-accelerated catalog resolved into each disposition,
/// labeled by `catalog` and `category` (`accelerated`, `skipped`, `excluded`,
/// `views_not_replicated`). A gauge, not a counter: it reflects the current
/// disposition after each catalog refresh re-plans the whole namespace, so it
/// can rise or fall.
pub static ACCELERATION_TABLES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CATALOGS_METER
        .u64_gauge("catalog_acceleration_tables")
        .with_description(
            "Relations resolved by a CDC-accelerated catalog, labeled by catalog and category (accelerated, skipped, excluded, views_not_replicated).",
        )
        .build()
});

/// Number of accelerated tables broken down by the CDC key that accelerates
/// them, labeled by `catalog` and `kind` (`primary_key`, `unique_index`,
/// `full`). A gauge for the same reason as [`ACCELERATION_TABLES`].
pub static ACCELERATION_TABLES_BY_KIND: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    CATALOGS_METER
        .u64_gauge("catalog_acceleration_accelerated_tables")
        .with_description(
            "Accelerated tables in a CDC-accelerated catalog, labeled by catalog and acceleration kind (primary_key, unique_index, full).",
        )
        .build()
});
