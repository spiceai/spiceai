/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Dataset (table) implementations for streaming benchmarks.
//!
//! Each dataset type represents a specific table that can be loaded
//! into a streaming source. Query sets determine which datasets to load.

mod customer;
mod lineitem;
mod nation;
mod orders;
mod part;
mod partsupp;
mod region;
mod supplier;

pub use customer::CustomerDataset;
pub use lineitem::LineitemDataset;
pub use nation::NationDataset;
pub use orders::OrdersDataset;
pub use part::PartDataset;
pub use partsupp::PartsuppDataset;
pub use region::RegionDataset;
pub use supplier::SupplierDataset;

/// Available dataset types (tables) for streaming benchmarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatasetType {
    /// TPCH lineitem table (6M rows at SF=1)
    Lineitem,
    /// TPCH orders table (1.5M rows at SF=1)
    Orders,
    /// TPCH customer table (150K rows at SF=1)
    Customer,
    /// TPCH part table (200K rows at SF=1)
    Part,
    /// TPCH supplier table (10K rows at SF=1)
    Supplier,
    /// TPCH partsupp table (800K rows at SF=1)
    Partsupp,
    /// TPCH nation table (25 rows)
    Nation,
    /// TPCH region table (5 rows)
    Region,
}

impl DatasetType {
    /// Returns the table name for this dataset type.
    #[must_use]
    pub fn table_name(self) -> &'static str {
        match self {
            Self::Lineitem => "lineitem",
            Self::Orders => "orders",
            Self::Customer => "customer",
            Self::Part => "part",
            Self::Supplier => "supplier",
            Self::Partsupp => "partsupp",
            Self::Nation => "nation",
            Self::Region => "region",
        }
    }

    /// Creates a boxed dataset instance for this dataset type.
    #[must_use]
    pub fn create_dataset(self) -> Box<dyn super::traits::StreamingDataset> {
        match self {
            Self::Lineitem => Box::new(LineitemDataset),
            Self::Orders => Box::new(OrdersDataset),
            Self::Customer => Box::new(CustomerDataset),
            Self::Part => Box::new(PartDataset),
            Self::Supplier => Box::new(SupplierDataset),
            Self::Partsupp => Box::new(PartsuppDataset),
            Self::Nation => Box::new(NationDataset),
            Self::Region => Box::new(RegionDataset),
        }
    }
}

impl std::fmt::Display for DatasetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.table_name())
    }
}
