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

//! # S3 Vectors Metadata Filtering
//! ## Supported Operators
//! ### Comparison Operators
//! | Operator | Description | Valid Types | Example |
//! |----------|-------------|-------------|---------|
//! | `$eq` | Exact match | String, Number, Boolean | `{"genre": {"$eq": "documentary"}}` |
//! | `$ne` | Not equal | String, Number, Boolean | `{"genre": {"$ne": "drama"}}` |
//! | `$gt` | Greater than | Number | `{"year": {"$gt": 2019}}` |
//! | `$gte` | Greater than or equal | Number | `{"year": {"$gte": 2020}}` |
//! | `$lt` | Less than | Number | `{"year": {"$lt": 2020}}` |
//! | `$lte` | Less than or equal | Number | `{"year": {"$lte": 2020}}` |
//!
//! ### Array Operators
//! | Operator | Description | Valid Types | Example |
//! |----------|-------------|-------------|---------|
//! | `$in` | Match any value in array | Array of primitives | `{"genre": {"$in": ["comedy", "documentary"]}}` |
//! | `$nin` | Match none of the values | Array of primitives | `{"genre": {"$nin": ["horror", "thriller"]}}` |
//!
//! ### Existence Operators
//! | Operator | Description | Valid Types | Example |
//! |----------|-------------|-------------|---------|
//! | `$exists` | Check if field exists | Boolean | `{"genre": {"$exists": true}}` |
//!
//! ### Logical Operators
//! | Operator | Description | Valid Types | Example |
//! |----------|-------------|-------------|---------|
//! | `$and` | Logical AND | Array of filters | `{"$and": [{"genre": "drama"}, {"year": {"$gte": 2020}}]}` |
//! | `$or` | Logical OR | Array of filters | `{"$or": [{"genre": "drama"}, {"year": {"$gte": 2020}}]}` |

#![allow(clippy::missing_errors_doc)]

pub mod datafusion;
pub mod error;
pub mod filter;

pub use datafusion::*;
pub use error::*;
pub use filter::*;
