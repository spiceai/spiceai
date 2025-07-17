// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
//!
//! ## Usage
//! ### Basic Usage
//! ```rust
//! use vector_metadata_filter::*;
//!
//! // Parse a simple equality filter
//! let filter_json = r#"{"genre": "documentary"}"#;
//! let filter = MetadataFilter::from_json(filter_json)?;
//!
//! // Validate the filter
//! filter.validate()?;
//!
//! // Convert to DataFusion expression
//! let datafusion_expr = convert_to_datafusion_expr(&filter)?;
//! ```
//!
//! ### Complex Filters
//! ```rust
//! use vector_metadata_filter::*;
//!
//! // Multiple conditions on the same field
//! let filter_json = r#"{"price": {"$gte": 10, "$lte": 50}}"#;
//! let filter = MetadataFilter::from_json(filter_json)?;
//! // Logical operations
//! let filter_json = r#"{
//!     "$and": [
//!         {"genre": {"$eq": "drama"}},
//!         {"year": {"$gte": 2020}},
//!         {"rating": {"$in": [8, 9, 10]}}
//!     ]
//! }"#;
//! let filter = MetadataFilter::from_json(filter_json)?;
//!
//! // Convert to DataFusion for query execution
//! let expr = convert_to_datafusion_expr(&filter)?;
//! ```
//!
//! ### Error Handling
//! ```rust
//! use vector_metadata_filter::*;
//!
//! // Invalid filter will return detailed error
//! let invalid_filter = r#"{"year": {"$gt": "not_a_number"}}"#;
//! let filter = MetadataFilter::from_json(invalid_filter)?;
//!
//! match filter.validate() {
//!     Ok(_) => println!("Filter is valid"),
//!     Err(Error::InvalidValueType { operator, expected, actual }) => {
//!         println!("Invalid value type for {}: expected {}, got {}",
//!                  operator, expected, actual);
//!     }
//!     Err(e) => println!("Validation error: {}", e),
//! }
//! ```

#![allow(clippy::missing_errors_doc)]

pub mod datafusion;
pub mod error;
pub mod filter;

pub use datafusion::*;
pub use error::*;
pub use filter::*;
