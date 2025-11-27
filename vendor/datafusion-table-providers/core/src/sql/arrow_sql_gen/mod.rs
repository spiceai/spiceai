//! # Generate SQL statements from Arrow schemas
//!
//! This module provides a set of functions to generate SQL statements (i.e. DML) from Arrow schemas.
//!
//! The primary use case is for generating `CREATE TABLE`/`INSERT INTO` statements for SQL databases from Arrow schemas/values with given constraints.
//!
//! ## Example
//!
//! ### `CREATE TABLE` statement
//! ```rust
//! use std::sync::Arc;
//! use datafusion::arrow::datatypes::{DataType, Field, Schema};
//! use datafusion_table_providers::sql::arrow_sql_gen::statement::CreateTableBuilder;
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("name", DataType::Utf8, false),
//!     Field::new("age", DataType::Int32, false),
//! ]));
//!
//! let sql = CreateTableBuilder::new(schema, "my_table").build_sqlite();
//!
//! assert_eq!(r#"CREATE TABLE IF NOT EXISTS "my_table" ( "id" integer NOT NULL, "name" text NOT NULL, "age" integer NOT NULL )"#, sql);
//! ```
//!
//! With primary key constraints:
//! ```rust
//! use std::sync::Arc;
//! use datafusion::arrow::datatypes::{DataType, Field, Schema};
//! use datafusion_table_providers::sql::arrow_sql_gen::statement::CreateTableBuilder;
//!
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("name", DataType::Utf8, false),
//!     Field::new("age", DataType::Int32, false),
//! ]));
//!
//! let sql = CreateTableBuilder::new(schema, "my_table")
//!     .primary_keys(vec!["id"])
//!     .build_sqlite();
//!
//! assert_eq!(r#"CREATE TABLE IF NOT EXISTS "my_table" ( "id" integer NOT NULL, "name" text NOT NULL, "age" integer NOT NULL, PRIMARY KEY ("id") )"#, sql);
//! ```

pub mod arrow;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
pub mod statement;
