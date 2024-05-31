use datafusion::arrow::array::{ArrayRef, Int32Builder, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use std::convert::TryInto;
use std::sync::Arc;

use super::PgNamespace;

macro_rules! table_builder {
	($type:ident $($field_name:ident: $builder_type:ty, $param_type:ty,)*) => {
		struct $type {
			$($field_name: $builder_type,)*
		}

		impl $type {
			fn new() -> Self {
				Self {
					$($field_name: <$builder_type>::new(),)*
				}
			}

			#[allow(dead_code)] // some tables aren't currently written to
			fn add_row(
				&mut self,
				$($field_name: $param_type,)*
			) -> Result<(), DataFusionError> {
				$(self.$field_name.append_value($field_name);)*
				Ok(())
			}
		}

		impl TryInto<Arc<dyn TableProvider>> for $type {
			type Error = DataFusionError;

			fn try_into(mut self) -> Result<Arc<dyn TableProvider>, Self::Error> {
				let columns: Vec<ArrayRef> = vec![
					$(Arc::new(self.$field_name.finish()),)*
				];

				let column_names = &[
					$(stringify!($field_name),)*
				];

				let fields: Vec<_> = columns.iter().zip(column_names).map(|(c, name)| Field::new(name.to_owned(), c.data_type().clone(), true)).collect();
				let schema = Arc::new(Schema::new(fields));
				let batch = RecordBatch::try_new(schema, columns)?;
				Ok(Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?))
			}
		}
	};
}

table_builder! {
	PgDatabaseBuilder
	datname: StringBuilder, &str,
}

table_builder! {
	PgTablesBuilder
	schemaname: StringBuilder, &str,
	tablename: StringBuilder, &str,
}

// table_builder! {
// 	PgNamespaceBuilder
// 	oid: UInt32Builder, u32,
// 	nspname: StringBuilder, &str,
// }

table_builder! {
	PgClassBuilder
	oid: UInt32Builder, u32,
	relname: StringBuilder, &str,
	relnamespace: UInt32Builder, u32,
	relkind: StringBuilder, &str,
}

table_builder! {
	PgProc
	oid: UInt32Builder, u32,
	proname: StringBuilder, &str,
	pronamespace: UInt32Builder, u32,
}

table_builder! {
	PgDescription
	objoid: UInt32Builder, u32,
	classoid: UInt32Builder, u32,
	objsubid: Int32Builder, i32,
	description: StringBuilder, &str,
}

// https://github.com/returnString/convergence/blob/85dfe3a83a5c7a59a8ce107a6f8d03a4d724e38e/convergence-arrow/src/metadata.rs#L102

pub fn pg_table_schemas() -> Result<MemorySchemaProvider, DataFusionError> {
    let schema = MemorySchemaProvider::new();

    schema.register_table("pg_tables".to_owned(), PgTablesBuilder::new().try_into()?)?;
    // schema.register_table("pg_namespace".to_owned(), PgNamespaceBuilder::new().try_into()?)?;
    schema.register_table("pg_class".to_owned(), PgClassBuilder::new().try_into()?)?;
    schema.register_table("pg_database".to_owned(), PgDatabaseBuilder::new().try_into()?)?;
    schema.register_table("pg_proc".to_owned(), PgProc::new().try_into()?)?;
    schema.register_table("pg_description".to_owned(), PgDescription::new().try_into()?)?;

    Ok(schema)
}