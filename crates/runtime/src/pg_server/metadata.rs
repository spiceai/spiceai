//! Supports generating Postgres metadata from a DataFusion catalog.

use datafusion::arrow::array::{ArrayRef, Int32Builder, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::DataFusionError;
use std::convert::TryInto;
use std::sync::Arc;

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

table_builder! {
	PgNamespaceBuilder
	oid: UInt32Builder, u32,
	nspname: StringBuilder, &str,
}

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

struct MetadataBuilder {
	next_oid: u32,
	pg_database: PgDatabaseBuilder,
	pg_namespace: PgNamespaceBuilder,
	pg_tables: PgTablesBuilder,
	pg_class: PgClassBuilder,
	pg_proc: PgProc,
	pg_description: PgDescription,
}

impl MetadataBuilder {
	fn new() -> Self {
		Self {
			next_oid: 0,
			pg_database: PgDatabaseBuilder::new(),
			pg_namespace: PgNamespaceBuilder::new(),
			pg_tables: PgTablesBuilder::new(),
			pg_class: PgClassBuilder::new(),
			pg_proc: PgProc::new(),
			pg_description: PgDescription::new(),
		}
	}

	fn alloc_oid(&mut self) -> u32 {
		self.next_oid += 1;
		self.next_oid
	}

	fn add_schema(&mut self, schema_name: &str, schema: &dyn SchemaProvider) -> Result<(), DataFusionError> {
		
//         builder.add_namespace(&PgNamespace {
//             oid: 11,
//             nspname: "pg_catalog",
//             nspowner: 10,
//             nspacl: "{test=UC/test,=U/test}",
//         });
//         builder.add_namespace(&PgNamespace {
//             oid: 2200,
//             nspname: "public",
//             nspowner: 10,
//             nspacl: "{test=UC/test,=U/test}",
//         });
//         builder.add_namespace(&PgNamespace {
//             oid: 13000,
//             nspname: "information_schema",
//             nspowner: 10,
//             nspacl: "{test=UC/test,=U/test}",
//         });
		
		let schema_oid = self.alloc_oid();

		for table_name in schema.table_names() {
			let table_oid = self.alloc_oid();

			self.pg_tables.add_row(schema_name, &table_name)?;
			self.pg_namespace.add_row(schema_oid, schema_name)?;
			self.pg_class.add_row(table_oid, &table_name, schema_oid, "r")?;
			let desc_oid = self.alloc_oid();
			self.pg_description.add_row(desc_oid, table_oid, 0, "")?;
		}

		Ok(())
	}

	fn into_schema(self) -> Result<MemorySchemaProvider, DataFusionError> {
		let schema = MemorySchemaProvider::new();

		schema.register_table("pg_tables".to_owned(), self.pg_tables.try_into()?)?;
		schema.register_table("pg_namespace".to_owned(), self.pg_namespace.try_into()?)?;
		schema.register_table("pg_class".to_owned(), self.pg_class.try_into()?)?;
		schema.register_table("pg_database".to_owned(), self.pg_database.try_into()?)?;
		schema.register_table("pg_proc".to_owned(), self.pg_proc.try_into()?)?;
		schema.register_table("pg_description".to_owned(), self.pg_description.try_into()?)?;

		Ok(schema)
	}
}

/// Wrapper catalog supporting generation of pg metadata (e.g. pg_catalog schema).
pub struct PgCatalog {
	wrapped: Arc<dyn CatalogProvider>,
}

impl PgCatalog {
	/// Create a new wrapper catalog that provides postgres metadata for the contained objects.
	pub fn new(wrapped: Arc<dyn CatalogProvider>) -> Self {
		Self { wrapped }
	}

	pub fn build_metadata_schema(&self) -> Result<MemorySchemaProvider, DataFusionError> {
		let mut builder = MetadataBuilder::new();
		//builder.pg_database.add_row("datafusion")?;

		for schema_name in self.wrapped.schema_names() {
			let schema = match self.wrapped.schema(&schema_name) {
				Some(s) => s,
				None => continue,
			};

			builder.add_schema(&schema_name, schema.as_ref())?;
		}

		builder.into_schema()
	}
}

impl CatalogProvider for PgCatalog {
	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn schema_names(&self) -> Vec<String> {
		let mut ret = self.wrapped.schema_names();
		ret.push("pg_catalog".to_owned());
		ret
	}

	fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
		if name.eq_ignore_ascii_case("pg_catalog") {
			return Some(Arc::new(
				self.build_metadata_schema().expect("failed to build metadata schema"),
			));
		}

		self.wrapped.schema(name)
	}
}