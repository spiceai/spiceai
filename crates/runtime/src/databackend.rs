use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::datasource::MemTable;

pub struct InMemoryBackend {
    pub mem_table: MemTable,
}

impl InMemoryBackend {
    #[must_use]
    pub fn new(schema: SchemaRef, records: Vec<RecordBatch>) -> Self {
        InMemoryBackend {
            mem_table: MemTable::try_new(schema, vec![records]),
        }
    }
}
