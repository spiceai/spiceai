use arrow::datatypes::SchemaRef;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub struct SchemaStore {
    schemas: HashMap<String, RwLock<SchemaRef>>,
}

impl SchemaStore {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub async fn get_schema(&self, name: &str) -> Option<SchemaRef> {
        let Some(mutex) = self.schemas.get(name) else {
            return None;
        };
        Some(mutex.read().await.clone())
    }

    pub async fn set_schema(&mut self, name: &str, schema: SchemaRef) {
        let mutex = self
            .schemas
            .entry(name.to_string())
            .or_insert_with(|| RwLock::new(Arc::clone(&schema)));
        *mutex.write().await = Arc::clone(&schema);
    }
}
