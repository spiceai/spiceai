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

use arrow::datatypes::SchemaRef;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::ReaderBuilder;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{project_schema, DataFusionError};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::TryStreamExt;
use mongodb::bson::{to_bson, Bson, Document};
use mongodb::options::FindOptions;
use mongodb::{Client, Collection};
use serde_json::{from_str, to_value, Value};
use snafu::{ResultExt, Snafu};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to find collection list: {source}"))]
    FailedToListCollections { source: mongodb::error::Error },

    #[snafu(display("Collection does not exist: {collection_name}"))]
    CollectionDoesNotExist { collection_name: Arc<str> },

    #[snafu(display("Failed to find document: {source}"))]
    FailedToFindDocument { source: mongodb::error::Error },

    #[snafu(display("Error occurred while fetching documents: {source}"))]
    FailedToStreamDocument { source: mongodb::error::Error },

    #[snafu(display("Failed to infer schema: {source}"))]
    FailedToInferSchema { source: arrow::error::ArrowError },

    #[snafu(display("Failed to parse `query`: {source}"))]
    FailedToParseQuery {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

const NUM_DOCUMENTS_TO_INFER_SCHEMA: u8 = 20;

#[derive(Debug)]
pub struct MongoDBTableProvider {
    client: Arc<Client>,
    database_name: Arc<str>,
    collection_name: Arc<str>, // A `collection` is the equivalent of an RDBMS `table`
    table_schema: SchemaRef,
    filter_document: Document,
}

impl MongoDBTableProvider {
    pub async fn try_new(
        client: Arc<Client>,
        database_name: Arc<str>,
        collection_name: Arc<str>,
        query_body: Arc<str>,
    ) -> Result<Self, Error> {
        Self::check_collection_exists(Arc::clone(&client), &database_name, &collection_name)
            .await?;

        let table_schema =
            Self::infer_schema(Arc::clone(&client), &database_name, &collection_name).await?;
        let filter_document = Self::parse_query(&query_body).context(FailedToParseQuerySnafu)?;

        Ok(Self {
            client,
            database_name,
            collection_name,
            table_schema,
            filter_document,
        })
    }

    async fn check_collection_exists(
        client: Arc<Client>,
        database_name: &str,
        collection_name: &str,
    ) -> Result<(), Error> {
        let existing_collections = client
            .database(database_name)
            .list_collection_names()
            .await
            .context(FailedToListCollectionsSnafu)?;

        if !existing_collections.contains(&collection_name.to_string()) {
            return CollectionDoesNotExistSnafu { collection_name }.fail();
        }

        Ok(())
    }

    async fn infer_schema(
        client: Arc<Client>,
        database_name: &str,
        collection_name: &str,
    ) -> Result<SchemaRef, Error> {
        let collection = client
            .database(database_name)
            .collection::<Document>(collection_name);

        let mut cursor = collection
            .find(Document::new())
            .limit(i64::from(NUM_DOCUMENTS_TO_INFER_SCHEMA))
            .await
            .context(FailedToFindDocumentSnafu)?;

        let mut extracted_schema_info = Vec::new();
        while let Some(document) = cursor
            .try_next()
            .await
            .context(FailedToStreamDocumentSnafu)?
        {
            extracted_schema_info.push(document_to_json_value(&document));
        }

        let schema = infer_json_schema_from_iterator(extracted_schema_info.iter().map(Ok))
            .context(FailedToInferSchemaSnafu)?;

        Ok(Arc::new(schema))
    }

    fn parse_query(input: &str) -> Result<Document, Box<dyn std::error::Error + Send + Sync>> {
        let json_value: Value = from_str(input)?;
        let bson_value = to_bson(&json_value)?;

        match bson_value {
            Bson::Document(doc) => Ok(doc),
            _ => Err("Input is not a valid document".into()),
        }
    }
}

fn document_to_json_value(document: &Document) -> Value {
    Value::Object(
        document
            .iter()
            .map(|(k, v)| (k.clone(), to_value(v).unwrap_or(Value::Null)))
            .collect(),
    )
}

#[async_trait]
impl TableProvider for MongoDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let collection = self
            .client
            .database(self.database_name.as_ref())
            .collection::<Document>(self.collection_name.as_ref());

        let projected_schema = project_schema(&self.table_schema, projection)?;
        let projection_document_for_mongodb =
            build_mongodb_projection(&self.table_schema, projection);

        let limit: Option<i64> = limit
            .map(i64::try_from)
            .transpose()
            .map_err(|_| DataFusionError::Execution("Limit is too large".to_string()))?;

        let find_options_builder = FindOptions::builder()
            .projection(projection_document_for_mongodb)
            .limit(limit);

        let find_options = find_options_builder.build();

        Ok(Arc::new(MongoDBTableProviderExec::new(
            collection,
            self.filter_document.clone(),
            Some(find_options),
            projected_schema,
        )))
    }
}

fn build_mongodb_projection(
    table_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Document> {
    let indices = projection?;
    let mut doc = Document::new();

    for &index in indices {
        let field = table_schema.field(index);
        doc.insert(field.name(), Bson::Int32(1)); // 1 : include this field / 0 : exclude this field
    }
    Some(doc)
}

pub struct MongoDBTableProviderExec {
    collection: Collection<Document>,
    filter_document: Document,
    find_options: Option<FindOptions>,
    table_schema: SchemaRef,
    properties: PlanProperties,
}

impl MongoDBTableProviderExec {
    #[must_use]
    pub fn new(
        collection: Collection<Document>,
        filter_document: Document,
        find_options: Option<FindOptions>,
        table_schema: SchemaRef,
    ) -> Self {
        Self {
            collection,
            filter_document,
            find_options,
            table_schema: Arc::clone(&table_schema),
            properties: PlanProperties::new(
                EquivalenceProperties::new(table_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl Debug for MongoDBTableProviderExec {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "MongoDBTableProviderExec")
    }
}

impl DisplayAs for MongoDBTableProviderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "MongoDBTableProviderExec")
    }
}

impl ExecutionPlan for MongoDBTableProviderExec {
    fn name(&self) -> &'static str {
        "MongoDBTableProviderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&self.table_schema), 2);
        let tx = builder.tx();
        let schema = Arc::clone(&self.table_schema);
        let collection = self.collection.clone();
        let find_options = self.find_options.clone();
        let filter_document = self.filter_document.clone();

        builder.spawn(async move {
            let mut cursor = collection
                .find(filter_document)
                .with_options(find_options)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            while let Some(document) = cursor
                .try_next()
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
            {
                let json_value = document_to_json_value(&document).to_string();

                let batches = ReaderBuilder::new(Arc::clone(&schema))
                    .build(Cursor::new(json_value.as_bytes()))
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                for batch in batches {
                    tx.send(Ok(batch)).await.map_err(|_| {
                        DataFusionError::Execution("Failed to send record batch".to_string())
                    })?;
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;
    use snafu::ResultExt;

    #[test]
    fn test_parsing_query() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // parsing fail
        let query_body: &str = "{ 123: aa }";
        let filter_document =
            MongoDBTableProvider::parse_query(query_body).context(FailedToParseQuerySnafu);
        assert_eq!(
            filter_document
                .expect_err("Must be an error because of parsing failure")
                .to_string(),
            "Failed to parse `query`: key must be a string at line 1 column 3"
        );

        // parsing success
        let query_body: &str = "{ \"status\": { \"$in\": [\"A\", \"D\"] } }";
        let filter_document =
            MongoDBTableProvider::parse_query(query_body).context(FailedToParseQuerySnafu)?;
        assert_eq!(filter_document, doc! {"status": {"$in": ["A", "D"]}});
        Ok(())
    }
}
