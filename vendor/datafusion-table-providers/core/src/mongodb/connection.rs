use async_stream::stream;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use futures::StreamExt;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Client, Collection,
};
use snafu::prelude::*;
use std::sync::Arc;

use crate::mongodb::utils::arrow::mongo_docs_to_arrow;
use crate::mongodb::utils::schema::infer_arrow_schema_from_documents;
use crate::mongodb::utils::unnest::{unnest_bson_documents, UnnestBehavior, UnnestParameters};
use crate::mongodb::{Error, QuerySnafu, Result, UnableToGetSchemaSnafu};

pub struct MongoDBConnection {
    pub client: Arc<Client>,
    pub db_name: String,
    tz: Option<String>,
    unnest_parameters: UnnestParameters,
    num_documents_to_infer_schema: i64,
}

impl MongoDBConnection {
    pub fn new(
        client: Arc<Client>,
        db_name: String,
        tz: Option<String>,
        unnest_parameters: UnnestParameters,
        num_documents_to_infer_schema: i64,
    ) -> Self {
        MongoDBConnection {
            client,
            db_name,
            tz,
            unnest_parameters,
            num_documents_to_infer_schema,
        }
    }

    fn get_collection(&self, collection: &str) -> Collection<Document> {
        self.client.database(&self.db_name).collection(collection)
    }

    pub async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error> {
        let collection_name = table_reference.table();
        let coll = self.get_collection(collection_name);

        let sample = coll
            .find(doc! {})
            .limit(self.num_documents_to_infer_schema)
            .await
            .boxed()
            .context(UnableToGetSchemaSnafu)?;

        let docs: Vec<Document> = sample
            .try_collect()
            .await
            .boxed()
            .context(UnableToGetSchemaSnafu)?;

        let unnested_docs = match self.unnest_parameters.behavior {
            UnnestBehavior::Depth(0) => docs,
            _ => unnest_bson_documents(docs, &self.unnest_parameters)?,
        };

        infer_arrow_schema_from_documents(&unnested_docs, self.tz.clone().as_deref())
            .boxed()
            .context(UnableToGetSchemaSnafu)
    }

    pub async fn query_arrow(
        &self,
        table_reference: &Arc<TableReference>,
        projected_schema: &SchemaRef,
        filters_doc: &Document,
        limit: Option<i32>,
    ) -> Result<SendableRecordBatchStream> {
        let collection_name = table_reference.table();
        let coll = self.get_collection(collection_name);

        let mut find = coll
            .find(filters_doc.clone())
            .projection(schema_to_mongo_projection(projected_schema));

        if let Some(l) = limit {
            find = find.limit(l.into());
        }

        let cursor = find.await.boxed().context(QuerySnafu)?;
        let chunked_stream = cursor.try_chunks(4_000);
        let projected_schema_clone = Arc::clone(projected_schema);
        let unnest_parameters = self.unnest_parameters.clone();

        let mut batch_stream = Box::pin(stream! {
            for await chunk in chunked_stream {
                match chunk {
                    Ok(docs) => {
                        let unnested_docs = match unnest_parameters.behavior {
                            UnnestBehavior::Depth(0) => docs,
                            _ => {
                                unnest_bson_documents(docs, &unnest_parameters)?
                            }
                        };

                        let batch = mongo_docs_to_arrow(&unnested_docs, Arc::clone(&projected_schema_clone))?;
                        yield Ok(batch);
                    }
                    Err(e) => yield Err(Error::QueryError { source: Box::new(e) }),
                }
            }
        });

        let Some(first_batch_result) = batch_stream.next().await else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(Schema::empty()),
                futures::stream::empty().boxed(),
            )));
        };

        let first_batch = first_batch_result?;
        let schema = first_batch.schema();

        let full_stream = Box::pin(stream! {
            yield Ok(first_batch);
            while let Some(batch_result) = batch_stream.next().await {
                yield batch_result.map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()));
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, full_stream)))
    }
}

pub fn schema_to_mongo_projection(projected_schema: &SchemaRef) -> Document {
    let mut projection = Document::new();

    if projected_schema.fields().is_empty() {
        return projection;
    }

    for field in projected_schema.fields() {
        projection.insert(field.name(), 1);
    }

    projection
}
