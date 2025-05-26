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

pub mod alias;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, RecordBatch},
        datatypes::{Field, Schema},
    };
    use arrow_schema::DataType;
    use datafusion::{assert_batches_eq, datasource::MemTable, prelude::SessionContext};

    #[tokio::test]
    async fn test_basic() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![0.9, 2.1, 3.0])),
            ],
        )?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("t1", Arc::new(table))?;
        let sql = "SELECT greatest(a, 2), least(a, 2), greatest(a, b), least(a, b), greatest(a, b, 2), least(a, b, 2) from t1";
        let actual = ctx.sql(sql).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+-------------------------+----------------------+---------------------+------------------+------------------------------+---------------------------+",
                "| greatest(t1.a,Int64(2)) | least(t1.a,Int64(2)) | greatest(t1.a,t1.b) | least(t1.a,t1.b) | greatest(t1.a,t1.b,Int64(2)) | least(t1.a,t1.b,Int64(2)) |",
                "+-------------------------+----------------------+---------------------+------------------+------------------------------+---------------------------+",
                "| 2                       | 1                    | 1.0                 | 0.9              | 2.0                          | 0.9                       |",
                "| 2                       | 2                    | 2.1                 | 2.0              | 2.1                          | 2.0                       |",
                "| 3                       | 2                    | 3.0                 | 3.0              | 3.0                          | 2.0                       |",
                "+-------------------------+----------------------+---------------------+------------------+------------------------------+---------------------------+",
            ],
            &actual
        );
        Ok(())
    }
}
