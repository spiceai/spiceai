/*
Copyright 2024 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider, execution::context::SessionState, logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use datafusion_table_providers::{
    sql::sql_provider_datafusion::expr::Engine,
    sqlite::{write::SqliteTableWriter, Sqlite},
    util,
};
use rusqlite::Transaction;
use std::sync::Arc;

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};

#[async_trait]
impl DeletionTableProvider for SqliteTableWriter {
    async fn delete_from(
        &self,
        _state: &SessionState,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(SqliteDeletionSink::new(self.sqlite(), filters)),
            &self.schema(),
        )))
    }
}

struct SqliteDeletionSink {
    sqlite: Arc<Sqlite>,
    filters: Vec<Expr>,
}

impl SqliteDeletionSink {
    fn new(sqlite: Arc<Sqlite>, filters: &[Expr]) -> Self {
        Self {
            sqlite,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for SqliteDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = self.sqlite.connect().await?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn)?;
        let sqlite = Arc::clone(&self.sqlite);
        let sql = util::filters_to_sql(&self.filters, Some(Engine::SQLite))?;

        let count: u64 = sqlite_conn
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                let count = delete_from(sqlite.table_name(), &tx, &sql)?;

                tx.commit()?;

                Ok(count)
            })
            .await?;

        Ok(count)
    }
}

fn delete_from(
    table_name: &str,
    transaction: &Transaction<'_>,
    where_clause: &str,
) -> rusqlite::Result<u64> {
    transaction.execute(
        format!(r#"DELETE FROM "{table_name}" WHERE {where_clause}"#).as_str(),
        [],
    )?;
    let count: u64 = transaction.query_row("SELECT changes()", [], |row| row.get(0))?;

    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        datasource::provider::TableProviderFactory,
        execution::context::SessionContext,
        logical_expr::{cast, col, lit, CreateExternalTable},
        physical_plan::{collect, test::exec::MockExec},
        scalar::ScalarValue,
    };
    use datafusion_table_providers::sqlite::SqliteTableProviderFactory;

    use crate::delete::get_deletion_provider;

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_round_trip_sqlite() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::default(),
        };
        let ctx = SessionContext::new();
        let table = SqliteTableProviderFactory::default()
            .create(&ctx.state(), &external_table)
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr1), Arc::new(arr3)])
            .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), false)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let table =
            get_deletion_provider(table).expect("table should be returned as deletion provider");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = table
            .delete_from(&ctx.state(), &vec![filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
        let plan = table
            .delete_from(&ctx.state(), &vec![filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![1]);
        assert_eq!(actual, &expected);
    }
}
