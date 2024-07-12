use async_trait::async_trait;
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use datafusion_federation_sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
    sql::{
        sqlparser::ast::{self, Interval},
        unparser::dialect::Dialect,
        TableReference,
    },
};

use super::{query_to_stream, FlightTable};

impl FlightTable {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = self.table_reference.to_string();
        tracing::trace!(
            table_name,
            %self.table_reference,
            "create_federated_table_source"
        );
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        )?))
    }

    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

struct SpiceAIDialect {}

impl Dialect for SpiceAIDialect {
    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn use_timestamp_for_date64(&self) -> bool {
        true
    }

    fn custom_scalar_to_sql(
        &self,
        scalar: &datafusion::scalar::ScalarValue,
    ) -> Option<datafusion::common::Result<ast::Expr>> {
        match scalar {
            datafusion::scalar::ScalarValue::IntervalYearMonth(v) => {
                let Some(v) = v else { return None };
                let interval = Interval {
                    value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                        v.to_string(),
                    ))),
                    leading_field: Some(ast::DateTimeField::Month),
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                };
                Some(Ok(ast::Expr::Interval(interval)))
            }
            datafusion::scalar::ScalarValue::IntervalDayTime(v) => {
                let Some(v) = v else { return None };
                let days = v.days;
                let secs = v.milliseconds / 1_000;
                let mins = secs / 60;
                let hours = mins / 60;

                let secs = secs - (mins * 60);
                let mins = mins - (hours * 60);

                let millis = v.milliseconds % 1_000;
                let interval = Interval {
                    value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                        "{days} {hours}:{mins}:{secs}.{millis:3}"
                    )))),
                    leading_field: Some(ast::DateTimeField::Day),
                    leading_precision: None,
                    last_field: Some(ast::DateTimeField::Second),
                    fractional_seconds_precision: None,
                };
                Some(Ok(ast::Expr::Interval(interval)))
            }
            datafusion::scalar::ScalarValue::IntervalMonthDayNano(v) => {
                let Some(v) = v else {
                    return None;
                };

                if v.months >= 0 && v.days == 0 && v.nanoseconds == 0 {
                    let interval = Interval {
                        value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                            v.months.to_string(),
                        ))),
                        leading_field: Some(ast::DateTimeField::Month),
                        leading_precision: None,
                        last_field: None,
                        fractional_seconds_precision: None,
                    };
                    Some(Ok(ast::Expr::Interval(interval)))
                } else if v.months == 0 && v.days >= 0 && v.nanoseconds % 1_000_000 == 0 {
                    let days = v.days;
                    let secs = v.nanoseconds / 1_000_000_000;
                    let mins = secs / 60;
                    let hours = mins / 60;

                    let secs = secs - (mins * 60);
                    let mins = mins - (hours * 60);

                    let millis = (v.nanoseconds % 1_000_000_000) / 1_000_000;

                    let interval = Interval {
                        value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(format!(
                            "{days} {hours}:{mins}:{secs}.{millis:03}"
                        )))),
                        leading_field: Some(ast::DateTimeField::Day),
                        leading_precision: None,
                        last_field: Some(ast::DateTimeField::Second),
                        fractional_seconds_precision: None,
                    };
                    Some(Ok(ast::Expr::Interval(interval)))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }
}

#[async_trait]
impl SQLExecutor for FlightTable {
    fn name(&self) -> &str {
        self.name
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.join_push_down_context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SpiceAIDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            query_to_stream(self.client.clone(), query),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        FlightTable::get_schema(self.client.clone(), &TableReference::bare(table_name))
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use arrow::compute::kernels::cast_utils::{
        parse_interval_day_time, parse_interval_month_day_nano, parse_interval_year_month,
    };
    use datafusion::{logical_expr::Expr, scalar::ScalarValue, sql::unparser::Unparser};

    #[test]
    fn test_custom_scalar_to_expr() {
        use super::*;

        let tests = [
            (
                ScalarValue::IntervalMonthDayNano(
                    parse_interval_month_day_nano(
                        "1 YEAR 1 MONTH 1 DAY 3 HOUR 10 MINUTE 20 SECOND",
                    )
                    .ok(),
                ),
                "INTERVAL '0 YEARS 13 MONS 1 DAYS 3 HOURS 10 MINS 20.000000000 SECS'",
            ),
            (
                ScalarValue::IntervalMonthDayNano(parse_interval_month_day_nano("1.5 MONTH").ok()),
                "INTERVAL '0 YEARS 1 MONS 15 DAYS 0 HOURS 0 MINS 0.000000000 SECS'",
            ),
            (
                ScalarValue::IntervalMonthDayNano(parse_interval_month_day_nano("1 MONTH").ok()),
                "INTERVAL '1' MONTH",
            ),
            (
                ScalarValue::IntervalMonthDayNano(parse_interval_month_day_nano("1.5 DAY").ok()),
                "INTERVAL '1 12:0:0.000' DAY TO SECOND",
            ),
            (
                ScalarValue::IntervalMonthDayNano(
                    parse_interval_month_day_nano("1.51234 DAY").ok(),
                ),
                "INTERVAL '1 12:17:46.176' DAY TO SECOND",
            ),
            (
                ScalarValue::IntervalDayTime(parse_interval_day_time("1.51234 DAY").ok()),
                "INTERVAL '1 12:17:46.176' DAY TO SECOND",
            ),
            (
                ScalarValue::IntervalYearMonth(parse_interval_year_month("1 YEAR").ok()),
                "INTERVAL '12' MONTH",
            ),
        ];

        for (value, expected) in tests {
            let dialect = SpiceAIDialect {};
            let unparser = Unparser::new(&dialect);

            let ast = unparser
                .expr_to_sql(&Expr::Literal(value))
                .expect("to be unparsed");

            let actual = format!("{ast}");

            assert_eq!(actual, expected);
        }
    }
}
