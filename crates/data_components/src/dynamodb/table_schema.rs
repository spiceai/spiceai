use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Encapsulates DynamoDB table schema, keys, and expression conversion logic.
/// This struct knows WHAT the table structure is and WHAT operations are supported.
#[derive(Debug, Clone)]
pub struct DynamoDBTableSchema {
    pub table_name: Arc<str>,
    pub table_schema: SchemaRef,
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub column_to_alias_map: HashMap<String, String>, // actual_name -> #c0
    pub alias_to_column_map: HashMap<String, String>, // #c0 -> actual_name
}

impl DynamoDBTableSchema {
    pub fn new(
        table_name: Arc<str>,
        table_schema: SchemaRef,
        partition_key: String,
        sort_key: Option<String>,
    ) -> Self {
        let (column_to_alias_map, alias_to_column_map) = build_column_alias_maps(&table_schema);

        Self {
            table_name,
            table_schema,
            partition_key,
            sort_key,
            column_to_alias_map,
            alias_to_column_map,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key.as_deref()
    }

    pub fn get_column_alias(&self, column_name: &str) -> Option<&str> {
        self.column_to_alias_map
            .get(column_name)
            .map(|s| s.as_str())
    }

    fn is_filter_supported(&self, expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let op_supported = matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::And
                        | Operator::Or
                );

                op_supported && self.is_filter_supported(left) && self.is_filter_supported(right)
            }
            Expr::Column(col) => {
                println!("Column: {:?}", col.name);
                println!("Column: {:?}", self.column_to_alias_map.keys());
                self.column_to_alias_map.contains_key(col.name.as_str())
            }
            Expr::Literal(scalar, _) => matches!(
                scalar,
                ScalarValue::Utf8(_)
                    | ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Float32(_)
                    | ScalarValue::Float64(_)
                    | ScalarValue::Boolean(_)
            ),
            _ => false,
        }
    }

    pub fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let support: Vec<_> = filters
            .iter()
            .map(|&expr| {
                if self.is_filter_supported(expr) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(support)
    }

    pub fn build_filter_expression(
        &self,
        filters: &[Expr],
    ) -> DataFusionResult<(String, HashMap<String, AttributeValue>)> {
        if filters.is_empty() {
            return Ok((String::new(), HashMap::new()));
        }

        let mut attribute_values = HashMap::new();
        let mut value_counter = 0;

        let filter_parts: Vec<String> = filters
            .iter()
            .filter_map(|expr| {
                self.expr_to_filter_string(expr, &mut attribute_values, &mut value_counter)
                    .ok()
            })
            .collect();

        if filter_parts.is_empty() {
            return Ok((String::new(), HashMap::new()));
        }

        let filter_expr = filter_parts.join(" AND ");
        Ok((filter_expr, attribute_values))
    }

    fn expr_to_filter_string(
        &self,
        expr: &Expr,
        attribute_values: &mut HashMap<String, AttributeValue>,
        value_counter: &mut usize,
    ) -> DataFusionResult<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left_str = self.expr_to_filter_string(left, attribute_values, value_counter)?;
                let right_str =
                    self.expr_to_filter_string(right, attribute_values, value_counter)?;

                let op_str = match op {
                    Operator::Eq => "=",
                    Operator::NotEq => "<>",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Operator {:?} not supported",
                            op
                        )));
                    }
                };

                Ok(format!("({} {} {})", left_str, op_str, right_str))
            }
            Expr::Column(col) => self
                .column_to_alias_map
                .get(col.name.as_str())
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Column {} not found", col.name))
                }),
            Expr::Literal(scalar, _) => {
                let value_key = format!(":v{}", value_counter);
                *value_counter += 1;

                let attr_value = scalar_to_attribute_value(scalar)?;
                attribute_values.insert(value_key.clone(), attr_value);

                Ok(value_key)
            }
            _ => Err(DataFusionError::NotImplemented(
                "Expression type not supported in filters".to_string(),
            )),
        }
    }

    pub fn extract_attribute_names(&self, filters: &[Expr]) -> HashMap<String, String> {
        let mut attribute_names = HashMap::new();
        for expr in filters {
            self.extract_columns_from_expr(expr, &mut attribute_names);
        }
        attribute_names
    }

    fn extract_columns_from_expr(
        &self,
        expr: &Expr,
        attribute_names: &mut HashMap<String, String>,
    ) {
        match expr {
            Expr::Column(col) => {
                if let Some(alias) = self.column_to_alias_map.get(col.name.as_str()) {
                    attribute_names.insert(alias.clone(), col.name.to_string());
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                self.extract_columns_from_expr(left, attribute_names);
                self.extract_columns_from_expr(right, attribute_names);
            }
            _ => {}
        }
    }

    pub fn build_key_condition_expression(
        &self,
        partition_expr: &Expr,
        sort_expr: Option<&Expr>,
    ) -> datafusion::error::Result<(String, HashMap<String, AttributeValue>)> {
        let mut attribute_values = HashMap::new();
        let mut value_counter = 0;

        let partition_str =
            self.expr_to_filter_string(partition_expr, &mut attribute_values, &mut value_counter)?;

        let key_condition = if let Some(sort) = sort_expr {
            let sort_str =
                self.expr_to_filter_string(sort, &mut attribute_values, &mut value_counter)?;
            format!("{} AND {}", partition_str, sort_str)
        } else {
            partition_str
        };

        Ok((key_condition, attribute_values))
    }
}

fn build_column_alias_maps(
    schema: &SchemaRef,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut column_to_alias_map = HashMap::new();
    let mut alias_to_column_map = HashMap::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let column_name = field.name().clone();
        let alias = format!("#c{i}");

        column_to_alias_map.insert(column_name.clone(), alias.clone());
        alias_to_column_map.insert(alias, column_name);
    }

    (column_to_alias_map, alias_to_column_map)
}

fn scalar_to_attribute_value(scalar: &ScalarValue) -> datafusion::error::Result<AttributeValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Ok(AttributeValue::S(s.clone())),
        ScalarValue::Int64(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int32(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Float64(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Float32(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Boolean(Some(b)) => Ok(AttributeValue::Bool(*b)),
        ScalarValue::Null => Ok(AttributeValue::Null(true)),
        _ => Err(DataFusionError::NotImplemented("ScalarValue type not supported".to_string())),
    }
}
