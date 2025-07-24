use crate::EditorHelper;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::array::{Array, StringArray};
use rustyline::Context;
use rustyline::completion::{Completer, Pair};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tokio::time::interval;
use tonic::transport::Channel;

#[rustfmt::skip]
const SQL_KEYWORDS: &[&str] = &[
    // Core SQL keywords
    "select", "from", "where", "and", "or", "not", "in", "like", "is", "null",
    "true", "false", "asc", "desc", "limit", "offset", "order", "by", "group",
    "having", "as", "distinct", "case", "when", "then", "else", "end",

    // JOIN operations
    "join", "inner", "left", "right", "full", "outer", "cross", "natural", "using", "on",

    // DML operations
    "insert", "into", "values", "update", "set", "delete",

    // DDL operations
    "create", "table", "alter", "drop", "index", "view", "database", "schema",
];

#[rustfmt::skip]
const FUNCTION_NAMES: &[&str] = &[
    // Aggregate functions
    "count", "sum", "avg", "min", "max", "stddev", "variance",

    // String functions
    "concat", "substring", "length", "upper", "lower", "trim", "replace",

    // Date functions
    "now", "current_date", "current_time", "current_timestamp", "extract",

    // Window functions
    "over", "partition", "row_number", "rank", "dense_rank", "lag", "lead",

    // Conditionals
    "if", "ifnull", "coalesce", "nullif",
];

#[derive(Debug, Clone)]
pub struct SchemaCache {
    tables: Vec<String>,
    columns: Vec<String>,
}

impl SchemaCache {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            tables: Vec::new(),
            columns: Vec::new(),
        }
    }

    fn update_tables(&mut self, tables: Vec<String>) {
        self.tables = tables;
    }

    fn update_columns(&mut self, columns: Vec<String>) {
        self.columns = columns;
    }
}

impl EditorHelper {
    /// Start the background refresh task
    /// refresh_interval: How often to refresh schema (in seconds)
    pub fn start_refreshing(&mut self, refresh_interval: u64) {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_sender = Some(shutdown_tx);

        let Some(client) = self.flight_client.clone() else {
            return;
        };
        let schema_cache = self.schema_cache.clone();
        let api_key = self.api_key.clone();
        let user_agent = self.user_agent.clone();

        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(refresh_interval));

            // Initial refresh
            refresh_schema(client.clone(), &schema_cache, api_key.as_ref(), &user_agent).await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        refresh_schema(
                            client.clone(),
                            &schema_cache,
                            api_key.as_ref(),
                            &user_agent,
                        ).await;
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        self.refresh_task_handle = Some(handle);
    }

    pub fn stop_refreshing(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }

        if let Some(handle) = self.refresh_task_handle.take() {
            handle.abort();
        }
    }
}

impl Completer for EditorHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let (start, word) = extract_word(line, pos);
        let word_lower = word.to_lowercase();
        let mut matches = Vec::new();

        let cache = self.schema_cache.try_read().map_err(|_| {
            rustyline::error::ReadlineError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Cache lock error",
            ))
        })?;

        // Only complete table names after FROM
        let before_cursor = &line[..pos].to_lowercase();
        if before_cursor.contains("from ") && !before_cursor.contains("where") {
            for table in &cache.tables {
                if table.to_lowercase().starts_with(&word_lower) {
                    matches.push(Pair {
                        display: table.to_string(),
                        replacement: format!("{} ", table),
                    });
                }
            }
        } else {
            for &keyword in SQL_KEYWORDS {
                if keyword.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: keyword.to_lowercase(),
                        replacement: format!("{} ", keyword.to_lowercase()),
                    });
                }
            }

            for &fn_name in FUNCTION_NAMES {
                if fn_name.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: fn_name.to_lowercase(),
                        replacement: fn_name.to_lowercase(),
                    });
                }
            }

            for table in &cache.tables {
                if table.to_lowercase().starts_with(&word_lower) {
                    matches.push(Pair {
                        display: table.to_string(),
                        replacement: format!("{} ", table),
                    });
                }
            }

            for column in &cache.columns {
                if column.to_lowercase().starts_with(&word_lower) {
                    matches.push(Pair {
                        display: column.to_string(),
                        replacement: format!("{} ", column),
                    });
                }
            }
        }

        Ok((start, matches))
    }
}

async fn refresh_schema(
    mut client: FlightServiceClient<Channel>,
    schema_cache: &Arc<RwLock<SchemaCache>>,
    api_key: Option<&String>,
    user_agent: &str,
) {
    if let Ok(tables) = get_tables(&mut client, api_key, user_agent).await {
        if let Ok(mut cache) = schema_cache.try_write() {
            cache.update_tables(tables);
        }
    }

    if let Ok(columns) = get_columns(&mut client, api_key, user_agent).await {
        if let Ok(mut cache) = schema_cache.try_write() {
            cache.update_columns(columns);
        }
    }
}

async fn get_tables(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let query = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime')";

    let records = crate::get_records(
        client.clone(),
        query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut tables = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for value in array.iter() {
                if let Some(table_name) = value {
                    tables.push(table_name.to_string());
                }
            }
        }
    }

    Ok(tables)
}

async fn get_columns(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let query = "SELECT column_name FROM information_schema.columns";

    let records = crate::get_records(
        client.clone(),
        &query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut columns = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for value in array.iter() {
                if let Some(column_name) = value {
                    columns.push(column_name.to_string());
                }
            }
        }
    }

    Ok(columns)
}

fn extract_word(line: &str, pos: usize) -> (usize, &str) {
    let pos = pos.min(line.len());
    let chars: Vec<char> = line.chars().collect();

    // Find start of current word
    let mut start = pos;
    while start > 0 {
        let ch = chars[start - 1];
        if is_word_boundary(ch) {
            break;
        }
        start -= 1;
    }

    // Find end of current word
    let mut end = pos;
    while end < chars.len() {
        let ch = chars[end];
        if is_word_boundary(ch) {
            break;
        }
        end += 1;
    }

    let start_byte = chars[..start].iter().map(|c| c.len_utf8()).sum();
    let end_byte = chars[..end].iter().map(|c| c.len_utf8()).sum();

    (start, &line[start_byte..end_byte])
}

fn is_word_boundary(ch: char) -> bool {
    match ch {
        ' ' | '\t' | '\n' | '\r' => true,
        '(' | ')' | ',' | ';' | '=' | '<' | '>' | '!' | '+' | '-' | '*' | '/' | '%' => true,
        '\'' | '"' | '`' => true,
        '.' | '[' | ']' | '{' | '}' | '|' | '&' | '^' | '~' => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyline::history::MemHistory;

    fn create_test_editor_helper() -> EditorHelper {
        let schema_cache = Arc::new(RwLock::new(SchemaCache {
            tables: vec![
                "users".to_string(),
                "products".to_string(),
                "orders".to_string(),
                "user_profiles".to_string(),
            ],
            columns: vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "age".to_string(),
                "price".to_string(),
                "product_name".to_string(),
                "user_id".to_string(),
                "order_date".to_string(),
                "profile_picture".to_string(),
            ],
        }));

        EditorHelper {
            schema_cache,
            flight_client: None,
            api_key: None,
            user_agent: "test".to_string(),
            refresh_task_handle: None,
            shutdown_sender: None,
        }
    }

    fn get_completions(helper: &EditorHelper, line: &str, pos: usize) -> Vec<String> {
        let history = MemHistory::new();
        let ctx = Context::new(&history);
        let result = helper.complete(line, pos, &ctx).unwrap();
        result.1.into_iter().map(|pair| pair.replacement).collect()
    }

    #[test]
    fn test_extract_word() {
        assert_eq!(extract_word("SELECT name FROM users", 6), (0, "SELECT"));
        assert_eq!(extract_word("SELECT name FROM users", 11), (7, "name"));
        assert_eq!(extract_word("SELECT u.name, u.email", 10), (9, "name"));
        assert_eq!(extract_word("SELECT ", 7), (7, ""));
    }

    #[test]
    fn test_is_word_boundary() {
        assert!(is_word_boundary(' '));
        assert!(is_word_boundary('\t'));
        assert!(is_word_boundary('\n'));

        assert!(is_word_boundary('('));
        assert!(is_word_boundary(')'));
        assert!(is_word_boundary(','));
        assert!(is_word_boundary(';'));
        assert!(is_word_boundary('='));

        assert!(!is_word_boundary('a'));
        assert!(!is_word_boundary('_'));
        assert!(!is_word_boundary('1'));
    }

    #[test]
    fn test_keyword_completion() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "sel", 3);
        assert!(completions.contains(&"select ".to_string()));

        let completions = get_completions(&helper, "select * fr", 11);
        assert!(completions.contains(&"from ".to_string()));

        let completions = get_completions(&helper, "SEL", 3);
        assert!(completions.contains(&"select ".to_string()));
    }

    #[test]
    fn test_function_completion() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "cou", 3);
        assert!(completions.contains(&"count".to_string()));

        let completions = get_completions(&helper, "su", 2);
        assert!(completions.contains(&"sum".to_string()));

        let completions = get_completions(&helper, "conc", 4);
        assert!(completions.contains(&"concat".to_string()));
    }

    #[test]
    fn test_table_completion_after_from() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "SELECT * FROM u", 15);
        assert!(completions.contains(&"users ".to_string()));
        assert!(completions.contains(&"user_profiles ".to_string()));

        // Should not have keywords or columns
        assert!(
            completions
                .iter()
                .all(|d| d == "users " || d == "user_profiles ")
        );
    }

    #[test]
    fn test_column_completion() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "na", 2);
        assert!(completions.contains(&"name ".to_string()));

        let completions = get_completions(&helper, "email", 5);
        assert!(completions.contains(&"email ".to_string()));

        let completions = get_completions(&helper, "user_", 5);
        assert!(completions.contains(&"user_id ".to_string()));
    }

    #[test]
    fn test_empty_matches() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "xyz", 3);
        assert!(completions.is_empty());

        let completions = get_completions(&helper, "qwerty", 6);
        assert!(completions.is_empty());
    }

    #[test]
    fn test_complex_completion() {
        let helper = create_test_editor_helper();

        let sql =
            "SELECT u.name, p.price FROM users u JOIN products p ON u.id = p.user_id WHERE u.a";
        let completions = get_completions(&helper, sql, sql.len());

        assert!(completions.contains(&"age ".to_string()));
    }

    #[test]
    fn test_multiline() {
        let helper = create_test_editor_helper();

        let sql = "SELECT name\nFROM u";
        let completions = get_completions(&helper, sql, sql.len());

        assert!(completions.contains(&"users ".to_string()));
    }

    #[test]
    fn test_completion_quotes() {
        let helper = create_test_editor_helper();

        let sql = "SELECT name FROM users WHERE name = 'john' AND a";
        let completions = get_completions(&helper, sql, sql.len());

        assert!(completions.contains(&"age ".to_string()));
        assert!(completions.contains(&"and ".to_string()));
    }

    #[test]
    fn test_completion_special_characters() {
        let helper = create_test_editor_helper();

        let test_cases = [
            "SELECT * FROM users WHERE age > a",
            "SELECT * FROM users WHERE age = a",
            "SELECT * FROM users WHERE name LIKE 'a%' AND a",
            "SELECT name, a",
        ];

        for sql in test_cases {
            let completions = get_completions(&helper, sql, sql.len());
            assert!(
                completions.contains(&"age ".to_string()),
                "Failed for: {}",
                sql
            );
        }
    }
}
