use crate::EditorHelper;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::sql::sqlparser::keywords::ALL_KEYWORDS;
use rustyline::Context;
use rustyline::completion::{Completer, Pair};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tokio::time::interval;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
struct StringValue {
    _original: Arc<str>,
    pub lower: Arc<str>,
}

impl From<String> for StringValue {
    fn from(s: String) -> Self {
        let original: Arc<str> = Arc::from(s.into_boxed_str());
        let lower: Arc<str> = Arc::from(original.to_lowercase());
        Self {
            _original: original,
            lower,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaCache {
    udfs: Vec<String>,
    tables: Vec<StringValue>,
    columns: Vec<StringValue>,

    keywords: Vec<String>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            udfs: Vec::new(),
            tables: Vec::new(),
            columns: Vec::new(),
            keywords: ALL_KEYWORDS.iter().map(|k| k.to_lowercase()).collect(),
        }
    }

    fn update_tables(&mut self, tables: Vec<String>) {
        self.tables = tables.into_iter().map(StringValue::from).collect();
    }

    fn update_columns(&mut self, columns: Vec<String>) {
        self.columns = columns.into_iter().map(StringValue::from).collect();
    }

    fn update_udfs(&mut self, udfs: Vec<String>) {
        self.udfs = udfs;
    }
}

impl EditorHelper {
    /// Start the background refresh task
    /// `refresh_interval`: How often to refresh schema (in seconds)
    pub fn start_refreshing(&mut self, refresh_interval: u64) {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_sender = Some(shutdown_tx);

        let Some(client) = self.flight_client.clone() else {
            return;
        };
        let schema_cache = Arc::clone(&self.schema_cache);
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
            rustyline::error::ReadlineError::Io(std::io::Error::other("Cache lock error"))
        })?;

        let before_cursor = &line[..pos].to_lowercase();

        // Check if we're in a context where only tables should be suggested
        let should_suggest_only_tables =
            if before_cursor.contains("from ") && !before_cursor.contains("where") {
                // Find the last occurrence of "from "
                if let Some(from_pos) = before_cursor.rfind("from ") {
                    let after_from = &before_cursor[from_pos + 5..].trim();

                    // If nothing after "from", suggest only tables
                    if after_from.is_empty() {
                        true
                    } else {
                        // Count words after "from" - if only 1 word (potentially incomplete), suggest tables
                        let words_after_from: Vec<&str> = after_from.split_whitespace().collect();
                        words_after_from.len() <= 1
                    }
                } else {
                    false
                }
            } else {
                // Check if the last word is "join" (could be after WHERE clause)
                before_cursor.trim().ends_with("join")
            };

        if should_suggest_only_tables {
            // Only suggest tables
            for table in &cache.tables {
                if table.lower.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: table.lower.to_string(),
                        replacement: format!("{} ", table.lower.as_ref()),
                    });
                }
            }
        } else {
            // Suggest everything
            for keyword in &cache.keywords {
                if keyword.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: keyword.to_lowercase(),
                        replacement: format!("{} ", keyword.to_lowercase()),
                    });
                }
            }

            for udf_name in &cache.udfs {
                if udf_name.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: udf_name.to_lowercase(),
                        replacement: udf_name.to_lowercase(),
                    });
                }
            }

            for table in &cache.tables {
                if table.lower.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: table.lower.to_string(),
                        replacement: format!("{} ", table.lower.as_ref()),
                    });
                }
            }

            for column in &cache.columns {
                if column.lower.starts_with(&word_lower) {
                    matches.push(Pair {
                        display: column.lower.to_string(),
                        replacement: format!("{} ", column.lower.as_ref()),
                    });
                }
            }
        }

        Ok((start, matches))
    }
}

async fn refresh_schema(
    client: FlightServiceClient<Channel>,
    schema_cache: &Arc<RwLock<SchemaCache>>,
    api_key: Option<&String>,
    user_agent: &str,
) {
    let mut client1 = client.clone();
    let mut client2 = client.clone();
    let mut client3 = client;

    let (tables_result, columns_result, udfs_result) = tokio::join!(
        get_tables(&mut client1, api_key, user_agent),
        get_columns(&mut client2, api_key, user_agent),
        get_udfs(&mut client3, api_key, user_agent)
    );

    if let Ok(mut cache) = schema_cache.try_write() {
        if let Ok(tables) = tables_result {
            cache.update_tables(tables);
        }

        if let Ok(columns) = columns_result {
            cache.update_columns(columns);
        }

        if let Ok(udfs) = udfs_result {
            cache.update_udfs(udfs);
        }
    }
}

async fn get_tables(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
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
            for table_name in array.iter().flatten() {
                tables.push(table_name.to_string());
            }
        }
    }

    Ok(tables)
}

async fn get_columns(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query = "SELECT column_name FROM information_schema.columns";

    let records = crate::get_records(
        client.clone(),
        query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut columns = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for column_name in array.iter().flatten() {
                columns.push(column_name.to_string());
            }
        }
    }

    Ok(columns)
}

async fn get_udfs(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query = "SELECT name FROM list_udfs()";

    let records = crate::get_records(
        client.clone(),
        query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut udfs = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for column_name in array.iter().flatten() {
                udfs.push(column_name.to_string());
            }
        }
    }

    Ok(udfs)
}

fn extract_word(line: &str, pos: usize) -> (usize, &str) {
    let pos = pos.min(line.len());
    let chars: Vec<char> = line.chars().collect();

    // Find start of current word
    let mut start = pos;
    while start > 0 {
        let Some(ch) = chars.get(start - 1) else {
            unreachable!("Start position should always be equal to the length of the characters");
        };
        if is_word_boundary(*ch) {
            break;
        }
        start -= 1;
    }

    // Find end of current word
    let mut end = pos;
    while end < chars.len() {
        let Some(ch) = chars.get(end) else {
            unreachable!("Start position should always be equal to the length of the characters");
        };
        if is_word_boundary(*ch) {
            break;
        }
        end += 1;
    }

    let start_byte = chars[..start].iter().map(|c| c.len_utf8()).sum();
    let end_byte = chars[..end].iter().map(|c| c.len_utf8()).sum();

    (start, &line[start_byte..end_byte])
}

fn is_word_boundary(ch: char) -> bool {
    #[allow(clippy::match_like_matches_macro)]
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
            udfs: vec!["count".to_string(), "sum".to_string(), "concat".to_string()],
            tables: vec![
                "users".to_string(),
                "products".to_string(),
                "orders".to_string(),
                "user_profiles".to_string(),
            ]
            .into_iter()
            .map(StringValue::from)
            .collect(),
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
            ]
            .into_iter()
            .map(StringValue::from)
            .collect(),
            keywords: ALL_KEYWORDS.iter().map(|k| k.to_lowercase()).collect(),
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
        let result = helper
            .complete(line, pos, &ctx)
            .expect("Should complete suggestion");
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

        let completions = get_completions(&helper, "select * fr", 11);
        assert!(completions.contains(&"from ".to_string()));

        let completions = get_completions(&helper, "select * from t1 w", 18);
        assert!(completions.contains(&"where ".to_string()));
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
                "Failed for: {sql}"
            );
        }
    }
}
