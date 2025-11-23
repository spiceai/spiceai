use crate::EditorHelper;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::array::{Array, StringArray};
use rustyline::Context;
use rustyline::completion::{Completer, Pair};
use rustyline::history::SearchDirection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tokio::time::interval;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
struct StringValue {
    original: Arc<str>,
    lower: Arc<str>,
}

impl StringValue {
    fn original(&self) -> &str {
        &self.original
    }

    fn lower(&self) -> &str {
        &self.lower
    }
}

impl From<String> for StringValue {
    fn from(s: String) -> Self {
        let original: Arc<str> = Arc::from(s.into_boxed_str());
        let lower: Arc<str> = Arc::from(original.to_lowercase());
        Self { original, lower }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaCache {
    udfs: Vec<String>,
    udtfs: Vec<String>,
    builtin_functions: Vec<String>,
    schemas: Vec<StringValue>,
    tables: Vec<StringValue>,
    columns: Vec<StringValue>,

    keywords: Vec<String>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            udfs: Vec::new(),
            udtfs: Vec::new(),
            builtin_functions: Vec::new(),
            schemas: Vec::new(),
            tables: Vec::new(),
            columns: Vec::new(),
            keywords: filter_useful_keywords(),
        }
    }

    fn update_tables(&mut self, tables: Vec<String>) {
        self.tables = tables.into_iter().map(StringValue::from).collect();
    }

    fn update_schemas(&mut self, schemas: Vec<String>) {
        self.schemas = schemas.into_iter().map(StringValue::from).collect();
    }

    fn update_columns(&mut self, columns: Vec<String>) {
        self.columns = columns.into_iter().map(StringValue::from).collect();
    }

    fn update_udfs(&mut self, udfs: Vec<String>) {
        self.udfs = udfs;
    }

    fn update_udtfs(&mut self, udtfs: Vec<String>) {
        self.udtfs = udtfs;
    }

    fn update_builtin_functions(&mut self, builtin_functions: Vec<String>) {
        self.builtin_functions = builtin_functions;
    }
}

impl EditorHelper {
    /// Perform an initial synchronous refresh of schema metadata
    pub async fn refresh_now(&mut self) {
        let Some(client) = self.flight_client.clone() else {
            return;
        };
        refresh_schema(
            client,
            &self.schema_cache,
            self.api_key.as_ref(),
            &self.user_agent,
        )
        .await;
    }

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
            // Skip the first tick since interval.tick() returns immediately the first time
            interval.tick().await;

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

/// Helper function to add table completions with proper quoting and deduplication
fn suggest_tables(
    tables: &[StringValue],
    word_lower: &str,
    seen: &mut std::collections::HashSet<String>,
    matches: &mut Vec<Pair>,
) {
    for table in tables {
        if table.lower().starts_with(word_lower) {
            let quoted_name = quote_identifier_if_needed(table.original());
            let replacement = format!("{quoted_name} ");
            if seen.insert(replacement.clone()) {
                matches.push(Pair {
                    display: table.original().to_string(),
                    replacement,
                });
            }
        }
    }
}

impl Completer for EditorHelper {
    type Candidate = Pair;

    #[allow(clippy::too_many_lines)]
    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let (start, word) = extract_word(line, pos);
        let word_lower = word.to_lowercase();
        let mut matches = Vec::new();
        let mut seen = std::collections::HashSet::new();

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
            // Only suggest tables after FROM or JOIN
            suggest_tables(&cache.tables, &word_lower, &mut seen, &mut matches);
        } else {
            // Suggest keywords
            for keyword in &cache.keywords {
                if keyword.starts_with(&word_lower) {
                    let replacement = format!("{} ", keyword.to_lowercase());
                    if seen.insert(replacement.clone()) {
                        matches.push(Pair {
                            display: keyword.to_lowercase(),
                            replacement,
                        });
                    }
                }
            }

            // Suggest UDFs
            for udf_name in &cache.udfs {
                if udf_name.starts_with(&word_lower) && seen.insert(udf_name.to_lowercase()) {
                    matches.push(Pair {
                        display: udf_name.to_lowercase(),
                        replacement: udf_name.to_lowercase(),
                    });
                }
            }

            // Suggest built-in functions
            for func_name in &cache.builtin_functions {
                if func_name.starts_with(&word_lower) && seen.insert(func_name.to_lowercase()) {
                    matches.push(Pair {
                        display: func_name.to_lowercase(),
                        replacement: func_name.to_lowercase(),
                    });
                }
            }

            // Suggest UDTFs
            for udtf_name in &cache.udtfs {
                if udtf_name.starts_with(&word_lower) && seen.insert(udtf_name.to_lowercase()) {
                    matches.push(Pair {
                        display: udtf_name.to_lowercase(),
                        replacement: udtf_name.to_lowercase(),
                    });
                }
            }

            // Suggest schemas
            for schema in &cache.schemas {
                if schema.lower().starts_with(&word_lower) {
                    let quoted_name = quote_identifier_if_needed(schema.original());
                    let replacement = format!("{quoted_name}.");
                    if seen.insert(replacement.clone()) {
                        matches.push(Pair {
                            display: schema.original().to_string(),
                            replacement,
                        });
                    }
                }
            }

            // Suggest tables
            suggest_tables(&cache.tables, &word_lower, &mut seen, &mut matches);

            // Suggest columns
            for column in &cache.columns {
                if column.lower().starts_with(&word_lower) {
                    let quoted_name = quote_identifier_if_needed(column.original());
                    let replacement = format!("{quoted_name} ");
                    if seen.insert(replacement.clone()) {
                        matches.push(Pair {
                            display: column.original().to_string(),
                            replacement,
                        });
                    }
                }
            }
        }

        // Add history-based suggestions only if the line is completely empty
        if line.trim().is_empty() {
            let history = ctx.history();
            // Iterate through history in reverse (most recent first)
            for idx in (0..history.len()).rev() {
                if let Ok(Some(result)) = history.get(idx, SearchDirection::Reverse) {
                    let entry_str = result.entry.as_ref();
                    if !entry_str.is_empty() && seen.insert(entry_str.to_string()) {
                        matches.push(Pair {
                            display: entry_str.to_string(),
                            replacement: entry_str.to_string(),
                        });
                    }
                }
            }
        }

        Ok((start, matches))
    }
}

#[allow(clippy::similar_names)]
async fn refresh_schema(
    client: FlightServiceClient<Channel>,
    schema_cache: &Arc<RwLock<SchemaCache>>,
    api_key: Option<&String>,
    user_agent: &str,
) {
    let mut client1 = client.clone();
    let mut client2 = client.clone();
    let mut client3 = client.clone();
    let mut client4 = client.clone();
    let mut client5 = client.clone();
    let mut client6 = client;

    let (
        tables_result,
        schemas_result,
        columns_result,
        udfs_result,
        udtfs_result,
        builtin_functions_result,
    ) = tokio::join!(
        get_tables(&mut client1, api_key, user_agent),
        get_schemas(&mut client2, api_key, user_agent),
        get_columns(&mut client3, api_key, user_agent),
        get_udfs(&mut client4, api_key, user_agent),
        get_udtfs(&mut client5, api_key, user_agent),
        get_builtin_functions(&mut client6, api_key, user_agent)
    );

    if let Ok(mut cache) = schema_cache.try_write() {
        if let Ok(tables) = tables_result {
            cache.update_tables(tables);
        }

        if let Ok(schemas) = schemas_result {
            cache.update_schemas(schemas);
        }

        if let Ok(columns) = columns_result {
            cache.update_columns(columns);
        }

        if let Ok(udfs) = udfs_result {
            cache.update_udfs(udfs);
        }

        if let Ok(udtfs) = udtfs_result {
            cache.update_udtfs(udtfs);
        }

        if let Ok(builtin_functions) = builtin_functions_result {
            cache.update_builtin_functions(builtin_functions);
        }
    }
}

/// Prefix for autocomplete metadata queries to help the runtime identify and potentially optimize these queries
const AUTOCOMPLETE_PREFIX: &str = "--autocomplete\n";

async fn get_tables(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query = format!(
        "{AUTOCOMPLETE_PREFIX}SELECT table_schema || '.' || table_name as full_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime') UNION SELECT table_schema || '.' || table_name FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'runtime') ORDER BY full_name"
    );

    let records = crate::get_records(
        client.clone(),
        &query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut tables = Vec::new();
    let mut table_set = std::collections::HashSet::new();

    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for full_name in array.iter().flatten() {
                // Always add the fully qualified name
                if table_set.insert(full_name.to_string()) {
                    tables.push(full_name.to_string());
                }

                // For public schema, also add unqualified name
                if let Some(unqualified) = full_name.strip_prefix("public.")
                    && table_set.insert(unqualified.to_string())
                {
                    tables.push(unqualified.to_string());
                }
            }
        }
    }

    Ok(tables)
}

async fn get_schemas(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    // Query schemata to include all schemas, not just those with tables
    let query = format!(
        "{AUTOCOMPLETE_PREFIX}SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'runtime')"
    );

    let records = crate::get_records(
        client.clone(),
        &query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut schemas = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for schema_name in array.iter().flatten() {
                schemas.push(schema_name.to_string());
            }
        }
    }

    Ok(schemas)
}

async fn get_columns(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query = format!("{AUTOCOMPLETE_PREFIX}SELECT column_name FROM information_schema.columns");

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
    let query = format!("{AUTOCOMPLETE_PREFIX}SELECT name FROM list_udfs()");

    let records = crate::get_records(
        client.clone(),
        &query,
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

async fn get_udtfs(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query = format!("{AUTOCOMPLETE_PREFIX}SELECT name FROM list_udtfs()");

    let records = crate::get_records(
        client.clone(),
        &query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut udtfs = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for udtf_name in array.iter().flatten() {
                udtfs.push(udtf_name.to_string());
            }
        }
    }

    Ok(udtfs)
}

async fn get_builtin_functions(
    client: &mut FlightServiceClient<Channel>,
    api_key: Option<&String>,
    user_agent: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let query =
        format!("{AUTOCOMPLETE_PREFIX}SELECT routine_name FROM information_schema.routines");

    let records = crate::get_records(
        client.clone(),
        &query,
        api_key,
        user_agent,
        crate::cache_control::CacheControl::NoCache,
    )
    .await?;

    let mut functions = Vec::new();
    for batch in records.0 {
        if let Some(array) = batch.column(0).as_any().downcast_ref::<StringArray>() {
            for func_name in array.iter().flatten() {
                functions.push(func_name.to_string());
            }
        }
    }

    Ok(functions)
}

/// Quote an identifier if it contains uppercase letters or special characters.
/// Handles schema-qualified names by quoting each part independently.
fn quote_identifier_if_needed(identifier: &str) -> String {
    // Always split on dots and quote each part as needed
    let parts: Vec<&str> = identifier.split('.').collect();
    let quoted_parts: Vec<String> = parts
        .iter()
        .map(|part| {
            if part
                .chars()
                .any(|c| c.is_uppercase() || !c.is_alphanumeric() && c != '_')
            {
                format!("\"{part}\"")
            } else {
                (*part).to_string()
            }
        })
        .collect();
    quoted_parts.join(".")
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

/// Filter `DataFusion`'s full keyword list to only include useful autocomplete suggestions
fn filter_useful_keywords() -> Vec<String> {
    let useful_keywords = [
        // Query statements
        "select",
        "from",
        "where",
        "having",
        "order",
        "group",
        "limit",
        "offset",
        "distinct",
        "union",
        "intersect",
        "except",
        // Join types
        "join",
        "inner",
        "left",
        "right",
        "full",
        "cross",
        "outer",
        "natural",
        "on",
        "using",
        // Boolean operators
        "and",
        "or",
        "not",
        "in",
        "exists",
        "between",
        "like",
        "ilike",
        "is",
        "null",
        // Common keywords
        "as",
        "asc",
        "desc",
        "by",
        "all",
        "any",
        "case",
        "when",
        "then",
        "else",
        "end",
        // DML statements
        "insert",
        "into",
        "values",
        "update",
        "set",
        "delete",
        // DDL statements
        "create",
        "alter",
        "drop",
        "truncate",
        "table",
        "view",
        "index",
        "schema",
        "database",
        // Constraints and keys
        "primary",
        "foreign",
        "key",
        "references",
        "unique",
        "constraint",
        "check",
        "default",
        // Functions and casts
        "cast",
        "extract",
        "interval",
        "current_date",
        "current_time",
        "current_timestamp",
        // CTEs and subqueries
        "with",
        "recursive",
        // SHOW commands
        "show",
        "tables",
        "schemas",
        "columns",
        "databases",
        // DESCRIBE/EXPLAIN
        "describe",
        "explain",
        "analyze",
    ];

    useful_keywords.iter().map(|&s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyline::history::MemHistory;

    fn create_test_editor_helper() -> EditorHelper {
        let schema_cache = Arc::new(RwLock::new(SchemaCache {
            udfs: vec!["count".to_string(), "sum".to_string(), "concat".to_string()],
            udtfs: vec!["generate_series".to_string(), "unnest".to_string()],
            builtin_functions: vec!["avg".to_string(), "max".to_string(), "min".to_string()],
            schemas: vec!["public".to_string(), "analytics".to_string()]
                .into_iter()
                .map(StringValue::from)
                .collect(),
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
            keywords: filter_useful_keywords(),
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

    #[test]
    fn test_schema_completion() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "pub", 3);
        assert!(completions.contains(&"public.".to_string()));

        let completions = get_completions(&helper, "analyt", 6);
        assert!(completions.contains(&"analytics.".to_string()));
    }

    #[test]
    fn test_udtf_completion() {
        let helper = create_test_editor_helper();

        let completions = get_completions(&helper, "generate", 8);
        assert!(completions.contains(&"generate_series".to_string()));

        let completions = get_completions(&helper, "unnest", 6);
        assert!(completions.contains(&"unnest".to_string()));
    }

    #[test]
    fn test_quote_identifier_if_needed() {
        // Lowercase identifiers don't need quoting
        assert_eq!(quote_identifier_if_needed("users"), "users");
        assert_eq!(quote_identifier_if_needed("my_table"), "my_table");

        // Uppercase identifiers need quoting
        assert_eq!(quote_identifier_if_needed("MyTable"), "\"MyTable\"");
        assert_eq!(quote_identifier_if_needed("USERS"), "\"USERS\"");

        // Schema-qualified names with uppercase
        assert_eq!(
            quote_identifier_if_needed("MySchema.MyTable"),
            "\"MySchema\".\"MyTable\""
        );
        assert_eq!(
            quote_identifier_if_needed("public.MyTable"),
            "public.\"MyTable\""
        );
        assert_eq!(
            quote_identifier_if_needed("MySchema.users"),
            "\"MySchema\".users"
        );
        assert_eq!(quote_identifier_if_needed("public.users"), "public.users");
    }

    #[test]
    fn test_uppercase_table_completion() {
        let mut cache = SchemaCache::new();
        cache.update_tables(vec![
            "MyTable".to_string(),
            "users".to_string(),
            "MyView".to_string(), // Unqualified version
            "public.MyView".to_string(),
        ]);

        let helper = EditorHelper {
            schema_cache: Arc::new(RwLock::new(cache)),
            flight_client: None,
            api_key: None,
            user_agent: String::new(),
            shutdown_sender: None,
            refresh_task_handle: None,
        };

        // Test that uppercase table is quoted
        let sql = "select * from My";
        let completions = get_completions(&helper, sql, sql.len());
        assert!(
            completions.iter().any(|c| c.contains("\"MyTable\"")),
            "Should suggest quoted MyTable, got: {completions:?}"
        );
        assert!(
            completions.iter().any(|c| c.contains("\"MyView\"")),
            "Should suggest quoted MyView, got: {completions:?}"
        );

        // Test lowercase table is not quoted
        let sql2 = "select * from user";
        let completions = get_completions(&helper, sql2, sql2.len());
        assert!(
            completions
                .iter()
                .any(|c| c.contains("users ") && !c.contains('"')),
            "Should suggest unquoted users, got: {completions:?}"
        );
    }
}
