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

use regex::Regex;
use serde_json::Value;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq)]
pub struct PaginationParameters {
    pub resource_name: String,
    pub count: usize,
    page_info_path: String,
}

impl PaginationParameters {
    pub fn parse(query: &str, pointer: &str) -> Option<Self> {
        let pagination_pattern = r"(?xsm)(\w*)\s*\([^)]*first:\s*(\d+).*\).*\{.*pageInfo.*\{.*hasNextPage.*endCursor.*?\}.*?\}";
        let regex = unsafe { Regex::new(pagination_pattern).unwrap_unchecked() };
        match regex.captures(query) {
            Some(captures) => {
                let resource_name = captures.get(1).map(|m| m.as_str().to_owned());
                let count = captures
                    .get(2)
                    .map(|m| m.as_str().parse::<usize>())
                    .transpose()
                    .unwrap_or(None);

                match (resource_name, count) {
                    (Some(resource_name), Some(count)) => {
                        let pattern = format!(r"^(.*?{resource_name})");
                        let regex = unsafe { Regex::new(pattern.as_str()).unwrap_unchecked() };

                        let captures = regex.captures(pointer);

                        let page_info_path = captures
                            .and_then(|c| c.get(1).map(|m| m.as_str().to_owned() + "/pageInfo"));

                        page_info_path.map(|page_info_path| Self {
                            resource_name,
                            count,
                            page_info_path,
                        })
                    }
                    _ => None,
                }
            }
            None => None,
        }
    }

    pub fn apply(
        &self,
        query: &str,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> (String, bool) {
        let mut limit_reached = false;

        match cursor {
            Some(cursor) => {
                let mut count = self.count;

                if let Some(limit) = limit {
                    if limit < count {
                        count = limit;
                        limit_reached = true;
                    }
                }
                let pattern = format!(r#"{}\s*\(.*\)"#, self.resource_name);
                let regex = unsafe { Regex::new(&pattern).unwrap_unchecked() };

                let new_query = regex.replace(
                    query,
                    format!(
                        r#"{} (first: {count}, after: "{cursor}")"#,
                        self.resource_name
                    )
                    .as_str(),
                );
                (new_query.to_string(), limit_reached)
            }
            _ => (query.to_string(), false),
        }
    }

    pub fn get_next_cursor_from_response(
        &self,
        response: &Value,
        limit_reached: bool,
    ) -> Option<String> {
        if limit_reached {
            return None;
        }
        let page_info = response
            .pointer(&self.page_info_path)
            .unwrap_or(&Value::Null);
        let end_cursor = page_info["endCursor"].as_str().map(ToString::to_string);
        let has_next_page = page_info["hasNextPage"].as_bool().unwrap_or(false);

        if has_next_page {
            end_cursor
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use super::PaginationParameters;

    #[test]
    fn test_pagination_parse() {
        let query = r"query {
      users(first: 10) {
        pageInfo {
          hasNextPage
          endCursor
        }
        }
    }";
        let pointer = r"/data/users";
        let pagination_parameters = PaginationParameters::parse(query, pointer);
        assert_eq!(
            pagination_parameters,
            Some(PaginationParameters {
                resource_name: "users".to_owned(),
                count: 10,
                page_info_path: "/data/users/pageInfo".to_owned(),
            })
        );

        let query = r"query {
            users(first: 10) {
                name
              }
          }";
        let pointer = r"/data/users";
        let pagination_parameters = PaginationParameters::parse(query, pointer);
        assert_eq!(pagination_parameters, None);
    }

    #[test]
    fn test_pagination_apply() {
        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");
        let (new_query, limit_reached) =
            pagination_parameters.apply(query, None, Some("new_cursor".to_string()));
        let expected_query = r#"query {
            users (first: 10, after: "new_cursor") {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        assert!(!limit_reached);
        assert_eq!(new_query, expected_query);

        let query = r#"query {
            users(after: "user_cursor", first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");
        let (new_query, limit_reached) =
            pagination_parameters.apply(query, None, Some("new_cursor".to_string()));
        let expected_query = r#"query {
            users (first: 10, after: "new_cursor") {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        assert!(!limit_reached);
        assert_eq!(new_query, expected_query);

        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");
        let (new_query, limit_reached) =
            pagination_parameters.apply(query, Some(5), Some("new_cursor".to_string()));
        let expected_query = r#"query {
            users (first: 5, after: "new_cursor") {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        assert!(limit_reached);
        assert_eq!(new_query, expected_query);
    }

    #[test]
    fn test_pagination_get_next_cursor_from_response() {
        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");

        let response = serde_json::from_str(
            r#"{
            "data": {
                "users": {
                    "pageInfo": {
                        "hasNextPage": true,
                        "endCursor": "new_cursor"
                    }
                }
            }
        }"#,
        )
        .expect("Invalid json");

        let next_cursor = pagination_parameters.get_next_cursor_from_response(&response, false);
        assert_eq!(
            next_cursor,
            Some("new_cursor".to_string()),
            "Expected next cursor to be new_cursor"
        );

        let next_cursor = pagination_parameters.get_next_cursor_from_response(&response, true);
        assert_eq!(
            next_cursor, None,
            "Next cursor should be None if we reached the limit"
        );

        let response = serde_json::from_str(
            r#"{
            "data": {
                "users": {

                }
            }
        }"#,
        )
        .expect("Invalid json");
        let next_cursor = pagination_parameters.get_next_cursor_from_response(&response, false);
        assert_eq!(next_cursor, None, "Should be None if no value returned");
    }
}
