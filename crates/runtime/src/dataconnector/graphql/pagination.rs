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

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq)]
pub struct PaginationParameters {
    pub resource_name: String,
    pub count: usize,
}

impl PaginationParameters {
    pub fn parse(query: &str, json_path: &str) -> Option<Self> {
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
                        if !json_path.contains(&resource_name) {
                            return None;
                        }
                        Some(Self {
                            resource_name,
                            count,
                        })
                    }
                    _ => None,
                }
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PaginationParameters;

    #[test]
    fn basic_test_pagination_parse() {
        let query = r"query {
      users(first: 10) {
        pageInfo {
          hasNextPage
          endCursor
        }
        }
    }";
        let json_path = r"data.users";
        let pagination_parameters = PaginationParameters::parse(query, json_path);
        assert_eq!(
            pagination_parameters,
            Some(PaginationParameters {
                resource_name: "users".to_owned(),
                count: 10
            })
        );
    }
}
