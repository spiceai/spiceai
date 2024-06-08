use regex::Regex;

#[derive(Debug)]
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
                    .map(|m| m.as_str().parse::<usize>().unwrap());

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
