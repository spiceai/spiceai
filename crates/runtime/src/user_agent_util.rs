use regex::Regex;
use std::sync::LazyLock;

static UA_REGEX: LazyLock<Result<Regex, regex::Error>> =
    LazyLock::new(|| Regex::new(r"^(\S+) (\S+) \((.*?)\)$"));

pub fn extract_user_agent(ua_header: Option<&str>) -> (String, String, String) {
    let ua_header = ua_header.unwrap_or("");
    if let Ok(regex) = &*UA_REGEX {
        if let Some(ua_parts) = regex.captures(ua_header) {
            let user_agent = ua_parts
                .get(1)
                .map_or_else(|| "unknown", |p| p.as_str())
                .to_string();
            let agent_version = ua_parts
                .get(2)
                .map_or_else(|| "unknown", |p| p.as_str())
                .to_string();
            let agent_os = ua_parts
                .get(3)
                .map_or_else(|| "unknown", |p| p.as_str())
                .to_string();
            (user_agent, agent_version, agent_os)
        } else {
            tracing::trace!("Could not parse user agent: {ua_header}");
            (
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
            )
        }
    } else {
        tracing::error!("Could not compile UA regex");
        (
            "unknown".to_string(),
            "unknown".to_string(),
            "unknown".to_string(),
        )
    }
}
