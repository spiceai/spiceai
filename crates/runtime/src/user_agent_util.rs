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

pub fn extract_user_agent(ua_header: Option<&str>) -> (String, String, String) {
    let ua_header = ua_header.unwrap_or("");
    let parts: Vec<&str> = ua_header.splitn(3, ' ').collect();
    if parts.len() == 3 {
        tracing::trace!("Client user agent: {ua_header}");
        (
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        )
    } else {
        tracing::trace!("Could not parse user agent: {ua_header}");
        (
            "unknown".to_string(),
            "unknown".to_string(),
            "unknown".to_string(),
        )
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_ua_extraction() {
        let (agent, version, os_str) =
            super::extract_user_agent(Some("gospice 0.1.0 (Pop!_OS 22.04 LTS)"));
        assert_eq!(agent, "gospice");
        assert_eq!(version, "0.1.0");
        assert_eq!(os_str, "(Pop!_OS 22.04 LTS)");

        let (agent, version, _os_str) = super::extract_user_agent(Some("gospice 0.1.0"));
        assert_eq!(agent, "unknown");
        assert_eq!(version, "unknown");
    }
}
