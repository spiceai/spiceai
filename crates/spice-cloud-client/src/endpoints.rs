/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Spice Cloud runtime endpoint helpers.

pub const LEGACY_DATA_ENDPOINT: &str = "https://data.spiceai.io";
pub const LEGACY_FLIGHT_HOST: &str = "flight.spiceai.io";
pub const DATA_HOST_SUFFIX: &str = "-prod-aws-data.spiceai.io";
pub const FLIGHT_HOST_SUFFIX: &str = "-prod-aws-flight.spiceai.io";

#[must_use]
pub fn data_endpoint(region: &str) -> String {
    format!("https://{region}{DATA_HOST_SUFFIX}")
}

#[must_use]
pub fn flight_endpoint(region: &str) -> String {
    format!("https://{region}{FLIGHT_HOST_SUFFIX}")
}

#[must_use]
pub fn is_valid_region(region: &str) -> bool {
    region
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && region
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && region
            .chars()
            .last()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

#[must_use]
pub fn endpoint_host(endpoint: &str) -> Option<String> {
    url::Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(ToString::to_string))
}

#[must_use]
pub fn is_legacy_flight_endpoint(endpoint: &str) -> bool {
    endpoint_host(endpoint).is_some_and(|host| host == LEGACY_FLIGHT_HOST)
}

#[must_use]
pub fn flight_endpoint_region(endpoint: &str) -> Option<String> {
    endpoint_host(endpoint).and_then(|host| {
        host.strip_suffix(FLIGHT_HOST_SUFFIX)
            .map(ToString::to_string)
    })
}

#[must_use]
pub fn is_spice_cloud_flight_endpoint(endpoint: &str) -> bool {
    is_legacy_flight_endpoint(endpoint) || flight_endpoint_region(endpoint).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_regional_endpoints() {
        assert_eq!(
            flight_endpoint("us-east-1"),
            "https://us-east-1-prod-aws-flight.spiceai.io"
        );
        assert_eq!(
            data_endpoint("us-east-1"),
            "https://us-east-1-prod-aws-data.spiceai.io"
        );
    }

    #[test]
    fn detects_cloud_flight_endpoints() {
        assert!(is_spice_cloud_flight_endpoint(
            "https://us-east-1-prod-aws-flight.spiceai.io"
        ));
        assert!(is_spice_cloud_flight_endpoint("https://flight.spiceai.io"));
        assert!(!is_spice_cloud_flight_endpoint("http://localhost:50051"));
    }

    #[test]
    fn extracts_flight_endpoint_region() {
        assert_eq!(
            flight_endpoint_region("https://us-west-2-prod-aws-flight.spiceai.io"),
            Some("us-west-2".to_string())
        );
        assert_eq!(flight_endpoint_region("https://flight.spiceai.io"), None);
    }

    #[test]
    fn validates_region_shape() {
        assert!(is_valid_region("us-east-1"));
        assert!(!is_valid_region(""));
        assert!(!is_valid_region("us-east-1-"));
        assert!(!is_valid_region("Us-East-1"));
    }
}
