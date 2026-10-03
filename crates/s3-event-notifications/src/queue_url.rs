/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Validate SQS queue URLs and read the region out of them.

/// The AWS region an SQS queue URL addresses, or `None` when the URL is not
/// an SQS queue URL.
#[must_use]
pub fn region_from_queue_url(queue_url: &str) -> Option<String> {
    let parsed = url::Url::parse(queue_url).ok()?;
    region_from_sqs_host(parsed.host_str()?)
}

/// Region embedded in an SQS queue-URL host, including FIPS and VPC endpoints.
fn region_from_sqs_host(host: &str) -> Option<String> {
    let host = host.to_ascii_lowercase();
    let labels: Vec<&str> = host.split('.').collect();
    let region = match labels.as_slice() {
        ["sqs" | "sqs-fips", region, "amazonaws", "com"]
        | ["sqs", region, "amazonaws", "com", "cn"]
        | ["sqs", region, "vpce", "amazonaws", "com"]
        | [_, "sqs", region, "vpce", "amazonaws", "com"] => *region,
        _ => return None,
    };
    is_aws_region(region).then(|| region.to_string())
}

/// An HTTPS SQS queue URL: AWS partition host and `/account/queue` path.
///
/// Loopback and instance-metadata URLs are not SQS queues, so a configuration
/// naming one is refused rather than polled. Custom SQS endpoints are not a
/// parameter.
#[must_use]
pub fn is_sqs_queue_url(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return false;
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    region_from_sqs_host(host).is_some() && sqs_queue_path_is_allowed(parsed.path())
}

fn is_aws_region(region: &str) -> bool {
    let bytes = region.as_bytes();
    (2..=32).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes.contains(&b'-')
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && !region.starts_with('-')
        && !region.ends_with('-')
        && !region.contains("--")
}

fn sqs_queue_path_is_allowed(path: &str) -> bool {
    let path = path.trim_end_matches('/');
    let Some((account, queue)) = path.strip_prefix('/').and_then(|p| p.split_once('/')) else {
        return false;
    };
    account.len() == 12
        && account.bytes().all(|b| b.is_ascii_digit())
        && !queue.is_empty()
        && !queue.contains('/')
        && queue.len() <= 80
        && {
            let name = queue.strip_suffix(".fifo").unwrap_or(queue);
            !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/s3-events";

    #[test]
    fn region_from_standard_and_china_queue_urls() {
        assert_eq!(
            region_from_queue_url(QUEUE_URL).as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            region_from_queue_url("https://sqs.cn-north-1.amazonaws.com.cn/123/queue").as_deref(),
            Some("cn-north-1")
        );
        assert_eq!(
            region_from_queue_url("https://localhost:4566/000000000000/queue"),
            None
        );
        assert_eq!(
            region_from_queue_url(
                "https://sqs-fips.us-east-1.amazonaws.com/123456789012/s3-events"
            )
            .as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            region_from_queue_url(
                "https://sqs.us-west-2.vpce.amazonaws.com/123456789012/s3-events"
            )
            .as_deref(),
            Some("us-west-2")
        );
        assert_eq!(
            region_from_queue_url(
                "https://vpce-abc.sqs.us-west-2.vpce.amazonaws.com/123456789012/s3-events"
            )
            .as_deref(),
            Some("us-west-2")
        );
    }

    #[test]
    fn is_sqs_queue_url_accepts_aws_partition_urls() {
        assert!(is_sqs_queue_url(QUEUE_URL));
        assert!(is_sqs_queue_url(
            "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://sqs-fips.us-east-1.amazonaws.com/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://sqs.us-east-1.vpce.amazonaws.com/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://vpce-abc.sqs.us-east-1.vpce.amazonaws.com/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://sqs.us-east-1.amazonaws.com/123456789012/s3-events.fifo"
        ));
    }

    /// Scheme-only validation accepted loopback and instance-metadata URLs
    /// (Copilot reproduction on #14121). Those must fail closed at registration.
    #[test]
    fn is_sqs_queue_url_rejects_non_sqs_hosts() {
        assert!(!is_sqs_queue_url("https://127.0.0.1/admin"));
        assert!(!is_sqs_queue_url("http://169.254.169.254/latest/meta-data"));
        assert!(!is_sqs_queue_url(
            "https://localhost:4566/000000000000/queue"
        ));
        assert!(!is_sqs_queue_url(
            "http://sqs.us-east-1.amazonaws.com/123456789012/s3-events"
        ));
        assert!(!is_sqs_queue_url(
            "https://example.com/123456789012/s3-events"
        ));
        assert!(!is_sqs_queue_url(
            "https://sqs.us-east-1.amazonaws.com/123/s3-events"
        ));
    }
}
