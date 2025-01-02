/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use http::HeaderMap;
use opentelemetry::KeyValue;
use std::iter;

use percent_encoding::percent_decode_str;

pub static BAGGAGE_HEADER: &str = "baggage";

/// Extract baggage from the headers, which is a comma-separated list of key-value pairs
/// with optional metadata. Spice will ignore the metadata and only use the key-value pairs.
///
/// This method assumes the baggage is in the format specified by the [W3C Baggage] specification.
///
/// This method returns an iterator instead of a `Vec<KeyValue>` to optimize for
/// the common case where baggage is not present and we can avoid allocating a
/// Vec. It needs to be a custom iterator type to avoid the overhead of boxing
/// the empty iterator.
///
/// [W3C Baggage]: https://w3c.github.io/baggage
pub fn from_headers(headers: &HeaderMap) -> BaggageIterator {
    let Some(baggage) = headers.get(BAGGAGE_HEADER) else {
        return BaggageIterator::Empty(iter::empty());
    };
    let Ok(baggage_str) = baggage.to_str() else {
        return BaggageIterator::Empty(iter::empty());
    };
    // This method is based on the opentelemetry-rust implementation of its baggage extractor,
    // which unfortunately can't be used directly because it doesn't return it directly, but into
    // its own Context which is unnecessary overhead we don't want to accept on every request.
    // See: <https://github.com/open-telemetry/opentelemetry-rust/blob/e0159ad91f426250eb921b50cc4816002a6c51a7/opentelemetry-sdk/src/propagation/baggage.rs#L99>
    let baggage_iter = baggage_str.split(',').filter_map(|context_value| {
        if let Some((name_and_value, _)) = context_value
            .split(';')
            .collect::<Vec<&str>>()
            .split_first()
        {
            let mut iter = name_and_value.split('=');
            if let (Some(name), Some(value)) = (iter.next(), iter.next()) {
                let decode_name = percent_decode_str(name).decode_utf8();
                let decode_value = percent_decode_str(value).decode_utf8();

                if let (Ok(name), Ok(value)) = (decode_name, decode_value) {
                    Some(KeyValue::new(
                        name.trim().to_owned(),
                        value.trim().to_string(),
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    });

    BaggageIterator::Some(Box::new(baggage_iter))
}

pub enum BaggageIterator<'a> {
    Empty(std::iter::Empty<KeyValue>),
    Some(Box<dyn Iterator<Item = KeyValue> + 'a>),
}

impl Iterator for BaggageIterator<'_> {
    type Item = KeyValue;

    fn next(&mut self) -> Option<KeyValue> {
        match self {
            BaggageIterator::Empty(iter) => iter.next(),
            BaggageIterator::Some(iter) => iter.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_from_headers_empty() {
        let headers = HeaderMap::new();
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert!(baggage.is_empty());
    }

    #[test]
    fn test_from_headers_invalid_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            BAGGAGE_HEADER,
            HeaderValue::from_bytes(&[0xFF]).expect("Failed to create header value"),
        );
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert!(baggage.is_empty());
    }

    #[test]
    fn test_from_headers_single_pair() {
        let mut headers = HeaderMap::new();
        headers.insert(BAGGAGE_HEADER, HeaderValue::from_static("key1=value1"));
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert_eq!(baggage.len(), 1);
        assert_eq!(baggage[0].key.as_str(), "key1");
        assert_eq!(baggage[0].value.as_str(), "value1");
    }

    #[test]
    fn test_from_headers_multiple_pairs() {
        let mut headers = HeaderMap::new();
        headers.insert(
            BAGGAGE_HEADER,
            HeaderValue::from_static("key1=value1,key2=value2"),
        );
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert_eq!(baggage.len(), 2);
        assert_eq!(baggage[0].key.as_str(), "key1");
        assert_eq!(baggage[0].value.as_str(), "value1");
        assert_eq!(baggage[1].key.as_str(), "key2");
        assert_eq!(baggage[1].value.as_str(), "value2");
    }

    #[test]
    fn test_from_headers_with_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert(
            BAGGAGE_HEADER,
            HeaderValue::from_static("key1=value1;property=true,key2=value2;other=123"),
        );
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert_eq!(baggage.len(), 2);
        assert_eq!(baggage[0].key.as_str(), "key1");
        assert_eq!(baggage[0].value.as_str(), "value1");
        assert_eq!(baggage[1].key.as_str(), "key2");
        assert_eq!(baggage[1].value.as_str(), "value2");
    }

    #[test]
    fn test_from_headers_with_percent_encoding() {
        let mut headers = HeaderMap::new();
        headers.insert(
            BAGGAGE_HEADER,
            HeaderValue::from_static("key%20with%20spaces=value%20with%20spaces"),
        );
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert_eq!(baggage.len(), 1);
        assert_eq!(baggage[0].key.as_str(), "key with spaces");
        assert_eq!(baggage[0].value.as_str(), "value with spaces");
    }

    #[test]
    fn test_from_headers_invalid_format() {
        let mut headers = HeaderMap::new();
        headers.insert(BAGGAGE_HEADER, HeaderValue::from_static("invalid_format"));
        let baggage = from_headers(&headers).collect::<Vec<_>>();
        assert!(baggage.is_empty());
    }
}
