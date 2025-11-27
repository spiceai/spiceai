use crate::mongodb::Error;
use mongodb::bson::{Bson, Document};

#[derive(Debug, Clone)]
pub enum DuplicateBehavior {
    Error,
}

#[derive(Debug, Clone)]
pub enum UnnestBehavior {
    Depth(usize),
    Custom(fn(&Document) -> crate::mongodb::Result<Document, Error>),
}

#[derive(Debug, Clone)]
pub struct UnnestParameters {
    pub behavior: UnnestBehavior,
    pub duplicate_behavior: DuplicateBehavior,
}

pub fn unnest_bson_documents(
    documents: Vec<Document>,
    unnest_parameters: &UnnestParameters,
) -> crate::mongodb::Result<Vec<Document>, Error> {
    let mut all_documents = Vec::new();

    for doc in documents {
        let result = unnest_bson_document(&doc, unnest_parameters)?;
        all_documents.push(result);
    }

    Ok(all_documents)
}

fn unnest_bson_document(
    document: &Document,
    unnest_parameters: &UnnestParameters,
) -> crate::mongodb::Result<Document, Error> {
    match unnest_parameters.behavior {
        UnnestBehavior::Depth(depth) => {
            let mut new_document = Document::new();
            flatten_document_recursive(
                document,
                "",
                &mut new_document,
                depth,
                0,
                &unnest_parameters.duplicate_behavior,
            )?;
            Ok(new_document)
        }
        UnnestBehavior::Custom(func) => Ok(func(document)?),
    }
}

fn flatten_document_recursive(
    document: &Document,
    current_path: &str,
    flattened_doc: &mut Document,
    max_depth: usize,
    current_depth: usize,
    duplicate_behavior: &DuplicateBehavior,
) -> crate::mongodb::Result<(), Error> {
    for (key, value) in document {
        let new_path = if current_path.is_empty() {
            key.clone()
        } else {
            format!("{current_path}.{key}")
        };

        match value {
            Bson::Document(inner_doc) if current_depth < max_depth => {
                flatten_document_recursive(
                    inner_doc,
                    &new_path,
                    flattened_doc,
                    max_depth,
                    current_depth + 1,
                    duplicate_behavior,
                )?;
            }
            _ => {
                let final_key = handle_duplicate_key(flattened_doc, &new_path, duplicate_behavior)?;
                flattened_doc.insert(final_key.clone(), value.clone());
            }
        }
    }

    Ok(())
}

fn handle_duplicate_key(
    document: &Document,
    key: &str,
    duplicate_behavior: &DuplicateBehavior,
) -> crate::mongodb::Result<String, Error> {
    match duplicate_behavior {
        DuplicateBehavior::Error => {
            if document.contains_key(key) {
                return Err(Error::InvalidDocumentAccess {
                    message: format!("Column '{key}' already exists in the document."),
                });
            }
            Ok(key.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mongodb::Error;
    use mongodb::bson::doc;

    #[test]
    fn test_unnest_empty_documents() {
        let documents = vec![];
        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_unnest_flat_document_depth_1() {
        let documents = vec![doc! {
            "name": "John",
            "age": 30,
            "active": true
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let flattened = &result[0];
        assert_eq!(flattened.get_str("name").unwrap(), "John");
        assert_eq!(flattened.get_i32("age").unwrap(), 30);
        assert!(flattened.get_bool("active").unwrap());
    }

    #[test]
    fn test_unnest_nested_document_depth_1() {
        let documents = vec![doc! {
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "New York"
            }
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let flattened = &result[0];
        assert_eq!(flattened.get_str("name").unwrap(), "John");
        assert_eq!(flattened.get_str("address.street").unwrap(), "123 Main St");
        assert_eq!(flattened.get_str("address.city").unwrap(), "New York");
        assert!(!flattened.contains_key("address"));
    }

    #[test]
    fn test_unnest_deeply_nested_document_depth_2() {
        let documents = vec![doc! {
            "name": "John",
            "address": {
                "street": "123 Main St",
                "location": {
                    "lat": 40.7128,
                    "lng": -74.0060
                }
            }
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(2),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let flattened = &result[0];
        assert_eq!(flattened.get_str("name").unwrap(), "John");
        assert_eq!(flattened.get_str("address.street").unwrap(), "123 Main St");
        assert_eq!(flattened.get_f64("address.location.lat").unwrap(), 40.7128);
        assert_eq!(flattened.get_f64("address.location.lng").unwrap(), -74.0060);
    }

    #[test]
    fn test_unnest_depth_limit() {
        let documents = vec![doc! {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep"
                    }
                }
            }
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(2),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let flattened = &result[0];
        assert!(flattened.contains_key("level1.level2.level3"));
        let level3_doc = flattened.get_document("level1.level2.level3").unwrap();
        assert_eq!(level3_doc.get_str("value").unwrap(), "deep");
    }

    #[test]
    fn test_unnest_multiple_documents() {
        let documents = vec![
            doc! {
                "name": "John",
                "profile": {
                    "age": 30
                }
            },
            doc! {
                "name": "Jane",
                "profile": {
                    "age": 25
                }
            },
        ];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 2);

        assert_eq!(result[0].get_str("name").unwrap(), "John");
        assert_eq!(result[0].get_i32("profile.age").unwrap(), 30);

        assert_eq!(result[1].get_str("name").unwrap(), "Jane");
        assert_eq!(result[1].get_i32("profile.age").unwrap(), 25);
    }

    #[test]
    fn test_duplicate_key_error() {
        let documents = vec![doc! {
            "details.address": "123 Main St",
            "details": {
                "address": "456 Oak Ave"
            }
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params);
        assert!(result.is_err());
    }

    #[test]
    fn test_unnest_with_arrays_and_other_types() {
        let documents = vec![doc! {
            "name": "John",
            "hobbies": ["reading", "swimming"],
            "metadata": {
                "created": mongodb::bson::DateTime::now(),
                "count": 42,
                "active": true,
                "score": 95.5
            }
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let flattened = &result[0];
        assert_eq!(flattened.get_str("name").unwrap(), "John");
        assert!(flattened.contains_key("hobbies")); // Array should remain as-is
        assert!(flattened.contains_key("metadata.created"));
        assert_eq!(flattened.get_i32("metadata.count").unwrap(), 42);
        assert!(flattened.get_bool("metadata.active").unwrap());
        assert_eq!(flattened.get_f64("metadata.score").unwrap(), 95.5);
    }

    #[test]
    fn test_custom_unnest_behavior() {
        fn custom_unnest(doc: &Document) -> crate::mongodb::Result<Document, Error> {
            let mut result = Document::new();
            result.insert("custom_processed", true);

            // Add all top-level keys with "custom_" prefix
            for (key, value) in doc {
                result.insert(format!("custom_{key}"), value.clone());
            }

            Ok(result)
        }

        let documents = vec![doc! {
            "name": "John",
            "age": 30
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Custom(custom_unnest),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let processed = &result[0];
        assert!(processed.get_bool("custom_processed").unwrap());
        assert_eq!(processed.get_str("custom_name").unwrap(), "John");
        assert_eq!(processed.get_i32("custom_age").unwrap(), 30);
    }

    #[test]
    fn test_custom_unnest_behavior_with_error() {
        fn failing_custom_unnest(_doc: &Document) -> crate::mongodb::Result<Document, Error> {
            Err(Error::InvalidDocumentAccess {
                message: "Custom processing failed".to_string(),
            })
        }

        let documents = vec![doc! { "name": "John" }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Custom(failing_custom_unnest),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params);
        assert!(result.is_err());

        if let Err(Error::InvalidDocumentAccess { message }) = result {
            assert_eq!(message, "Custom processing failed");
        } else {
            panic!("Expected InvalidDocumentAccess error");
        }
    }

    #[test]
    fn test_empty_document() {
        let documents = vec![Document::new()];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn test_zero_depth_unnesting() {
        let documents = vec![doc! {
            "name": "John",
            "address": {
                "street": "123 Main St"
            }
        }];

        let params = UnnestParameters {
            behavior: UnnestBehavior::Depth(0),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let result = unnest_bson_documents(documents, &params).unwrap();
        assert_eq!(result.len(), 1);

        let flattened = &result[0];
        assert_eq!(flattened.get_str("name").unwrap(), "John");
        assert!(flattened.contains_key("address"));
        let addr_doc = flattened.get_document("address").unwrap();
        assert_eq!(addr_doc.get_str("street").unwrap(), "123 Main St");
    }
}
