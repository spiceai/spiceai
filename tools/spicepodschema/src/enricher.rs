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

//! Enricher module for merging connector-specific parameter schemas into the base Spicepod schema.

use crate::collector::{CatalogConnectorSchema, ConnectorSchema, ModelSourceSchema};
use crate::transform::{connector_params_to_schema, to_pascal_case};
use schemars::Schema;
use serde_json::{Map, Value};

/// Enriches the root schema with connector-specific parameter definitions.
///
/// This function adds connector parameter schemas to the `$defs` section of the root schema
/// and creates a combined schema that documents all available connector parameters.
pub fn enrich_params_schema(
    root_schema: &mut Schema,
    data_connectors: &[ConnectorSchema],
    data_accelerators: &[ConnectorSchema],
    catalog_connectors: &[CatalogConnectorSchema],
    model_sources: &[ModelSourceSchema],
) {
    // Get the schema as a mutable JSON object
    let Some(schema_obj) = root_schema.as_object_mut() else {
        return;
    };

    // Ensure $defs exists
    let defs = schema_obj
        .entry("$defs")
        .or_insert_with(|| Value::Object(Map::new()));

    let Some(defs_obj) = defs.as_object_mut() else {
        return;
    };

    // Add data connector parameter schemas
    for connector in data_connectors {
        let schema_name = format!("{}DataConnectorParams", to_pascal_case(&connector.name));
        let schema = connector_params_to_schema(connector);
        defs_obj.insert(schema_name, schema);
    }

    // Add data accelerator parameter schemas
    for accelerator in data_accelerators {
        let schema_name = format!("{}AcceleratorParams", to_pascal_case(&accelerator.name));
        let schema = connector_params_to_schema(accelerator);
        defs_obj.insert(schema_name, schema);
    }

    // Add catalog connector parameter schemas
    for catalog in catalog_connectors {
        let connector_schema = ConnectorSchema {
            name: catalog.name.to_string(),
            prefix: catalog.prefix,
            parameters: catalog.parameters,
        };
        let schema_name = format!("{}CatalogParams", to_pascal_case(catalog.name));
        let schema = connector_params_to_schema(&connector_schema);
        defs_obj.insert(schema_name, schema);
    }

    // Add model source parameter schemas
    for model_source in model_sources {
        let connector_schema = ConnectorSchema {
            name: model_source.name.to_string(),
            prefix: model_source.prefix,
            parameters: model_source.parameters,
        };
        let schema_name = format!("{}ModelParams", to_pascal_case(model_source.name));
        let schema = connector_params_to_schema(&connector_schema);
        defs_obj.insert(schema_name, schema);
    }

    // Add connector-specific Dataset definitions with conditional params
    add_connector_specific_definitions(defs_obj, data_connectors, data_accelerators);

    // Add catalog-specific Catalog definitions with conditional params
    add_catalog_specific_definitions(defs_obj, catalog_connectors);

    // Add model-specific Model definitions with conditional params
    add_model_specific_definitions(defs_obj, model_sources);

    // Update the Dataset, Catalog, Model definitions to use conditional schemas
    update_dataset_to_use_conditional_schemas(defs_obj, data_connectors);
    update_catalog_to_use_conditional_schemas(defs_obj, catalog_connectors);
    update_model_to_use_conditional_schemas(defs_obj, model_sources);

    // Update Acceleration params field
    update_acceleration_params(defs_obj, data_accelerators);

    // Add connector metadata as extension
    add_connector_metadata_extension(
        schema_obj,
        data_connectors,
        data_accelerators,
        catalog_connectors,
        model_sources,
    );
}

/// Creates connector-specific Dataset definitions that enforce the correct params schema
/// based on the `from` field pattern.
fn add_connector_specific_definitions(
    defs_obj: &mut Map<String, Value>,
    data_connectors: &[ConnectorSchema],
    data_accelerators: &[ConnectorSchema],
) {
    // Get the base Dataset schema to clone properties from
    let base_dataset = defs_obj.get("Dataset").cloned();

    for connector in data_connectors {
        let def_name = format!("{}Dataset", to_pascal_case(&connector.name));
        let params_ref = format!(
            "#/$defs/{}DataConnectorParams",
            to_pascal_case(&connector.name)
        );

        let schema = create_connector_specific_component_schema(
            base_dataset.as_ref(),
            connector,
            &params_ref,
        );
        defs_obj.insert(def_name, schema);
    }

    // Also create connector-specific Dataset definitions for accelerators
    // These are used when acceleration.engine is specified
    for accelerator in data_accelerators {
        let def_name = format!("{}AcceleratedDataset", to_pascal_case(&accelerator.name));
        let accel_params_ref = format!(
            "#/$defs/{}AcceleratorParams",
            to_pascal_case(&accelerator.name)
        );

        let schema = create_accelerated_dataset_schema(
            base_dataset.as_ref(),
            accelerator,
            &accel_params_ref,
        );
        defs_obj.insert(def_name, schema);
    }
}

/// Creates catalog-specific Catalog definitions that enforce the correct params schema
/// based on the `from` field pattern.
fn add_catalog_specific_definitions(
    defs_obj: &mut Map<String, Value>,
    catalog_connectors: &[CatalogConnectorSchema],
) {
    // Get the base Catalog schema to clone properties from
    let base_catalog = defs_obj.get("Catalog").cloned();

    for catalog in catalog_connectors {
        let def_name = format!("{}Catalog", to_pascal_case(catalog.name));
        let params_ref = format!("#/$defs/{}CatalogParams", to_pascal_case(catalog.name));

        let connector_schema = ConnectorSchema {
            name: catalog.name.to_string(),
            prefix: catalog.prefix,
            parameters: catalog.parameters,
        };

        let schema =
            create_catalog_specific_schema(base_catalog.as_ref(), &connector_schema, &params_ref);
        defs_obj.insert(def_name, schema);
    }
}

/// Creates model-specific Model definitions that enforce the correct params schema
/// based on the `from` field pattern.
fn add_model_specific_definitions(
    defs_obj: &mut Map<String, Value>,
    model_sources: &[ModelSourceSchema],
) {
    // Get the base Model schema to clone properties from
    let base_model = defs_obj.get("Model").cloned();

    for model_source in model_sources {
        let def_name = format!("{}Model", to_pascal_case(model_source.name));
        let params_ref = format!("#/$defs/{}ModelParams", to_pascal_case(model_source.name));

        let schema = create_model_specific_schema(base_model.as_ref(), model_source, &params_ref);
        defs_obj.insert(def_name, schema);
    }
}

/// Creates a connector-specific component schema (Dataset or Catalog) that:
/// 1. Requires `from` to match a specific pattern
/// 2. Restricts `params` to only the connector-specific parameters
fn create_connector_specific_component_schema(
    base_schema: Option<&Value>,
    connector: &ConnectorSchema,
    params_ref: &str,
) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));

    // Build the from pattern - connector name followed by colon
    let from_pattern = format!("^{}:", regex::escape(&connector.name));

    let mut properties = Map::new();

    // from field with pattern constraint
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    from_schema.insert("pattern".to_string(), Value::String(from_pattern));
    from_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Data source path for {} connector. Format: {}:<path>",
            connector.name, connector.name
        )),
    );
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // params field with connector-specific reference
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Connection parameters for the {} data connector.",
            connector.name
        )),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert("$ref".to_string(), Value::String(params_ref.to_string()));
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema.as_ref()
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Creates a catalog-specific schema that:
/// 1. Requires `from` to match a specific pattern
/// 2. Restricts `params` to only the catalog-specific parameters
fn create_catalog_specific_schema(
    base_schema: Option<&Value>,
    connector: &ConnectorSchema,
    params_ref: &str,
) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));

    // Build the from pattern - connector name followed by colon
    let from_pattern = format!("^{}:", regex::escape(&connector.name));

    let mut properties = Map::new();

    // from field with pattern constraint
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    from_schema.insert("pattern".to_string(), Value::String(from_pattern));
    from_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Catalog source for {} connector. Format: {}:<catalog_path>",
            connector.name, connector.name
        )),
    );
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // params field with catalog-specific reference
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Connection parameters for the {} catalog connector.",
            connector.name
        )),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert("$ref".to_string(), Value::String(params_ref.to_string()));
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema.as_ref()
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Creates a model-specific schema that:
/// 1. Requires `from` to match a specific pattern
/// 2. Restricts `params` to only the model source-specific parameters
fn create_model_specific_schema(
    base_schema: Option<&Value>,
    model_source: &ModelSourceSchema,
    params_ref: &str,
) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));

    // Build the from pattern - model source name followed by colon
    let from_pattern = format!("^{}:", regex::escape(model_source.name));

    let mut properties = Map::new();

    // from field with pattern constraint
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    from_schema.insert("pattern".to_string(), Value::String(from_pattern));
    from_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Model source for {} provider. Format: {}:<model_id>",
            model_source.name, model_source.name
        )),
    );
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // params field with model source-specific reference
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Configuration parameters for the {} model provider.",
            model_source.name
        )),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert("$ref".to_string(), Value::String(params_ref.to_string()));
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema.as_ref()
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Creates an accelerated dataset schema that includes acceleration params
fn create_accelerated_dataset_schema(
    base_schema: Option<&Value>,
    accelerator: &ConnectorSchema,
    accel_params_ref: &str,
) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Dataset with {} acceleration engine.",
            accelerator.name
        )),
    );

    let mut properties = Map::new();

    // from field - any string for accelerated datasets
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // acceleration field with engine-specific params
    let mut accel_schema = Map::new();
    accel_schema.insert("type".to_string(), Value::String("object".to_string()));

    let mut accel_properties = Map::new();

    // engine field with const value
    let mut engine_schema = Map::new();
    engine_schema.insert("type".to_string(), Value::String("string".to_string()));
    engine_schema.insert("const".to_string(), Value::String(accelerator.name.clone()));
    accel_properties.insert("engine".to_string(), Value::Object(engine_schema));

    // params field for acceleration
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String(format!(
            "Configuration parameters for the {} acceleration engine.",
            accelerator.name
        )),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert(
        "$ref".to_string(),
        Value::String(accel_params_ref.to_string()),
    );
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    accel_properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other acceleration properties from base Acceleration schema
    accel_schema.insert("properties".to_string(), Value::Object(accel_properties));
    accel_schema.insert("additionalProperties".to_string(), Value::Bool(true));

    properties.insert("acceleration".to_string(), Value::Object(accel_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema.as_ref()
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "acceleration" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Updates the Dataset definition to use `oneOf` with connector-specific schemas
/// selected by the `from` field pattern.
fn update_dataset_to_use_conditional_schemas(
    defs_obj: &mut Map<String, Value>,
    data_connectors: &[ConnectorSchema],
) {
    // Build if/then conditionals for each connector
    // This provides better IDE support than anyOf - the IDE will only show
    // the relevant params schema based on the 'from' field pattern
    let conditionals: Vec<Value> = data_connectors
        .iter()
        .map(|c| {
            let from_pattern = format!("^{}:", regex::escape(&c.name));

            // Build the "if" condition - matches when "from" starts with connector prefix
            let mut if_props = Map::new();
            let mut from_pattern_obj = Map::new();
            from_pattern_obj.insert("pattern".to_string(), Value::String(from_pattern));
            if_props.insert("from".to_string(), Value::Object(from_pattern_obj));

            let mut if_obj = Map::new();
            if_obj.insert("properties".to_string(), Value::Object(if_props));

            // Build the "then" clause - reference the connector-specific dataset schema
            let mut then_ref = Map::new();
            then_ref.insert(
                "$ref".to_string(),
                Value::String(format!("#/$defs/{}Dataset", to_pascal_case(&c.name))),
            );

            // Combine into if/then object
            let mut conditional = Map::new();
            conditional.insert("if".to_string(), Value::Object(if_obj));
            conditional.insert("then".to_string(), Value::Object(then_ref));

            Value::Object(conditional)
        })
        .collect();

    // Add a fallback generic Dataset schema for unknown connectors
    let generic_dataset = create_generic_dataset_schema(defs_obj.get("Dataset"));
    defs_obj.insert("GenericDataset".to_string(), generic_dataset);

    // Create base dataset schema with common properties
    let base_dataset = create_base_dataset_schema(defs_obj.get("Dataset"));

    // Build allOf array: base schema + all conditionals
    let mut all_of: Vec<Value> = vec![base_dataset];
    all_of.extend(conditionals);

    // Replace Dataset with a schema that uses allOf with if/then conditionals
    let mut new_dataset = Map::new();
    new_dataset.insert(
        "description".to_string(),
        Value::String(
            "A dataset definition. The params field is validated based on the connector type specified in 'from'.".to_string()
        ),
    );
    new_dataset.insert("allOf".to_string(), Value::Array(all_of));

    defs_obj.insert("Dataset".to_string(), Value::Object(new_dataset));
}

/// Creates a base dataset schema with common properties (without params validation)
fn create_base_dataset_schema(base_schema: Option<&Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));

    let mut properties = Map::new();

    // from field - required string
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    from_schema.insert(
        "description".to_string(),
        Value::String("Data source identifier in the format: <connector>:<path>".to_string()),
    );
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field - required string
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    name_schema.insert(
        "description".to_string(),
        Value::String("The unique name for this dataset.".to_string()),
    );
    properties.insert("name".to_string(), Value::Object(name_schema));

    // Copy other properties from base schema if available (excluding from, name, params)
    if let Some(Value::Object(base)) = base_schema
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" && key != "name" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );

    Value::Object(schema)
}

/// Updates the Catalog definition to use `oneOf` with catalog-specific schemas
/// selected by the `from` field pattern.
fn update_catalog_to_use_conditional_schemas(
    defs_obj: &mut Map<String, Value>,
    catalog_connectors: &[CatalogConnectorSchema],
) {
    // Build if/then conditionals for each catalog connector
    let conditionals: Vec<Value> = catalog_connectors
        .iter()
        .map(|c| {
            let from_pattern = format!("^{}:", regex::escape(c.name));

            // Build the "if" condition
            let mut if_props = Map::new();
            let mut from_pattern_obj = Map::new();
            from_pattern_obj.insert("pattern".to_string(), Value::String(from_pattern));
            if_props.insert("from".to_string(), Value::Object(from_pattern_obj));

            let mut if_obj = Map::new();
            if_obj.insert("properties".to_string(), Value::Object(if_props));

            // Build the "then" clause
            let mut then_ref = Map::new();
            then_ref.insert(
                "$ref".to_string(),
                Value::String(format!("#/$defs/{}Catalog", to_pascal_case(c.name))),
            );

            let mut conditional = Map::new();
            conditional.insert("if".to_string(), Value::Object(if_obj));
            conditional.insert("then".to_string(), Value::Object(then_ref));

            Value::Object(conditional)
        })
        .collect();

    // Add a fallback generic Catalog schema for unknown connectors
    let generic_catalog = create_generic_catalog_schema(defs_obj.get("Catalog"));
    defs_obj.insert("GenericCatalog".to_string(), generic_catalog);

    // Create base catalog schema
    let base_catalog = create_base_catalog_schema(defs_obj.get("Catalog"));

    // Build allOf array: base schema + all conditionals
    let mut all_of: Vec<Value> = vec![base_catalog];
    all_of.extend(conditionals);

    // Replace Catalog with a schema that uses allOf with if/then conditionals
    let mut new_catalog = Map::new();
    new_catalog.insert(
        "description".to_string(),
        Value::String(
            "A catalog definition. The params field is validated based on the catalog connector type specified in 'from'.".to_string()
        ),
    );
    new_catalog.insert("allOf".to_string(), Value::Array(all_of));

    defs_obj.insert("Catalog".to_string(), Value::Object(new_catalog));
}

/// Creates a base catalog schema with common properties (without params validation)
fn create_base_catalog_schema(base_schema: Option<&Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));

    let mut properties = Map::new();

    // from field - required string
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    from_schema.insert(
        "description".to_string(),
        Value::String("Catalog source identifier in the format: <connector>:<path>".to_string()),
    );
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field - required string
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    name_schema.insert(
        "description".to_string(),
        Value::String("The unique name for this catalog.".to_string()),
    );
    properties.insert("name".to_string(), Value::Object(name_schema));

    // Copy other properties from base schema if available (excluding from, name, params)
    if let Some(Value::Object(base)) = base_schema
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" && key != "name" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );

    Value::Object(schema)
}

/// Creates a generic Dataset schema for unknown/custom connectors
fn create_generic_dataset_schema(base_schema: Option<&Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert(
        "description".to_string(),
        Value::String("Generic dataset for custom or unknown connectors.".to_string()),
    );

    let mut properties = Map::new();

    // from field - any string
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // params field - generic object
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String("Connection parameters for the data connector.".to_string()),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert(
        "$ref".to_string(),
        Value::String("#/$defs/Params".to_string()),
    );
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" && key != "name" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Creates a generic Catalog schema for unknown/custom connectors
fn create_generic_catalog_schema(base_schema: Option<&Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert(
        "description".to_string(),
        Value::String("Generic catalog for custom or unknown connectors.".to_string()),
    );

    let mut properties = Map::new();

    // from field - any string
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // params field - generic object
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String("Connection parameters for the catalog connector.".to_string()),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert(
        "$ref".to_string(),
        Value::String("#/$defs/Params".to_string()),
    );
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" && key != "name" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Updates the Model definition to use `allOf` with conditional schemas
/// selected by the `from` field pattern.
fn update_model_to_use_conditional_schemas(
    defs_obj: &mut Map<String, Value>,
    model_sources: &[ModelSourceSchema],
) {
    // Build if/then conditionals for each model source
    let conditionals: Vec<Value> = model_sources
        .iter()
        .map(|m| {
            let from_pattern = format!("^{}:", regex::escape(m.name));

            // Build the "if" condition - matches when "from" starts with model source prefix
            let mut if_props = Map::new();
            let mut from_pattern_obj = Map::new();
            from_pattern_obj.insert("pattern".to_string(), Value::String(from_pattern));
            if_props.insert("from".to_string(), Value::Object(from_pattern_obj));

            let mut if_obj = Map::new();
            if_obj.insert("properties".to_string(), Value::Object(if_props));

            // Build the "then" clause - reference the model-specific schema
            let mut then_ref = Map::new();
            then_ref.insert(
                "$ref".to_string(),
                Value::String(format!("#/$defs/{}Model", to_pascal_case(m.name))),
            );

            // Combine into if/then object
            let mut conditional = Map::new();
            conditional.insert("if".to_string(), Value::Object(if_obj));
            conditional.insert("then".to_string(), Value::Object(then_ref));

            Value::Object(conditional)
        })
        .collect();

    // Add a fallback generic Model schema for unknown model sources
    let generic_model = create_generic_model_schema(defs_obj.get("Model"));
    defs_obj.insert("GenericModel".to_string(), generic_model);

    // Create base model schema
    let base_model = create_base_model_schema(defs_obj.get("Model"));

    // Build allOf array: base schema + all conditionals
    let mut all_of: Vec<Value> = vec![base_model];
    all_of.extend(conditionals);

    // Replace Model with a schema that uses allOf with if/then conditionals
    let mut new_model = Map::new();
    new_model.insert(
        "description".to_string(),
        Value::String(
            "A model definition. The params field is validated based on the model source type specified in 'from'.".to_string()
        ),
    );
    new_model.insert("allOf".to_string(), Value::Array(all_of));

    defs_obj.insert("Model".to_string(), Value::Object(new_model));
}

/// Creates a base model schema with common properties (without params validation)
fn create_base_model_schema(base_schema: Option<&Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));

    let mut properties = Map::new();

    // from field - required string
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    from_schema.insert(
        "description".to_string(),
        Value::String("Model source identifier in the format: <provider>:<model_id>".to_string()),
    );
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field - required string
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    name_schema.insert(
        "description".to_string(),
        Value::String("The unique name for this model.".to_string()),
    );
    properties.insert("name".to_string(), Value::Object(name_schema));

    // Copy other properties from base schema if available (excluding from, name, params)
    if let Some(Value::Object(base)) = base_schema
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" && key != "name" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );

    Value::Object(schema)
}

/// Creates a generic Model schema for unknown/custom model sources
fn create_generic_model_schema(base_schema: Option<&Value>) -> Value {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert(
        "description".to_string(),
        Value::String("Generic model for custom or unknown model sources.".to_string()),
    );

    let mut properties = Map::new();

    // from field - any string
    let mut from_schema = Map::new();
    from_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("from".to_string(), Value::Object(from_schema));

    // name field
    let mut name_schema = Map::new();
    name_schema.insert("type".to_string(), Value::String("string".to_string()));
    properties.insert("name".to_string(), Value::Object(name_schema));

    // params field - generic object
    let mut params_schema = Map::new();
    params_schema.insert(
        "description".to_string(),
        Value::String("Configuration parameters for the model provider.".to_string()),
    );
    let mut ref_obj = Map::new();
    ref_obj.insert(
        "$ref".to_string(),
        Value::String("#/$defs/Params".to_string()),
    );
    params_schema.insert(
        "anyOf".to_string(),
        Value::Array(vec![
            Value::Object(ref_obj),
            Value::Object({
                let mut null_obj = Map::new();
                null_obj.insert("type".to_string(), Value::String("null".to_string()));
                null_obj
            }),
        ]),
    );
    properties.insert("params".to_string(), Value::Object(params_schema));

    // Copy other properties from base schema if available
    if let Some(Value::Object(base)) = base_schema
        && let Some(Value::Object(base_props)) = base.get("properties")
    {
        for (key, value) in base_props {
            if key != "from" && key != "params" && key != "name" {
                properties.insert(key.clone(), value.clone());
            }
        }
    }

    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert(
        "required".to_string(),
        Value::Array(vec![
            Value::String("from".to_string()),
            Value::String("name".to_string()),
        ]),
    );
    schema.insert("additionalProperties".to_string(), Value::Bool(false));

    Value::Object(schema)
}

/// Updates the Acceleration params field with anyOf referencing all accelerator schemas
fn update_acceleration_params(
    defs_obj: &mut Map<String, Value>,
    data_accelerators: &[ConnectorSchema],
) {
    // Find the Arrow accelerator for the default case
    let arrow_accelerator = data_accelerators.iter().find(|a| a.name == "arrow");

    // Build if/then conditionals for each accelerator based on engine field
    let mut conditionals: Vec<Value> = data_accelerators
        .iter()
        .map(|a| {
            // Build the "if" condition - matches when "engine" equals accelerator name
            let mut if_props = Map::new();
            let mut engine_const = Map::new();
            engine_const.insert("const".to_string(), Value::String(a.name.clone()));
            if_props.insert("engine".to_string(), Value::Object(engine_const));

            let mut if_obj = Map::new();
            if_obj.insert("properties".to_string(), Value::Object(if_props));

            // Build the "then" clause - params uses the accelerator-specific schema
            let mut then_props = Map::new();
            let mut params_schema = Map::new();
            let mut ref_obj = Map::new();
            ref_obj.insert(
                "$ref".to_string(),
                Value::String(format!(
                    "#/$defs/{}AcceleratorParams",
                    to_pascal_case(&a.name)
                )),
            );
            // Allow null as well
            params_schema.insert(
                "anyOf".to_string(),
                Value::Array(vec![
                    Value::Object(ref_obj),
                    Value::Object({
                        let mut null_obj = Map::new();
                        null_obj.insert("type".to_string(), Value::String("null".to_string()));
                        null_obj
                    }),
                ]),
            );
            then_props.insert("params".to_string(), Value::Object(params_schema));

            let mut then_obj = Map::new();
            then_obj.insert("properties".to_string(), Value::Object(then_props));

            // Combine into if/then object
            let mut conditional = Map::new();
            conditional.insert("if".to_string(), Value::Object(if_obj));
            conditional.insert("then".to_string(), Value::Object(then_obj));

            Value::Object(conditional)
        })
        .collect();

    // Add default case: when engine is not specified, use Arrow params (Arrow is the default engine)
    if let Some(arrow) = arrow_accelerator {
        // Match when engine property is not present using JSON Schema "not" + "required"
        let mut required_obj = Map::new();
        required_obj.insert(
            "required".to_string(),
            Value::Array(vec![Value::String("engine".to_string())]),
        );
        let mut not_obj = Map::new();
        not_obj.insert("not".to_string(), Value::Object(required_obj));

        let mut then_props = Map::new();
        let mut params_schema = Map::new();
        let mut ref_obj = Map::new();
        ref_obj.insert(
            "$ref".to_string(),
            Value::String(format!(
                "#/$defs/{}AcceleratorParams",
                to_pascal_case(&arrow.name)
            )),
        );
        params_schema.insert(
            "anyOf".to_string(),
            Value::Array(vec![
                Value::Object(ref_obj),
                Value::Object({
                    let mut null_obj = Map::new();
                    null_obj.insert("type".to_string(), Value::String("null".to_string()));
                    null_obj
                }),
            ]),
        );
        then_props.insert("params".to_string(), Value::Object(params_schema));

        let mut then_obj = Map::new();
        then_obj.insert("properties".to_string(), Value::Object(then_props));

        let mut default_conditional = Map::new();
        default_conditional.insert("if".to_string(), Value::Object(not_obj));
        default_conditional.insert("then".to_string(), Value::Object(then_obj));

        conditionals.push(Value::Object(default_conditional));
    }

    // Update Acceleration definition to use allOf with conditionals
    if let Some(Value::Object(accel_def)) = defs_obj.get_mut("Acceleration")
        && !conditionals.is_empty()
    {
        let accelerator_names: Vec<&str> =
            data_accelerators.iter().map(|a| a.name.as_str()).collect();

        // Update description for params field
        if let Some(Value::Object(properties)) = accel_def.get_mut("properties")
            && let Some(Value::Object(params_prop)) = properties.get_mut("params")
        {
            params_prop.insert(
                "description".to_string(),
                Value::String(format!(
                    "Configuration parameters for the acceleration engine. The available parameters depend on the engine type specified in 'engine' (default: arrow). Available engines: {}.",
                    accelerator_names.join(", ")
                )),
            );
        }

        // Add allOf with conditionals to the Acceleration schema
        accel_def.insert("allOf".to_string(), Value::Array(conditionals));
    }
}

/// Adds connector metadata as a JSON Schema extension (`x-spice-connectors`).
///
/// This extension provides a machine-readable list of all available connectors
/// with their names and prefixes for tooling integration.
fn add_connector_metadata_extension(
    schema_obj: &mut Map<String, Value>,
    data_connectors: &[ConnectorSchema],
    data_accelerators: &[ConnectorSchema],
    catalog_connectors: &[CatalogConnectorSchema],
    model_sources: &[ModelSourceSchema],
) {
    let mut connectors_metadata = Map::new();

    // Data connectors metadata
    let data_connector_list: Vec<Value> = data_connectors
        .iter()
        .map(|c| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), Value::String(c.name.clone()));
            obj.insert("prefix".to_string(), Value::String(c.prefix.to_string()));
            obj.insert(
                "paramsRef".to_string(),
                Value::String(format!(
                    "#/$defs/{}DataConnectorParams",
                    to_pascal_case(&c.name)
                )),
            );
            obj.insert(
                "schemaRef".to_string(),
                Value::String(format!("#/$defs/{}Dataset", to_pascal_case(&c.name))),
            );
            Value::Object(obj)
        })
        .collect();
    connectors_metadata.insert(
        "dataConnectors".to_string(),
        Value::Array(data_connector_list),
    );

    // Data accelerators metadata
    let accelerator_list: Vec<Value> = data_accelerators
        .iter()
        .map(|a| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), Value::String(a.name.clone()));
            obj.insert("prefix".to_string(), Value::String(a.prefix.to_string()));
            obj.insert(
                "paramsRef".to_string(),
                Value::String(format!(
                    "#/$defs/{}AcceleratorParams",
                    to_pascal_case(&a.name)
                )),
            );
            Value::Object(obj)
        })
        .collect();
    connectors_metadata.insert(
        "dataAccelerators".to_string(),
        Value::Array(accelerator_list),
    );

    // Catalog connectors metadata
    let catalog_list: Vec<Value> = catalog_connectors
        .iter()
        .map(|c| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), Value::String(c.name.to_string()));
            obj.insert("prefix".to_string(), Value::String(c.prefix.to_string()));
            obj.insert(
                "paramsRef".to_string(),
                Value::String(format!("#/$defs/{}CatalogParams", to_pascal_case(c.name))),
            );
            obj.insert(
                "schemaRef".to_string(),
                Value::String(format!("#/$defs/{}Catalog", to_pascal_case(c.name))),
            );
            Value::Object(obj)
        })
        .collect();
    connectors_metadata.insert("catalogConnectors".to_string(), Value::Array(catalog_list));

    // Model sources metadata
    let model_list: Vec<Value> = model_sources
        .iter()
        .map(|m| {
            let mut obj = Map::new();
            obj.insert("name".to_string(), Value::String(m.name.to_string()));
            obj.insert("prefix".to_string(), Value::String(m.prefix.to_string()));
            obj.insert(
                "paramsRef".to_string(),
                Value::String(format!("#/$defs/{}ModelParams", to_pascal_case(m.name))),
            );
            obj.insert(
                "schemaRef".to_string(),
                Value::String(format!("#/$defs/{}Model", to_pascal_case(m.name))),
            );
            Value::Object(obj)
        })
        .collect();
    connectors_metadata.insert("modelSources".to_string(), Value::Array(model_list));

    // Add the extension to the root schema
    schema_obj.insert(
        "x-spice-connectors".to_string(),
        Value::Object(connectors_metadata),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_parameters::ParameterSpec;
    use schemars::schema_for;

    fn create_test_connector() -> ConnectorSchema {
        static TEST_PARAMS: &[ParameterSpec] = &[
            ParameterSpec::component("host").description("The database host"),
            ParameterSpec::component("port").default("5432"),
        ];

        ConnectorSchema {
            name: "test_db".to_string(),
            prefix: "test",
            parameters: TEST_PARAMS,
        }
    }

    #[derive(serde::Serialize, schemars::JsonSchema)]
    struct TestSchema {
        name: String,
    }

    #[test]
    fn test_enrich_params_schema_adds_definitions() {
        // Create a minimal root schema
        let mut root_schema = schema_for!(TestSchema);

        let connectors = vec![create_test_connector()];
        let accelerators: Vec<ConnectorSchema> = vec![];
        let catalogs: Vec<CatalogConnectorSchema> = vec![];

        let models: Vec<ModelSourceSchema> = vec![];

        enrich_params_schema(
            &mut root_schema,
            &connectors,
            &accelerators,
            &catalogs,
            &models,
        );

        // Check that the definition was added
        let schema_obj = root_schema.as_object().expect("should be object schema");
        let defs = schema_obj.get("$defs").expect("should have $defs");
        let defs_obj = defs.as_object().expect("$defs should be object");
        assert!(defs_obj.contains_key("TestDbDataConnectorParams"));
        assert!(defs_obj.contains_key("TestDbDataset"));
    }

    #[test]
    fn test_connector_specific_schema_has_pattern() {
        let connector = create_test_connector();
        let schema = create_connector_specific_component_schema(
            None,
            &connector,
            "#/$defs/TestDbDataConnectorParams",
        );

        let obj = schema.as_object().expect("should be object");
        let props = obj
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("should have properties");
        let from_prop = props
            .get("from")
            .and_then(|v| v.as_object())
            .expect("should have from property");

        assert!(from_prop.contains_key("pattern"));
        let pattern = from_prop
            .get("pattern")
            .and_then(|v| v.as_str())
            .expect("pattern should be string");
        assert!(pattern.starts_with("^test_db:"));
    }
}
