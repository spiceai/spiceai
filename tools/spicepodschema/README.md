# Spicepod JSON Schema Generator

This tool generates a JSON Schema for the Spicepod specification (`spicepod.yaml`), enriched with connector-specific parameter schemas for data connectors, accelerators, and catalog connectors.

## Overview

The Spicepod JSON schema (`spicepod.schema.json`) provides validation and IDE support for `spicepod.yaml` configuration files. This tool:

1. Generates a base schema from the `SpicepodDefinition` struct using `schemars`
2. Collects `ParameterSpec` definitions from all registered connectors and accelerators
3. Enriches the schema with connector-specific parameter definitions
4. Outputs the final JSON Schema to a specified file

## Related Tools

| Tool | Purpose | Output |
|------|---------|--------|
| `tools/spicepodschema` | Generate JSON Schema for spicepod.yaml | `.schema/spicepod.schema.json` |
| `tools/spiceschema` | Generate OpenAPI schema for HTTP endpoints | stdout (JSON/YAML) |

## Usage

### Running Locally

From the repository root:

```bash
cargo run --manifest-path tools/spicepodschema/Cargo.toml -- .schema/spicepod.schema.json
```

### CI Workflow

The schema is automatically generated via `.github/workflows/generate_json_schema.yml`:

**Triggers:**
- Push to `trunk` with changes in `crates/spicepod/**`
- Manual `workflow_dispatch`

**Steps:**
1. Build spicepodschema tool
2. Run tool to generate `.schema/spicepod.schema.json`
3. Upload as artifact
4. (On manual dispatch) Create PR with updated schema

## Architecture

### Source Structure

```
tools/spicepodschema/
├── Cargo.toml
└── src/
    ├── main.rs        # Entry point, orchestration
    ├── collector.rs   # Collect ParameterSpecs from runtime registries
    ├── transform.rs   # ParameterSpec → JSON Schema conversion
    └── enricher.rs    # Merge connector schemas into base schema
```

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ runtime crate                                                   │
│                                                                 │
│  DATA_CONNECTOR_REGISTRATIONS ─────────────────────┐            │
│  (pub static, linkme distributed slice)            │            │
│                                                    │            │
│  DATA_ACCELERATOR_REGISTRATIONS ───────────────────┤            │
│  (pub static, linkme distributed slice)            │            │
│                                                    │            │
│  Catalog PARAMETERS consts ────────────────────────┤            │
└────────────────────────────────────────────────────┼────────────┘
                                                     │
                                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ spicepodschema                                                  │
│                                                                 │
│  collector.rs ──► transform.rs ──► enricher.rs ──► main.rs      │
│  (collect specs)  (to JSON Schema)  (merge)        (write out)  │
└─────────────────────────────────────────────────────────────────┘
```

### Dependencies

The tool depends on:
- `spicepod` with `schemars` feature - for base schema generation
- `runtime` with specific connector features - for `ParameterSpec` access
- `runtime-parameters` - for `ParameterSpec` type definitions

## Key Concepts

### ParameterSpec

Each connector defines its parameters as a compile-time `ParameterSpec` array:

```rust
const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections in the pool.")
        .default("5"),
];
```

### ParameterSpec → JSON Schema Mapping

| ParameterSpec Field | JSON Schema |
|---------------------|-------------|
| `name` | Property name (with prefix handling) |
| `required: true` | Added to `required` array |
| `default` | `default` value |
| `secret: true` | `x-secret: true` extension |
| `description` | `description` |
| `help_link` | Appended to description |
| `examples` | `examples` array |
| `one_of` | `enum` array |
| `deprecation_message` | `deprecated: true` + message in description |
| `type: Component` | Prefixed property name (e.g., `pg_host`) |
| `type: Runtime` | Unprefixed property name (e.g., `connection_pool_size`) |

### Parameter Types

| Type | Prefix | Purpose |
|------|--------|---------|
| `Component` | Yes (`{connector}_`) | Passed to underlying component (e.g., `pg_host`) |
| `Runtime` | No | Controls Spice runtime behavior (e.g., `connection_pool_size`) |

### Where Parameters Are Defined

| Component Type | Location |
|---------------|----------|
| Data Connectors | `crates/runtime/src/dataconnector/*.rs` |
| Data Accelerators | `crates/runtime/src/dataaccelerator/*.rs` |
| Catalog Connectors | `crates/runtime/src/catalogconnector/*.rs` |

## Feature Flags

The tool uses feature flags to control which connectors are included in the schema:

```toml
[features]
default = ["databricks", "delta_lake"]
databricks = []
delta_lake = []
```

Runtime features in `Cargo.toml` determine which connectors' `ParameterSpec` definitions are available for schema generation.

## Schema Output

**Location:** `.schema/spicepod.schema.json`

**Format:** JSON Schema Draft 2020-12

**Example structure:**
```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Spicepod Definition",
  "type": "object",
  "properties": { ... },
  "required": ["name", "version", "kind"],
  "$defs": {
    "Acceleration": { ... },
    "Dataset": { ... },
    "PostgresParams": {
      "type": "object",
      "properties": {
        "pg_connection_string": {
          "type": "string",
          "x-secret": true
        },
        "pg_host": { "type": "string" },
        "connection_pool_size": {
          "type": "string",
          "default": "5",
          "description": "The maximum number of connections..."
        }
      }
    },
    ...
  }
}
```

### Connector Discrimination with `if/then/else`

The schema uses `allOf` with `if/then` conditionals to provide connector-specific parameter validation based on the `from` field pattern. This approach provides excellent IDE support - when you specify a `from` value like `github:...`, the IDE will only show the GitHub-specific parameters.

**Schema Structure:**
```json
{
  "Dataset": {
    "allOf": [
      { /* base schema with common properties */ },
      {
        "if": { "properties": { "from": { "pattern": "^github:" } } },
        "then": { "$ref": "#/$defs/GithubDataset" }
      },
      {
        "if": { "properties": { "from": { "pattern": "^postgres:" } } },
        "then": { "$ref": "#/$defs/PostgresDataset" }
      }
      // ... more connectors
    ]
  }
}
```

**Why `if/then` instead of `anyOf`:**
- `anyOf` shows **all possible schemas** in IDE tooltips, making it hard to find relevant parameters
- `if/then` conditionals allow the IDE to narrow down to the **specific connector schema** based on the `from` field pattern
- This provides a much better developer experience with context-aware autocomplete and documentation

This design allows multiple datasets of the same connector type in a single spicepod while providing connector-specific parameter validation and IDE support.

## Schema Coverage

The schema generator enriches the base Spicepod schema with connector-specific parameter validation. This section documents which Spicepod components have connector/source-specific parameter schemas.

### Coverage Status

| Component | Has `from` Field | Has Connectors/Sources | Has `params` | Schema Coverage |
|-----------|------------------|------------------------|--------------|-----------------|
| **Datasets** | ✅ | Data Connectors | ✅ | ✅ **Covered** |
| **Datasets.acceleration** | - | Data Accelerators | ✅ | ✅ **Covered** |
| **Catalogs** | ✅ | Catalog Connectors | ✅ | ✅ **Covered** |
| **Models** | ✅ | Model Sources | ✅ | ❌ Not yet covered |
| **Embeddings** | ✅ | Embedding Sources | ✅ | ❌ Not yet covered |
| **Tools** | ✅ | Tool Types | ✅ | ❌ Not yet covered |
| **Secrets** | ✅ | Secret Stores | ✅ | ❌ Not yet covered |
| **Views** | ❌ | None (SQL-based) | ❌ | N/A |
| **Workers** | ❌ | None | ✅ (generic) | N/A |
| **Evals** | ❌ | None | ❌ | N/A |

### Currently Covered

#### Data Connectors (Datasets)
Location: `crates/runtime/src/dataconnector/*.rs`

Connectors are registered via `DATA_CONNECTOR_REGISTRATIONS` distributed slice. Each connector implements `DataConnectorFactory` trait with `parameters()` method.

#### Data Accelerators (Datasets.acceleration)
Location: `crates/runtime-acceleration/src/*.rs`

Accelerators are registered via `DATA_ACCELERATOR_REGISTRATIONS` distributed slice. Each accelerator implements `DataAccelerator` trait with `parameters()` method.

#### Catalog Connectors (Catalogs)
Location: `crates/runtime/src/catalogconnector/*.rs`

Catalog connectors define `PARAMETERS` constants. Currently includes:
- `unity_catalog` (requires `delta_lake` feature)
- `databricks` (requires `databricks` feature)
- `iceberg`
- `spice.ai`

### Not Yet Covered

#### Model Sources (Models)
Location: `crates/runtime/src/model/params/*.rs`

Model sources define parameters in separate modules:
- `openai` - OpenAI API parameters
- `azure` - Azure OpenAI parameters
- `anthropic` - Anthropic API parameters
- `perplexity` - Perplexity API parameters
- `xai` - xAI API parameters
- `bedrock` - AWS Bedrock parameters
- `databricks` - Databricks model parameters
- `huggingface` - Hugging Face parameters
- `file` - Local file model parameters

Access pattern: `get_params_spec(ModelSource) -> Option<&'static [ParameterSpec]>`

#### Embedding Sources (Embeddings)
Location: Uses similar parameters to models

Embedding prefixes (from `EmbeddingPrefix` enum):
- `openai`, `azure`, `huggingface`, `file`, `databricks`, `bedrock`, `model2vec`

#### Tool Types (Tools)
Location: `crates/runtime/src/tools/`

Tool types:
- `auto` - Builtin tools (get_readiness, list_datasets, sql, search, etc.)
- `mcp` - Model Context Protocol tools
- `memory` - Memory tools (store, load)

#### Secret Stores (Secrets)
Location: `crates/runtime-secrets/src/stores/`

Secret store types (from `SecretStoreType` enum):
- `env` - Environment variables (optional `file_path` param)
- `keyring` - System keyring (feature-gated)
- `kubernetes` - Kubernetes secrets
- `aws_secrets_manager` - AWS Secrets Manager (feature-gated)

## Adding New Connectors to Schema

1. Ensure the connector has a `PARAMETERS` constant with `ParameterSpec` definitions
2. Implement the appropriate trait (`DataConnectorFactory`, `DataAccelerator`, etc.)
3. Add the connector's feature to `Cargo.toml` dependencies if needed
4. Regenerate the schema

## Current Limitations

1. **Generic ComponentOrReference**: Generates numbered refs (`ComponentOrReference`, `ComponentOrReference2`, etc.) instead of descriptive names

2. **Params schema**: The `params` field validation uses `if/then` conditionals based on the `from` field pattern, which provides good IDE support but may not work with all JSON Schema validators

3. **Platform-specific connectors**: Some connectors are platform-specific and may not be included in all builds

4. **Custom deserializers**: Types with custom `Deserialize` impl may have schema that doesn't fully reflect runtime behavior

5. **Incomplete component coverage**: Models, Embeddings, Tools, and Secrets components have connector-specific parameters that are not yet included in the schema. See [Schema Coverage](#schema-coverage) section for details.

## Future Work

To achieve full schema coverage, the following components need to be added:

1. **Models** - Add model source parameter collection from `crates/runtime/src/model/params/`
2. **Embeddings** - Add embedding source parameter collection (similar to models)
3. **Tools** - Add tool type parameter collection from `crates/runtime/src/tools/`
4. **Secrets** - Add secret store parameter collection from `crates/runtime-secrets/src/stores/`

Each would follow the same pattern as data connectors:
1. Add collector function in `collector.rs`
2. Add enrichment logic in `enricher.rs`
3. Update `main.rs` to collect and process the new schemas
