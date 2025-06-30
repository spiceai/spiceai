# Add AWS Bedrock Embeddings Provider

## Summary

This PR adds comprehensive support for AWS Bedrock embeddings in Spice.ai, enabling users to leverage Amazon Titan and Cohere embedding models directly through the Bedrock service.

## Features Added

### Supported Models
- **Amazon Titan Text Embeddings V1** (`amazon.titan-embed-text-v1`)
  - Configurable dimensions: 256, 512, 1024 (default: 1024)
  - Input limit: 8,192 tokens
- **Amazon Titan Text Embeddings V2** (`amazon.titan-embed-text-v2:0`)
  - Configurable dimensions: 256, 512, 1024 (default: 1024)
  - Input limit: 8,192 tokens
- **Cohere Embed English V3** (`cohere.embed-english-v3`)
  - Fixed dimensions: 1024
  - Input limit: 2,048 characters, max 96 texts per request
- **Cohere Embed Multilingual V3** (`cohere.embed-multilingual-v3`)
  - Fixed dimensions: 1024
  - Input limit: 2,048 characters, max 96 texts per request

### Configuration Options

#### AWS Authentication
- `aws_region` - AWS region (default: us-east-1)
- `aws_profile` - AWS profile name
- `aws_access_key_id` / `aws_secret_access_key` - Direct credentials
- `aws_session_token` - Optional session token

#### Model Parameters
- `dimensions` - Vector dimensions (Titan models only, validated: 256, 512, or 1024)
- `normalize` - Normalize embeddings (default: true)
- `input_type` - Input type for Cohere models (validated: `search_document`, `search_query`, `classification`, `clustering`)
- `truncate` - Truncation strategy for Cohere models (validated: `NONE`, `START`, `END`)

## Usage Examples

### Titan Embeddings
```yaml
embeddings:
  - from: bedrock:amazon.titan-embed-text-v1
    name: titan-embeddings
    params:
      aws_region: us-east-1
      dimensions: 1024
      normalize: true
```

### Cohere Embeddings
```yaml
embeddings:
  - from: bedrock:cohere.embed-english-v3
    name: cohere-embeddings
    params:
      aws_region: us-west-2
      input_type: search_document
      truncate: END
```

## Implementation Details

### Architecture
- **Modular Design**: New `bedrock` module in `llms` crate with clean separation between client and embedding logic
- **Feature Flag**: Protected behind `bedrock` feature flag (enabled by default)
- **OpenAI Compatibility**: Full support for OpenAI-compatible embedding APIs
- **Error Handling**: Comprehensive error handling with proper AWS SDK integration
- **Input Validation**: Early validation of model parameters with clear error messages

### Key Components
- `crates/llms/src/bedrock/mod.rs` - Core Bedrock client and API structures
- `crates/llms/src/bedrock/embed.rs` - Embeddings implementation with model-specific logic
- `crates/runtime/src/model/embed.rs` - Integration into embedding factory
- `crates/spicepod/src/component/embeddings.rs` - Configuration support

### Testing
- Comprehensive integration tests for all supported models
- Edge case testing (empty strings, long texts, special characters, Unicode)
- Dimension configuration testing for Titan models
- Input validation and error handling tests
- Parameter validation tests for all model types
- Token counting accuracy tests

### Performance Optimizations
- **Batch Processing**: Cohere models support batching up to 96 texts per request
- **Input Validation**: Automatic truncation for inputs exceeding model limits
- **Async Operations**: Full async/await support with concurrent request handling
- **Accurate Token Counting**: Real token usage tracking from API responses for billing and monitoring

## Technical Improvements

### Code Quality Enhancements
- **Accurate Token Counting**: Fixed token usage calculation to use actual counts from API responses instead of approximations
- **Improved Text Truncation**: Enhanced truncation logic for Titan models to respect token limits rather than character limits
- **Parameter Validation**: Added comprehensive validation for model-specific parameters:
  - Titan models: dimensions must be 256, 512, or 1024
  - Cohere models: input_type and truncate values are validated against allowed options
- **Better Error Messages**: Clear, actionable error messages for invalid configurations
- **Warning Logging**: Added warnings for token array conversions and text truncation events

### Robustness Improvements
- **Thread-Safe Token Tracking**: Implemented atomic counters for accurate token usage across concurrent requests
- **Input Type Safety**: Early validation prevents runtime errors from invalid model configurations
- **Edge Case Handling**: Proper handling of empty inputs, oversized text, and special characters

## Breaking Changes
None. This is a purely additive feature.

## Dependencies Added
- `aws-sdk-bedrockruntime = "1.55.0"` (optional, feature-gated)

## Testing
- Unit tests for all embedding conversion logic
- Integration tests for each supported model (marked with `#[ignore]` requiring AWS credentials)
- Lint and type checking passed

## Documentation
- Inline code documentation following project standards
- Configuration examples in PR description
- Test cases demonstrate usage patterns

## Checklist
- [x] Implementation follows existing codebase patterns
- [x] Proper error handling and logging
- [x] Feature flag protection
- [x] OpenAI API compatibility
- [x] Comprehensive testing suite
- [x] Lint and type checking passed
- [x] No breaking changes
- [x] Documentation provided
- [x] Parameter validation implemented
- [x] Accurate token counting fixed
- [x] Text truncation improved
- [x] Edge cases handled properly

## Notes
- Tests require AWS credentials and Bedrock model access, so they are marked with `#[ignore]` by default
- The implementation supports all major Bedrock embedding models available as of the implementation date
- Authentication follows standard AWS SDK patterns (environment variables, profiles, IAM roles, etc.)
- Token counting is accurate for Titan models using API response data; estimated for Cohere models
- Text truncation uses word-based approximation for token limits to prevent API failures