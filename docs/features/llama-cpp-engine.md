# llama.cpp Inference Engine

## Overview

Spice now supports **llama.cpp** as an alternative inference engine for LLM models, in addition to the default **mistral-rs** engine. This provides users with more flexibility in choosing their inference backend.

## Status

✅ **Fully Functional**: The llama.cpp engine integration is now fully implemented with complete inference and streaming support. Both **mistral-rs** and **llama.cpp** are production-ready options.

### Feature Comparison

| Feature          | mistral-rs              | llama.cpp      |
| ---------------- | ----------------------- | -------------- |
| Text Generation  | ✅                      | ✅             |
| Streaming        | ✅                      | ✅             |
| Token Counting   | ✅                      | ✅             |
| Model Formats    | Safetensors, GGUF, GGML | GGUF only      |
| GPU Acceleration | ✅ CUDA, Metal          | ✅ CUDA, Metal |
| Production Ready | ✅                      | ✅             |

## Configuration

### Selecting the Engine

You can specify which engine to use via the `engine` field in your spicepod model configuration:

```yaml
models:
  - from: huggingface
    name: my-model
    params:
      huggingface_model_id: 'TheBloke/Llama-2-7B-GGUF'
      huggingface_file: 'llama-2-7b.Q4_K_M.gguf'
    engine: llama.cpp # Use llama.cpp engine
```

If the `engine` field is omitted, **mistral-rs** is used by default.

### Supported Values

- `mistral-rs` (default) - Fully supported inference engine
- `llama.cpp` - Alternative engine with GGUF focus

## Requirements

### Model Format

The llama.cpp engine requires models in **GGUF format**. When using HuggingFace models, ensure you specify a GGUF file:

```yaml
params:
  huggingface_model_id: 'TheBloke/Llama-2-7B-GGUF'
  huggingface_file: 'llama-2-7b.Q4_K_M.gguf' # Must be .gguf format
```

### Hardware Acceleration

Hardware acceleration support is available through feature flags:

- **CUDA** (NVIDIA GPUs): Build with `SPICED_CUSTOM_FEATURES="llama_cpp,cuda"`
- **Metal** (Apple Silicon): Build with `SPICED_CUSTOM_FEATURES="llama_cpp,metal"`

## Build Instructions

### Standard Build (CPU-only)

```bash
# Build with llama_cpp support (included in models feature)
make install-with-models
```

### GPU-Accelerated Build

```bash
# For NVIDIA GPUs (CUDA)
SPICED_CUSTOM_FEATURES="models,cuda" make install

# For Apple Silicon (Metal)
SPICED_CUSTOM_FEATURES="models,metal" make install
```

## Implementation Details

The llama.cpp engine provides:

1. ✅ **Complete inference** - Full text generation with greedy sampling
2. ✅ **Streaming support** - Real-time token-by-token generation
3. ✅ **Accurate token counting** - Proper prompt and completion token metrics
4. ✅ **Health checks** - Validates model functionality on startup
5. ✅ **Error handling** - Clear, actionable error messages
6. ✅ **Async execution** - Non-blocking inference using `tokio::spawn_blocking`

### Current Limitations

- **Model format**: Only GGUF format is supported (mistral-rs supports Safetensors, GGUF, and GGML).
- **Context size**: Hardcoded to 2048 tokens (will be made configurable in a future release).

### Future Enhancements

The following improvements are planned:

1. **Configurable context size** - Allow users to specify the context window size (currently hardcoded to 2048)
2. **Batch processing** - Optimize for multiple concurrent requests

## Architecture

### Code Structure

- **spicepod**: `crates/spicepod/src/component/model.rs` - Added `engine` field to Model struct
- **llms crate**: `crates/llms/src/chat/llama_cpp.rs` - llama.cpp Chat trait implementation
- **runtime**: `crates/runtime/src/model/chat.rs` - Engine routing logic

### Integration Pattern

The implementation follows Spice's extensibility model:

1. **Optional dependency**: llama-cpp-2 is gated behind `llama_cpp` feature flag
2. **Non-regressive**: Default behavior unchanged, mistral-rs remains default
3. **Feature flags**: Properly propagated through workspace crates
4. **Error handling**: Clear error messages with actionable guidance

### Inference Flow

1. **Tokenization**: Convert prompt text to tokens using model's tokenizer
2. **Batch creation**: Create llama.cpp batch with prompt tokens
3. **Context decode**: Process prompt tokens through the model
4. **Generation loop**: Sample next token → decode → repeat until EOS or max_tokens
5. **Streaming**: Tokens sent via async channel for real-time delivery
6. **Token counting**: Accurate counts using model's tokenizer

## Recommendations

### When to Use mistral-rs (Default)

- ✅ Need support for multiple model formats (Safetensors, GGUF, GGML)
- ✅ Want the most battle-tested inference engine
- ✅ Require advanced features and optimizations
- ✅ Default choice for most users

### When to Use llama.cpp

- ✅ Prefer llama.cpp's specific optimizations
- ✅ Already have GGUF quantized models
- ✅ Want C++-based inference backend
- ✅ Exploring alternative inference engines

Both engines are production-ready and provide reliable inference.

## Example Spicepods

### Using mistral-rs (Default)

```yaml
version: v1beta1
kind: Spicepod
name: my-app

models:
  - from: huggingface
    name: llama2-7b
    params:
      huggingface_model_id: 'TheBloke/Llama-2-7B-GGUF'
      huggingface_file: 'llama-2-7b.Q4_K_M.gguf'
    # engine: mistral-rs  # Default, can be omitted
```

### Using llama.cpp

```yaml
version: v1beta1
kind: Spicepod
name: my-app

models:
  - from: huggingface
    name: llama2-7b
    params:
      huggingface_model_id: 'TheBloke/Llama-2-7B-GGUF'
      huggingface_file: 'llama-2-7b.Q4_K_M.gguf'
    engine: llama.cpp # Explicitly use llama.cpp engine
```

## API Usage

Both engines support the OpenAI-compatible chat completions API:

### Non-streaming Request

```bash
curl -X POST http://localhost:8090/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama2-7b",
    "messages": [
      {"role": "user", "content": "What is Rust?"}
    ]
  }'
```

### Streaming Request

```bash
curl -X POST http://localhost:8090/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama2-7b",
    "messages": [
      {"role": "user", "content": "What is Rust?"}
    ],
    "stream": true
  }'
```

## Performance Characteristics

### Greedy Sampling

The current implementation uses greedy sampling (always selects the most likely token), which provides:

- ✅ **Deterministic output** - Same input always produces same output
- ✅ **Fast inference** - No probabilistic sampling overhead
- ⚠️ **Less creative** - No randomness in generation
- ⚠️ **Repetitive** - May produce repetitive text without proper penalties

### Memory Usage

- **Model loading**: Model loaded once per instance
- **Context per request**: Each inference creates a new context
- **Batch size**: Fixed at 512 tokens per batch
- **Streaming**: Minimal memory overhead with channel-based streaming

## Troubleshooting

### Model Loading Issues

If models fail to load:

1. **Verify GGUF format**: Ensure the model file has `.gguf` extension
2. **Check file path**: Confirm the model file exists at the specified location
3. **Review logs**: Check spiced logs for detailed error messages

### Inference Errors

If inference fails:

1. **Check context size**: Very long prompts may exceed context window
2. **Verify model compatibility**: Ensure the GGUF model is compatible with llama.cpp
3. **Review memory**: Large models may require significant RAM/VRAM

### Performance Issues

If inference is slow:

1. **Enable GPU acceleration**: Build with CUDA or Metal support
2. **Use quantized models**: Q4_K_M quantization balances quality and speed
3. **Monitor resources**: Check CPU/GPU utilization

## Contributing

To contribute enhancements to the llama.cpp integration:

1. **Implementation**: `crates/llms/src/chat/llama_cpp.rs`
2. **Tests**: Add integration tests in `test/models/`
3. **Documentation**: Update this file and API docs
4. **PR**: Follow the contributing guidelines in `CONTRIBUTING.md`

## See Also

- [Model Configuration](https://docs.spiceai.org/components/models)
- [Extensibility Guide](../../EXTENSIBILITY.md)
- [mistral-rs Documentation](https://github.com/EricLBuehler/mistral.rs)
- [llama.cpp Project](https://github.com/ggerganov/llama.cpp)
- [llama-cpp-2 Rust Bindings](https://github.com/utilityai/llama-cpp-rs)
