# Spice Connect Examples

This directory contains examples demonstrating how to configure Spice Connect with different transport protocols.

## Examples

### 1. Flight Transport (Default) - `spice-connect-flight/`

Uses Apache Arrow Flight RPC for communication with Spice Cloud. This is the **default** and **recommended** transport protocol.

**Benefits:**

- High performance binary protocol
- Efficient streaming of large datasets
- Built-in compression
- Native Arrow format support

**Configuration:**

```yaml
management:
  enabled: true
  api_key: ${secrets:SPICE_API_KEY}
  params:
    transport: flight # Optional: This is the default
```

### 2. HTTP SSE Transport - `spice-connect-http-sse/`

Uses HTTP Server-Sent Events (SSE) for communication with Spice Cloud.

**When to use:**

- Firewall restrictions prevent gRPC/Flight connections
- Debugging connectivity issues
- Development environments with HTTP-only access

**Configuration:**

```yaml
management:
  enabled: true
  api_key: ${secrets:SPICE_API_KEY}
  params:
    transport: http-sse # Use HTTP SSE instead of Flight
```

### 3. Custom Endpoint - `spice-connect-custom-endpoint/`

Connect to a custom Spice Cloud endpoint or self-hosted Spice infrastructure.

**Configuration:**

```yaml
management:
  enabled: true
  api_key: ${secrets:SPICE_API_KEY}
  params:
    endpoint: custom-flight.example.com:443
```

## Configuration Reference

### `transport` Parameter

Specifies the transport protocol to use. Valid values:

- `flight` - Apache Arrow Flight RPC (default)
- `http-sse` - HTTP Server-Sent Events

**Default:** `flight`

### `endpoint` Parameter

Specifies a custom endpoint URL. If not provided, defaults are:

- Flight: `flight.spiceai.io:443`
- HTTP SSE: `https://data.spiceai.io/v1/connect`

The transport protocol can be auto-detected from the endpoint format:

- `grpc://` or `grpc+tls://` prefix → Flight
- `https://` or `http://` prefix → HTTP SSE
- Port 443 without http prefix → Flight (assumes TLS)

## Running the Examples

1. Set your Spice Cloud API key:

```bash
export SPICE_API_KEY="your-api-key-here"
```

2. Run the desired example:

```bash
cd spice-connect-flight
spice run
```

3. In another terminal, test remote SQL execution:

```bash
spice sql
> SELECT COUNT(*) FROM eth_blocks;
```

The Spice Connect management connection will receive and execute SQL commands from Spice Cloud.

## Troubleshooting

### Connection Issues

**Flight transport not working:**

- Check firewall rules allow outbound connections on port 443
- Verify gRPC/HTTP2 traffic is not blocked
- Try HTTP SSE transport as fallback

**HTTP SSE transport not working:**

- Verify HTTPS traffic is allowed
- Check proxy settings if behind corporate proxy
- Ensure API key is valid

### Logging

Enable debug logging to troubleshoot:

```bash
SPICED_LOG=runtime::management=debug spice run
```

View transport connection details:

```bash
SPICED_LOG=runtime::management=trace spice run
```
