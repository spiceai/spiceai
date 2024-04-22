# Rust Spice SDK

Rust SDK for Spice.ai

## Installation

Add Spice SDK

```bash
cargo add spiceai
```

## Usage

<!-- NOTE: If you're changing the code examples below, make sure you update `tests/readme_test.rs`. -->

### New client

```rust
use spiceai::Client;

#[tokio::main]
async fn main() {
  let mut client = Client::new("API_KEY").await.unwrap();
}
```

### Arrow Query

SQL Query

```rust
use spiceai::Client;

#[tokio::main]
async fn main() {
  let mut client = Client::new("API_KEY").await.unwrap();
  let data = client.query("SELECT * FROM eth.recent_blocks LIMIT 10;").await;
}

```

### Firecache Query

Firecache SQL Query

```rust
use spiceai::Client;

#[tokio::main]
async fn main() {
  let mut client = Client::new("API_KEY").await.unwrap();
  let data = client.fire_query("SELECT * FROM eth.recent_blocks LIMIT 10;").await;
}

```

## Documentation

Check out our [Documentation](https://docs.spice.ai/sdks/rust-sdk) to learn more about how to use the Rust SDK.
