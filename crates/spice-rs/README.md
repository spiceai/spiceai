# Rust Spice SDK

Rust SDK for Spice.ai

## Installation

Add Spice SDK

```bash
cargo add spiceai
```

## Usage

<!-- NOTE: If you're changing the code examples below, make sure you update `tests/readme_test.rs`. -->

### Usage with locally running [spice runtime](https://github.com/spiceai/spiceai)

Follow the [quiqstart guide](https://github.com/spiceai/spiceai?tab=readme-ov-file#%EF%B8%8F-quickstart-local-machine) to install and run spice locally

```rust
use spiceai::ClientBuilder;

#[tokio::main]
async fn main() {
  let mut client = ClientBuilder::new()
    .flight_url("http://localhost:50051")
    .build()
    .await
    .unwrap();

  let data = client.query("SELECT trip_distance, total_amount FROM taxi_trips ORDER BY trip_distance DESC LIMIT 10;").await;
}
```

### New client with https://spice.ai cloud

```rust
use spiceai::ClientBuilder;

#[tokio::main]
async fn main() {
  let mut client = ClientBuilder::new()
    .api_key("API_KEY")
    .use_spiceai_cloud()
    .build()
    .await
    .unwrap();
}
```

### Arrow Query

SQL Query

```rust
use spiceai::ClientBuilder;

#[tokio::main]
async fn main() {
  let mut client = ClientBuilder::new()
    .api_key("API_KEY")
    .use_spiceai_cloud()
    .build()
    .await
    .unwrap();

  let data = client.query("SELECT * FROM eth.recent_blocks LIMIT 10;").await;
}

```

### Firecache Query

Firecache SQL Query

```rust
use spiceai::ClientBuilder;

#[tokio::main]
async fn main() {
  let mut client = ClientBuilder::new()
    .api_key("API_KEY")
    .use_spiceai_cloud()
    .build()
    .await
    .unwrap();

  let data = client.fire_query("SELECT * FROM eth.recent_blocks LIMIT 10;").await;
}

```

## Documentation

Check out our [Documentation](https://docs.spice.ai/sdks/rust-sdk) to learn more about how to use the Rust SDK.
