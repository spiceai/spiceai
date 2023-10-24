# Rust Spice SDK

Rust SDK for Spice.ai

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
spice-rs = { git = "https://github.com/spiceai/spice-rs", tag = "v1.0.0" }
```

## Usage

### Arrow Query

**SQL Query**

```rust
use spice_rs::new_spice_client;

let client = new_spice_client("API_KEY".to_string());
let data = client.query("SELECT * FROM eth.recent_blocks LIMIT 10;".to_string()).await;
```

### HTTP API
#### Prices

Get the supported pairs:

```rust
let supported_pairs = client.prices.get_supported_pairs().await;
```

Get the latest price for a token pair:

```rust
let price_data = client.prices.get_prices(&["BTC-USDC"]).await;
```

Get historical data:

```rust
let now = Utc::now();
let start = now.sub(Duration::seconds(3600));

let historical_price_data = client.prices.get_historical_prices(&["BTC-USDC"], Some(start),Some(now), Option::None).await;
```

## Documentation
Check out our [Documentation](https://docs.spice.ai/sdks/rust) to learn more about how to use the Rust SDK.
