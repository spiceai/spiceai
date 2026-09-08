
# Rusoto S3Vectors
Rust SDK for Amazon S3 Vectors

<a href="https://docs.rs/rusoto_s3_vectors/0.48.0" title="API Docs"><img src="https://img.shields.io/badge/API-docs-blue.svg" alt="api-docs-badge"></img></a>
<a href="https://crates.io/crates/rusoto_s3_vectors/0.48.0" title="Crates.io"><img src="https://img.shields.io/crates/v/rusoto_core.svg" alt="crates-io"></img></a>
<a href="#license" title="License: MIT"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="license-badge"></img></a>
<a href="https://discordapp.com/invite/WMJ4DWp"><img src="https://img.shields.io/discord/670751965273391124"></img></a>

You may be looking for:

* [An overview of Rusoto][rusoto-overview]
* [AWS services supported by Rusoto][supported-aws-services]
* [API documentation][api-documentation]
* [Getting help with Rusoto][rusoto-help]

## Requirements

Rust stable or beta are required to use Rusoto. Nightly is tested, but not guaranteed to be supported. Older
versions _may_ be supported. The currently supported Rust versions can be found in the Rusoto project
[`travis.yml`](https://github.com/rusoto/rusoto/blob/master/.travis.yml).

On Linux, OpenSSL is required if using the `native-tls` feature.

## Installation

To use `rusoto_s3_vectors` in your application, add it as a dependency in your `Cargo.toml`:

```toml
[dependencies]
s3_vectors = "0.48.0"
```

## Crate Features
- `native-tls` - use platform-specific TLS implementation.
- `rustls` - use rustls TLS implementation.
- `serialize_structs` - output structs of most operations get `derive(Serialize)`.
- `deserialize_structs` - input structs of most operations get `derive(Deserialize)`.
Note: the crate will use the `native-tls` TLS implementation by default.

## Contributing

See [CONTRIBUTING][contributing].

## License

Rusoto is distributed under the terms of the MIT license.

See [LICENSE][license] for details.

[api-documentation]: https://docs.rs/rusoto_s3_vectors "API documentation"
[license]: https://github.com/rusoto/rusoto/blob/master/LICENSE "MIT License"
[contributing]: https://github.com/rusoto/rusoto/blob/master/CONTRIBUTING.md "Contributing Guide"
[rusoto-help]: https://www.rusoto.org/help.html "Getting help with Rusoto"
[rusoto-overview]: https://www.rusoto.org/ "Rusoto overview"
[supported-aws-services]: https://www.rusoto.org/supported-aws-services.html "List of AWS services supported by Rusoto"
