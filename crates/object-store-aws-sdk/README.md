# object-store-aws-sdk

This crate provides a seamless integration between the AWS SDK and the `object_store` crate, specifically addressing the credential discovery and authentication capabilities that are missing in the standard `object_store` S3 provider.

## Why This Crate Exists

The standard `object_store` crate's S3 provider implements a limited subset of AWS credential discovery mechanisms. While this works well for production environments with standard credential configurations, it falls short in local development and testing scenarios where developers commonly use AWS CLI credential chains, including:

- AWS SSO
- Credential profiles
- SAML authentication
- MFA-protected credentials
- Credential caching
- And other interactive authentication methods

The AWS SDK, on the other hand, implements the full suite of credential discovery mechanisms that AWS users expect, matching the behavior of the AWS CLI and other AWS tools.

## Example

TODO

```rust
use object_store_aws_sdk::ObjectStoreBuilder;

let object_store = ObjectStoreBuilder::new()
    .with_region("us-west-2")
    .build()
    .await?;
```
