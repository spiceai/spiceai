# Guidelines for error handlling

## Displaying errors to the user

1. **Specific and Actionable**: The error message should be specific enough to help the user understand what went wrong and how to fix it.

> Example:
> `Cannot connect to postgres. Authentication failed. Ensure that the username and password are correctly configured in the spicepod.` is better than `Error connecting to postgres`.

1. **Avoid Debugging Information**: The error message should not contain any debugging information. This includes stack traces, Rust debug representations, or any other technical information that the user cannot act on.
  - It can be helpful for developers to access this, but it should be gated behind a debug mode/log level/log file, etc and not shown by default.
1. **Internal/Unknown Errors**: If the error is internal to the application or unknown, the error message should be generic and not expose any internal details. The error message should indicate how to report the error to the developers.
  - Strive to have as few of these as possible.

## Data Connector Errors

`dataconnector` module provides common bucket of errors with predefined messages. Each data connector implementation should use these to provide consistent error messages to the user.

https://github.com/spiceai/spiceai/blob/2a9fab7905737e1af182e17f40aecc5c4b5dd236/crates/runtime/src/dataconnector.rs#L113-L169

Example: postgres connector catches erros from underlying postgres crate and maps them to `DataConnectorError::...`.

https://github.com/spiceai/spiceai/blob/2a9fab7905737e1af182e17f40aecc5c4b5dd236/crates/runtime/src/dataconnector/postgres.rs#L54-L76

If a new error is needed, it should be added to the `DataConnectorError` enum and the error message should be specific and actionable.

If it is not possible to provide a specific error message, use common  `DataConnectorError::InvalidConfiguration` to wrap the error.

## Rust Error Traits

In Rust, the Error trait implements both the Debug and Display traits. All user-facing errors should use the Display trait, not the Debug trait.

i.e.

Good (uses Display trait)
```rust
if let Err(user_facing_err) = upload_data(datasource) {
    tracing::error!("Unable to upload data to {datasource}: {user_facing_err}");
}
```

Bad (uses Debug trait `:?`)
```rust
if let Err(user_facing_err) = upload_data(datasource) {
    tracing::error!("Unable to upload data to {datasource}: {user_facing_err:?}");
}
```
