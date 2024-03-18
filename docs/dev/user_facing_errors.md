# Guidelines for displaying errors to the user

1. **Specific**: The error message should be specific enough to help the user understand what went wrong and how to fix it.
1. **Avoid Debugging Information**: The error message should not contain any debugging information. This includes stack traces, Rust debug representations, or any other technical information that the user cannot act on.
  - It can be helpful for developers to access this, but it should be gated behind a debug mode/log level/log file, etc and not shown by default.
1. **Internal/Unknown Errors**: If the error is internal to the application or unknown, the error message should be generic and not expose any internal details. The error message should indicate how to report the error to the developers.
  - We should strive to have as few of these as possible.

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