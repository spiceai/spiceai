# Guidelines for Error Handling

## Summary

Error handling should prioritise the user-experience of using Spice, with errors that use simple but specific actionable error messages. These error messages should make it easy for users to recover from failures, and point them to where they can go for help.

At a glance, error handling guidelines:

1. Use simple but specific language
2. Specify the affected resource
3. Specify an action to take
4. Use specific errors
5. Refer to documentation
6. Split long lines into new lines
7. Exclude internal concepts
8. Exclude debug information

## Examples

As an example of applying these guidelines to existing error messages, take the previous `DataConnectorError::UnableToGetReadProvider` error message. The error message is `Unable to get read provider for {dataconnector}: {source}`.

Following these guidelines, this existing error message does not follow guidelines 1, 2, or 7:

* The error message does not use simple language (what is a read provider? what does it mean if we failed to get one?)
* The error message does not refer to the resource which it affects
* The error message refers to internal concepts (read provider)

Applying these guidelines, the new error message would be `Failed to read from {component_type} {component_name} ({dataconnector}): {source}`. This error message uses specific language (`Failed to read from`), removes the internal concept of `read provider`, and makes reference to the specific component in which it applies - using a variable, as it could apply to either a dataset or catalog.

The inner source error would still need to ensure these guidelines are applied to it, as the generic error would not provide a specific action.

## Guidelines

These guidelines were created based on the First Principle of Developer Experience First and Align to Industry Standard. No direct industry standards exist, but "best practices" are available. Loosely inspired by error handling material from:

* [Error Messages in Windows 7](https://learn.microsoft.com/en-us/windows/win32/uxguide/mess-error)
* [Cypress](https://docs.cypress.io/app/references/error-messages)

Related reading:

* [Microcopy: A complete guide](https://www.microcopybook.com/)

1. **Use simple but specific language**

    Use simple to understand language, and more specific language, like `Invalid parameter value specified for ...`. If specific language is not available, prefer using errors in the format of `Failed to ...` or `Cannot ...`.

    If a failure is due to an unsupported feature, specify that in the inner error. For example, if a data type is not supported by a connector: `Failed to register dataset mytable (duckdb): The data type ... is not supported.`.

2. **Specify the affected resource in the error**

    When returning an error, ensure the message contains the resource that is impacted by the error.

    *Good:*

    For datasets, specify both the dataset name and the connector source.

    `Failed to register dataset mytable (duckdb): ...`

    *Bad:*

    `Failed to register duckdb: ...`

    *Good:*

    For models, specify the name of the model.

    `Failed to run model mymodel: ...`

    *Bad:*

    `Failed to run model: ...`

3. **Provide specific actions**

    For any given error, except `Unknown` errors, provide specific actions for a user to fix the error.

    For example, if a user has provided an invalid configuration value, direct them to the documentation for the valid values.

    If an error is due to an unsupported feature, direct them to a workaround if available, or to file an issue requesting support for the feature.

    *Good:*

    Provide an action for the user - specifically, updating their spicepod configuration.

    ```bash
        Cannot setup the dataset taxi_trips (s3) with an invalid configuration.
        Failed to find any files matching the extension '.csv'.
        Is your `file_format` parameter correct? Spice found the following file extensions: '.parquet'.
        For details, visit: <https://spiceai.org/docs/components/data-connectors#object-store-file-formats>
    ```

    *Bad:*

    No action for the user to take is provided. It simply states as a matter of fact that no files were found.

    ```bash
        Cannot setup the dataset taxi_trips (s3) with an invalid configuration.
        Failed to find any files matching the extension '.csv'.
    ```

    *Good:*

    For errors where a specific action is not available, encourage the user to verify their operation and submit the issue as a bug if all else fails.

    `Failed to register dataset mytable (databricks): A query error occurred: ... . Verify the query is valid, and report this bug at https://github.com/spiceai/spiceai/issues`

    *Bad:*

    A specific action is not available, and the user is left without any action to take.

    `Failed to register dataset mytable (databricks): A query error occurred: ... .`

4. **Use the most specific error available**

    Use the most specific error available for that component. For example, if a dataset fails to register due to a configuration error do not use `DataConnectorError::UnableToGetReadProvider` and instead use a more specific error for `InvalidConfiguration`.

    If a specific error does not exist, but the use-case is common enough that it should exist, create the new error.

    Use `DataConnectorError::Internal` only when no other error will suffice, in catastrophic failures. `Unknown` or completely generic errors should be a last-resort error handling.

5. **Reference documentation in errors**

    Where documentation is available for a specific error (like a mis-configuration), a link to the documentation should be included in the error message. If documentation is not available for a specific error, consider if it should be created.

    Documentation does not always have to be included in error messages for plainly-obvious errors, or errors where the documentation will not help. For example, if a MySQL table does not exist that a user is trying to connect to, documentation will not help them. In this situation, the error can simply tell the user about the missing table and what actions to take.

    *Good:*

    ```bash
        Cannot setup the dataset taxi_trips (s3) with an invalid configuration.
        Failed to find any files matching the extension '.csv'.
        Is your `file_format` parameter correct? Spice found the following file extensions: '.parquet'.
        For details, visit: <https://spiceai.org/docs/components/data-connectors#object-store-file-formats>
    ```

    *Bad:*

    ```bash
        Cannot setup the dataset taxi_trips (s3) with an invalid configuration.
        Failed to find any files matching the extension '.csv'.
        Is your `file_format` parameter correct? Spice found the following file extensions: '.parquet'.
    ```

6. **Split long lines into new lines**

    Long message lines can be difficult to interpret.

    Typically, the error message, action to take, and any additional context/documentation should be split into separate lines.

    This makes it easier for a user to consume each part of the message, and identify what steps they need to take.

    If you're creating an error message which expects to receive an inner/source error, separate the inner error on a new line.

    *Good:*

    ```bash
        Error initializing dataset taxi_trips.
        The `s3_key` parameter cannot be set unless the `s3_auth` parameter is set to `key`.
        For more information, visit: https://spiceai.org/docs/components/data-connectors/s3#auth
    ```

    *Bad:*

    ```bash
        Error initializing dataset taxi_trips. The `s3_key` parameter cannot be set unless the `s3_auth` parameter is set to `key`. For more information, visit: https://spiceai.org/docs/components/data-connectors/s3#auth`
    ```

7. **Do not use internal concepts in error messages**

    Users generally do not care about the fact that the logical plan builder failed to construct - they care about the fact that their query failed.

    For user-facing errors, use simple language that does not include internal concepts. Print internal concepts only in verbose error messages.

    *Good:*

    `Failed to execute query: ...`

    *Bad:*

    `Failed to construct logical plan builder: ...`

8. **Don't display debug information in non-debug errors**

    When outputting non-debug errors, use the `Display` trait instead of the `Debug` trait.

    *Good:*

    ```rust
    if let Err(user_facing_err) = upload_data(datasource) {
        tracing::error!("Failed to upload data to {datasource}: {user_facing_err}");
    }
    ```

    *Bad:*

    ```rust
    if let Err(user_facing_err) = upload_data(datasource) {
        tracing::error!("Failed to upload data to {datasource}: {user_facing_err:?}");
    }
    ```
