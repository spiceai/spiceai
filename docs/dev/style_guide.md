## Spice.ai Rust Style Guide

This document provides guidelines for writing idiomatic Rust code in the context of Spice.ai.

The principles guiding this work are as follows:

### Principles

- **Simplicity**: Code should be simple, easy to understand, and maintainable. Code that takes more than a few seconds to comprehend is too complex.
- **Consistency**: Adherence to Rust community conventions and language standards is required.
- **Unsurprising**: Code, particularly public interfaces, aims to be intuitive, allowing a Rust developer to make accurate assumptions.
- **Modular**: Thoughtful organization of code into crates and modules is practiced. Smaller, focused crates and modules are preferred over large, monolithic structures.

### Style

**Error handling**: Error handling in Rust is a nuanced topic with many different approaches. The [Snafu design philosophy](https://docs.rs/snafu/latest/snafu/guide/philosophy/index.html#snafus-design-philosophy) is followed, utilizing the the [Snafu crate](https://docs.rs/snafu/latest/snafu/index.html) for defining and using error types.

### All errors use SNAFU functionality

*Good*:

* Derives `Snafu` and `Debug` functionality
* Has a useful, end-user-friendly display message

```rust
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Conversion needs at least one line of data"#))]
    NeedsAtLeastOneLine,
    // ...
}
```

*Bad*:

```rust
pub enum Error {
    NeedsAtLeastOneLine,
    // ...
}
```

### Employing the `ensure!` macro for condition checking and error return

*Good*:

* Resembles `assert!`
* More concise

```rust
ensure!(!self.schema_sample.is_empty(), NeedsAtLeastOneLine);
```

*Bad*:

```rust
if self.schema_sample.is_empty() {
    return Err(Error::NeedsAtLeastOneLine {});
}
```

### Define errors in their originating module

*Good*:

* Grouping related error conditions with their generating code
* Minimizing unnecessary `match` statements on irrelevant errors

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Needs more cowbell: {}", song_name))]
    NeedsMoreCowbell { song_name: String }
}
// ...
ensure!(foo.has_cowbell(), NeedsMoreCowbell {
    song_name: "foo",
})
```

*Bad*:

```rust
use crate::errors::NeedsMoreCowbell;
// ...
ensure!(foo.is_implemented(), NotImplemented {
    operation_name: "foo",
})
```

### Establishing the `Result` type alias in each module

*Good*:

* Reduces repetition

```rust
pub type Result<T, E = Error> = std::result::Result<T, E>;
...
fn foo() -> Result<bool> { true }
```

*Bad*:

```rust
...
fn foo() -> Result<bool, Error> { true }
```

### Utilize `context` to encapsulate underlying errors into module-specific errors

*Good*:

* Reduces boilerplate

```rust
input_reader
    .read_to_string(&mut buf)
    .context(UnableToReadInput {
        input_filename,
    })?;
```

*Bad*:

```rust
input_reader
    .read_to_string(&mut buf)
    .map_err(|e| Error::UnableToReadInput {
        name: String::from(input_filename),
        source: e,
    })?;
```

### Create distinct `Error` enum variants for each error cause in a module

Specific error types are preferred over a generic error with a `message` or `kind` field.

*Good*:

- Makes it easier to track down the offending code based on a specific failure
- Reduces the size of the error enum
- Simplifying removal of outdated errors
- More concise

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing remaining lines {}", source))]
    UnableToWriteGoodLines { source: IngestError },

    #[snafu(display("Error while closing the table writer {}", source))]
    UnableToCloseTableWriter { source: IngestError },
}

// ...

write_lines.context(UnableToWriteGoodLines)?;
close_writer.context(UnableToCloseTableWriter)?;
```

*Bad*:

```rust
pub enum Error {
    #[snafu(display("Error {}: {}", message, source))]
    WritingError {
        source: IngestError,
        message: String,
    },
}

write_lines.context(WritingError {
    message: String::from("Error while writing remaining lines"),
})?;
close_writer.context(WritingError {
    message: String::from("Error while closing the table writer"),
})?;
```

**Code linting**: [Clippy](https://doc.rust-lang.org/stable/clippy/index.html) is used for code linting to enhance idiomatic Rust usage. All warnings are treated as errors, with several non-standard lints enabled. Disabling lints using `#[allow(...)]` is acceptable when the lint is not applicable in certain contexts.

**API Guidelines**: The [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/about.html) are followed for all public interfaces.