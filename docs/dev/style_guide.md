## Spice.ai Style Guide

This document provides a set of guidelines for writing idiomatic Rust code for Spice.ai.

As with everything we do at Spice, we start with outlining the principles that inform our work.

### Principles

- **Simplicity**: We write simple code that is easy to understand and maintain. If it takes you more than a few seconds to understand a piece of code, it's too complex.
- **Consistency**: We follow the conventions of the Rust community and the Rust language. We don't invent our own conventions.
- **Unsurprising**: Our code, especially our public interfaces, should strive to be intuitive enough that if a Rust developer had to guess, they would guess correctly.
- **Modular**: We are thoughtful about how we organize our code into crates and modules. We prefer smaller, focused crates and modules over large, monolithic ones.

### Style

**Error handling**: Error handling in Rust is a nuanced topic with many different approaches. We follow the [Snafu design philosophy](https://docs.rs/snafu/latest/snafu/guide/philosophy/index.html#snafus-design-philosophy) and use the [Snafu crate](https://docs.rs/snafu/latest/snafu/index.html) to define and use error types.

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

### Use the `ensure!` macro to check a condition and return an error

*Good*:

* Reads more like an `assert!`
* Is more concise

```rust
ensure!(!self.schema_sample.is_empty(), NeedsAtLeastOneLine);
```

*Bad*:

```rust
if self.schema_sample.is_empty() {
    return Err(Error::NeedsAtLeastOneLine {});
}
```

### Errors should be defined in the module they are instantiated

*Good*:

* Groups related error conditions together most closely with the code that produces them
* Reduces the need to `match` on unrelated errors that would never happen

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

### The `Result` type alias should be defined in each module

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

### Use `context` to wrap underlying errors into module specific errors

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

### Each error cause in a module should have a distinct `Error` enum variant

Specific error types are preferred over a generic error with a `message` or `kind` field.

*Good*:

- Makes it easier to track down the offending code based on a specific failure
- Reduces the size of the error enum
- Makes it easier to remove vestigial errors
- Is more concise

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

**Code linting**: We use [Clippy](https://doc.rust-lang.org/stable/clippy/index.html) to lint our code and make it more idiomatic. We treat all warnings as errors and enable several non-default lints. Using `#[allow(...)]` to disable lints is expected for lints that don't make sense given the context. Use your best judgement.

**API Guidelines**: We follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/about.html) for all public interfaces.