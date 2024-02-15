# Contribution Guidelines

Consider configuring your IDE to automatically apply the rustfmt style on save.

Also consider using the same Clippy rules we enforce in CI as the default for your IDE.

In VSCode, you can add the following in your User Settings JSON file:

```json
  "[rust]": {
    "editor.defaultFormatter": "rust-lang.rust-analyzer",
    "editor.formatOnSave": true,
  },
  "rust-analyzer.check.command": "clippy",
  "rust-analyzer.check.features": "all",
  "rust-analyzer.check.extraArgs": ["--", "-Dclippy::pedantic", "-Dclippy::unwrap_used", "-Dclippy::expect_used"]
```

# Dev Setup

## Mac

```bash

# Install Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Rust
brew install rust