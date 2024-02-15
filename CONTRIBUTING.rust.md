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
# Install Xcode Command Line Tools
xcode-select --install

# Install Homebrew
# Note: Be sure to follow the steps in the Homebrew installation output to add Homebrew to your PATH.
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Rust
brew install rust

# Install Go
brew install go

# Clone SpiceAI OSS Repo
git clone https://github.com/spiceai/spiceai.git
cd spiceai

# Checkout rust branch
git checkout rust

# Build and install OSS project
make install

# Run the following to temporarily add spice to your PATH.
# Add it to the end of your .bashrc or .zshrc to permanently add spice to your PATH.
export PATH="$PATH:$HOME/.spice/bin"

# Initialize and run a test app to ensure everything is working
cd ../
mkdir test-app
cd test-app
spice init test-app
spice run
```