# Contribution Guidelines

Thank you for your interest in Spice.ai!

This project welcomes contributions and suggestions.

Contributions come in many forms: submitting issues, writing code, participating in discussions and community calls.

This document provides the guidelines for how to contribute to the Spice.ai project.

## Issues

This section describes the guidelines for submitting issues

### Issue Types

There are 4 types of issues:

- Issue/Bug: You've found a bug with the code, and want to report it, or create an issue to track the bug.
- Issue/Discussion: You have something on your mind, which requires input from others in a discussion, before it eventually manifests as a proposal.
- Issue/Proposal: Used for items that propose a new idea or functionality. This allows feedback from others before code is written.
- Issue/Question: Use this issue type, if you need help or have a question.

### Before You File

Before you file an issue, make sure you've checked the following:

1. Check for existing issues
   - Before you create a new issue, please do a search in [open issues](https://github.com/spiceai/spiceai/issues) to see if the issue or feature request has already been filed.
   - If you find your issue already exists, make relevant comments and add your [reaction](https://github.com/blog/2119-add-reaction-to-pull-requests-issues-and-comments). Use a reaction:
     - 👍 up-vote
     - 👎 down-vote
1. For bugs
   - You have as much data as possible. This usually comes in the form of logs and/or stacktraces. Screenshots or screen recordings of the buggy behavior are very helpful.

## Contributing to Spice.ai

This section describes the guidelines for contributing code / docs to Spice.ai.

### Pull Requests

All contributions come through pull requests. To submit a proposed change, we recommend following this workflow:

1. Make sure there's an issue (bug or proposal) raised, which sets the expectations for the contribution you are about to make.
1. Fork the repo and create a new branch
1. Create your change
   - Code changes require tests
1. Update relevant documentation for the change
1. Commit and open a PR
1. Wait for the CI process to finish and make sure all checks are green
1. A maintainer of the project will be assigned, and you can expect a review within a few days

### Use work-in-progress PRs for early feedback

A good way to communicate before investing too much time is to create a "Work-in-progress" PR and share it with your reviewers. The standard way of doing this is to add a "[WIP]" prefix in your PR's title and assign the **do-not-merge** label. This will let people looking at your PR know that it is not well baked yet.

## Security Vulnerability Reporting

Spice.ai takes security and our users' trust very seriously. If you believe you have found a security issue in any of our open source projects, please responsibly disclose it by contacting security@spice.ai.

### Use of Third-party code

- Third-party code must include licenses.

## Setting up your development environment

### MacOS

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

### VSCode Configuration

Configuring VSCode to automatically apply the rustfmt style on save.

Also configure VSCode to us the same Clippy rules we enforce in CI as the default.

Add the following in your User Settings JSON file:

```json
  "[rust]": {
    "editor.defaultFormatter": "rust-lang.rust-analyzer",
    "editor.formatOnSave": true,
  },
  "rust-analyzer.check.command": "clippy",
  "rust-analyzer.check.features": "all",
  "rust-analyzer.check.extraArgs": ["--", "-Dclippy::pedantic", "-Dclippy::unwrap_used", "-Dclippy::expect_used"]
```

**Thank You!** - Your contributions to open source, large or small, make projects like this possible. Thank you for taking the time to contribute.
