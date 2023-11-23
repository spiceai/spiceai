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

## Building from Source

### Installing Dependencies

Spice.ai requires Go 1.21, Python 3.8.x and Docker.

#### Go 1.21

Download & install the latest 1.21 release for Go: https://golang.org/dl/ or run `brew install golang`

To make it easy to manage multiple versions of Go on your machine, see https://github.com/moovweb/gvm

#### Docker

Download and install Docker from: https://www.docker.com/products/docker-desktop

#### Python 3.8

Download & install the latest release for Python: https://www.python.org/downloads/

To make it easy to manage multiple versions of Python on your machine, see https://github.com/pyenv/pyenv

##### Set up Python venv and install python dependencies

It is a best practice to use a virtual environment (venv) when working with Python projects. To set up a venv for the AI Engine, run the following:

```bash
$ cd ai/src
# Ensure python 3+ is installed
$ python --version
Python 3.8.12
$ python -m venv venv
```

Activate the venv and install the python dependencies:

```bash
$ source venv/bin/activate
$ pip install -r requirements/development.txt
```

If you are running in GitHub Codespaces or other Ubuntu/Debian environment, you may need to install additional libraries. Do this by running:

```bash
 make install-codespaces
```

### Run Makefile in Project Root

This is necessary before building the CLI or Runtime

```bash
 make
```

### Building the Spice CLI

```bash
 cd cmd/spice
 make
```

### Building the Spice Runtime (spiced)

```bash
 cd cmd/spiced
 make
```

### Running the AI Engine

```bash
 cd ai/src
 source venv/bin/activate
 python main.py
```

### Running test suite

```bash
 go test ./...
 cd ai/src
 python -m unittest discover -s ./tests
```

**Thank You!** - Your contributions to open source, large or small, make projects like this possible. Thank you for taking the time to contribute.
