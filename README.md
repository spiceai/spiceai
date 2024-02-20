# Spice.ai

[![build](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml)
[![CodeQL](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.gg/kZnTfneP5u)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

**Spice.ai** is a runtime environment that enables developers to leverage rapid access to time-series data for edge-based devices and applications.

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

---

📢 Read the Spice.ai announcement blog post at [blog.spiceai.org] TODO: NEW BLOG POST HERE.

📺 View a getting started walkthrough of Spice.ai in action [here] TODO: NEW VIDEO HERE.

💻 Learn about our [approach to building intelligent applications] TODO:

## Features

The Spice.ai runtime is written in (Golang for CLI) Rust and runs as a container or microservice. It's deployable to any public cloud, on-premises, and edge. It is configured with a simple manifest and accessed by HTTP APIs.

Spice.ai includes:

- A lightweight, portable ML runtime accessible by simple HTTP APIs, allowing developers to use their preferred languages and frameworks
- A developer-friendly CLI
- Simple, git-committable, configuration and code

### Community-Driven Data Components

## Will this feature be available in the future?

Spice.ai also includes a library of community-driven [data components](https://github.com/spiceai/data-components-contrib) for streaming and processing time series data, enabling developers to quickly and easily combine data with learning to create intelligent models.

### Spicepod registry

Modern developers build with the community by leveraging registries such as npm, NuGet, and pip. The Spice.ai platform includes [spicerack.org](https://spicerack.org), the registry for ML building blocks.

INSERT EXAMPLE OF A SPICEPOD YAML HERE

As the community shares their ML building blocks (aka Spicepods, or pods for short), developers can quickly add them to their Spice.ai applications enabling them to stream data into their applications quickly and easily. Spicepods contain simple definitions of how the app should learn, and eventually will enable the sharing and use of fully-trained models.

<p align="center">
  <img src="https://user-images.githubusercontent.com/80174/132382372-c32cc8b7-25f2-4f82-8f9f-e4778fb69254.png" width="600" />
</p>

## Why Spice.ai? Use case section

Spice.ai is for developers who want to build intelligent applications but don't have the time or resources to learn, build and integrate the required ML to do so.

Imagine you have timestamped measurements of the room temperature and access to air-conditioning controls. If you had a time-series ML engine, your application could optimize when the A/C activates. You could reduce energy usage by not overcooling the room as the temperature drops.

Now imagine learning Python or R, neural networks, deep-learning algorithms and building a system that streams and processes time-series data to do that. With Spice.ai — which includes a time-series ML engine accessible over HTTP APIs, a library of community-driven components for data streaming and processing, and an ecosystem of pre-created ML configurations — you can build upon the experience of the community instead of doing it all yourself. You can focus on business logic and building your application instead of the ML.

### Pre-release software

⚠️ The vision to make it easy to build intelligent applications that learn is a vast undertaking. We haven't figured it all out or solved all the problems yet, so we’re inviting you on this journey and are looking for feedback the direction.

Spice.ai and spicerack.org are both pre-release, early, alpha software. Until v1.0, Spice.ai may have gaps, including limited deep learning algorithms, training-at-scale, and simulated environments. Also, Spicepods aren't searchable or listed on spicerack.org yet.

Our intention with this preview is to work with developers early to define and create the developer experience together. 🚀 See the [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/rust/docs/RELEASE.md) for upcoming features.

### Join us!

We greatly appreciate and value your support! You can help Spice.ai in a number of ways:

- ⭐️ Star this repo.
- Build an app with Spice.ai and send us feedback and suggestions at [hey@spice.ai](mailto:hey@spice.ai) or on [Discord](https://discord.gg/kZnTfneP5u).
- [File an issue](https://github.com/spiceai/spiceai/issues/new) if you see something not quite working correctly.
- Follow us on [Reddit](https://www.reddit.com/r/spiceai), [Twitter](https://twitter.com/SpiceAIHQ), and [LinkedIn](https://www.linkedin.com/company/74148478).
- Join our team ([We’re hiring!](https://spice.ai/careers))
- Contribute code or documentation to the project (see [CONTRIBUTING.md](CONTRIBUTING.md)).

We’re also starting a community call series soon!

Thank you for sharing this journey with us.

## Getting started with Spice.ai

First, ⭐️ star this repo! Thank you for your support! 🙏

Then, follow this guide to get started quickly with Spice.ai. For a more comprehensive guide, see the full [online documentation](https://docs.spiceai.org/). ADD A VIDEO OF THE DATA DEMO HERE.

### Current hosting limitations

- Only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.

⭐️ We highly recommend using [GitHub Codespaces](https://github.com/features/codespaces) to get started. Codespaces enables you to run Spice.ai in a virtual environment in the cloud. If you use Codespaces, the install is not required and you may skip to the [Getting Started with Codespaces](#getting-started-with-codespaces) section.

### Installation (local machine)

1. Install the Spice CLI.

**Step 1. Clone the Rust branch of the SpicaAI repo**: Use the following command to clone the SpiceAI repo's Rust branch in your working directory:

```bash
git clone -b rust https://github.com/spiceai/spiceai.git
```

**Step 2. Install the CLI**: Running the following command will compile the source code and add the Spice command to your path:

```bash
Make install
```

### Run the data demo

## Community

Spice.ai started with the vision to make AI easy for developers. We are building Spice.ai in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.gg/kZnTfneP5u)
- Reddit: [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [hey@spice.ai](mailto:hey@spice.ai)

### Contributing to Spice.ai

See [CONTRIBUTING.md](/CONTRIBUTING.md).
