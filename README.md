# Spice.ai

[![build](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml)
[![CodeQL](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.gg/kZnTfneP5u)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

**Spice.ai** makes it easy for developers to build intelligent apps. It streamlines the use of machine learning (ML) in software. Combined with time-series data, developers can create applications that continuously learn and adapt using ML recommendations.

Spice.ai takes a developer-first approach, and is focused on a fast, iterative, inner development loop, enabling developers to get started with ML in minutes instead of months.

---

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

---

📢 Read the Spice.ai announcement blog post at [blog.spiceai.org](https://blog.spiceai.org).

📺 View a 60 second demo of Spice.ai in action [here](https://www.youtube.com/watch?v=FPPGyPq41kQ).

## Features

The Spice.ai runtime is written in Golang and Python and runs as a container or microservice. It's deployable to any public cloud, on-premises, and edge. It is configured with a simple manifest and accessed by HTTP APIs.

Spice.ai includes:

- A lightweight, portable ML runtime accessible by simple HTTP APIs, allowing developers to use their preferred languages and frameworks
- A dashboard to visualize data and learning
- A developer-friendly CLI
- Simple, git-committable, configuration and code

### Community-Driven Data Components

Spice.ai also includes a library of community-driven [data components](https://github.com/spiceai/data-components-contrib) for streaming and processing time series data, enabling developers to quickly and easily combine data with learning to create intelligent models.

### Spicepod registry

Modern developers build with the community by leveraging registries such as npm, NuGet, and pip. The Spice.ai platform includes [spicerack.org](https://spicerack.org), the registry for ML building blocks.

As the community shares their ML building blocks (aka Spicepods, or pods for short), developers can quickly add them to their Spice.ai applications enabling them to stream data and build learning into their applications quickly and easily. Initially, Spicepods contain simple definitions of how the app should learn, and eventually will enable the sharing and use of fully-trained models.

## Why Spice.ai?

Spice.ai is for developers who want to build intelligent applications but don't have the time or resources to learn, build and integrate the required ML to do so.

Imagine you have timestamped measurements of the room temperature and access to air-conditioning controls. If you had a time-series ML engine, your application could optimize when the A/C activates. You could reduce energy usage by not overcooling the room as the temperature drops.

Now imagine learning Python or R, neural networks, deep-learning algorithms and building a system that streams and processes time-series data to do that. With Spice.ai — which includes a time-series ML engine accessible over HTTP APIs, a library of community-driven components for data streaming and processing, and an ecosystem of pre-created ML configurations — you can build upon the experience of the community instead of doing it all yourself. You can focus on business logic and building your application instead of the ML.

<p align="center">
  <img src="https://user-images.githubusercontent.com/80174/132382372-c32cc8b7-25f2-4f82-8f9f-e4778fb69254.png" width="600" />
</p>

### Pre-release software

⚠️ The vision to make it easy to build intelligent applications that learn is a vast undertaking. We haven't figured it all out or solved all the problems yet (we only started in June 2021!), so we’re inviting you on this journey and are looking for feedback the direction.

Spice.ai and spicerack.org are both pre-release, early, alpha software. Until v1.0, Spice.ai may have gaps, including limited deep learning algorithms, training-at-scale, and simulated environments. Also, Spicepods aren't searchable or listed on spicerack.org yet.

Our intention with this preview is to work with developers early to define and create the developer experience together. 🚀 See the [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#spice-ai-v10-stable-roadmap) for upcoming features.

### Join us!

We greatly appreciate and value your support! You can help Spice.ai in a number of ways:

- ⭐️ Star this repo.
- Build an app with Spice.ai and send us feedback and suggestions at [hey@spiceai.io](mailto:hey@spiceai.io) or on [Discord](https://discord.gg/kZnTfneP5u).
- [File an issue](https://github.com/spiceai/spiceai/issues/new) if you see something not quite working correctly.
- Follow us on [Reddit](https://www.reddit.com/r/spiceai), [Twitter](https://twitter.com/SpiceAIHQ), and [LinkedIn](https://www.linkedin.com/company/74148478).
- Join our team ([We’re hiring!](https://spiceai.io/careers))
- Contribute code or documentation to the project (see [CONTRIBUTING.md](CONTRIBUTING.md)).

We’re also starting a community call series soon!

Thank you for sharing this journey with us.

## Getting started with Spice.ai

First, ⭐️ star this repo! Thank you for your support! 🙏

Then, follow this guide to get started quickly with Spice.ai. For a more comprehensive guide, see the full [online documentation](https://docs.spiceai.org/).

### Current hosting limitations

- Docker is required. Self-host and metal support is on the roadmap.
- Only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- darwin/arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon. :-)

⭐️ We highly recommend using [GitHub Codespaces](https://github.com/features/codespaces) to get started. Codespaces enables you to run Spice.ai in a virtual environment in the cloud. If you use Codespaces, the install is not required and you may skip to the [Getting Started with Codespaces](#getting-started-with-codespaces) section.

### Installation (local machine)

1. Install Docker
2. Install the Spice CLI

**Step 1. Install Docker**: While self-hosting on baremetal hardware will be supported, the Developer Preview currently requires Docker. To install Docker, please follow [these instructions](https://docs.docker.com/get-docker/).

**Step 2. Install the Spice CLI**: Run the following `curl` command in your terminal.

```bash
curl https://install.spiceai.org | /bin/bash
```

You may need to restart your terminal for the `spice` command to be added to your PATH.

### Getting started with Codespaces

The recommended way to get started with Spice.ai is to use GitHub Codespaces.

Create a new GitHub Codespace in the `spiceai/quickstarts` repo at [github.com/spiceai/quickstarts/codespaces](https://github.com/spiceai/quickstarts/codespaces).

<img src="https://user-images.githubusercontent.com/80174/130397022-e882fc26-06fd-49da-ae35-03383221c63d.png" width="300">

Once you open the Codespace, Spice.ai and everything you need to get started will already be installed. Continue on to train your first pod.

### Create your first Spicepod and train it

A [Spicepod](https://docs.spiceai.org/concepts/#pod) is simply a collection of configuration and data that is used to train and deploy your own AI.

We will add intelligence to a sample application, **ServerOps**, by creating and training a Spicepod that offers recommendations to the application for different server operations, such as performing server maintenance.

If you are using GitHub Codespaces, skip Step 1. and continue with Step 2., as the repository will already be cloned.

Step 1. Clone the Spice.ai quickstarts repository:

```bash
cd $HOME
git clone https://github.com/spiceai/quickstarts
cd quickstarts/serverops
```

Step 2. Start the Spice runtime with `spice run`:

```bash
cd $HOME/quickstarts/serverops
spice run
```

Step. 3. In a new terminal, add the ServerOps quickstart pod:

So that we can leave Spice.ai running, add the quickstart pod in a new terminal tab or window. If you are running in GitHub Codespaces, you can open a new terminal by clicking the split-terminal button in VS Code.

```bash
spice add quickstarts/serverops
```

The Spice.ai CLI will download the ServerOps quickstart pod and add the pod manifest to your project at `spicepods/serverops.yaml`.

The Spice runtime will then automatically detect the pod and start your first training run!

> Note, automatic training relies on your system's filewatcher. In some cases, this might be disabled or not work as expected. If training does not start, follow the command to [retrain the pod](#retrain-the-pod) below.

### Observe the pod training

Navigate to [http://localhost:8000](http://localhost:8000) in your favorite browser. You will see an overview of your pods. From here, you can click on the `serverops` pod to see a chart of the pod's training progress.

### Retrain the pod

In addition to automatic training on each manifest change, training can be started by using the Spice CLI from within your app directory.

```bash
spice train serverops
```

### Get a recommendation

After training the pod, you can now get a recommendation for an action from it!

```bash
curl http://localhost:8000/api/v0.1/pods/serverops/recommendation
```

### Run the ServerOps application

To see how Spice.ai makes creating intelligent applications easy, try running and reviewing the sample ServerOps Node or Powershell apps, `serverops.js` and `serverops.ps1`.

Node:

```bash
npm install
node serverops.js
```

Powershell:

```ps
./serverops.ps1
```

### Next steps

Congratulations! In just a few minutes you downloaded and installed the Spice.ai CLI and runtime, created your first Spicepod, trained it, and got a recommendation from it.

This is just the start of the journey with Spice.ai. Next, try one of the quickstart tutorials or in-depth samples for creating intelligent applications.

**Try:**

- [ServerOps sample](https://github.com/spiceai/samples/tree/trunk/serverops/README.md) - a more in-depth version of the quickstart you just completed, using CPU metrics from your own machine
- [Gardener](https://github.com/spiceai/samples/tree/trunk/gardener/README.md) - Intelligently water a simulated garden
- [Trader](https://github.com/spiceai/quickstarts/tree/trunk/trader/README.md) - a basic Bitcoin trading bot

## Community

Spice.ai started with the vision to make AI easy for developers. We are building Spice.ai in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.gg/kZnTfneP5u)
- Reddit: [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [team@spiceai.io](mailto:team@spiceai.io)

### Contributing to Spice.ai

See [CONTRIBUTING.md](/CONTRIBUTING.md).
