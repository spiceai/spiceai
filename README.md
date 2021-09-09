# Spice.ai

[![build](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.gg/mXZdsRjRdN)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)


**Spice.ai** is an open source, portable runtime for training and using deep learning on time series data.


---

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

---

**The vision for Spice.ai is to make creating intelligent applications as easy as building a modern website.** Spice.ai brings AI development to your editor, in any language or framework with a fast, iterative, inner development loop, with continuous-integration (CI) and continuous-deployment (CD) workflows.

Spice.ai is written in Golang and Python and runs as a container or microservice with applications calling a simple HTTP API. It's deployable to any public cloud, on-premises, and edge.

📢 Read the Spice.ai announcement blog post at [blog.spiceai.org](https://blog.spiceai.org).

📺 View a 60 second demo of Spice.ai in action [here](https://www.youtube.com/watch?v=FPPGyPq41kQ).

### Community-driven data components

The Spice.ai runtime also includes a library of [community-driven data components](https://github.com/spiceai/data-components-contrib) for streaming and processing time series data, enabling developers to quickly and easily combine data with learning to create intelligent models.

### Spice.ai pod registry

Modern developers also build with the community by leveraging registries such as npm, NuGet, and pip. The registry for sharing and using Spice.ai packages is [spicerack.org](https://spicerack.org). As the community shares more and more AI building blocks, developers can quickly build intelligence into their applications, initially with definitions of AI projects and eventually by sharing and reusing fully-trained models.

<p align="center">
  <img src="https://user-images.githubusercontent.com/80174/132382372-c32cc8b7-25f2-4f82-8f9f-e4778fb69254.png" width="600" />
</p>

### Pre-release software

⚠️ The vision to bring intelligent application development to the maturity of modern web development is a vast undertaking. We haven't figured it all out or solved all the problems yet. We're looking for feedback on the direction. Spice.ai is not finished, in fact, we only just started in June, and we invite you on the journey.

Spice.ai and spicerack.org are both pre-release, early, alpha software. Spice.ai v0.1-alpha has many gaps, including limited deep learning algorithms and training scale, streaming data, simulated environments, and offline learning modes. Packages aren't searchable or even listed on spicerack.org yet.

Our intention with this preview is to work with developers early to co-define and co-develop the developer experience, aligning to the goal of making AI easy for developers. 🚀 Thus, due to the stage of development and as we focus, there are currently several [limitations](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#current-limitations) on the general [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#spice-ai-v10-stable-roadmap).

### Join us!

We greatly appreciate and value your feedback. Please feel free to [file an issue](https://github.com/spiceai/spiceai/issues/new) and get in touch with the team through [Discord](https://discord.gg/mXZdsRjRdN) or by sending us mail at [team@spiceai.io](mailto:team@spiceai.io).

Thank you for sharing this journey with us! 🙏

## Getting started with Spice.ai

First, ⭐️ star this repo! Thank you for your support! 🙏 

Then, follow this guide to get started quickly with Spice.ai. For a more comprehensive getting started guide, see the full [online documentation](https://docs.spiceai.org/).

### Current hosting limitations

- Docker is required. We are targeting self-host support in v0.3.0-alpha.
- Only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon :-)

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

### Create your first Spice.ai Pod and train it

A [Spice.ai Pod](https://docs.spiceai.org/concepts/#pod) is simply a collection of configuration and data that is used to train and deploy your own AI.

We will add intelligence to a sample application, **ServerOps**, by creating and training a Spice.ai pod that offers recommendations to the application for different server operations, such as performing server maintenance.

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

In addition to automatic training upon manifest changes, training can be started by using the Spice CLI from within your app directory.

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

Congratulations! In just a few minutes you downloaded and installed the Spice.ai CLI and runtime, created your first Spice.ai Pod, trained it, and got a recommendation from it.

This is just the start of the journey with Spice.ai. Next, try one of the quickstart tutorials or in-depth samples for creating intelligent applications.

**Try:**

- [ServerOps sample](https://github.com/spiceai/samples/tree/trunk/serverops/README.md) - a more in-depth version of the quickstart you just completed, using CPU metrics from your own machine
- [Gardener](https://github.com/spiceai/samples/tree/trunk/gardener/README.md) - Intelligently water a simulated garden
- [Trader](https://github.com/spiceai/quickstarts/tree/trunk/trader/README.md) - a basic Bitcoin trading bot

## Community

Spice.ai started with the vision to make AI easy for developers. We are building Spice.ai in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.gg/mXZdsRjRdN)
- Reddit: [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [team@spiceai.io](mailto:team@spiceai.io)

### Contributing to Spice.ai

See [CONTRIBUTING.md](/CONTRIBUTING.md).
