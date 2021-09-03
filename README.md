# Spice.ai

[![e2e_test](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.com/channels/803820740868571196/803820740868571199)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

Spice.ai is an open source, portable runtime for training and using deep learning on time series data. It's written in Golang and Python and runs as a container or microservice with applications calling a simple HTTP API. It's deployable to any public cloud, on-premises, and edge.

The vision for Spice.ai is to make creating intelligent applications as easy as possible for developers in their development environment of choice. Spice.ai brings AI development to their editor, in any language or framework with a fast, iterative, inner development loop, with continuous-integration (CI) and continuous-deployment (CD) workflows.

The Spice.ai runtime also includes a library of [community-driven components](https://github.com/spiceai/data-components-contrib) for streaming and processing time series data, with the vision of enabling developers to quickly and easily combine data with learning to create intelligent models.

📢 Read the Spice.ai announcement blog post at [blog.spiceai.org](https://blog.spiceai.org)

---

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

---

The vision to bring intelligent application development to the maturity of modern web development is a vast undertaking. We haven't figured it all out or solved all the problems yet. We're looking for feedback on the direction. It's not finished, in fact, we only just started in June, and we invite you on the journey.

Modern developers also build with the community by leveraging registries such as npm, NuGet, and pip. The registry for sharing and using Spice.ai packages is [spicerack.org](https://spicerack.org). As the community shares more and more AI building blocks, developers can quickly build intelligence into their applications, initially with definitions of AI projects and eventually by sharing and reusing fully-trained models.

Spice.ai and spicerack.org are both pre-release, early, alpha software. Spice.ai v0.1-alpha has many gaps, including limited deep learning algorithms and training scale, streaming data, simulated environments, and offline learning modes. Packages aren't searchable or even listed on spicerack.org yet.

Our intention with this preview is to work with developers early to co-define and co-develop the developer experience, aligning to the goal of making AI easy for developers. 🚀 Thus, due to the stage of development and as we focus, there are currently several [limitations](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#current-limitations) on the general [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#spice-ai-v10-stable-roadmap).

We greatly appreciate and value your feedback. Please feel free to [file an issue](https://github.com/spiceai/spiceai/issues/new) and get in touch with the team through [Discord](https://discord.com/channels/803820740868571196) or by sending us mail at [team@spiceai.io](mailto:team@spiceai.io).

Thank you for sharing this journey with us! 🙏

## Getting started with Spice.ai

Follow this guide to get started quickly with Spice.ai. For a more comprehensive getting started guide, see the full [online documentation](https://docs.spiceai.org/).

### Current hosting limitations

- Running in Docker is required. We are targeting a baremetal experience by v0.3-alpha.
- Only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- arm64 is not yet supported (e.g. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon :-)

⭐️ We highly recommend using [GitHub Codespaces](https://github.com/features/codespaces) to get started. Codespaces enables you to run Spice.ai in a virtual environment in the cloud. If you use Codespaces, the following prerequisites are not required and you may skip to the [Getting Started with Codespaces](https://github.com/spiceai/spiceai#getting-started-with-codespaces) section.

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

A [Spice.ai Pod](https://docs.spiceai.org/concepts/#pod--pod-manifest) is simply a collection of configuration and data that Spice.ai uses to train and deploy AI.

The first Spice.ai Pod you will create and train is based off of a problem that many system administrators are familiar with: **server maintenance**. Application and system logging is critical part of running a production service, but letting those logs build up can cause other issues, especially if those logs end up filling the entire disk! It is simple enough to run a utility at a certain time every day to ensure this doesn't happen, but what if we choose to run the cleanup in the middle of peak traffic to the server?

We will use Spice.ai to train a pod that can intelligently learn when the best times are to run a cleanup job on a server. Let's call this example the `LogPruner`.

Clone the Spice.ai quickstarts repo in a directory where you would normally put your code. E.g.

```bash
cd $HOME
git clone https://github.com/spiceai/quickstarts
cd quickstarts/logpruner
```

In a new terminal window or tab, navigate to the directory and start the Spice runtime in development mode with `spice run`.

```bash
cd $HOME/quickstarts/logpruner
spice run
```

In the original terminal instance, add the LogPruner sample pod:

```bash
spice add quickstarts/logpruner
```

The Spice CLI will download the LogPruner sample pod manifest and add it to your project at `.spice/pods/logpruner.yaml`.

The Spice.ai runtime will then automatically detect the manifest and start your first training run!

> Note, automatic training relies on your system's filewatcher. In some cases, this might be disabled or not work as expected, especially when using containers. If training does not start, follow the command to [retrain your pod](https://github.com/spiceai/spiceai#retrain-your-pod) below.

### Observe your pod training

Navigate to [http://localhost:8000](http://localhost:8000) in your favorite browser. You will see an overview of your pods. From here, you can click on the `logpruner` pod to see a chart of the pod's training progress.

### Retrain your pod

The runtime will automatically detect changes to your pod manifest and start training. In addition, you can trigger training by using the Spice CLI from within your app directory.

```bash
spice train logpruner
```

### Get a recommendation from your pod

After training your pod, you can now fetch a recommendation for an action!

```bash
curl http://localhost:8000/api/v0.1/pods/logpruner/recommendation
```

### Conclusion and next steps

Congratulations! In just a few minutes you downloaded and installed the Spice.ai CLI and runtime, created your first Spice.ai Pod, trained it, and got a recommendation from it.

This is just the start of your journey with AI. Next, try one of the quickstart tutorials or in-depth samples for creating intelligent applications with Spice.ai.

**Try:**

- [Log Pruner sample](https://github.com/spiceai/samples/tree/trunk/logpruner/README.md) - a more in-depth version of the quickstart you just completed, using CPU metrics from your own machine
- [Gardener](https://github.com/spiceai/samples/tree/trunk/gardener/README.md) - Intelligently water a simulated garden
- [Trader](https://github.com/spiceai/quickstarts/tree/trunk/trader/README.md) - a basic Bitcoin trading bot

**Kubernetes:**

Spice.ai can be deployed to Kubernetes! Try out the [Kubernetes sample](https://github.com/spiceai/samples/tree/trunk/kubernetes).

## Community

Spice.ai started with the vision to make AI easy for developers. We are building Spice.ai in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.com/channels/803820740868571196/803820740868571199)
- Reddit: [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [team@spiceai.io](mailto:team@spiceai.io)

### Contributing to Spice.ai

See [CONTRIBUTING.md](/CONTRIBUTING.md).
