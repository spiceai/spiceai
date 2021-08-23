# Spice AI

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](https://opensource.org/licenses/Apache-2.0)
[![spiced](https://github.com/spiceai/spice/actions/workflows/spiced.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spice/actions/workflows/spiced.yml)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.com/channels/803820740868571196/803820740868571199)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

Spice AI is an open source runtime and distribution system for time series AI built for developers.

---

⚠️ **DEVELOPER PREVIEW ONLY** Spice AI is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

---

Welcome and thank you for your engagement in Spice AI early development with our Developer Preview.

Our intention with this preview is to work with developers early to co-define and co-develop the developer experience aligning to the goal of making AI easy for developers. 🚀 Thus, due to the stage of development and as we focus, there are currently several [limitations](https://github.com/spiceai/spiceai/docs/ROADMAP.md#current-limitations) on the general [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/docs/ROADMAP.md#spice-ai-1.0-stable-roadmap).

We greatly appreciate and value your feedback. Please feel free to [file an issue](https://github.com/spiceai/spiceai/issues/new) and get in touch with the team through [Discord](https://discord.com/channels/803820740868571196/803820740868571199) or by sending us mail at [team@spiceai.io](mailto:team@spiceai.io).

Thank you for sharing this journey with us! 🙏

## Getting started with Spice AI

Follow this guide to get started quickly with Spice AI. For a more comphrehensive getting started guide, see the full [online documentation](https://laughing-doodle-19648c61.pages.github.io/).

### Prerequisites (Developer Preview only)

- Currently, only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon :-)

- Currently, only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon :-)

1. Install Docker
2. Generate and export a GitHub PAT

**Step 1. Install Docker**: While self-hosting on baremetal hardware will be supported, the Developer Preview currently requires Docker. To install Docker, please follow [these instructions](https://docs.docker.com/get-docker/).

**Step 2. Generate and export a GitHub PAT**: To access private repositories and resources, you will need to generate a [GitHub Personal Access Token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) with `repo` and `read:packages` scopes.

Once you have created a token, use it to log in to the Spice AI Docker Repository:

```bash
docker login ghcr.io/spiceai
Username: <your GitHub username>
Password: <your token>
```

Add the token to an environment variable named SPICE_GH_TOKEN:

```bash
export SPICE_GH_TOKEN=<your token>
```

You may want to add to your terminal configuration, E.g. `.bashrc` or `.zshrc`.

### Installation

Install the Spice CLI by running the following `curl` command in your terminal.

```bash
curl https://raw.githubusercontent.com/spiceai/spiceai/trunk/install/install.sh\?token\=AAATSLRBS4STDET7UCNWQFDBFQ7E2 | /bin/bash
```

The installation path is not currently added to your PATH, so we recommend to add it manually with the following command which you may want to add to your terminal configuration, E.g. `.bashrc` or `.zshrc`. This step won't be required after public release.

```bash
export PATH="$HOME/.spice/bin:$PATH"
```

### Create your first Spice AI Pod and train it

A [Spice AI Pod](https://laughing-doodle-19648c61.pages.github.io/#/Concepts?id=pod) is simply a collection of configuration and data that you use to train and deploy your own AI.

The first Spice AI Pod you will create and train is based off an [Open AI gym](https://gym.openai.com/) example called [CartPole-v1](https://gym.openai.com/envs/CartPole-v1/).

Create a directory for the CartPole where you would normally put your code. E.g.

```bash
cd $HOME
mkdir cartpole
cd cartpole
```

In a new terminal window or tab, navigate to the directory and start the Spice runtime in development mode with `spice run`.

```bash
cd $HOME\cartpole
spice run
```

In the original terminal instance, add the CartPole-v1 sample:

```bash
spice pod add samples/CartPole-v1
```

The Spice CLI will download the CartPole-v1 sample pod manifest and add it to your project at `.spice/pods/cartpole-v1.yaml`.

The Spice Runtime will automatically detect the manifest and start your first training run!

### Observe your pod training

Navigate to [http://localhost:8000](http://localhost:8000) in your favorite browser. You will see an overview of your pods. From here, you can click on `cartpole-v1` Pod to see a chart of your training progress.

### Retrain your pod

The runtime will automatically detect changes to your pod manifest and start training. In addition, you can trigger training by using the Spice CLI from within your app directory.

```bash
spice pod train cartpole-v1
```

### Get a recommendation from your pod

After training your pod, you can now get a recommendation for an action from it!

```bash
curl http://localhost:8000/api/v0.1/pods/cartpole-v1/inference
```

### Conclusion and next steps

Congratulations! In just a few minutes you downloaded and installed the Spice AI CLI and runtime, created your first Spice AI Pod, trained it, and got a recommendation from it.

This is just the start of your journey with AI. Next try one of the quickstart tutorials for creating intelligent applications with Spice AI.

**Quickstarts:**

- [Trader](https://github.com/spiceai/quickstarts/tree/trunk/trader) - a basic Bitcoin trading bot

**Kubernetes:**

Spice AI integrates with your Kubernetes hosted apps! Try out the [Kubernetes sample](https://github.com/spiceai/samples/tree/trunk/kubernetes) for yourself.

## Community

Spice AI started with the vision to make AI easy for developers. We are building Spice AI in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.com/channels/803820740868571196/803820740868571199)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [team@spiceai.io](mailto:team@spiceai.io)

### Contributing to Spice AI

See [CONTRIBUTING.md](/CONTRIBUTING.md).
