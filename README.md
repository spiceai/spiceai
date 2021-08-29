# Spice.ai

[![spiced](https://github.com/spiceai/spice/actions/workflows/spiced.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spice/actions/workflows/spiced.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.com/channels/803820740868571196/803820740868571199)
![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

Spice.ai is an open source runtime and distribution system for time series AI built for developers.

---

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

---

Welcome to the Spice.ai Developer Preview and thank you for your engagement in early Spice.ai development.

Our intention with this preview is to work with developers early to co-define and co-develop the developer experience, aligning to the goal of making AI easy for developers. 🚀 Thus, due to the stage of development and as we focus, there are currently several [limitations](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#current-limitations) on the general [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md#spice-ai-v10-stable-roadmap).

We greatly appreciate and value your feedback. Please feel free to [file an issue](https://github.com/spiceai/spiceai/issues/new) and get in touch with the team through [Discord](https://discord.com/channels/803820740868571196) or by sending us mail at [team@spiceai.io](mailto:team@spiceai.io).

Thank you for sharing this journey with us! 🙏

## Getting started with Spice.ai

Follow this guide to get started quickly with Spice.ai. For a more comphrehensive getting started guide, see the full [online documentation](https://docs.spiceai.org/).

### Current Limitations

- Running in Docker is required. We will support a baremetal experience at launch.
- Only macOS and Linux are natively supported. [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/install-win10) is required for Windows.
- arm64 is not yet supported (i.e. Apple's M1 Macs). We use M1s ourselves, so we hope to support this very soon :-)

### Prerequisites (Developer Preview only)

We highly recommend using [GitHub Codespaces](https://github.com/features/codespaces) to get started. Codespaces enables you to run Spice.ai in a virtual environment in the cloud. If you use Codespaces, the following prerequisites are not required and you may skip to the [Getting Started with Codespaces](https://github.com/spiceai/spiceai#getting-started-with-codespaces) section.

To continue with installation on your local machine, follow these steps:

1. Install Docker
2. Generate and export a GitHub PAT

**Step 1. Install Docker**: While self-hosting on baremetal hardware will be supported, the Developer Preview currently requires Docker. To install Docker, please follow [these instructions](https://docs.docker.com/get-docker/).

**Step 2. Generate and export a GitHub PAT**: To access private repositories and resources, you will need to generate a [GitHub Personal Access Token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) with `repo` and `read:packages` scopes.

Once you have created a token, use it to log in to the Spice.ai Docker Repository:

```bash
docker login ghcr.io/spiceai
Username: <your GitHub username>
Password: <your token>
```

Add the token to an environment variable named SPICE_GH_TOKEN:

```bash
export SPICE_GH_TOKEN=<your token>
```

You will need to set the SPICE_GH_TOKEN in each terminal you use, so you may want to add to your terminal configuration, E.g. `.bashrc` or `.zshrc`. You can manually edit the file or use a command like:

```bash
cat "export SPICE_GH_TOKEN=<your token>" >> ~/.bashrc
```

These steps won't be required after public release.

### Installation (local machine)

Install the Spice CLI by running the following `curl` command in your terminal.

```bash
curl https://raw.githubusercontent.com/spiceai/spiceai/trunk/install/install.sh\?token\=AAATSLSSFWUX6ZVJ6LZI4XDBFRYHC | /bin/bash
```

The installation path is not currently added to your PATH, so we recommend to add it manually with the following command which you may want to add to your terminal configuration, E.g. `.bashrc` or `.zshrc`. This step won't be required after public release.

```bash
export PATH="$HOME/.spice/bin:$PATH"
```

You can also add to your `.bashrc`

```bash
cat "export PATH="$HOME/.spice/bin:$PATH" >> ~/.bashrc
```

### Getting started with Codespaces

The recommended way to get started with Spice.ai is to use GitHub Codespaces.

Create a new GitHub Codespace in the `spiceai/quickstarts` repo at [github.com/spiceai/quickstarts/codespaces](https://github.com/spiceai/quickstarts/codespaces).

<img src="https://user-images.githubusercontent.com/80174/130397022-e882fc26-06fd-49da-ae35-03383221c63d.png" width="300">

Once you open the Codespace, Spice.ai and everything you need to get started will already be installed. You may continue on below.

### Create your first Spice.ai Pod and train it

A [Spice.ai Pod](https://crispy-dollop-c329115a.pages.github.io/#/concepts/README?id=pod-pod-manifest) is simply a collection of configuration and data that you use to train and deploy your own AI.

The first Spice.ai Pod you will create and train is based off an [Open AI gym](https://gym.openai.com/) example called [CartPole-v1](https://gym.openai.com/envs/CartPole-v1/). Open AI describes CartPole as:

> A pole is attached by an un-actuated joint to a cart, which moves along a frictionless track. The system is controlled by applying a force of +1 or -1 to the cart. The pendulum starts upright, and the goal is to prevent it from falling over. A reward of +1 is provided for every timestep that the pole remains upright. The episode ends when the pole is more than 15 degrees from vertical, or the cart moves more than 2.4 units from the center.
> Source: [gym.openai.com/envs/CartPole-v1](https://gym.openai.com/envs/CartPole-v1/)

We will use Spice.ai to train a pod that can play the game.

Create a directory for the CartPole where you would normally put your code. E.g.

```bash
cd $HOME
mkdir cartpole
cd cartpole
```

In a new terminal window or tab, navigate to the directory and start the Spice runtime in development mode with `spice run`.

```bash
cd $HOME/cartpole
spice run
```

In the original terminal instance, add the CartPole-v1 sample pod:

```bash
spice add samples/CartPole-v1
```

The Spice CLI will download the CartPole-v1 sample pod manifest and add it to your project at `.spice/pods/cartpole-v1.yaml`.

The Spice runtime will then automatically detect the manifest and start your first training run!

> Note, automatic training relies on your system's filewatcher. In some cases, this might be disabled or not work as expected, especially when using containers. If training does not start, follow the command to [retrain your pod](https://github.com/spiceai/spiceai#retrain-your-pod) below.

### Observe your pod training

Navigate to [http://localhost:8000](http://localhost:8000) in your favorite browser. You will see an overview of your pods. From here, you can click on the `cartpole-v1` pod to see a chart of the pod's training progress.

### Retrain your pod

The runtime will automatically detect changes to your pod manifest and start training. In addition, you can trigger training by using the Spice CLI from within your app directory.

```bash
spice train cartpole-v1
```

### Get a recommendation from your pod

After training your pod, you can now get a recommendation for an action from it!

```bash
curl http://localhost:8000/api/v0.1/pods/cartpole-v1/inference
```

### Conclusion and next steps

Congratulations! In just a few minutes you downloaded and installed the Spice.ai CLI and runtime, created your first Spice.ai Pod, trained it, and got a recommendation from it.

This is just the start of your journey with AI. Next, try one of the quickstart tutorials for creating intelligent applications with Spice.ai.

**Quickstarts:**

- [Log Pruner](https://github.com/spiceai/quickstarts/tree/trunk/log-pruner) - a CPU load based log pruner
- [Trader](https://github.com/spiceai/quickstarts/tree/trunk/trader) - a basic Bitcoin trading bot

**Kubernetes:**

Spice.ai can be deployed to Kubernetes! Try out the [Kubernetes sample](https://github.com/spiceai/samples/tree/trunk/kubernetes).

## Community

Spice.ai started with the vision to make AI easy for developers. We are building Spice.ai in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.com/channels/803820740868571196/803820740868571199)
- Reddit: ![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [team@spiceai.io](mailto:team@spiceai.io)

### Contributing to Spice.ai

See [CONTRIBUTING.md](/CONTRIBUTING.md).
