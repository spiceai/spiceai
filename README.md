# Spice.ai

[![build](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml)
[![CodeQL](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.gg/kZnTfneP5u)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

**Spice.ai** is an open source runtime environment that enables developers to leverage rapid access to time-series data for building applications at the edge.

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

## Why Spice.ai? Use case section

Highlight three use cases.

1.  Front ends that need to be continuously updated
2.  High performance queries for data applications.
3.  Machine learning at the edge. Empowering edge devices to continuously improve inference

---

📺 View a getting started walkthrough of Spice.ai in action [here] TODO: NEW VIDEO HERE. Should showcase how to create a local spicepod form cloud database (Dremio, Postgress, etc).

## Quikc Start

**Step 1.** Install the SpiceAI CLI:

```bash
curl https://install.spiceai.org | /bin/bash
```

**Step 2.** Navigate to `https://spice.ai/` and create a new account by clicking on Try for Free.

<p align="center">
  <img src="spiceai_try_for_free.png" width="600" />
</p>

**Step 3.** In a new empty directory, login and authenticate from the command line. A pop up browser window will prompt yuou to authenticate:

```bash
spice login
```

**Step 4.** Initiate a new project

```bash
spice init <PROJECT_NAME_HERE>
```

**Step 5** Configure a new datasets. You can select from any publically available dataset listed in Spice.ai: https://docs.spice.ai/building-blocks/datasets

For this example, we'll use eth.recent_logs.

```bash
spice dataset configure
```

You will be prompted to enter a name:
`What is the dataset name? eth_recent_logs`

Enter the location of the dataset:
`Where is your dataset located? spice.ail/eth.recent_logs`

Select `y` when prompted whether you want to accelerate the data:
`Locally accelerate this dataset (y/n)? y`

**Step 6** Start the SpiceAI Runtime

```bash
spice run
```

You should now see the following:

```bash
Spice.ai runtime starting...
Using latest 'local' runtime version.
2024-02-20T23:54:31.313288Z  INFO runtime::http: Spice Runtime HTTP listening on 127.0.0.1:3000
2024-02-20T23:54:31.313347Z  INFO runtime::flight: Spice Runtime Flight listening on 127.0.0.1:50051
2024-02-20T23:54:31.313532Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
2024-02-20T23:54:32.208615Z  INFO runtime: Loaded dataset: eth_recent_logs
2024-02-20T23:54:32.209376Z  INFO runtime::dataconnector: Refreshing data for eth_recent_logs
```

**Step 7** In a seperate terminal window, you can query against the dataset using the spice sql tool:

```bash
spice sql
```

You should now see:

```bash
Welcome to the interactive Spice.ai SQL Query Utility! Type 'help' for help.

show tables; -- list available tables
```

Entering `show tables;` should print out the following table:

```
sql> show tables;
+---------------+--------------------+-----------------+------------+
| table_catalog | table_schema       | table_name      | table_type |
+---------------+--------------------+-----------------+------------+
| datafusion    | public             | eth_recent_logs | BASE TABLE |
| datafusion    | information_schema | tables          | VIEW       |
| datafusion    | information_schema | views           | VIEW       |
| datafusion    | information_schema | columns         | VIEW       |
| datafusion    | information_schema | df_settings     | VIEW       |
+---------------+--------------------+-----------------+------------+
```

You can enter custom queries:

```bash
sql> SELECT topics FROM eth_recent_logs LIMIT 5;
```

### Run through a data demo step by step here. Include a video showing the steps

### Community-Driven Data Components

## Will this feature be available in the future?

### Spicepod registry

Modern developers build with the community by leveraging registries such as npm, NuGet, and pip. The Spice.ai platform includes [spicerack.org](https://spicerack.org), the registry for publically avaialble datasets and machine learning models.

### INSERT EXAMPLE OF A SPICEPOD YAML HERE

As the community shares their ML building blocks (aka Spicepods, or pods for short), developers can quickly add them to their Spice.ai applications enabling them to stream data and upload pre-trained models into their applications quickly and easily.

#### This diagram should be accompanied by a summary or step by step explanation.

<p align="center">
  <img src="https://user-images.githubusercontent.com/80174/132382372-c32cc8b7-25f2-4f82-8f9f-e4778fb69254.png" width="600" />
</p>

### Pre-release software

⚠️ The vision to make it easy to build intelligent applications that learn is a vast undertaking. We haven't figured it all out or solved all the problems yet, so we’re inviting you on this journey and are looking for feedback the direction.

The team at SpiceAI is committed to creating a developer community

Spice.ai and spicerack.org are both pre-release, early, alpha software. Until v1.0, Spice.ai may have gaps, including limited deep learning algorithms, training-at-scale, and simulated environments..

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

## Community

Spice.ai started with the vision to make AI easy for developers. We are building Spice.ai in the open and with the community. Reach out on Discord or by email to get involved. We will be starting a community call series soon!

- Discord: [![Discord Banner](https://discord.com/api/guilds/803820740868571196/widget.png?style=shield)](https://discord.gg/kZnTfneP5u)
- Reddit: [![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
- Twitter: [@SpiceAIHQ](https://twitter.com/spiceaihq)
- Email: [hey@spice.ai](mailto:hey@spice.ai)

### Contributing to Spice.ai

See [CONTRIBUTING.md](/CONTRIBUTING.md).

```

```
