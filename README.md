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

## Quikc Start

**Step 1.** Install the SpiceAI CLI:

```bash
curl https://install.spiceai.org | /bin/bash
```

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

TODO: ADD VIDEO SHOWING HOW TO RUN THE ABOVE STEPS HERE.

## Importing a predefined set of datasets using Spicepods

TODO: Use `spice add <public_spicd_pod>`

## Importing a public dataset from SpiceAI

TODO: Use `spice dataset configure` for a web3 dataset from spiceAI.

**Step 1** Configure a public datasets from spice.ai. You can select from any publically available dataset listed here: https://docs.spice.ai/building-blocks/datasets. We will use the eth.recent_logs dataset, but you can choose any available dataset from the list.

In order access these datasets, you will first need to create an account.

**Step 1.** Navigate to `https://spice.ai/` and create a new account by clicking on Try for Free.

<p align="center">
  <img src="spiceai_try_for_free.png" width="600" />
</p>

**Step 2.** In a new empty directory, login and authenticate from the command line. A pop up browser window will prompt you to authenticate:

```bash
spice login
```

**Step 3.** Initiate a new project

```bash
spice init <PROJECT_NAME_HERE>
```

```bash
spice dataset configure
```

You will be prompted to enter a name:
`What is the dataset name? eth_recent_logs`

Enter the location of the dataset:
`Where is your dataset located? spice.ail/eth.recent_logs`

Select `y` when prompted whether you want to accelerate the data:
`Locally accelerate this dataset (y/n)? y`

## Importing dataset from dremio using dummie login credentials

TODO: Use `spice dataset configure` for a Dremio dataset.

## Sample project using the SpiceAI runtime.

TODO: Make a simple app that showcases the data querying from the runtime.

#### This diagram should be accompanied by a summary or step by step explanation.

<p align="center">
  <img src="https://user-images.githubusercontent.com/80174/132382372-c32cc8b7-25f2-4f82-8f9f-e4778fb69254.png" width="600" />
</p>

### Upcoming Features

🚀 See the [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/rust/docs/RELEASE.md) for upcoming features.

### Connect with us!

We greatly appreciate and value your support! You can help Spice.ai in a number of ways:

- Build an app with Spice.ai and send us feedback and suggestions at [hey@spice.ai](mailto:hey@spice.ai) or on [Discord](https://discord.gg/kZnTfneP5u).
- [File an issue](https://github.com/spiceai/spiceai/issues/new) if you see something not quite working correctly.
  [Twitter](https://twitter.com/SpiceAIHQ), and [LinkedIn](https://www.linkedin.com/company/74148478).
- Join our team ([We’re hiring!](https://spice.ai/careers))
- Contribute code or documentation to the project (see [CONTRIBUTING.md](CONTRIBUTING.md)).

⭐️ star this repo! Thank you for your support! 🙏

For a more comprehensive guide, see the full [online documentation](https://docs.spiceai.org/).
