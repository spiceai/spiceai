# Spice.ai

[![build](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test.yml)
[![CodeQL](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml/badge.svg?branch=trunk&event=push)](https://github.com/spiceai/spiceai/actions/workflows/codeql-analysis.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Discord](https://img.shields.io/discord/803820740868571196)](https://discord.gg/kZnTfneP5u)
[![Subreddit subscribers](https://img.shields.io/reddit/subreddit-subscribers/spiceai?style=social)](https://www.reddit.com/r/spiceai)
[![Follow on Twitter](https://img.shields.io/twitter/follow/spiceaihq.svg?style=social&logo=twitter)](https://twitter.com/intent/follow?screen_name=spiceaihq)

## What is SpiceAI?

**Spice.ai** is an open source runtime environment that enables developers to leverage rapid access to time-series data. The Spice AI Runtime enables the best in class high performance queries for powering an array of data driven applications.

⚠️ **DEVELOPER PREVIEW ONLY** Spice.ai is under active **alpha** stage development and is not intended to be used in production until its **1.0-stable** release.

## Quick Start

**Step 1.** Install the SpiceAI CLI:

```bash
curl https://install.spiceai.org | /bin/bash
```

**Step 2** Choose a project name and initialize a new project

```bash
spice init <PROJECT_NAME>
```

This creates a spicepod.yaml file in your directory.

```
version: v1beta1
kind: Spicepod
name: PROJECT_NAME
```

**Step 3** start the SpiceAI runtime

```bash
spice run
```

You should see the following output:

```bash
Spice.ai runtime starting...
Using latest 'local' runtime version.
2024-02-21T06:11:56.381793Z  INFO runtime::http: Spice Runtime HTTP listening on 127.0.0.1:3000
2024-02-21T06:11:56.381853Z  INFO runtime::flight: Spice Runtime Flight listening on 127.0.0.1:50051
2024-02-21T06:11:56.382038Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
```

**Step 4** Add a public pre-defined spicepod.
In a new terminal window, enter the following command:

```bash
spice add <NAME_OF_PUBLIC_SPICEPOD>
```

You should see the runtime updated with the new dataset.
Also, the spicepod.yaml file will be updated with a new dependency that
references the imported spicepod.

```
version: v1beta1
kind: Spicepod
name: PROJECT_NAME
dependencies:
- <SPICEPOD_PATH>
```

**Step 5** You can now query against the dataset using the SpiceAI SQL REPL. Enter the command below:

```bash
spice sql
```

You should now see:

```bash
Welcome to the interactive Spice.ai SQL Query Utility! Type 'help' for help.

show tables; -- list available tables
```

Entering `show tables;` should print out the following table:
TODO: REPLACE TABLE WITH TABLE FROM THE DATASET IN THE PUBLIC SPICEPOD

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
TODO: MODIFY THE QUERY TO REFLECT COLUMNS FROM THE PUBLIC DATASET

```bash
sql> SELECT topics FROM eth_recent_logs LIMIT 5;
```

TODO: ADD VIDEO SHOWING HOW TO RUN THE ABOVE STEPS HERE.

## Next Steps

You can use any number of predefined datasets available from Spice.ai in the SpiceAI Runtime. You can also import your own datasets hosted in Dremio (and Postgress).

A list of publically available datasets from Spice.ai can be found here: https://docs.spice.ai/building-blocks/datasets.

In order access these datasets, you will first need to create an account with Spice.ai by selecting the free tier membership.

Navigate to https://spice.ai/` and create a new account by clicking on Try for Free.

<p align="center">
  <img src="spiceai_try_for_free.png" width="600" />
</p>

After creating an account, you will need to create an app in order to create to an API key.

<p align="center">
  <img src="create_app.png" width="400" />
</p>

You will now be able to access datasets from Spice.ai.

## Importing a public dataset from SpiceAI

**Step 1.** In a new empty directory, login and authenticate from the command line. A pop up browser window will prompt you to authenticate:

```bash
spice login
```

**Step 2.** Initialize a new project

```bash
spice init <PROJECT_NAME_HERE>
```

```bash
spice dataset configure
```

You will be prompted to enter a name:
`What is the dataset name? PROJECT_NAME

Enter the location of the dataset:
`Where is your dataset located? spice.ai/eth.recent_logs`

Select `y` when prompted whether you want to accelerate the data:
`Locally accelerate this dataset (y/n)? y`

**Step 5** Start the SpiceAI Runtime.

```bash
spice run
```

**Step 6** In a new terminal window, use the SpiceAI SQL RPL to query the dataset

```bash
spice sql
```

```bash
sql> SELECT * FROM eth_recent_transactions LIMIT 5;
```

## Importing dataset from dremio using dummie login credentials

**Step 1** Log in to dremio. You can use the following credentials to access the taxi_trips dataset

```bash
spice login dremio -u <USERNAME> -p <PASSWORD>
```

**Step2** We now configure the dataset:

```bash
spice dataset configure
```

We will now be prompted for the name. Enter "taxi_trips"

```bash
What is the dataset name? taxi_trips
```

TODO: Update the location of the dataset.
Specify the location of the dataset as "dremio/datasets.taxi_trips"

```bash
Where is your dataset located? dremio/datasets.taxi_trips
```

Select "y" when prompted whether to locally accelerate the dataset:

```bash
Locally accelerate this dataset (y/n)? y
```

We should now see the following output:

```
Dataset settings written to `datasets/taxi_trips/dataset.yaml`!
```

If our login credentials were entered correctly, the taxi_trips datasets has been loaded into the runtime. You should see the following in the SpiceAI runtime terminal :

```
2024-02-14T18:34:15.174564Z  INFO spiced: Loaded dataset: taxi_trips
2024-02-14T18:34:15.175189Z  INFO runtime::datasource: Refreshing data for taxi_trips
```

**Step3** Run queries against the dataset using the SpiceAI SQL REPL.

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
  [X](https://twitter.com/SpiceAIHQ), and [LinkedIn](https://www.linkedin.com/company/74148478).
- Join our team ([We’re hiring!](https://spice.ai/careers))
- Contribute code or documentation to the project (see [CONTRIBUTING.md](CONTRIBUTING.md)).

⭐️ star this repo! Thank you for your support! 🙏

For a more comprehensive guide, see the full [online documentation](https://docs.spiceai.org/).
