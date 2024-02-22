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

**Step 2.** Choose a project name and initialize a new project with the `spice init` command:

```bash
spice init my_spiceai_project
```

This creates a spicepod.yaml file in your directory.

**Step 3.** Log in to Dremio in order to access the dataset. Use the credentials below:
:

```bash
spice login dremio -u demo -p demo1234
```

**Step 4.** Start the SpiceAI runtime:

```bash
spice run
```

You should see the following output (with timestamps reflecting when you started the runtime):

```bash
Spice.ai runtime starting...
Using latest 'local' runtime version.
2024-02-21T06:11:56.381793Z  INFO runtime::http: Spice Runtime HTTP listening on 127.0.0.1:3000
2024-02-21T06:11:56.381853Z  INFO runtime::flight: Spice Runtime Flight listening on 127.0.0.1:50051
2024-02-21T06:11:56.382038Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
```

**Step 5.** In a new terminal window, import the spicepod. The spicepod contains information about how datasets and other components are configured. You can add public spicepods using `spice add` command:

```bash
spice add spiceai/quickstart
```

You should see the runtime updated with the new datasets.
Also, the spicepod.yaml file will be updated with a new dependency that
references the spiceai/quickstart spicepod.

```yaml
version: v1beta1
kind: Spicepod
name: PROJECT_NAME
dependencies:
  - spiceai/quickstart
```

In the runtime terminal window, you should see that the a new dataset has been added to the runtime:

```bash
2024-02-22T05:53:48.222952Z  INFO runtime: Loaded dataset: taxi_trips
2024-02-22T05:53:48.223101Z  INFO runtime::dataconnector: Refreshing data for taxi_trips
```

**Step 6.** You can now query the dataset using the SpiceAI SQL REPL. Enter the command below to start the REPL:

```bash
spice sql
```

You should now see:

```bash
Welcome to the interactive Spice.ai SQL Query Utility! Type 'help' for help.

show tables; -- list available tables
```

Entering `show tables;` prints out the following table along with the time it took to execute the query:

```
sql> show tables;
+---------------+--------------------+-------------+------------+
| table_catalog | table_schema       | table_name  | table_type |
+---------------+--------------------+-------------+------------+
| datafusion    | public             | taxi_trips  | BASE TABLE |
| datafusion    | information_schema | tables      | VIEW       |
| datafusion    | information_schema | views       | VIEW       |
| datafusion    | information_schema | columns     | VIEW       |
| datafusion    | information_schema | df_settings | VIEW       |
+---------------+--------------------+-------------+------------+

Query took: 0.004728897 seconds
```

You can enter custom queries:

```bash
sql> SELECT trip_distance_mi, fare_amount FROM taxi_trips LIMIT 10;
```

Output:

```bash
+------------------+-------------+
| trip_distance_mi | fare_amount |
+------------------+-------------+
| 1.1              | 7.5         |
| 6.1              | 23.0        |
| 0.6              | 4.5         |
| 16.7             | 52.0        |
| 11.3             | 37.5        |
| 1.1              | 6.0         |
| 5.3              | 18.5        |
| 1.3              | 7.0         |
| 1.0              | 7.0         |
| 3.5              | 17.5        |
+------------------+-------------+

Query took: 0.002458976 seconds
```

TODO: ADD VIDEO SHOWING HOW TO RUN THE ABOVE STEPS HERE.

## Next Steps

You can use any number of predefined datasets available from Spice.ai in the SpiceAI Runtime.

A list of publically available datasets from Spice.ai can be found here: https://docs.spice.ai/building-blocks/datasets.

In order to access public datasets from SpiceAI, you will first need to create an account with Spice.ai by selecting the free tier membership.

Navigate to https://spice.ai/ and create a new account by clicking on Try for Free.

<p align="center">
  <img src="spiceai_try_for_free.png" width="600" />
</p>

After creating an account, you will need to create an app in order to create to an API key.

<p align="center">
  <img src="create_app.png" width="400" />
</p>

You will now be able to access datasets from Spice.ai. For this demonstration, we will be using the Spice.ai/eth.recent_blocks dataset.

**Step 1.** In a new directory, log in and authenticate from the command line using the `spice login` command. A pop up browser window will prompt you to authenticate:

```bash
spice login
```

**Step 2.** Initialize a new project if you haven't already done so. Then, start the runtime:

```bash
spice init my_spiceai_project
```

```bash
spice run
```

**Step 3.** Configure the dataset:

In a new terminal window, configure a new dataset using the `spice dataset configure` command:

```bash
spice dataset configure
```

You will be prompted to enter a name. Enter a name that represents the contents of the dataset

```bash
What is the dataset name? eth_recent_blocks
```

Enter the location of the dataset:

```bash
Where is your dataset located? spice.ai/eth.recent_blocks
```

Select `y` when prompted whether to accelerate the data:

```bash
Locally accelerate this dataset (y/n)? y
```

You should see the following output from your runtime terminal:

```bash
2024-02-21T22:49:10.038461Z  INFO runtime: Loaded dataset: eth_recent_blocks
```

**Step 4.** In a new terminal window, use the SpiceAI SQL REPL to query the dataset

```bash
spice sql
```

```bash
sql> select number, size, gas_used from eth_recent_blocks Limit 10;
```

The output displays the results of the query along with the query execution time:

```bash
+----------+--------+----------+
| number   | size   | gas_used |
+----------+--------+----------+
| 19281345 | 400378 | 16150051 |
| 19281344 | 200501 | 16480224 |
| 19281343 | 97758  | 12605531 |
| 19281342 | 89629  | 12035385 |
| 19281341 | 133649 | 13335719 |
| 19281340 | 307584 | 18389159 |
| 19281339 | 89233  | 13391332 |
| 19281338 | 75250  | 12806684 |
| 19281337 | 100721 | 11823522 |
| 19281336 | 150137 | 13418403 |
+----------+--------+----------+

Query took: 0.004057791 seconds
```

You can experiment with the time it takes to generate queries when using non-accelerated datasets. You can change the acceleration setting from `true` to `false` in the datasets.yaml file.

## Importing dataset from Dremio

**Step 1.** If you have a dataset hosted in Dremio, you can load it into the SpiceAI Runtime as follows:

```bash
spice login dremio -u <USERNAME> -p <PASSWORD>
```

**Step 2.** If you haven't already initialized a new project, you need to do so. Then, start the SpiceAI Runtime.

```bash
spice init dremio-demo-project
```

```bash
spice run
```

**Step 3.** We now configure the dataset from Dremio:

```bash
spice dataset configure
```

Enter the name of the dataset:

```bash
What is the dataset name? my_dataset
```

Specify the location of the dataset:

```bash
Where is your dataset located? dremio/datasets.my_dataset
```

Select "y" when prompted whether to locally accelerate the dataset:

```bash
Locally accelerate this dataset (y/n)? y
```

We should now see the following output:

```
Dataset settings written to `datasets/my_dataset/dataset.yaml`!
```

If the login credentials were entered correctly, your dataset will have loaded into the runtime. You should see the following in the SpiceAI runtime terminal :

```
2024-02-14T18:34:15.174564Z  INFO spiced: Loaded dataset: my_dataset
2024-02-14T18:34:15.175189Z  INFO runtime::datasource: Refreshing data for my_dataset
```

**Step 4.** Run queries against the dataset using the SpiceAI SQL REPL.

In a new terminal, start the SpiceAI SQL REPL

```bash
spice sql
```

You can now now query `my_dataset` in the runtime.

## Sample project using the SpiceAI runtime.

TODO: Make a simple app that showcases the data querying from the runtime.
This will replace the Python script in the examples folder.

### Upcoming Features

🚀 See the [Roadmap to v1.0-stable](https://github.com/spiceai/spiceai/blob/rust/docs/RELEASE.md) for upcoming features.

### Connect with us!

We greatly appreciate and value your support! You can help Spice.ai in a number of ways:

- Build an app with Spice.ai and send us feedback and suggestions at [hey@spice.ai](mailto:hey@spice.ai) or on [Discord](https://discord.gg/kZnTfneP5u), [X] (https://twitter.com/SpiceAIHQ), or [LinkedIn](https://www.linkedin.com/company/74148478).
- [File an issue](https://github.com/spiceai/spiceai/issues/new) if you see something not quite working correctly.
- Join our team ([We’re hiring!](https://spice.ai/careers))
- Contribute code or documentation to the project (see [CONTRIBUTING.md](CONTRIBUTING.md)).

⭐️ star this repo! Thank you for your support! 🙏

For a more comprehensive guide, see the full [online documentation](https://docs.spiceai.org/).
