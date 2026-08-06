# Hadoop Test Data

* Install Spark 3.5.6 with Hadoop 3.3
* Run `./setup_spark.sh` with the `SPARK_HOME` variable defined, like `/opt/spark`. This downloads the required JARs from Maven to setup Hadoop catalogs on `file://` and `s3a://` (MinIO).

The configured warehouse for each source is the same, with 2 namespaces:

* `test`: Setup with 2 tables, `my_table_1` and `my_table_2`. Each table contains 2 rows:
    * `my_table_1`:
        ```console
+---+----+
| id|name|
+---+----+
|  1| foo|
|  2| bar|
+---+----+
        ```
    * `my_table_2`:
        ```console
+---+----+
| id|name|
+---+----+
|  3| foo|
|  4| bar|
+---+----+
        ```
* `nested.test`: Setup with 1 table, `my_table_3`. The table contains 2 rows:
    * `my_table_2`:
        ```console
+---+----+
| id|name|
+---+----+
|  5| foo|
|  6| bar|
+---+----+
        ```

The `setup_file_hadoop.sh` and `setup_minio_hadoop.sh` files setup each respective catalog, which is used in the `Dockerfile` to build the testing image.

`setup_file_hadoop.sh` is configured to call `setup_minio_hadoop.sh`, and expects to be used within the Dockerfile image.

## Running `hadoop_catalog_test`

The test builds one catalog per backend and asserts the same expectations against each. Which
backends it builds is decided by the environment, so no cargo feature has to be enabled:

* `MINIO_ENDPOINT` (**required**) — the MinIO holding the seeded `hadoop` bucket. It drives the
  `s3a` and `s3-to-s3a-inferred` catalogs. The value depends on where the test process runs, because
  `docker-compose.yml` only `expose`s port 9000 on the compose network rather than publishing it:
  * **From the host** — publish the port first (add a `ports: ["9000:9000"]` mapping, or run
    `docker compose run --publish 9000:9000 …`), then use `http://127.0.0.1:9000`. Without a
    published port the container address is not routable from the host.
  * **From another container on the compose network** — use the service name:
    `http://minio:9000`. No port publishing is needed.

  `MINIO_ACCESS_KEY_ID` and `MINIO_SECRET_ACCESS_KEY` default to `admin` / `password`, matching
  `docker-compose.yml`.
* `HADOOP_FILE_WAREHOUSE_ROOT` (optional) — a warehouse directory on the local filesystem, as
  produced by `setup_file_hadoop.sh` (`/tmp/hadoop_warehouse` by default). Set it to also run the
  `file` catalog; leave it unset to run the MinIO-backed catalogs alone. It must be an **absolute**
  directory path — the test converts it to a `file:` URL and panics on a relative one.

Run it from the host, against a published MinIO port:

```bash
MINIO_ENDPOINT=http://127.0.0.1:9000 \
  cargo nextest run -p data_components --test hadoop_catalog_test
```

Or from a container attached to the compose network:

```bash
MINIO_ENDPOINT=http://minio:9000 \
  cargo nextest run -p data_components --test hadoop_catalog_test
```

## Importing TPCH

This example shows how to import a TPCH dataset from CSV into Iceberg tables under Hadoop, on the local filesystem:

```scala
spark.conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.warehouse", "file:///tmp/hadoop_warehouse")

val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./lineitem.csv")
csv_df.writeTo("hadoop_prod.tpch.lineitem").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./customer.csv")
csv_df.writeTo("hadoop_prod.tpch.customer").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./orders.csv")
csv_df.writeTo("hadoop_prod.tpch.orders").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./supplier.csv")
csv_df.writeTo("hadoop_prod.tpch.supplier").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./part.csv")
csv_df.writeTo("hadoop_prod.tpch.part").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./partsupp.csv")
csv_df.writeTo("hadoop_prod.tpch.partsupp").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./nation.csv")
csv_df.writeTo("hadoop_prod.tpch.nation").using("iceberg").create()
val csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv("./region.csv")
csv_df.writeTo("hadoop_prod.tpch.region").using("iceberg").create()
```