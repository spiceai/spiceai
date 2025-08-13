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