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