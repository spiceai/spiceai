#!/bin/bash

/opt/spark/bin/spark-shell <<EOF
spark.conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.warehouse", "file:///tmp/hadoop_warehouse")

spark.sql("CREATE NAMESPACE hadoop_prod.test")
spark.sql("CREATE TABLE hadoop_prod.test.my_table_1 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.test.my_table_1 VALUES (1, 'foo'), (2, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.test.my_table_1").show()
spark.sql("CREATE TABLE hadoop_prod.test.my_table_2 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.test.my_table_2 VALUES (3, 'foo'), (4, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.test.my_table_2").show()

spark.sql("CREATE NAMESPACE hadoop_prod.nested.test")
spark.sql("CREATE TABLE hadoop_prod.nested.test.my_table_3 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO hadoop_prod.nested.test.my_table_3 VALUES (5, 'foo'), (6, 'bar')")
spark.sql("SELECT * FROM hadoop_prod.nested.test.my_table_3").show()
EOF

/usr/local/bin/setup_minio_hadoop.sh