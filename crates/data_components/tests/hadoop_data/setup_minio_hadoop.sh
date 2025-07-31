#!/bin/bash

mc alias set minio http://minio:9000 admin password
mc mb minio/hadoop
mc anonymous set public minio/hadoop

/opt/spark/bin/spark-shell <<EOF
spark.conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.warehouse", "s3a://hadoop")
spark.conf.set("spark.sql.catalog.hadoop_prod.s3.endpoint", "http://minio:9000")
spark.conf.set("spark.sql.defaultCatalog", "hadoop_prod")

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "admin")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "password")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://minio:9000")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.filesystem", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark.sql("CREATE TABLE test.my_table_1 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO test.my_table_1 VALUES (1, 'foo'), (2, 'bar')")
spark.sql("SELECT * FROM test.my_table_1").show()
spark.sql("CREATE TABLE test.my_table_2 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO test.my_table_2 VALUES (3, 'foo'), (4, 'bar')")
spark.sql("SELECT * FROM test.my_table_2").show()

spark.sql("CREATE TABLE nested.test.my_table_3 (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO nested.test.my_table_3 VALUES (5, 'foo'), (6, 'bar')")
spark.sql("SELECT * FROM nested.test.my_table_3").show()
EOF

echo "done" > /opt/setup_done
tail -f /dev/null