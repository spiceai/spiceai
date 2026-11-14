#!/bin/bash

echo "Spark Home: $SPARK_HOME"

# Iceberg publishes a Spark runtime per Spark minor (iceberg-spark-runtime-<minor>_2.13), and
# hadoop-aws must match the Hadoop the Spark image ships (3.4.2 in Spark 4.1) with the AWS SDK
# bundle that Hadoop release names. Move all four with the FROM in the Dockerfile.
cd $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar
wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar
