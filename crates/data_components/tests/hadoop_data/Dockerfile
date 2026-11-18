FROM spark:4.1.4@sha256:1d89b302e585e0de2b1034e50fad2ae6e21d09d6a1682037111654365a56bca3
USER root
WORKDIR /data
RUN apt-get update && apt-get install wget -y && wget https://public-data.spiceai.org/mc.linux-amd64.RELEASE.2025-08-13T08-35-41Z -O /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc
COPY ./setup_spark.sh /usr/local/bin/setup_spark.sh
RUN /usr/local/bin/setup_spark.sh
RUN chmod +x /usr/local/bin/setup_spark.sh

COPY ./setup_file_hadoop.sh /usr/local/bin/setup_file_hadoop.sh
COPY ./setup_minio_hadoop.sh /usr/local/bin/setup_minio_hadoop.sh
RUN chmod +x /usr/local/bin/setup_file_hadoop.sh
RUN chmod +x /usr/local/bin/setup_minio_hadoop.sh

ENTRYPOINT ["/usr/local/bin/setup_file_hadoop.sh"]