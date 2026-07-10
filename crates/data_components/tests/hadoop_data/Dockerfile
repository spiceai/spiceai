FROM spark:3.5.6
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