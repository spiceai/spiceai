FROM spark:3.5.6
USER root
WORKDIR /data
RUN apt-get update && apt-get install wget -y && wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc
COPY ./setup_spark.sh /usr/local/bin/setup_spark.sh
RUN /usr/local/bin/setup_spark.sh
RUN chmod +x /usr/local/bin/setup_spark.sh

COPY ./setup_file_hadoop.sh /usr/local/bin/setup_file_hadoop.sh
COPY ./setup_minio_hadoop.sh /usr/local/bin/setup_minio_hadoop.sh
RUN chmod +x /usr/local/bin/setup_file_hadoop.sh
RUN chmod +x /usr/local/bin/setup_minio_hadoop.sh

ENTRYPOINT ["/usr/local/bin/setup_file_hadoop.sh"]