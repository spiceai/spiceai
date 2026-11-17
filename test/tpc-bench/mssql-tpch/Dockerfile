FROM mcr.microsoft.com/mssql/server:2022-CU27-GDR2-ubuntu-22.04@sha256:11927f5edc5122658e90b3a80ee36e8aef779310bf771415c1d89271859c73fd
ENV ACCEPT_EULA=Y
ENV MSSQL_SA_PASSWORD=S3cretP@ssw0rd
ENV DB_NAME=tpch

# restore from backup
USER root

RUN mkdir -p /data & \
    mkdir -p /srv

COPY ./data /data
COPY ./setup.sh /srv/setup.sh
COPY ./entrypoint.sh /srv/entrypoint.sh

RUN chmod 755 -R /data & \
    chmod 755 -R /srv & \
    chmod +x /srv/setup.sh & \
    chmod +x /srv/entrypoint.sh

USER mssql

CMD ["/bin/bash", "/srv/entrypoint.sh"]