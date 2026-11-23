FROM mysql:26.10.0@sha256:3819b2b23835c4a73c8b022fafc33c8095c0f81c929a8b7a84613d2fba6c52f5

ENV MYSQL_ROOT_PASSWORD=root

COPY backup_*.sql /docker-entrypoint-initdb.d/

COPY ./conf.d/my.cnf /etc/mysql/conf.d/my.cnf

EXPOSE 3306