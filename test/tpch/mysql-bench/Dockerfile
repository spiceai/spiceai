FROM mysql:latest

ENV MYSQL_ROOT_PASSWORD=root

COPY backup_*.sql /docker-entrypoint-initdb.d/

EXPOSE 3306