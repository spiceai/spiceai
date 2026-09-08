FROM mysql:latest

ENV MYSQL_ROOT_PASSWORD=root

COPY backup_*.sql /docker-entrypoint-initdb.d/

COPY ./conf.d/my.cnf /etc/mysql/conf.d/my.cnf

EXPOSE 3306