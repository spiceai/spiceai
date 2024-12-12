#!/bin/bash
sleep 30s
echo "Restoring database $DB_NAME"
/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $MSSQL_SA_PASSWORD -d master -Q "RESTORE DATABASE $DB_NAME FROM DISK = '/data/$DB_NAME.bak' WITH REPLACE"