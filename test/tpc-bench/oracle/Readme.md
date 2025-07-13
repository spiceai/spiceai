
1. create schema usign sqlplus:
@setup_tpch.sql

2. Prepare export data. 
 - Generate or obtain DuckDB tpch database: `tpch.db`
 - Export data as csv:

```
for table in customer lineitem nation orders part partsupp region supplier; do
  duckdb tpch.db "COPY $table TO '${table}.csv' (HEADER, DELIMITER ',');"
done
```

3. Load data using `sqlldr`

```
sqlldr admin/password_replace_me@connection_string_replace_me control=customer.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=orders.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=lineitem.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=part.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=partsupp.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=nation.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=region.ctl direct=true
sqlldr admin/password_replace_me@connection_string_replace_me control=supplier.ctl direct=true
```

or 

```
USER="admin"
PASS="password_replace_me"
CONNECT_STRING="connection_string_replace_me"

for table in customer orders lineitem part partsupp nation region supplier; do
  echo "Loading $table as ADMIN into TPCH_SF1.$table..."
  sqlldr "${USER}/${PASS}@${CONNECT_STRING}" control="${table}.ctl" direct=true
done
```