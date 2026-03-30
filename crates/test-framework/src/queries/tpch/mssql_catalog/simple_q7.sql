SELECT * FROM (SELECT o_orderkey FROM mssql.public.orders LIMIT 10) AS c(key) LIMIT 10;
