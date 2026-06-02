SELECT * FROM (SELECT o_orderkey FROM mssql.dbo.orders LIMIT 10) AS c(key) LIMIT 10;
