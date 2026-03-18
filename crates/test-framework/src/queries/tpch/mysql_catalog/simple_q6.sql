SELECT * FROM (SELECT o_orderkey + 1 FROM mysql.public.orders) AS c(key) LIMIT 10;
