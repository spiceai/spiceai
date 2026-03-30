SELECT * FROM (SELECT o_orderkey FROM mysql.public.orders LIMIT 10) AS c(key) LIMIT 10;
