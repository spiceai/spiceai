SELECT * FROM (SELECT o_orderkey FROM orders LIMIT 10) AS c(key) LIMIT 10;
