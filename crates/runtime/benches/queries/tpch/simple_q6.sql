SELECT * FROM (SELECT o_orderkey + 1 FROM orders) AS c(key) LIMIT 10
