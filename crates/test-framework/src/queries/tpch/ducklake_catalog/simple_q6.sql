SELECT * FROM (SELECT o_orderkey + 1 FROM ducklake.main.orders) AS c(key) LIMIT 10;
