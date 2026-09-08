SELECT * FROM (SELECT o_orderkey FROM ducklake.main.orders LIMIT 10) AS c(key) LIMIT 10;
