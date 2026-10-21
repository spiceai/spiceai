SELECT * FROM (SELECT o_orderkey + 1 FROM mysql.tpch_sf1.orders) AS c(key) LIMIT 10;
