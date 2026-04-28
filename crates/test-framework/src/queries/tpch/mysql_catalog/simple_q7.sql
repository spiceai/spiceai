SELECT * FROM (SELECT o_orderkey FROM mysql.tpch_sf1.orders LIMIT 10) AS c(key) LIMIT 10;
