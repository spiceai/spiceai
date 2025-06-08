SELECT * FROM (SELECT o_orderkey + 1 FROM glue.tpch.orders) AS c(key) LIMIT 10;
