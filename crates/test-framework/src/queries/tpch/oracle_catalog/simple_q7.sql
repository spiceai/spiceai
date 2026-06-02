SELECT * FROM (SELECT "O_ORDERKEY" as "key" FROM oracle_tpch."TPCH_SF1"."ORDERS" limit 10) AS c("KEY") limit 10;
