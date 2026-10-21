SELECT * FROM (SELECT "O_ORDERKEY" as "key" FROM snowflake.TPCH_SF1.ORDERS limit 10) AS c("KEY") limit 10;
