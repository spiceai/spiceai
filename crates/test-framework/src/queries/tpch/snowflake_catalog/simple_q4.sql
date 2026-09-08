select AVG("L_TAX"), "L_LINENUMBER", "L_QUANTITY" from snowflake.TPCH_SF1.LINEITEM group by "L_LINENUMBER", "L_QUANTITY" order by "L_QUANTITY" desc limit 10;
