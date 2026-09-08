select AVG("L_TAX"), "L_LINENUMBER", "L_QUANTITY" from oracle_tpch."TPCH_SF1"."LINEITEM" group by "L_LINENUMBER", "L_QUANTITY" order by "L_QUANTITY" desc limit 10;
