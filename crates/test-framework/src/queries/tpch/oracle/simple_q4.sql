select AVG("L_TAX"), "L_LINENUMBER", "L_QUANTITY" from lineitem group by "L_LINENUMBER", "L_QUANTITY" order by "L_QUANTITY" desc limit 10;
