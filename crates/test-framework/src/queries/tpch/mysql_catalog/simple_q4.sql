select AVG(l_tax), l_linenumber, l_quantity from mysql.public.lineitem group by l_linenumber, l_quantity order by l_quantity desc limit 10;
