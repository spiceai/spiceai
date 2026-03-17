SELECT l_linenumber FROM mysql.public.lineitem WHERE l_linenumber = (SELECT MAX(l_linenumber) FROM mysql.public.lineitem);
