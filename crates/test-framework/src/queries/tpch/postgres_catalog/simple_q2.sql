SELECT l_linenumber FROM pg.public.lineitem WHERE l_linenumber = (SELECT MAX(l_linenumber) FROM pg.public.lineitem);
