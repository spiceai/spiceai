SELECT "SearchPhrase" FROM hits WHERE CAST("SearchPhrase" AS TEXT) <> '' ORDER BY "SearchPhrase" LIMIT 10;
