SELECT "SearchPhrase" FROM hits WHERE CAST("SearchPhrase" AS TEXT) <> '' ORDER BY to_timestamp("EventTime"), "SearchPhrase" LIMIT 10;
