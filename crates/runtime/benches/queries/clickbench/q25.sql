SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY to_timestamp("EventTime") LIMIT 10;
