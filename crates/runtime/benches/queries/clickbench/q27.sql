SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY to_timestamp_seconds("EventTime"), "SearchPhrase" LIMIT 10;
