SELECT * FROM hits WHERE CAST("URL" AS VARCHAR) LIKE '%google%' ORDER BY to_timestamp("EventTime") LIMIT 10;
