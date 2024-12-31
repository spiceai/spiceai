SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY from_unixtime("EventTime") LIMIT 10;
