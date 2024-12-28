SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY datetime("EventTime", 'unixepoch') LIMIT 10;
