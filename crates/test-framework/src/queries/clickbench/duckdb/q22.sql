SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM hits WHERE "URL" LIKE '%google%' AND CAST("SearchPhrase" AS TEXT) <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;
