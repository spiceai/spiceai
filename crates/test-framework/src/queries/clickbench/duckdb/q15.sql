SELECT "SearchEngineID", "SearchPhrase", COUNT(*) AS c FROM hits WHERE CAST("SearchPhrase" AS TEXT) <> '' GROUP BY "SearchEngineID", "SearchPhrase" ORDER BY c DESC LIMIT 10;
