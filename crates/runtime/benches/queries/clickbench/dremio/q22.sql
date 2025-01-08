SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM hits WHERE CAST("URL" AS VARCHAR) LIKE '%google%' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10;
