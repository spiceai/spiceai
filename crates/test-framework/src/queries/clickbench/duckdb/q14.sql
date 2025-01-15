SELECT "SearchPhrase", COUNT(DISTINCT "UserID") AS u FROM hits WHERE CAST("SearchPhrase" AS TEXT) <> '' GROUP BY "SearchPhrase" ORDER BY u DESC LIMIT 10;
