SELECT "UserID", extract(minute FROM to_timestamp("EventTime")::timestamp) AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;
