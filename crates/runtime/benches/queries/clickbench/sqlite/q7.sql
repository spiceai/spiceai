SELECT MIN(date_trunc('day', from_unixtime("EventTime"))), MAX(date_trunc('day', from_unixtime("EventTime"))) FROM hits;
