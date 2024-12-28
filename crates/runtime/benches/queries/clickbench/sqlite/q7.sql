SELECT MIN(date("EventTime", 'unixepoch')), MAX(date("EventTime", 'unixepoch')) FROM hits;
