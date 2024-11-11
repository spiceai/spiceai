SELECT MIN(to_timestamp("EventDate" * 86400)), MAX(to_timestamp("EventDate" * 86400)) FROM hits;
