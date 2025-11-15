-- Sender Lookup by Sender Identity Query
-- Returns sender records from sender_info table by SenderIdentity
-- Used for sender identity resolution and validation

SELECT AccountSid, NumberPoolSid, SenderSid, SenderIdentity, Region, Rate, SenderType, LastUsed, DateCreated, DateUpdated
FROM sender_info
WHERE AccountSid = ? 
  AND SenderIdentity = ?
ORDER BY DateCreated ASC, SenderSid ASC
LIMIT 1;