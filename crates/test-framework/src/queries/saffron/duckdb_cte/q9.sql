-- Sender Selection by Sender Type Query
-- Returns senders from sender_info table filtered by account, pool, and sender type
-- Uses offset-based selection for random-like behavior

SELECT AccountSid, NumberPoolSid, SenderSid, SenderIdentity, Region, Rate, SenderType, LastUsed, DateCreated, DateUpdated
FROM sender_info
WHERE AccountSid = ? 
  AND NumberPoolSid = ? 
  AND SenderType = ?
LIMIT 1 OFFSET ?;