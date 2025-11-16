-- Sender Selection by Sender Type Query (Views)
-- Returns senders from sender_info table filtered by account, pool, and sender type
-- Uses offset-based selection for random-like behavior

SELECT AccountSid, NumberPoolSid, SenderSid, SenderIdentity, Region, Rate, SenderType, LastUsed, DateCreated, DateUpdated
FROM sender_info
WHERE AccountSid = ? 
  AND NumberPoolSid = ? 
  AND SenderType = ?
ORDER BY SenderSid
LIMIT 1 OFFSET ?;