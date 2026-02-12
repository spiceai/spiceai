-- Sender Selection by Sender Type and Region Query (Views)
-- Returns senders from sender_info table filtered by account, pool, sender type, and region
-- Used for regional sender routing

SELECT AccountSid, NumberPoolSid, SenderSid, SenderIdentity, Region, Rate, SenderType, LastUsed, DateCreated, DateUpdated
FROM sender_info
WHERE AccountSid = ? 
  AND NumberPoolSid = ? 
  AND SenderType = ? 
  AND Region = ?
ORDER BY SenderSid
LIMIT 1 OFFSET ?;