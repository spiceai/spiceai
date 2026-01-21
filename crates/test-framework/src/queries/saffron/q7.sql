-- Alpha Sender IDs by Region and Capability Query
-- Returns Alpha Sender IDs filtered by region, number type, and capability
-- Used for international messaging sender selection

SELECT A.DateCreated, A.DateUpdated, A.AccountSid, A.NumberPoolSid, A.NumberSid, 
  A.MaxRate, A.NumberDid, A.NumberType, A.SupportedDestRegion, A.NumberRegion, 
  A.CurrentRate, A.IsAvailable, A.ProviderSid, A.AreaCodeRegion, 
  A.AvailableForNumberSelection, B.Capability 
FROM number_info AS A
INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid
WHERE A.NumberPoolSid = ? 
  AND A.AccountSid = ? 
  AND (A.SupportedDestRegion = '00' OR A.SupportedDestRegion = ?)
  AND (A.NumberRegion = ? OR A.NumberRegion IS NULL)
  AND A.NumberSid NOT IN (?)
  AND A.NumberType = ?
  AND B.Capability = ?
ORDER BY A.NumberSid, B.Capability
LIMIT ?;