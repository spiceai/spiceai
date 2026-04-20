-- Alpha Sender IDs by Region and Capability Query (Views)
-- Returns Alpha Sender IDs filtered by region, number type, and capability
-- Used for international messaging sender selection

SELECT A.* FROM number_info_with_cap AS A
WHERE A.NumberPoolSid = ? 
  AND A.AccountSid = ? 
  AND (A.SupportedDestRegion = '00' OR A.SupportedDestRegion = ?)
  AND (A.NumberRegion = ? OR A.NumberRegion IS NULL)
  AND A.NumberSid NOT IN (?)
  AND A.NumberType = ?
  AND A.Capability = ?
ORDER BY A.NumberSid, A.Capability
LIMIT ?;