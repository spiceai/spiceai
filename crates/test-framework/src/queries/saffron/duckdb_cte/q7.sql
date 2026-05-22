-- Alpha Sender IDs by Region and Capability Query
-- Returns Alpha Sender IDs (NumberType='as') filtered by region and capability
-- Used for international messaging sender selection

SELECT A.* FROM number_info_with_cap AS A
WHERE A.NumberPoolSid = ? 
  AND A.AccountSid = ? 
  AND (A.SupportedDestRegion = '00' OR A.SupportedDestRegion = ?)
  AND (A.NumberRegion = ? OR A.NumberRegion IS NULL)
  AND A.NumberSid NOT IN (?)
  AND A.NumberType = 'as'
  AND A.Capability = ?
LIMIT ?;