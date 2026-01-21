-- Number Lookup by Identity/DID Query
-- Returns number records from number_info table by NumberDid
-- Used for identity-based number resolution

SELECT AccountSid,
  NumberPoolSid,
  NumberSid,
  MaxRate,
  NumberDid,
  NumberType,
  SupportedDestRegion,
  NumberRegion,
  CurrentRate,
  IsAvailable,
  ProviderSid,
  AreaCodeRegion,
  AvailableForNumberSelection
FROM number_info
WHERE AccountSid = ? 
  AND NumberPoolSid = ? 
  AND NumberDid = ?
ORDER BY NumberSid