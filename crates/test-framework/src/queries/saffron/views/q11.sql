-- Number Lookup by Identity/DID Query (Views)
-- Returns number records from number_info_with_cap view by NumberDid
-- Used for identity-based number resolution

SELECT DISTINCT 
  AccountSid,
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
FROM number_info_with_cap
WHERE AccountSid = ?
  AND NumberPoolSid = ?
  AND NumberDid = ?
ORDER BY NumberSid