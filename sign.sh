codesign --remove-signature ~/.spice/bin/spiced

cat > entitlements.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?><plist version="1.0"><dict>
  <key>com.apple.security.get-task-allow</key><true/>
</dict></plist>
PLIST

codesign --force --timestamp --options runtime \
  --entitlements entitlements.plist -s - ~/.spice/bin/spiced


codesign --remove-signature ~/.spice/bin/spiced_1_8_0

codesign --force --timestamp --options runtime \
  --entitlements entitlements.plist -s - ~/.spice/bin/spiced_1_8_0


# xcrun xctrace record \
#   --template 'Leaks' \
#   --launch ~/.spice/bin/spiced \
#   --env MallocStackLogging=1 \
#   --env MallocStackLoggingNoCompact=1 \
#   --output /tmp/spiced-leaks.trace