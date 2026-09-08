SELECT "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE CAST("MobilePhoneModel" AS TEXT) <> '' GROUP BY "MobilePhoneModel" ORDER BY u DESC LIMIT 10;
