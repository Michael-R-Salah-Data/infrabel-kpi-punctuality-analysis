-- Execute this script within the [infrabel_punctuality_dwh] database

-- This script updates the Is_Holiday column for the 7 Belgian holidays with fixed dates across seven years (2020 to 2026),
-- and for the 3 Belgian holidays with variable dates across four years (2023 to 2026).
-- Therefore, 61 rows are expected to be updated:  
-- (7 holidays * 7 years) + (3 holidays * 4 years) = 49 + 12 = 61


SELECT DISTINCT [YEAR]
FROM dim.Dim_Date
ORDER BY [YEAR];


SELECT COUNT(*) AS 'No_Holidays_Before_Update'
FROM dim.Dim_Date
WHERE Is_Holiday = 1;


UPDATE dim.Dim_Date SET Is_Holiday = 1 
WHERE ([Month] = 1 AND [Day] = 1)
	OR ([Month] = 5 AND [Day] = 1)
	OR ([Month] = 7 AND [Day] = 21)
	OR ([Month] = 8 AND [Day] = 15)
	OR ([Month] = 11 AND [Day] IN (1, 11))
	OR ([Month] = 12 AND [Day] = 25);


UPDATE dim.Dim_Date SET Is_Holiday = 1
WHERE ([YEAR] = 2026)
	AND (
		([Month] = 4 AND [Day] = 6)
			OR ([Month] = 5 AND [Day] IN (14, 25))
		);

UPDATE dim.Dim_Date SET Is_Holiday = 1
WHERE ([YEAR] = 2025)
	AND (
		([Month] = 4 AND [Day] = 21)
			OR ([Month] = 5 AND [Day] = 29)
			OR ([Month] = 6 and [Day] = 9)
		);

UPDATE dim.Dim_Date SET Is_Holiday = 1
WHERE ([YEAR] = 2024)
	AND (
		([Month] = 4 AND [Day] = 1)
			OR ([Month] = 5 AND [Day] IN (9, 20))
		);

UPDATE dim.Dim_Date SET Is_Holiday = 1
WHERE ([YEAR] = 2023)
	AND (
		([Month] = 4 AND [Day] = 10)
			OR ([Month] = 5 AND [Day] IN (18, 29))
		);


SELECT COUNT(*) AS 'No_Holidays_After_Update'
FROM dim.Dim_Date
WHERE Is_Holiday = 1;
