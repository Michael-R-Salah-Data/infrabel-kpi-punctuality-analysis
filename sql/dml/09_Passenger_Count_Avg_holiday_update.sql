-- Execute this script within the [infrabel_punctuality_dwh] database

--Update Passenger_Count_Avg in fact.Fact_Punctuality with Sunday values 
--after updating Is_Holiday in dim.Dim_Date.

--Check the number of rows before the update.
SELECT COUNT(*)
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Date AS DD
		ON FP.Date_Service = DD.DateID
WHERE DD.Is_Holiday = 1
	AND FP.Has_Passenger_Count = 1;


--Update Passenger_Count_Avg and verify the number of rows affected.
BEGIN TRANSACTION;
	
WITH SundayValues AS (
	SELECT DISTINCT	FP.SK_Station AS 'SV_SK_Station',
					FP.Passenger_Count_Avg AS 'SV_Passenger_Count'
	FROM fact.Fact_Punctuality AS FP
		INNER JOIN dim.Dim_Date AS DD
			ON FP.Date_Service = DD.DateID
	WHERE DD.DayWeek = 7
	)
UPDATE FP_Target
SET FP_Target.Passenger_Count_Avg = SV.SV_Passenger_Count
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Date AS DDA
		ON FP_Target.Date_Service = DDA.DateID
	INNER JOIN SundayValues AS SV
		ON FP_Target.SK_Station = SV.SV_SK_Station
WHERE DDA.Is_Holiday = 1
	AND FP_Target.Has_Passenger_Count = 1;

SELECT @@ROWCOUNT

--ROLLBACK;
COMMIT;