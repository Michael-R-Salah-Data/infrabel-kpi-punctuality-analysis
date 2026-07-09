-- Execute this script within the [infrabel_punctuality_dwh] database

--Create two empty columns
ALTER TABLE fact.Fact_Punctuality 
ADD	Passenger_Delay_Arr DECIMAL(18,2) NULL,
	Passenger_Delay_Dep DECIMAL(18,2) NULL;

--Update Passenge_Delay_Arr
---------------------------
--Check the number of rows before the update
SELECT COUNT(*)
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Date AS DDA
		ON FP_Target.Date_Service = DDA.DateID
WHERE FP_Target.Delay_Arr IS NOT NULL;

--Update the two new columns
BEGIN TRANSACTION; 

WITH Trains_Per_Station_Per_Day AS (
	SELECT	FP.SK_Station,
			DD.DayWeek,
			COUNT(*) AS 'No_Trains'
	FROM fact.Fact_Punctuality AS FP
		LEFT JOIN dim.Dim_Date AS DD
			ON FP.Date_Service = DD.DateID
	WHERE FP.Date_Service IN	(20241005, 
								20241006, 
								20241007, 
								20241008,
								20241009, 
								20241010, 
								20241011, 
								20241012, 
								20241013)
	GROUP BY FP.SK_Station, DD.DayWeek
	)
UPDATE FP_Target
SET	FP_Target.Passenger_Delay_Arr =	CAST(
										ROUND(
											CAST(FP_Target.Weighted_Delay_Arr AS DECIMAL(18,2)) / CAST(TPS.No_Trains AS DECIMAL(18,2)), 
											2)
									AS DECIMAL(18,2))
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Date AS DDA
		ON FP_Target.Date_Service = DDA.DateID
	LEFT JOIN Trains_Per_Station_Per_Day AS TPS
		ON FP_Target.SK_Station = TPS.SK_Station
		AND DDA.DayWeek = TPS.DayWeek
WHERE FP_Target.Delay_Arr IS NOT NULL;

SELECT @@ROWCOUNT;

--ROLLBACK;
COMMIT;


--Update Passenger_Delay_Dep
----------------------------
--Check the number of rows before the update
SELECT COUNT(*)
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Date AS DDA
		ON FP_Target.Date_Service = DDA.DateID
WHERE FP_Target.Delay_Dep IS NOT NULL;

--Update the two new columns
BEGIN TRANSACTION; 

WITH Trains_Per_Station_Per_Day AS (
	SELECT	FP.SK_Station,
			DD.DayWeek,
			COUNT(*) AS 'No_Trains'
	FROM fact.Fact_Punctuality AS FP
		LEFT JOIN dim.Dim_Date AS DD
			ON FP.Date_Service = DD.DateID
	WHERE FP.Date_Service IN	(20241005, 
								20241006, 
								20241007, 
								20241008,
								20241009, 
								20241010, 
								20241011, 
								20241012, 
								20241013)
	GROUP BY FP.SK_Station, DD.DayWeek
	)
UPDATE FP_Target
SET	FP_Target.Passenger_Delay_Dep =	CAST(
										ROUND(
											CAST(FP_Target.Weighted_Delay_Dep AS DECIMAL(18,2)) / CAST(TPS.No_Trains AS DECIMAL(18,2)), 
											2)
									AS DECIMAL(18,2))
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Date AS DDA
		ON FP_Target.Date_Service = DDA.DateID
	LEFT JOIN Trains_Per_Station_Per_Day AS TPS
		ON FP_Target.SK_Station = TPS.SK_Station
		AND DDA.DayWeek = TPS.DayWeek
WHERE FP_Target.Delay_Dep IS NOT NULL;

SELECT @@ROWCOUNT;

--ROLLBACK;
COMMIT;