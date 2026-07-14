--Execute this script within the [infrabel_punctuality_dwh] database


--Explore 'L' without direction
-----------------------------
--There are 31.022 entries for 'L' train services without direction. 
--They span from 03 February 2024 to 12 December 2025.
--They have stopped at stations on different local train services (Liege, Brussels...) 
--and two international train services (WIEN -> BRUSSEL-ZUID and BRUSSEL-ZUID -> WIEN).
SELECT	PA.SK_Train_Service,
		DS.Station_Name_French,
		PA.Date_Service,
		PA.Train_No,
		DH.FullTime,
		CAST(ROUND(PA.Delay_Arr, 2) AS DECIMAL(18,2)) AS 'Delay_Arr'
FROM vw.Punctuality_Arrivals AS PA
	INNER JOIN dim.Dim_Train_Service AS DT
		ON PA.SK_Train_Service = DT.SK_Train_Service
	LEFT JOIN dim.Dim_Station AS DS
		ON PA.SK_Station = DS.SK_Station
	LEFT JOIN dim.Dim_Time AS DH
		ON PA.Real_Time_Arr = DH.TimeKey
WHERE DT.Relation_Direction IN ('L')
ORDER BY PA.Date_Service DESC;

--Temporal patterns for 'L' train service without direction in view Punctuality_Arrivals
SELECT	PA.Date_Service,
		COUNT(*) AS 'No_Arrivals'
FROM vw.Punctuality_Arrivals AS PA
	INNER JOIN dim.Dim_Train_Service AS DT
		ON PA.SK_Train_Service = DT.SK_Train_Service
WHERE DT.Relation_Direction IN ('L')
GROUP BY PA.Date_Service
ORDER BY PA.Date_Service DESC;

--There are 51 distinct train numbers for 'L' train service without direction. 
--The two requests below identify four alternative directions for this 'L' train service: 
--'INT: WIEN HBF -> BRUSSEL-ZUID' 
--'INT: BRUSSEL-ZUID -> WIEN HBF' 
--'S7: HALLE -> MECHELEN' 
--'S7: MECHELEN -> HALLE' 
SELECT DISTINCT FP.Train_No AS 'L_Train_No',
				COUNT(FP.Train_No) AS 'No_Rows'
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE DT.Relation_Direction IN ('L')
GROUP BY FP.Train_No
ORDER BY No_Rows DESC;

	--Identify the SK_Train_Service values matching the 'L' Relation_Direction.
SELECT DISTINCT		FP.SK_Train_Service,
					DT.Relation_Direction
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE DT.Relation_Direction IN ('L');


--Identify alternative directions to 'L' direction grouping arrivals at stations by train number
-------------------------------------------------------------------------------------------------
--Four alternative relation directions have been found: 
--'INT: WIEN HBF -> BRUSSEL-ZUID' (SK_Train_Service 155) for Train_No 468, with 1050 Fact_Punctuality rows matched to incorrect 'L' direction.
--'INT: BRUSSEL-ZUID -> WIEN HBF' (SK_Train_Service 150) for Train_No 469, with 1125 Fact_Punctuality rows matched to incorrect 'L' direction.
--'S7: HALLE -> MECHELEN' (SK_Train_Service 498) for Train_No 3156 -> 3171, with 10,699 Fact_Punctuality rows matched to incomplete 'L' direction.
--'S7: MECHELEN -> HALLE' (SK_Train_Service ) for Train_No 3176 -> 3191, with 10,693 Fact_Punctuality rows matched to incomplete 'L' direction.

SELECT		DT.Relation_Direction,
			FP.Train_No,
			COUNT(*) AS 'No_Arrivals'
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							INNER JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE DT.Relation_Direction IN ('L'))
GROUP BY DT.Relation_Direction, FP.Train_No
ORDER BY FP.Train_No;


--'INT: WIEN HBF -> BRUSSEL-ZUID'
---------------------------------
--Train_No matching both 'L' and 'INT: WIEN HBF -> BRUSSEL-ZUID' directions = 468.
--'INT: WIEN HBF -> BRUSSEL-ZUID' SK_Train_Service = 155.
SELECT DISTINCT	DT.Relation_Direction,
				FP.SK_Train_Service,
				FP.Train_No
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							INNER JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE DT.Relation_Direction IN ('L'))
	OR DT.Relation_Direction LIKE ('INT: WIEN HBF -> BRUSSEL-ZUID')
ORDER BY FP.Train_No;

--Number of rows matching both 'L' and 'INT: WIEN HBF -> BRUSSEL-ZUID' directions: 1050.
SELECT COUNT(*) AS 'No_Arrivals'
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No = 468)
	AND (DT.Relation_Direction = 'L');


--'INT: BRUSSEL-ZUID -> WIEN HBF'
---------------------------------
--Train_No matching both 'L' and 'INT: BRUSSEL-ZUID -> WIEN HBF' directions = 469
--SK_Train_Service = 150.
SELECT DISTINCT	DT.Relation_Direction,
				FP.SK_Train_Service,
				FP.Train_No
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							INNER JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE DT.Relation_Direction IN ('L'))
	OR DT.Relation_Direction LIKE ('INT: BRUSSEL-ZUID -> WIEN HBF')
ORDER BY FP.Train_No;

--Number of arrivals at stations for rows matching both 'L' and 'INT: BRUSSEL-ZUID -> WIEN HBF' directions: 1125.
SELECT COUNT(*) AS 'No_Arrivals'
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No = 469)
	AND (DT.Relation_Direction = 'L');


--'S7: HALLE -> MECHELEN'
-------------------------
--Train_No matching both 'L' and 'S7: HALLE -> MECHELEN' directions BETWEEN 3156 AND 3171.
--'S7: HALLE -> MECHELEN' SK_Train_Service = 498.
SELECT DISTINCT		PA.Train_No,
					PA.SK_Train_Service
FROM vw.Punctuality_Arrivals AS PA
	INNER JOIN dim.Dim_Train_Service AS DT
		ON PA.SK_Train_Service = DT.SK_Train_Service
WHERE PA.Train_No IN	(SELECT DISTINCT PA.Train_No
						FROM vw.Punctuality_Arrivals AS PA
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON PA.SK_Train_Service = DT.SK_Train_Service
						WHERE  (DT.Relation_Direction LIKE 'S7: HALLE -> MECHELEN')
						)
	AND PA.Train_No IN	(SELECT DISTINCT PA.Train_No
						FROM vw.Punctuality_Arrivals AS PA
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON PA.SK_Train_Service = DT.SK_Train_Service
						WHERE  (DT.Relation_Direction LIKE 'L')
						)
ORDER BY PA.Train_No;

--Number of rows for Train_No matching both 'L' and 'S7: HALLE -> MECHELEN' directions, grouped by SK_Train_Service: 10,699.
SELECT		DT.Relation_Direction,
			FP.SK_Train_Service,
			COUNT(*) AS 'No_Rows'
FROM fact.Fact_Punctuality AS FP
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE	(FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE DT.Relation_Direction IN ('L'))
		)
	AND (FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE  (DT.Relation_Direction LIKE 'S7: HALLE -> MECHELEN')
						)
		)
GROUP BY DT.Relation_Direction, FP.SK_Train_Service;


--'S7: MECHELEN -> HALLE'
-------------------------
--Train_No matching both 'L' and 'S7: MECHELEN -> HALLE' directions BETWEEN 3176 AND 3191.
--'S7: MECHELEN -> HALLE' SK_Train_Service = 500.
SELECT DISTINCT		FP.Train_No,
					FP.SK_Train_Service
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE  (DT.Relation_Direction LIKE 'S7: MECHELEN -> HALLE')
						)
	AND FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE  (DT.Relation_Direction LIKE 'L')
						)
ORDER BY FP.Train_No;

--Number of rows for Train_No matching both 'L' and 'S7: MECHELEN -> HALLE' directions, grouped by SK_Train_Service: 10,693.
SELECT		DT.Relation_Direction,
			FP.SK_Train_Service,
			COUNT(*) AS 'No_Rows'
FROM fact.Fact_Punctuality AS FP
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE	(FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE DT.Relation_Direction IN ('L'))
		)
	AND (FP.Train_No IN	(SELECT DISTINCT FP.Train_No
						FROM fact.Fact_Punctuality AS FP
							LEFT JOIN dim.Dim_Train_Service AS DT
								ON FP.SK_Train_Service = DT.SK_Train_Service
						WHERE  (DT.Relation_Direction LIKE 'S7: MECHELEN -> HALLE')
						)
		)
GROUP BY DT.Relation_Direction, FP.SK_Train_Service;


--Other Train_No with high arrival numbers
------------------------------------------
--Explore Train_No 478 and 479

--Train_No 478 and 479 appear to serve an alternative line with stations that do not match any SNCB commercial line. 
--This alternative line was opened in April 2025 and was active only three days a week.
SELECT DISTINCT	FP.Train_No,
				DT.Relation,
				DT.Relation_Direction,
				DT.Operator_Train_Service
FROM fact.Fact_Punctuality AS FP
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON FP.SK_Train_Service = DT.SK_Train_Service
WHERE FP.Train_No IN (478, 479);

SELECT DISTINCT FP.Train_No,
				DS.Station_Name_French
FROM fact.Fact_Punctuality AS FP
	LEFT JOIN dim.Dim_Station AS DS
		ON FP.SK_Station = DS.SK_Station
WHERE FP.Train_No IN (478, 479)
ORDER BY DS.Station_Name_French;

SELECT DISTINCT	FP.Train_No,
				FP.Date_Service,
				DD.[DayName]
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Date AS DD
		ON FP.Date_Service = DD.DateID
WHERE FP.Train_No IN (478, 479)
ORDER BY FP.Date_Service DESC;

SELECT	Train_No,
		CAST(ROUND(AVG(Delay_Arr), 2) AS DECIMAL(18,2)) AS 'Delay by Train Service_avg_sec',
		CAST((ROUND(AVG(Delay_Arr), 1) / 60) AS DECIMAL(18,2)) AS 'Delay by Train Service_avg_min'
FROM fact.Fact_Punctuality
WHERE Train_No IN (478, 479, 3155)
GROUP BY Train_No
ORDER BY [Delay by Train Service_avg_sec] DESC


--Explore Train_No 3155

--Train_No 3155 was a local train that served a portion of S5, S7, or S19 routes in Brussels,
--but only between 5:30 a.m. and 6:30 a.m. during the 2025 summer (between June and August).
--Therefore, it does not match any Relation_Direction of the train service dimension.
SELECT	FP.Train_No,
		DS.Station_Name_French,
		COUNT(*) AS 'No_Arrivals'
FROM fact.Fact_Punctuality AS FP
	LEFT JOIN dim.Dim_Station AS DS
		ON FP.SK_Station = DS.SK_Station
WHERE FP.Train_No IN (3155)
GROUP BY FP.Train_NO, DS.Station_Name_French
ORDER BY DS.Station_Name_French;

SELECT DISTINCT	DS.Station_Name_French,
				DT.FullTime
FROM fact.Fact_Punctuality AS FP
	INNER JOIN dim.Dim_Time AS DT
		ON FP.Real_Time_Arr = DT.TimeKey
	LEFT JOIN dim.Dim_Station AS DS
		ON FP.SK_Station = DS.SK_Station
WHERE FP.Train_No = 3155
ORDER BY FullTime, Date_Service;

SELECT DISTINCT Date_Service
FROM fact.Fact_Punctuality
WHERE Train_No = 3155
ORDER BY Date_Service DESC;
