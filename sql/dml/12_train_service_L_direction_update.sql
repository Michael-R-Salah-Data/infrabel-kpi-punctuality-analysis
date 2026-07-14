--Execute this script within the [infrabel_punctuality_dwh] database

--Context: see queries in the 11_train_service_L_missing_direction_analysis.sql file.

--'L' -> 'INT: WIEN HBF -> BRUSSEL-ZUID'
----------------------------------------
--This script replaces, in Fact_Punctuality, the SK_Train_Service that matches the 'L' Relation_Direction, in Dim_Train_Service,
--by the SK_Train_Service that matches 'INT: WIEN HBF -> BRUSSEL-ZUID',
--for all rows that have a Train_No value of 468.

--1050 rows are expected to change.

SELECT COUNT(*) AS 'Rows_To_Update'
FROM fact.Fact_Punctuality AS FP_Target
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No = 468)
	AND (DT.Relation_Direction = 'L');

BEGIN TRANSACTION;

UPDATE FP_Target
SET FP_Target.SK_Train_Service =	(SELECT DISTINCT	DT.SK_Train_Service
									FROM fact.Fact_Punctuality AS FP
										INNER JOIN dim.Dim_Train_Service AS DT
											ON FP.SK_Train_Service = DT.SK_Train_Service
									WHERE (FP.Train_No = 468)
										AND (DT.Relation_Direction != 'L')
									)
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No = 468)
	AND (DT.Relation_Direction = 'L');

SELECT @@ROWCOUNT;

COMMIT;


--'L' -> 'INT: BRUSSEL-ZUID -> WIEN HBF'
----------------------------------------
--This script replaces, in Fact_Punctuality, the SK_Train_Service that matches the 'L' Relation_Direction, in Dim_Train_Service,
--by the SK_Train_Service that matches 'INT: BRUSSEL-ZUID -> WIEN HBF',
--for all rows that have a Train_No value of 469.

--1125 rows are expected to change.

SELECT COUNT(*) AS 'Rows_To_Correct'
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No = 469)
	AND (DT.Relation_Direction = 'L');

BEGIN TRANSACTION;

UPDATE FP_Target
SET FP_Target.SK_Train_Service =	(SELECT DISTINCT	DT.SK_Train_Service
									FROM fact.Fact_Punctuality AS FP
										INNER JOIN dim.Dim_Train_Service AS DT
											ON FP.SK_Train_Service = DT.SK_Train_Service
									WHERE (FP.Train_No = 469) 
										AND (DT.Relation_Direction != 'L')
									)
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No = 469)
	AND (DT.Relation_Direction = 'L');

SELECT @@ROWCOUNT;

COMMIT;


--'L' -> 'S7: HALLE -> MECHELEN'
----------------------------------------
--This script replaces, in Fact_Punctuality, the SK_Train_Service that matches the 'L' Relation_Direction, in Dim_Train_Service,
--by the SK_Train_Service that matches 'S7: HALLE -> MECHELEN',
--for all rows that have Train_No values between 3156 and 3171.

--10,699 rows are expected to change.

SELECT COUNT(*) AS 'Rows_To_Correct'
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No BETWEEN 3156 AND 3171)
	AND (DT.Relation_Direction = 'L');

BEGIN TRANSACTION;

UPDATE FP_Target
SET FP_Target.SK_Train_Service =	(SELECT DISTINCT	DT.SK_Train_Service
									FROM fact.Fact_Punctuality AS FP
										INNER JOIN dim.Dim_Train_Service AS DT
											ON FP.SK_Train_Service = DT.SK_Train_Service
									WHERE (FP.Train_No BETWEEN 3156 AND 3171) 
										AND (DT.Relation_Direction != 'L')
									)
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No BETWEEN 3156 AND 3171)
	AND (DT.Relation_Direction = 'L');

SELECT @@ROWCOUNT;

COMMIT;


--'L' -> 'S7: MECHELEN -> HALLE'
----------------------------------------
--This script replaces, in Fact_Punctuality, the SK_Train_Service that matches the 'L' Relation_Direction, in Dim_Train_Service,
--by the SK_Train_Service that matches 'S7: MECHELEN -> HALLE',
--for all rows that have Train_No values between 3176 and 3191.

--10,693 rows are expected to change.

SELECT COUNT(*) AS 'Rows_To_Correct'
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No BETWEEN 3176 AND 3191)
	AND (DT.Relation_Direction = 'L');

BEGIN TRANSACTION;

UPDATE FP_Target
SET FP_Target.SK_Train_Service =	(SELECT DISTINCT	DT.SK_Train_Service
									FROM fact.Fact_Punctuality AS FP
										INNER JOIN dim.Dim_Train_Service AS DT
											ON FP.SK_Train_Service = DT.SK_Train_Service
									WHERE (FP.Train_No BETWEEN 3176 AND 3191) 
										AND (DT.Relation_Direction != 'L')
									)
FROM fact.Fact_Punctuality AS FP_Target
	INNER JOIN dim.Dim_Train_Service AS DT
		ON FP_Target.SK_Train_Service = DT.SK_Train_Service
WHERE (FP_Target.Train_No BETWEEN 3176 AND 3191)
	AND (DT.Relation_Direction = 'L');

SELECT @@ROWCOUNT;

COMMIT;
