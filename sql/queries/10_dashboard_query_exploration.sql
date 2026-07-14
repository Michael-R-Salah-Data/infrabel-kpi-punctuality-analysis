-- Execute this script within the [infrabel_punctuality_dwh] database


--Future averages for dashboards
------------------------------

--Total delay average by year
SELECT	DT.[Year],
		CAST(ROUND(AVG(PA.Delay_Arr), 2) AS DECIMAL(18,2)) AS 'Delay_Arrivals_Avg_sec',
		CAST(ROUND(((AVG(PA.Delay_Arr)) / 60), 2) AS DECIMAL(18,2)) AS 'Delay_Arrivals_Avg_min'
FROM vw.Punctuality_Arrivals AS PA
	LEFT JOIN dim.Dim_Date AS DT
		ON PA.Date_Service = DT.DateID
GROUP BY DT.[Year];


--Delay average by SNCB station for national trains
SELECT	DS.Station_Name_French,
		DS.Station_Class,
		CAST(ROUND(AVG(PA.Delay_Arr), 2) AS DECIMAL(18,2)) AS 'Delay by Station_avg_sec',
		CAST((ROUND(AVG(PA.Delay_Arr), 1) / 60) AS DECIMAL(18,2)) AS 'Delay by Station_avg_min'
FROM vw.Punctuality_Arrivals AS PA
	INNER JOIN dim.Dim_Station AS DS
		ON PA.SK_Station = DS.SK_Station
	INNER JOIN dim.Dim_Train_Service AS DT
		ON PA.SK_Train_Service = DT.SK_Train_Service
		AND DT.Is_National = 1
WHERE PA.Has_Passenger_Count = 1
GROUP BY DS.Station_Name_French, DS.Station_Class
ORDER BY 'Delay by Station_avg_sec' DESC;


--Delay average by train service for national trains
SELECT	PA.SK_Train_Service AS 'ID_Train_Service',
		DT.Relation_Direction AS 'Train_Service',
		CAST(ROUND(AVG(PA.Delay_Arr), 2) AS DECIMAL(18,2)) AS 'Delay by Train Service_avg_sec',
		CAST((ROUND(AVG(PA.Delay_Arr), 1) / 60) AS DECIMAL(18,2)) AS 'Delay by Train Service_avg_min'
FROM vw.Punctuality_Arrivals AS PA
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON PA.SK_Train_Service = DT.SK_Train_Service
WHERE DT.Is_National = 1
	AND DT.Direction_Is_Inferred = 0
GROUP BY PA.SK_Train_Service, DT.Relation_Direction
ORDER BY 'Delay by Train Service_avg_sec' DESC;


--Delay average by train service for all trains
--The international trains are outliers that introduce a bias in the resulting averages
SELECT	PA.SK_Train_Service AS 'ID_Train_Service',
		DT.Relation AS 'Train_Service',
		DT.Relation_Direction AS 'Train_Direction',
		CAST(ROUND(AVG(PA.Delay_Arr), 2) AS DECIMAL(18,2)) AS 'Delay by Train Service_avg_sec',
		CAST((ROUND(AVG(PA.Delay_Arr), 1) / 60) AS DECIMAL(18,2)) AS 'Delay by Train Service_avg_min'
FROM vw.Punctuality_Arrivals AS PA
	LEFT JOIN dim.Dim_Train_Service AS DT
		ON PA.SK_Train_Service = DT.SK_Train_Service
GROUP BY PA.SK_Train_Service, DT.Relation, DT.Relation_Direction
ORDER BY 'Delay by Train Service_avg_sec' DESC;



