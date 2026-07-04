-- Execute this script within the [infrabel_punctuality_dwh] database

--To facilitate future analyses, two SQL views will be created in the data warehouse:
--Vw_Punctuality_Arrivals — filters out records where arrival data is unavailable, enabling arrival delay analysis 
--Vw_Punctuality_Departures — filters out records where departure data is unavailable, enabling departure delay analysis 

--The current analysis focuses on arrival delays, 
--as they are the most directly perceived by passengers,
--and given that planned departure columns contain anomalies such as trains whose real departure times are perfectly equal 
--to planned departure times, to the second, even though the real arrival times of the same trains are more than 1 hour late 
--compared to their planned arrival times (i.e. trains that depart from a station before arriving at the same station). 
--Departure delay analysis is therefore left for future work.

--Delay_Arr, Delay_Arr_Infrabel, Weighted_Delay_Arr, Delay_Dep, Delay_Dep_Infrabel, and Weighted_Delay_Dep are cast to 
--DECIMAL(18,4) to enable large-volume aggregations and keep the decimals.

IF OBJECT_ID('vw.Punctuality_Arrivals', N'V') IS NULL
BEGIN
	EXEC(
		'CREATE VIEW vw.Punctuality_Arrivals AS
			SELECT	Date_Service, 
					SK_Station,
					SK_Train_Service,
					Train_No,
					Planned_Time_Arr,
					Real_Time_Arr,
					CAST(Delay_Arr AS DECIMAL(18,4)) AS "Delay_Arr",
					CAST(Delay_Arr_Infrabel AS DECIMAL(18,4)) AS "Delay_Infrabel_Arr",
					CAST(Weighted_Delay_Arr AS DECIMAL(18,4)) AS "Weighted_Delay_Arr",
					Is_6Min_Late_Arr,
					Is_5Min_Late_Arr,
					Has_Passenger_Count,
					Passenger_Count_Avg
			FROM fact.Fact_Punctuality
			WHERE Planned_Time_Arr IS NOT NULL
				AND Real_Time_Arr IS NOT NULL'
		);
END
GO


IF OBJECT_ID('vw.Punctuality_Departures', N'V') IS NULL
BEGIN
	EXEC(
		'CREATE VIEW vw.Punctuality_Departures AS 
			SELECT	Date_Service,
					SK_Station,
					SK_Train_Service,
					Train_No,
					Planned_Time_Dep,
					Real_Time_Dep,
					CAST(Delay_Dep AS DECIMAL(18,4)) AS "Delay_Dep",
					CAST(Delay_Dep_Infrabel AS DECIMAL(18,4)) AS "Delay_Infrabel_Dep",
					CAST(Weighted_Delay_Dep AS DECIMAL(18,4)) AS "Weighted_Delay_Dep",
					Is_6Min_Late_Dep,
					Is_5Min_Late_Dep,
					Has_Passenger_Count,
					Passenger_Count_Avg
			FROM fact.Fact_Punctuality
			WHERE Planned_Time_Dep IS NOT NULL
				AND Real_Time_Dep IS NOT NULL'
		);
END
GO
		