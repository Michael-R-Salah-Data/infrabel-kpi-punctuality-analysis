-- Execute this script within the [infrabel_punctuality_dwh] database

IF OBJECT_ID('fact.Fact_Punctuality', N'U') IS NULL
BEGIN
	CREATE TABLE fact.Fact_Punctuality (
		Date_Service INT NOT NULL,  
		SK_Station INT NOT NULL,  
		SK_Train_Service INT NOT NULL, 
		Train_No INT NOT NULL,  
		Planned_Time_Arr INT,  
		Real_Time_Arr INT,
		Delay_Arr INT,
		Delay_Arr_Infrabel INT,  
		Weighted_Delay_Arr INT,  
		Planned_Time_Dep INT,  
		Real_Time_Dep INT, 
		Delay_Dep INT,  
		Delay_Dep_Infrabel INT, 
		Weighted_Delay_Dep INT,  
		Is_6Min_Late_Arr BIT,
		Is_5Min_Late_Arr BIT,  
		Is_6Min_Late_Dep BIT,  
		Is_5Min_Late_Dep BIT,  
		Has_Passenger_Count BIT,  
		Passenger_Count_Avg INT,  
		Line_No_Arr NVARCHAR(10), 
		Line_No_Dep NVARCHAR(10),    
		Planned_Date_Arr DATETIME2,  
		Real_Date_Arr DATETIME2,  
		Planned_Date_Dep DATETIME2,  
		Real_Date_Dep DATETIME2
	)
END;