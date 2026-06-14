-- Execute this script within the [infrabel_punctuality_dwh] database

-- 5 stations have no longitude/latitude coordinates and are not assigned to a municipality;
-- therefore, NULL is allowed for the corresponding columns


IF OBJECT_ID('dim.Dim_Station', N'U') IS NULL
BEGIN
	CREATE TABLE dim.Dim_Station (
		SK_STATION INT IDENTITY(1,1) NOT NULL,
		Ptcar_ID INT UNIQUE NOT NULL,
		Station_Name_French NVARCHAR(60) NOT NULL,
		Station_Name_Dutch NVARCHAR(60) NOT NULL,
		Station_Class NVARCHAR(30) NOT NULL,
		Station_Status NVARCHAR(30) NOT NULL,
		REFNIS_Municipality INT,
		Communes NVARCHAR(60),
		Gemeenten NVARCHAR(60),
		REFNIS_Province INT,
		Provinces NVARCHAR(60),
		Provincies NVARCHAR(60),
		REFNIS_Region INT,
		Regions NVARCHAR(60),
		Geweesten NVARCHAR(60),
		Population_Density_Category NVARCHAR(30),
		lon DECIMAL(9,6),
		lat DECIMAL(9,6),
		CONSTRAINT PK_Dim_Station PRIMARY KEY (SK_STATION),
		CONSTRAINT UK_Ptac_ID UNIQUE (Ptcar_ID),
		CONSTRAINT CK_lon CHECK (lon BETWEEN -180 AND 180),
		CONSTRAINT CK_lat CHECK (lat BETWEEN -90 AND 90)
	)
END