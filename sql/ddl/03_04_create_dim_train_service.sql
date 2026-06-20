-- Execute this script within the [infrabel_punctuality_dwh] database

-- SK_Train_Service is loaded directly from the Dim_Train_Service pandas DataFrame

IF OBJECT_ID('dim.Dim_Station', N'U') IS NULL
BEGIN
	CREATE TABLE dim.Dim_Train_Service(
		SK_Train_Service INT NOT NULL,
		Operator_Train_Service NVARCHAR(20) NOT NULL,
		Relation_Category NVARCHAR(20) NOT NULL,
		Relation NVARCHAR(20) NOT NULL,
		Relation_Direction NVARCHAR(100) NOT NULL,
		Direction_Is_Inferred BIT NOT NULL,
		Is_Local BIT NOT NULL,
		Is_National BIT NOT NULL,
		CONSTRAINT PK_Service PRIMARY KEY (SK_Train_Service)
		)
END;