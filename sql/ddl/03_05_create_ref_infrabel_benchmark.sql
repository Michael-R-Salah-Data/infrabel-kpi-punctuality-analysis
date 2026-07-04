--Execute this script within the [infrabel_punctuality_dwh] database


IF OBJECT_ID('ref.Infrabel_Benchmark', N'U') IS NULL
BEGIN
	CREATE TABLE ref.Infrabel_Benchmark (
		[Year] INT NOT NULL,
		[Month] DATE NOT NULL,
		Punctuality_Pct DECIMAL(4,2) NOT NULL,
		Train_Count INT NOT NULL,
		Train_6Min_Late_Count INT NOT NULL,
		Delay_Minutes INT NOT NULL,
		CONSTRAINT UK_Infrabel_Punctuality_Month UNIQUE ([Month])
		)
END;