-- Execute this script within the [infrabel_punctuality_dwh] database

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Station FOREIGN KEY (SK_Station)
	REFERENCES dim.Dim_Station (SK_Station); 

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Train_Service FOREIGN KEY (SK_Train_Service)
	REFERENCES dim.Dim_Train_Service (SK_Train_Service);

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Date FOREIGN KEY (Date_Service)
	REFERENCES dim.Dim_Date (Date_ID);

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Planned_Time_Arr FOREIGN KEY (Planned_Time_Arr)
	REFERENCES dim.Dim_Time (TimeKey);

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Real_Time_Arr FOREIGN KEY (Real_Time_Arr)
	REFERENCES dim.Dim_Time (TimeKey);

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Planned_Time_Dep FOREIGN KEY (Planned_Time_Dep)
	REFERENCES dim.Dim_Time (TimeKey);

ALTER TABLE fact.Fact_Punctuality
ADD CONSTRAINT FK_Punctuality_Real_Time_Dep FOREIGN KEY (Real_Time_Dep)
	REFERENCES dim.Dim_Time (TimeKey);

