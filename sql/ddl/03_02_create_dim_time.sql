-- Execute this script within the [infrabel_punctuality_dwh] database

DROP TABLE IF EXISTS dim.Dim_Time;

CREATE TABLE dim.Dim_Time (
    TimeKey INT NOT NULL,              
    FullTime TIME(0) NOT NULL,          
    Hour INT NOT NULL,
    Minute INT NOT NULL,     
    Second INT NOT NULL,                
    TimePeriod NVARCHAR(30) NOT NULL,   
    Is_PeakHour BIT NOT NULL,            
    CONSTRAINT PK_Time PRIMARY KEY (TimeKey)
);

DECLARE @TimeKey INT = 0;               
DECLARE @Hour INT = 0;
DECLARE @Minute INT = 0;
DECLARE @Second INT = 0;                
DECLARE @TimePeriod NVARCHAR(30);

WHILE @Hour < 24
BEGIN
    -- Railway peakhours
    SET @TimePeriod = CASE
        WHEN @Hour BETWEEN 6 AND 9 THEN 'AM_Peak'
        WHEN @Hour BETWEEN 10 AND 15 THEN 'Midday_OffPeak'
        WHEN @Hour BETWEEN 16 AND 19 THEN 'PM_Peak'
        WHEN @Hour BETWEEN 20 AND 23 THEN 'Evening_OffPeak'
        ELSE 'Night'
    END;

    SET @Minute = 0;
    WHILE @Minute < 60
    BEGIN
        
        SET @Second = 0;
        WHILE @Second < 60
        BEGIN

            INSERT INTO dim.Dim_Time(TimeKey, FullTime, [Hour], [Minute], [Second], TimePeriod, Is_PeakHour)
            VALUES (
                @TimeKey,                                       
                TIMEFROMPARTS(@Hour, @Minute, @Second, 0, 0),   
                @Hour,
                @Minute,
                @Second,
                @TimePeriod,
                CASE WHEN @Hour BETWEEN 6 AND 9 OR @Hour BETWEEN 16 AND 19 THEN 1 ELSE 0 END
            );

            SET @TimeKey += 1; 
            SET @Second += 1;
        END

        SET @Minute += 1;
    END

    SET @Hour += 1;
END;