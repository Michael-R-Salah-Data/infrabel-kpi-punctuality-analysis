--- Execute this script within the [infrabel_punctuality_dwh] database


DROP TABLE IF EXISTS dim.Time;

CREATE TABLE dim.Time (
    TimeId INT PRIMARY KEY,
    Time TIME NOT NULL,     
    Hour INT NOT NULL,
    Minute INT NOT NULL,     
    TimePeriod NVARCHAR(30) NOT NULL,
    IsPeakHour BIT NOT NULL
);

DECLARE @TimeId INT = 0;
DECLARE @Hour INT = 0;
DECLARE @Minute INT = 0;
DECLARE @TimePeriod NVARCHAR(30);

WHILE @Hour < 24
BEGIN
    SET @Minute = 0;

    SET @TimePeriod = CASE
        WHEN @Hour BETWEEN 6 AND 9 THEN 'AM_Peak'
        WHEN @Hour BETWEEN 10 AND 15 THEN 'Midday_OffPeak'
        WHEN @Hour BETWEEN 16 AND 19 THEN 'PM_Peak'
		WHEN @Hour BETWEEN 20 AND 23 THEN 'Evening_OffPeak'
        ELSE 'Night'
    END;

    WHILE @Minute < 60
    BEGIN
        SET @TimeId = (@Hour * 100) + @Minute;

        INSERT INTO dim.Time(TimeId, [Time], [Hour], [Minute], TimePeriod, IsPeakHour)

        VALUES (
        @TimeId,
        TIMEFROMPARTS(@Hour, @Minute, 0, 0, 0),
        @Hour,
        @Minute,
        @TimePeriod,
        CASE WHEN @Hour BETWEEN 6 AND 9 OR @Hour BETWEEN 16 AND 19 THEN 1 ELSE 0 END
        )

        SET @Minute += 1;
    END
   

    SET @Hour += 1;
END;