--- Execute this script within the [infrabel_punctuality_dwh] database


SET DATEFIRST 1

DROP TABLE IF EXISTS dim.Date;

CREATE TABLE dim.Date (
    DateId INT PRIMARY KEY,             
    Year INT NOT NULL,
    Month INT NOT NULL,                
    MonthName NVARCHAR(20) NOT NULL,   
	[Day] INT NOT NULL,
    [DayWeek] INT NOT NULL,
    [DayName] NVARCHAR(20) NOT NULL,
    Quarter INT NOT NULL,
    QuarterName NVARCHAR(20) NOT NULL,
    WeekOfYear INT NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL
);

DECLARE @StartDate DATE = '2020-01-01';
DECLARE @EndDate DATE = '2026-12-31';

DECLARE @Quarter INT;
DECLARE @QuarterName NVARCHAR(20);

WHILE @StartDate <= @EndDate
BEGIN
    SET @Quarter = DATEPART(QUARTER, @StartDate);
    
    SET @QuarterName = 
        CASE @Quarter
            WHEN 1 THEN 'Q1'
            WHEN 2 THEN 'Q2'
            WHEN 3 THEN 'Q3'
            WHEN 4 THEN 'Q4'
        END;

    INSERT INTO dim.Date (
		DateId,
		[Year],
		[Month],
		[MonthName],
		[Day],
		[DayWeek],
		[DayName],
		Quarter,
		QuarterName,
		WeekOfYear,
		IsWeekend,
		IsHoliday
    )
    VALUES (
        CONVERT(INT, FORMAT(@StartDate, 'yyyyMMdd')), 
        YEAR(@StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DAY(@StartDate),
        DATEPART(WEEKDAY, @StartDate),
        DATENAME(WEEKDAY, @StartDate),
        @Quarter,
        @QuarterName,
        DATEPART(ISO_WEEK, @StartDate),
		CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (6, 7) THEN 1 ELSE 0 END,
		0 -- TODO : to update with holiday script later
    );

    SET @StartDate = DATEADD(DAY, 1, @StartDate);

END;

