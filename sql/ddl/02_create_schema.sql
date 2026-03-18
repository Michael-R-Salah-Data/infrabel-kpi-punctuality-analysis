USE infrabel_punctuality_dwh
GO

IF NOT EXISTS (
    SELECT 1 
    FROM sys.schemas
    WHERE name = N'dim'
)
BEGIN 
    EXEC('CREATE SCHEMA dim')
END
GO

IF NOT EXISTS (
    SELECT 1 
    FROM sys.schemas
    WHERE name = N'fact'
)
BEGIN 
    EXEC('CREATE SCHEMA fact')
END
GO

IF NOT EXISTS (
    SELECT 1 
    FROM sys.schemas
    WHERE name = N'ref'
)
BEGIN 
    EXEC('CREATE SCHEMA ref')
END
GO

