USE master
GO

IF NOT EXISTS (
	SELECT 1
	FROM sys.databases
	WHERE name = N'infrabel_punctuality_dwh'
	)
BEGIN
	CREATE DATABASE infrabel_punctuality_dwh
END
GO
