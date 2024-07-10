
IF OBJECT_ID('dbo.FinalDisease', 'U') IS NOT NULL
DROP TABLE dbo.FinalDisease;

CREATE TABLE [dbo].[FinalDisease] (
    YearStart INT,
    YearEnd INT,
    LocationAbbr VARCHAR(10),
    LocationDesc VARCHAR(100),
    DataSource VARCHAR(100),
    Topic VARCHAR(100),
    Question VARCHAR(255),
    Response VARCHAR(50),
    DataValueUnit VARCHAR(50),
    DataValueType VARCHAR(50),
    DataValue DECIMAL(18, 2),
    DataValueAlt DECIMAL(18, 2)
);
