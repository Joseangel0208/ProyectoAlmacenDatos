-- Crear tablas dimensionales
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    YearStart INT,
    YearEnd INT
);

CREATE TABLE DimLocation (
    LocationKey INT PRIMARY KEY IDENTITY(1,1),
    LocationAbbr VARCHAR(10),
    LocationDesc VARCHAR(100)
);

CREATE TABLE DimSource (
    SourceKey INT PRIMARY KEY IDENTITY(1,1),
    DataSource VARCHAR(100)
);

CREATE TABLE DimTopic (
    TopicKey INT PRIMARY KEY IDENTITY(1,1),
    Topic VARCHAR(100)
);

CREATE TABLE DimQuestion (
    QuestionKey INT PRIMARY KEY IDENTITY(1,1),
    Question VARCHAR(255)
);

CREATE TABLE DimResponse (
    ResponseKey INT PRIMARY KEY IDENTITY(1,1),
    Response VARCHAR(50)
);