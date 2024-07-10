-- Crear la tabla de hechos
CREATE TABLE FactDisease (
    FactKey INT PRIMARY KEY IDENTITY(1,1),
    DateKey INT,
    LocationKey INT,
    SourceKey INT,
    TopicKey INT,
    QuestionKey INT,
    ResponseKey INT,
    DataValue DECIMAL(18, 2),
    DataValueAlt DECIMAL(18, 2),
    DataValueUnit VARCHAR(50),
    DataValueType VARCHAR(50),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (LocationKey) REFERENCES DimLocation(LocationKey),
    FOREIGN KEY (SourceKey) REFERENCES DimSource(SourceKey),
    FOREIGN KEY (TopicKey) REFERENCES DimTopic(TopicKey),
    FOREIGN KEY (QuestionKey) REFERENCES DimQuestion(QuestionKey),
    FOREIGN KEY (ResponseKey) REFERENCES DimResponse(ResponseKey)
);
